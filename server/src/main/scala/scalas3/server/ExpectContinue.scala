package scalas3.server

import cats.effect.{IO, Ref, Resource}
import cats.effect.std.Mutex
import cats.syntax.all.*
import com.comcast.ip4s.{Host, IpAddress, Port, SocketAddress}
import fs2.io.net.{Network, Socket}
import fs2.{Chunk, Stream}
import scodec.bits.ByteVector
import scalas3.runtime.SocketOptions

import java.nio.charset.StandardCharsets.US_ASCII

/** Ember は `Expect: 100-continue` に応えないため、S3 クライアントは本文を送る前に約 1 秒待ってしまう
  * （AWS SDK、AWS CLI、curl がいずれも大きな PUT で Expect を付ける）。
  *
  * そこで公開ポートで受けた接続を内部の Ember へ中継し、中継しながらリクエストヘッダーの終わりを探して、
  * Expect があればすぐに `100 Continue` を返す。
  * 接続を使い回すために Content-Length で本文の長さを数えて次のリクエストの頭を見つける。
  * chunked など長さが分からない本文が来たら、その接続では以後なにもしない（安全側）。
  *
  * 既定の公開ポートは NettyHttpServer が直接受けるので、これは `SCALAS3_HTTP_SERVER=ember` のときだけ使う。
  */
object ExpectContinue:

  private val Continue = Chunk.array("HTTP/1.1 100 Continue\r\n\r\n".getBytes(US_ASCII))
  private val MaxHeader = 64 * 1024

  private[server] enum Mode:
    case Header(buf: ByteVector)
    case Body(remaining: Long)
    case Off

  /** 公開アドレスで待ち受け、backend（内部の Ember）へ中継する。 */
  def front(host: Host, port: Port, backend: SocketAddress[Host]): Resource[IO, SocketAddress[IpAddress]] =
    Network[IO].bind(SocketAddress(host, port), SocketOptions.default).flatMap { server =>
      val loop = server.accept
        .map(client => Stream.eval(relay(client, backend).handleError(_ => ())))
        .parJoin(4096)
        .compile
        .drain
      loop.background.as(server.address.asIpUnsafe)
    }

  private def relay(client: Socket[IO], backend: SocketAddress[Host]): IO[Unit] =
    Network[IO].connect(backend, SocketOptions.default).use { be =>
      (Ref.of[IO, Mode](Mode.Header(ByteVector.empty)), Mutex[IO]).flatMapN { (mode, lock) =>
        val toClient = (c: Chunk[Byte]) => lock.lock.surround(client.write(c))
        val up = chunks(client)
          .evalTap(c => mode.modify(m => step(m, c.toByteVector, sendContinue = false)).flatMap(n => if n then toClient(Continue) else IO.unit))
          .evalMap(be.write)
          .compile
          .drain >> be.endOfOutput
        val down = chunks(be).evalMap(toClient).compile.drain
        // 応答側が終わったら（Ember が接続を閉じたら）中継も終える。クライアント側が先に閉じても、応答は最後まで流す
        IO.race(down, up.attempt >> IO.never).void
      }
    }

  private final case class Scanned(expect: Boolean, chunked: Boolean, length: Long)

  /** "\r\n\r\n" の位置（なければ -1）。 */
  private def headerEnd(a: Array[Byte]): Int =
    var i = 0
    var found = -1
    while found < 0 && i + 3 < a.length do
      if a(i) == '\r' && a(i + 1) == '\n' && a(i + 2) == '\r' && a(i + 3) == '\n' then found = i
      i += 1
    found

  /** ヘッダー（1 行目の後から end まで）から、必要な 3 つだけを拾う。要求ごとに通るので文字列を作らない。 */
  private def scanHeaders(a: Array[Byte], end: Int): Scanned =
    def lower(b: Byte): Int = if b >= 'A' && b <= 'Z' then b + 32 else b
    def nameIs(from: Int, colon: Int, name: String): Boolean =
      var lo = from
      var hi = colon
      while lo < hi && (a(lo) == ' ' || a(lo) == '\t') do lo += 1
      while hi > lo && (a(hi - 1) == ' ' || a(hi - 1) == '\t') do hi -= 1
      hi - lo == name.length && (0 until name.length).forall(k => lower(a(lo + k)) == name.charAt(k))
    def value(colon: Int, lineEnd: Int): String = new String(a, colon + 1, lineEnd - colon - 1, US_ASCII).trim.toLowerCase
    var expect = false
    var chunked = false
    var length = 0L
    // 1 行目（リクエスト行）を飛ばす
    var i = 0
    while i < end && a(i) != '\n' do i += 1
    i += 1
    while i < end do
      var lineEnd = i
      while lineEnd < end && a(lineEnd) != '\r' do lineEnd += 1
      var colon = i
      while colon < lineEnd && a(colon) != ':' do colon += 1
      if colon < lineEnd then
        val n = colon - i
        // 長さで先にふるい分ける（ほとんどのヘッダーはここで外れる）
        if n >= 6 && n <= 20 then
          if nameIs(i, colon, "expect") then expect = value(colon, lineEnd) == "100-continue"
          else if nameIs(i, colon, "transfer-encoding") then chunked = value(colon, lineEnd).contains("chunked")
          else if nameIs(i, colon, "content-length") then length = value(colon, lineEnd).toLongOption.getOrElse(0L)
      i = lineEnd + 2
    Scanned(expect, chunked, length)

  /** 大きめの単位で読む（`reads` の既定の単位では、大きな本体で中継の処理回数が増えて遅かった）。 */
  private def chunks(s: Socket[IO]): Stream[IO, Chunk[Byte]] = Stream.repeatEval(s.read(ReadSize)).unNoneTerminate

  private val ReadSize = 256 * 1024

  /** 状態を進める。戻り値の Boolean は「100 Continue を送るべきか」。 */
  @annotation.tailrec
  private[server] def step(m: Mode, data: ByteVector, sendContinue: Boolean): (Mode, Boolean) =
    if data.isEmpty then (m, sendContinue)
    else
      m match
        case Mode.Off => (Mode.Off, sendContinue)
        case Mode.Body(rem) =>
          if data.size < rem then (Mode.Body(rem - data.size), sendContinue)
          else step(Mode.Header(ByteVector.empty), data.drop(rem), sendContinue)
        case Mode.Header(buf) =>
          val all = buf ++ data
          val arr = all.toArrayUnsafe
          val end = headerEnd(arr)
          if end < 0 then
            if all.size > MaxHeader then (Mode.Off, sendContinue) else (Mode.Header(all), sendContinue)
          else
            val h = scanHeaders(arr, end)
            val expect = sendContinue || h.expect
            val rest = all.drop(end + 4L)
            if h.chunked then (Mode.Off, expect)
            else if h.length == 0 then step(Mode.Header(ByteVector.empty), rest, expect)
            else step(Mode.Body(h.length), rest, expect)
