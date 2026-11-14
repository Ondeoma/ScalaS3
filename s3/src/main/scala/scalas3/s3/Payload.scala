package scalas3.s3

import cats.effect.{Deferred, IO}
import fs2.{Chunk, Pull, Stream}
import scodec.bits.ByteVector

import java.nio.charset.StandardCharsets.{US_ASCII, UTF_8}
import java.security.MessageDigest
import java.util.Base64

/** 追加チェックサム（x-amz-checksum-*）。値は big-endian のバイト列を base64 にしたもの。 */
trait Checksummer:
  def update(bytes: Array[Byte], off: Int, len: Int): Unit

  /** 値（big-endian のバイト列）。ダイジェストの場合は状態がリセットされる。 */
  protected def result: Array[Byte]

  final def base64: String = Base64.getEncoder.encodeToString(result)

object Checksummer:
  val Algorithms: List[String] = List("crc32", "crc32c", "crc64nvme", "sha1", "sha256")

  /** 対応しているアルゴリズムか（大文字小文字を区別しない）。 */
  def supported(alg: String): Boolean = Algorithms.contains(alg.toLowerCase)

  def apply(alg: String): Option[Checksummer] = alg.toLowerCase match
    case "crc32"     => Some(jdk(new java.util.zip.CRC32(), 4))
    case "crc32c"    => Some(jdk(new java.util.zip.CRC32C(), 4))
    case "crc64nvme" => Some(Crc64Nvme())
    case "sha1"      => Some(digest("SHA-1"))
    case "sha256"    => Some(digest("SHA-256"))
    case _           => None

  private def jdk(c: java.util.zip.Checksum, bytes: Int): Checksummer = new Checksummer:
    def update(b: Array[Byte], off: Int, len: Int): Unit = c.update(b, off, len)
    protected def result: Array[Byte] = ByteVector.fromLong(c.getValue, bytes).toArray

  private def digest(alg: String): Checksummer = new Checksummer:
    private val md = MessageDigest.getInstance(alg)
    def update(b: Array[Byte], off: Int, len: Int): Unit = md.update(b, off, len)
    protected def result: Array[Byte] = md.digest()

/** CRC-64/NVME（反転多項式 0x9a6c9329ac4bc9b5、初期値と最終 XOR は全ビット 1）。 */
final class Crc64Nvme extends Checksummer:
  private var crc = -1L
  def update(b: Array[Byte], off: Int, len: Int): Unit =
    var i = off
    while i < off + len do
      crc = Crc64Nvme.table(((crc ^ b(i)) & 0xff).toInt) ^ (crc >>> 8)
      i += 1
  def value: Long = ~crc
  protected def result: Array[Byte] = ByteVector.fromLong(value).toArray

object Crc64Nvme:
  private val Poly = 0x9a6c9329ac4bc9b5L
  val table: Array[Long] = Array.tabulate(256) { n =>
    var c = n.toLong
    for _ <- 0 until 8 do c = if (c & 1) != 0 then (c >>> 1) ^ Poly else c >>> 1
    c
  }

/** aws-chunked 形式の本文を元のバイト列に戻す。
  * {{{
  * chunk   = hex-size [";chunk-signature=" sig] CRLF data CRLF
  * last    = "0" [";chunk-signature=" sig] CRLF
  * trailer = (name ":" value CRLF)* CRLF
  * }}}
  * 署名付きなら各チャンクの署名を検証する（前のチャンクの署名を連鎖させる）。トレーラーは完了時に trailers へ入れる。
  *
  * チャンクの中身は、届いた分ずつそのまま流す（チャンクの大きさはクライアントが決めるので、溜めるとメモリを使い切られうる）。
  * 署名はチャンクの終わりで確かめ、合わなければストリームをエラーで終える（途中まで流した分は使われない）。
  */
object AwsChunked:

  final case class Signing(key: Array[Byte], amzDate: String, scope: SigV4.Scope, seed: String)

  private val Crlf = ByteVector('\r', '\n')
  private val MaxLine = 64 * 1024
  private val SignatureExt = "chunk-signature="
  private val TrailerSignature = "x-amz-trailer-signature"

  def decode(signing: Option[Signing], trailers: Deferred[IO, Map[String, String]]): fs2.Pipe[IO, Byte, Byte] = in =>
    // 前のチャンクの署名（次のチャンクの署名の計算に使う）。Pull は順に 1 度ずつ評価されるので、ここで持ってよい
    var previous = signing.fold("")(_.seed)

    def fail(msg: String) = Pull.raiseError[IO](S3Error.invalidArgument(s"Malformed aws-chunked body: $msg"))
    def sigFail = Pull.raiseError[IO](S3Error.signatureMismatch)

    /** CRLF で終わる 1 行を読む。 */
    def line(s: Stream[IO, Byte], buf: ByteVector): Pull[IO, Nothing, Option[(String, ByteVector, Stream[IO, Byte])]] =
      val idx = buf.indexOfSlice(Crlf)
      if idx >= 0 then Pull.pure(Some((new String(buf.take(idx).toArrayUnsafe, UTF_8), buf.drop(idx + 2), s)))
      else if buf.size > MaxLine then fail("line too long")
      else
        s.pull.uncons.flatMap {
          case None             => Pull.pure(None)
          case Some((ch, rest)) => line(rest, buf ++ ch.toByteVector)
        }

    /** n バイト読む（チャンクの後の CRLF 用）。 */
    def take(s: Stream[IO, Byte], buf: ByteVector, n: Long): Pull[IO, Nothing, Option[(ByteVector, ByteVector, Stream[IO, Byte])]] =
      if buf.size >= n then Pull.pure(Some((buf.take(n), buf.drop(n), s)))
      else
        s.pull.uncons.flatMap {
          case None             => Pull.pure(None)
          case Some((ch, rest)) => take(rest, buf ++ ch.toByteVector, n)
        }

    def emit(d: ByteVector, md: Option[MessageDigest]): Pull[IO, Byte, Unit] =
      if d.isEmpty then Pull.done
      else
        val arr = d.toArrayUnsafe
        md.foreach(_.update(arr))
        Pull.output(Chunk.array(arr))

    /** チャンクの中身 n バイトを流す（md があれば SHA-256 も計算する）。残りのバッファとストリームを返す。途中で終われば None。 */
    def data(s: Stream[IO, Byte], buf: ByteVector, n: Long, md: Option[MessageDigest]): Pull[IO, Byte, Option[(ByteVector, Stream[IO, Byte])]] =
      if buf.size >= n then
        val (d, rest) = buf.splitAt(n)
        emit(d, md) >> Pull.pure(Some((rest, s)))
      else
        emit(buf, md) >> s.pull.uncons.flatMap {
          case None             => Pull.pure(None)
          case Some((ch, next)) => data(next, ch.toByteVector, n - buf.size, md)
        }

    /** チャンクの署名を確かめる（署名なしの本文なら常に true）。合えば次のチャンクのために覚える。 */
    def verify(sig: Option[String], chunkSha256Hex: => String): Boolean =
      signing.forall { sg =>
        sig.exists { s =>
          val ok = SigV4.safeEquals(SigV4.sign(sg.key, SigV4.chunkStringToSign(sg.amzDate, sg.scope, previous, chunkSha256Hex)), s)
          if ok then previous = s
          ok
        }
      }

    def chunks(s: Stream[IO, Byte], buf: ByteVector): Pull[IO, Byte, Unit] =
      line(s, buf).flatMap {
        case None => fail("unexpected end of body")
        case Some((header, rest, s2)) =>
          val (sizeHex, ext) = header.split(";", 2) match
            case Array(h, e) => (h.trim, Some(e.trim))
            case Array(h)    => (h.trim, None)
            case _           => ("", None)
          val sig = ext.filter(_.startsWith(SignatureExt)).map(_.stripPrefix(SignatureExt))
          scala.util.Try(java.lang.Long.parseLong(sizeHex, 16)).toOption.filter(_ >= 0) match
            case None => fail(s"bad chunk size '$sizeHex'")
            case Some(0L) =>
              if !verify(sig, SigV4.EmptySha256) then sigFail
              else trailerLines(s2, rest, Map.empty)
            case Some(n) =>
              val md = signing.map(_ => MessageDigest.getInstance("SHA-256"))
              data(s2, rest, n, md).flatMap {
                case None => fail("truncated chunk")
                case Some((rest2, s3)) =>
                  if !verify(sig, md.fold("")(m => ByteVector.view(m.digest()).toHex)) then sigFail
                  else
                    take(s3, rest2, 2).flatMap {
                      case Some((crlf, rest3, s4)) if crlf == Crlf => chunks(s4, rest3)
                      case _                                       => fail("missing CRLF after chunk")
                    }
              }
      }

    def trailerLines(s: Stream[IO, Byte], buf: ByteVector, acc: Map[String, String]): Pull[IO, Byte, Unit] =
      line(s, buf).flatMap {
        case None             => checkTrailer(acc) // 末尾の空行がない実装も受け入れる（署名は同じように確かめる）
        case Some(("", _, _)) => checkTrailer(acc)
        case Some((l, rest, s2)) =>
          l.split(":", 2) match
            case Array(k, v) => trailerLines(s2, rest, acc.updated(k.trim.toLowerCase, v.trim))
            case _           => fail("bad trailer")
      }

    /** 署名付きなら、トレーラーがあるときはその署名を確かめる。 */
    def checkTrailer(acc: Map[String, String]): Pull[IO, Byte, Unit] =
      val ok = signing.forall { sg =>
        acc.isEmpty || acc.get(TrailerSignature).exists { sig =>
          val canonical = acc.removed(TrailerSignature).toVector.sortBy(_._1).map((k, v) => s"$k:$v\n").mkString
          val sts = SigV4.trailerStringToSign(sg.amzDate, sg.scope, previous, SigV4.sha256Hex(canonical.getBytes(US_ASCII)))
          SigV4.safeEquals(SigV4.sign(sg.key, sts), sig)
        }
      }
      if !ok then sigFail else Pull.eval(trailers.complete(acc)).void

    chunks(in, ByteVector.empty).stream
