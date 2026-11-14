package scalas3.runtime

import cats.effect.std.Mutex
import cats.effect.{IO, Resource}
import fs2.{Chunk, Stream}

import java.io.{FileInputStream, IOException}
import java.net.{InetSocketAddress, ServerSocket, Socket}
import java.security.KeyStore
import java.util.concurrent.atomic.AtomicReference
import javax.net.ssl.{KeyManagerFactory, SSLContext, SSLSocket, TrustManagerFactory}
import scala.concurrent.duration.*

/** 相互認証つき TLS のノード間通信。
  *
  * 鍵ストア（PKCS12）にはノードの鍵と証明書を、トラストストアにはクラスタの CA（または共通の自己署名証明書）を入れる。
  * 相手は「トラストストアの証明書で署名されているか」で確かめ、ホスト名は検証しない（ノード ID で呼び合うため）。
  *
  * JDK の SSLSocket をブロッキングで使う（読み込みは接続ごとに 1 本のブロッキング用スレッド）。fs2 の TLS は
  * 書き込みを 16 KiB のレコードごとに非同期で待つため、ブロック転送で TCP の半分ほどの速さしか出なかった
  * （docs/benchmark.md）。ノード間の接続は数が少ないので、スレッドを専有しても問題にならない。
  *
  * SSLSocket は平文のソケットの上に重ねる。止めるときは下の平文のソケットを閉じる（SSLSocket の close は
  * TLS の終了の手順で待つことがあり、読み込み中のスレッドを確実に抜けさせられない）。
  *
  * 証明書の更新（`reloading`）：鍵ストアとトラストストアの更新時刻を定期的に見て、変わっていれば読み直す。新しい
  * 接続から新しい証明書を使う。張ったままの接続はそのまま（証明書の期限は、ハンドシェイクのときにしか確かめない）。
  */
final class TlsTransport private (current: () => SSLContext) extends Transport:

  /** いま使っている SSLContext（テスト用）。 */
  private[scalas3] def context: SSLContext = current()

  private val ReadBuffer = 256 * 1024

  /** ブロッキング中の処理は取り消せないので、取り消されたら平文のソケットを閉じて抜けさせる。 */
  private def closingOnCancel[A](plain: Socket)(io: IO[A]): IO[A] = io.cancelable(IO.blocking(plain.close()))

  /** 平文のソケットに TLS を重ねる（ハンドシェイクはまだ）。 */
  private def layer(plain: Socket, client: Boolean): SSLSocket =
    plain.setTcpNoDelay(true)
    val s = current().getSocketFactory.createSocket(plain, plain.getInetAddress.getHostAddress, plain.getPort, true).asInstanceOf[SSLSocket]
    s.setEnabledProtocols(Array("TLSv1.3"))
    s.setUseClientMode(client)
    if !client then s.setNeedClientAuth(true)
    s

  private def connection(plain: Socket, s: SSLSocket, remoteAddr: NodeAddress): Resource[IO, Connection] =
    Resource.make(Mutex[IO])(_ => IO.blocking(plain.close())).map { writeLock =>
      val in = s.getInputStream
      val rawIn = plain.getInputStream
      val out = s.getOutputStream
      // 読み込み用のバッファ。読み込みは 1 つのコンシューマーから順にしか呼ばれないので使い回せる。
      // 少ししか読めなかったときは、その分だけを写して返す（毎回 256 KiB を確保すると、小さな RPC で無駄が大きい）。
      // 多く読めたときは写さずにそのまま渡し、次の読み込みでは新しく確保する
      var readBuf = new Array[Byte](ReadBuffer)
      new Connection:
        val remote = remoteAddr
        // 閉じた後の読み込みの失敗（相手が切った、こちらが閉じた）は、ストリームの終わりとして扱う。
        // ブロッキング中の読み込みは取り消せないので、取り消されたら平文のソケットを閉じて抜けさせる（でないと、
        // 片付けのときに「読み込みの取り消し」と「ソケットを閉じる」が互いを待って止まる）
        def reads: Stream[IO, Byte] =
          Stream
            .repeatEval(closingOnCancel(plain)(IO.blocking {
              val buf = readBuf
              try
                // SSLSocket の read は 1 回にレコード 1 つ分（最大 16 KiB）しか返さない。1 回ずつブロッキング用の
                // スレッドと受け渡しすると遅いので、下の平文のソケットに届いている分があるうちは続けて読む
                var n = in.read(buf)
                if n < 0 then None
                else
                  var more = true
                  while more && n < buf.length - 16 * 1024 && (in.available() > 0 || rawIn.available() > 0) do
                    val m = in.read(buf, n, buf.length - n)
                    if m < 0 then more = false else n += m
                  if n < buf.length / 4 then Some(Chunk.array(java.util.Arrays.copyOf(buf, n)))
                  else
                    readBuf = new Array[Byte](ReadBuffer)
                    Some(Chunk.array(buf, 0, n))
              catch case _: IOException if plain.isClosed => None
            }))
            .unNoneTerminate
            .unchunks
        def write(bytes: Chunk[Byte]): IO[Unit] =
          writeLock.lock.surround(closingOnCancel(plain)(IO.blocking {
            val a = bytes.toArraySlice
            out.write(a.values, a.offset, a.length)
            out.flush()
          }))
    }

  def connect(to: NodeAddress): Resource[IO, Connection] =
    for
      plain <- Resource.make(IO.blocking(Socket()))(p => IO.blocking(p.close()))
      _ <- Resource.eval(closingOnCancel(plain)(IO.blocking(plain.connect(InetSocketAddress(to.host, to.port)))))
      // 接続する側は、ここでハンドシェイクまで済ませる（証明書の誤りを接続の失敗として返すため）
      s <- Resource.eval(closingOnCancel(plain)(IO.blocking { val s = layer(plain, client = true); s.startHandshake(); s }))
      c <- connection(plain, s, to)
    yield c

  def listen(at: NodeAddress): Resource[IO, (NodeAddress, Stream[IO, Connection])] =
    Resource
      .make(IO.blocking {
        val server = ServerSocket()
        server.setReuseAddress(true)
        server.bind(InetSocketAddress(at.host, at.port))
        server
      })(server => IO.blocking(server.close()))
      .map { server =>
        // 閉じると accept が例外で抜ける。そこで待ち受けを終える
        val accepted = Stream
          .repeatEval(
            IO.blocking(try Some(server.accept()) catch case _: IOException if server.isClosed => None)
              .cancelable(IO.blocking(server.close()))
          )
          .unNoneTerminate
        // 受けた側のハンドシェイクは、最初の読み書きで SSLSocket が行う（待ち受けを止めないため）。失敗した接続
        // （証明書が違う、平文で来たなど）は、読み書きの失敗として RpcPeer が閉じる。
        // ここで parJoin して並行させてはいけない：内側のストリームが終わった時点でソケットが解放されてしまう
        val conns = accepted.flatMap { plain =>
          Stream.eval(IO(layer(plain, client = false)).attempt).flatMap {
            case Right(s) => Stream.resource(connection(plain, s, NodeAddress(plain.getInetAddress.getHostAddress, plain.getPort)))
            case Left(_)  => Stream.exec(IO.blocking(plain.close()))
          }
        }
        (NodeAddress(at.host, server.getLocalPort), conns)
      }

object TlsTransport:
  /** @param truststore None なら keystore の証明書をそのまま信頼する（全ノードで共通の自己署名証明書を使う場合） */
  def fromKeyStore(keystore: java.nio.file.Path, password: String, truststore: Option[java.nio.file.Path]): IO[TlsTransport] =
    load(keystore, password, truststore).map(ssl => new TlsTransport(() => ssl))

  /** fromKeyStore と同じだが、ファイルが更新されたら読み直す（interval ごとに更新時刻を見る）。読み直しに失敗したら
    * 前のものを使い続けて警告する（同じ更新時刻のあいだは繰り返さない）。
    */
  def reloading(keystore: java.nio.file.Path, password: IO[String], truststore: Option[java.nio.file.Path], interval: FiniteDuration = 1.minute): Resource[IO, TlsTransport] =
    def stamp = IO.blocking((keystore :: truststore.toList).map(p => java.nio.file.Files.getLastModifiedTime(p).toMillis))
    for
      first <- Resource.eval(stamp)
      ssl0 <- Resource.eval(password.flatMap(load(keystore, _, truststore)))
      ref = AtomicReference(ssl0)
      _ <- {
        def loop(seen: List[Long]): IO[Unit] =
          IO.sleep(interval) >> stamp.attempt.flatMap {
            case Right(now) if now != seen =>
              password.flatMap(load(keystore, _, truststore)).attempt.flatMap {
                case Right(ssl) => IO(ref.set(ssl)) >> IO.println(s"[scalas3] reloaded the node TLS certificate from $keystore")
                case Left(e)    => IO.println(s"[scalas3] WARNING: could not reload the node TLS certificate ($e); keeping the previous one")
              } >> loop(now)
            case _ => loop(seen)
          }
        loop(first).background
      }
    yield new TlsTransport(() => ref.get)

  private def load(keystore: java.nio.file.Path, password: String, truststore: Option[java.nio.file.Path]): IO[SSLContext] =
    IO.blocking {
      def readStore(p: java.nio.file.Path): KeyStore =
        val ks = KeyStore.getInstance("PKCS12")
        val in = FileInputStream(p.toFile)
        try ks.load(in, password.toCharArray)
        finally in.close()
        ks
      val ks = readStore(keystore)
      val kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm)
      kmf.init(ks, password.toCharArray)
      val tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
      tmf.init(truststore.fold(ks)(readStore))
      val ssl = SSLContext.getInstance("TLSv1.3")
      ssl.init(kmf.getKeyManagers, tmf.getTrustManagers, null)
      ssl
    }
