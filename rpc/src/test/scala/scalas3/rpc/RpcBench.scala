package scalas3.rpc

import cats.effect.IO
import cats.syntax.all.*
import scalas3.runtime.{NodeAddress, TcpTransport, TlsTransport, Transport}
import scodec.bits.ByteVector
import scodec.codecs.*

import scala.concurrent.duration.*

/** RPC の転送速度（ブロック転送を想定した大きな本体）。TCP と TLS を比べる。
  * {{{
  * sbt "rpc/Test/runMain scalas3.rpc.RpcBench <keystore.p12> <password> [本体の大きさ] [並行数] [秒数]"
  * }}}
  */
object RpcBench:
  private val Put = Method(1, "put", bytes, provide(()), Priority.Bulk)

  def main(args: Array[String]): Unit =
    if System.getProperty("cats.effect.tracing.mode") == null then System.setProperty("cats.effect.tracing.mode", "none"): Unit
    val ks = java.nio.file.Paths.get(args(0))
    val pw = args(1)
    val size = args.lift(2).fold(1 << 20)(_.toInt)
    val conc = args.lift(3).fold(4)(_.toInt)
    val secs = args.lift(4).fold(10)(_.toInt)
    import cats.effect.unsafe.implicits.global
    val body = ByteVector.view(Array.tabulate[Byte](size)(i => (i * 31).toByte))

    def measure(name: String, t: Transport): IO[Unit] =
      RpcServer.serve(t, NodeAddress("127.0.0.1", 0), RpcHandler.of(Put.handledBy(_ => IO.unit))).use { addr =>
        RpcClient(t).use { c =>
          for
            _ <- c.call(addr, Put, body) // 接続とハンドシェイク
            start <- IO.monotonic
            deadline = start + secs.seconds
            counts <- (0 until conc).toList.parTraverse { _ =>
              def loop(n: Long): IO[Long] =
                IO.monotonic.flatMap(now => if now >= deadline then IO.pure(n) else c.call(addr, Put, body) >> loop(n + 1))
              loop(0)
            }
            end <- IO.monotonic
            total = counts.sum
            secsTaken = (end - start).toMillis / 1000.0
            _ <- IO.println(f"[rpc] $name size=$size conc=$conc: ${total * size / secsTaken / 1048576}%.1f MiB/s (${total / secsTaken}%.1f calls/s)")
          yield ()
        }
      }

    (for
      tls <- TlsTransport.fromKeyStore(ks, pw, None)
      _ <- measure("tcp", TcpTransport)
      _ <- measure("tls", tls)
    yield ()).unsafeRunSync()
