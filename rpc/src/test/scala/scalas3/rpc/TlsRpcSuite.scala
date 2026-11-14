package scalas3.rpc

import cats.effect.IO
import cats.syntax.all.*
import fs2.io.file.Files
import munit.CatsEffectSuite
import scalas3.runtime.{NodeAddress, TcpTransport, TlsTransport}
import scodec.codecs.*

import scala.concurrent.duration.*

class TlsRpcSuite extends CatsEffectSuite:

  override def munitIOTimeout: Duration = 2.minutes

  private val Echo = Method(1, "echo", utf8_32, utf8_32)

  /** JDK の keytool で自己署名の鍵ストアを作る。 */
  private def keystore(dir: java.nio.file.Path, name: String): IO[java.nio.file.Path] = IO.blocking {
    val p = dir.resolve(s"$name.p12")
    val keytool = java.nio.file.Paths.get(System.getProperty("java.home"), "bin", "keytool").toString
    val proc = new ProcessBuilder(
      keytool, "-genkeypair", "-alias", "node", "-keyalg", "EC", "-groupname", "secp256r1", "-validity", "30",
      "-dname", s"CN=$name", "-keystore", p.toString, "-storetype", "PKCS12", "-storepass", "changeit", "-keypass", "changeit"
    ).redirectErrorStream(true).start()
    val out = new String(proc.getInputStream.readAllBytes())
    if proc.waitFor() != 0 then throw new Exception(s"keytool failed: $out")
    p
  }

  test("相互認証つき TLS で RPC でき、信頼していない証明書や平文の相手は拒否される") {
    Files[IO].tempDirectory.use { tmp =>
      for
        cluster <- keystore(tmp.toNioPath, "cluster")
        other <- keystore(tmp.toNioPath, "other")
        tls <- TlsTransport.fromKeyStore(cluster, "changeit", None)
        stranger <- TlsTransport.fromKeyStore(other, "changeit", None)
        r <- RpcServer.serve(tls, NodeAddress("127.0.0.1", 0), RpcHandler.of(Echo.handledBy(IO.pure))).use { addr =>
          for
            ok <- RpcClient(tls).use(c => (1 to 20).toList.parTraverse(i => c.call(addr, Echo, s"hi $i")))
            bad <- RpcClient(stranger).use(c => c.call(addr, Echo, "x").timeout(10.seconds).attempt)
            plain <- RpcClient(TcpTransport).use(c => c.call(addr, Echo, "x").timeout(10.seconds).attempt)
          yield (ok, bad, plain)
        }
      yield
        val (ok, bad, plain) = r
        assertEquals(ok, (1 to 20).map(i => s"hi $i").toList)
        assert(bad.isLeft, "信頼していない証明書は拒否されるはず")
        assert(plain.isLeft, "平文の接続は拒否されるはず")
    }
  }

