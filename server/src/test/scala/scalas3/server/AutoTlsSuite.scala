package scalas3.server

import cats.effect.IO
import fs2.io.file.Files
import munit.CatsEffectSuite
import scalas3.pki.{JoinToken, Pki}
import scalas3.rpc.{Method, RpcClient, RpcHandler, RpcServer}
import scalas3.runtime.{NodeAddress, TlsTransport}
import scodec.codecs.utf8_32

import scala.concurrent.duration.*

class AutoTlsSuite extends CatsEffectSuite:

  private val Echo = Method(1, "echo", utf8_32, utf8_32)

  /** 内部 CA を作る（scalas3 cert init-ca と同じもの）。トークンを返す。 */
  private def initCa(dir: java.nio.file.Path): JoinToken =
    val (kp, ca) = Pki.createCa("test CA", 30)
    val secret = Pki.newSecret()
    java.nio.file.Files.createDirectories(dir)
    java.nio.file.Files.writeString(dir.resolve("ca.crt"), Pki.certPem(ca))
    java.nio.file.Files.writeString(dir.resolve("ca.key"), Pki.keyPem(kp.getPrivate))
    java.nio.file.Files.writeString(dir.resolve("join.secret"), secret)
    JoinToken(secret, Pki.sha256Hex(ca))

  test("CA を持つノードは自分で発行し、後から加わるノードは参加トークンで受け取り、相互認証の TLS で話せる") {
    Files[IO].tempDirectory.use { tmp =>
      val root = tmp.toNioPath
      val token = initCa(root.resolve("ca"))
      val listen = NodeAddress("127.0.0.1", 0)
      for
        a <- AutoTls.ensure(root.resolve("a"), "a", AutoTls.Settings(Some(root.resolve("ca")), None, None, listen)).map(_.get)
        r <- AutoTls.joinServer(listen, a, root.resolve("ca")).use { addr =>
          val endpoint = NodeAddress("127.0.0.1", addr.getPort)
          for
            b <- AutoTls.ensure(root.resolve("b"), "b", AutoTls.Settings(None, Some(token), Some(endpoint), listen)).map(_.get)
            // 2 回目は発行済みのものを使う（受け付けに問い合わせない）
            again <- AutoTls.ensure(root.resolve("b"), "b", AutoTls.Settings(None, Some(token.copy(secret = "wrong")), Some(endpoint), listen))
            badSecret <- AutoTls.ensure(root.resolve("c"), "c", AutoTls.Settings(None, Some(token.copy(secret = "wrong")), Some(endpoint), listen)).attempt
            badCa <- AutoTls.ensure(root.resolve("d"), "d", AutoTls.Settings(None, Some(token.copy(caSha256 = "0" * 64)), Some(endpoint), listen)).attempt
            ta <- TlsTransport.fromKeyStore(a.keystore, a.password, Some(a.truststore))
            tb <- TlsTransport.fromKeyStore(b.keystore, b.password, Some(b.truststore))
            echoed <- RpcServer.serve(ta, NodeAddress("127.0.0.1", 0), RpcHandler.of(Echo.handledBy(IO.pure))).use { at =>
              RpcClient(tb).use(_.call(at, Echo, "hello over mTLS"))
            }
          yield (again.map(_.keystore), b.keystore, badSecret, badCa, echoed)
        }
      yield
        val (again, bKeystore, badSecret, badCa, echoed) = r
        assertEquals(again, Some(bKeystore))
        assert(badSecret.isLeft, "wrong secret must be refused")
        assert(badCa.isLeft, "a join endpoint that does not match the token's CA must not be trusted")
        assertEquals(echoed, "hello over mTLS")
    }
  }

  test("有効期間の 2/3 を過ぎると更新する：CA を持つノードは自分で、参加したノードはいまの証明書で受け付けに頼み、TLS は読み直す") {
    Files[IO].tempDirectory.use { tmp =>
      val root = tmp.toNioPath
      val token = initCa(root.resolve("ca"))
      val other = root.resolve("other-ca")
      initCa(other)
      val listen = NodeAddress("127.0.0.1", 0)
      val caSettings = AutoTls.Settings(Some(root.resolve("ca")), None, None, listen, days = 3)
      def serial(m: AutoTls.Material) = IO.blocking(AutoTls.nodeCertificate(m).getSerialNumber)
      for
        a <- AutoTls.ensure(root.resolve("a"), "a", caSettings).map(_.get)
        r <- AutoTls.joinServer(listen, a, root.resolve("ca"), days = 3).use { addr =>
          val endpoint = NodeAddress("127.0.0.1", addr.getPort)
          for
            b <- AutoTls.ensure(root.resolve("b"), "b", AutoTls.Settings(None, Some(token), Some(endpoint), listen)).map(_.get)
            now <- IO.realTimeInstant
            later = now.plus(java.time.Duration.ofHours(60)) // 3 日の 2/3（48 時間）を過ぎた
            notYet <- AutoTls.renewIfDue(root.resolve("a"), "a", caSettings, a, now)
            sa0 <- serial(a)
            sb0 <- serial(b)
            r <- TlsTransport.reloading(b.keystore, IO.pure(b.password), Some(b.truststore), 50.millis).use { tb =>
              val ctx0 = tb.context
              for
                _ <- IO.sleep(20.millis)
                renewedA <- AutoTls.renewIfDue(root.resolve("a"), "a", caSettings, a, later)
                // 環境変数の参加先がなくても、参加したときに保存したアドレスで更新できる（トークンも要らない）
                renewedB <- AutoTls.renewIfDue(root.resolve("b"), "b", AutoTls.Settings(None, None, None, listen), b, later)
                _ <- IO.sleep(300.millis)
                reloaded = !(tb.context eq ctx0)
                sa1 <- serial(a)
                sb1 <- serial(b)
                cb <- IO.blocking(AutoTls.nodeCertificate(b))
                ta <- TlsTransport.fromKeyStore(a.keystore, a.password, Some(a.truststore))
                echoed <- RpcServer.serve(ta, NodeAddress("127.0.0.1", 0), RpcHandler.of(Echo.handledBy(IO.pure))).use { at =>
                  RpcClient(tb).use(_.call(at, Echo, "after renewal"))
                }
              yield (renewedA, renewedB, reloaded, sa1 != sa0, sb1 != sb0, cb.getSubjectX500Principal.getName, echoed)
            }
            // 別の CA の証明書では更新を頼めない
            x <- AutoTls.ensure(root.resolve("x"), "x", AutoTls.Settings(Some(other), None, None, listen, days = 3)).map(_.get)
            foreign <- AutoTls.renewIfDue(root.resolve("x"), "x", AutoTls.Settings(None, None, Some(endpoint), listen), x, later).attempt
          yield (notYet, r, foreign)
        }
      yield
        val (notYet, (renewedA, renewedB, reloaded, aChanged, bChanged, bName, echoed), foreign) = r
        assert(!notYet, "a fresh certificate must not be renewed")
        assert(renewedA && aChanged, "the CA holder renews its own certificate")
        assert(renewedB && bChanged, "a joined node renews through the join endpoint")
        assert(bName.contains("CN=b"), bName)
        assert(reloaded, "the transport must pick up the renewed key store")
        assertEquals(echoed, "after renewal")
        assert(foreign.isLeft, "a certificate from another CA must not be renewed")
    }
  }

