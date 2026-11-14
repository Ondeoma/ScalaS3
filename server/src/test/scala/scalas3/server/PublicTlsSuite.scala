package scalas3.server

import cats.effect.IO
import munit.CatsEffectSuite
import org.http4s.*
import scalas3.pki.Pki

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.file.Files
import java.security.cert.X509Certificate
import javax.net.ssl.{SSLContext, TrustManagerFactory}
import scala.concurrent.duration.*

class PublicTlsSuite extends CatsEffectSuite:
  override def munitIOTimeout: Duration = 30.seconds

  private val app: HttpApp[IO] = HttpApp[IO] { req =>
    IO.pure(Response[IO](Status.Ok).withEntity(s"secure=${req.isSecure.getOrElse(false)}"))
  }

  /** CA が署名したサーバー証明書。鍵は PKCS#8 ではない形式（EC の SEC1、"BEGIN EC PRIVATE KEY"）で書く。 */
  private def issue(caKey: java.security.PrivateKey, ca: X509Certificate): (String, String, X509Certificate) =
    // OpenSSL が書く形（曲線のパラメーターと公開鍵を含む SEC1）にするため、Bouncy Castle で鍵を作る
    val kpg = java.security.KeyPairGenerator.getInstance("EC", org.bouncycastle.jce.provider.BouncyCastleProvider())
    kpg.initialize(java.security.spec.ECGenParameterSpec("secp256r1"))
    val kp = kpg.generateKeyPair()
    val cert = Pki.signCsr(Pki.csr("localhost", kp), "localhost", caKey, ca, 1)
    val sw = java.io.StringWriter()
    val w = org.bouncycastle.openssl.jcajce.JcaPEMWriter(sw)
    w.writeObject(kp)
    w.close()
    (Pki.certPem(cert) + Pki.certPem(ca), sw.toString, cert)

  private def client(ca: X509Certificate): HttpClient =
    val ks = java.security.KeyStore.getInstance("PKCS12")
    ks.load(null, null)
    ks.setCertificateEntry("ca", ca)
    val tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
    tmf.init(ks)
    val ssl = SSLContext.getInstance("TLS")
    ssl.init(null, tmf.getTrustManagers, null)
    // 接続を使い回さない（読み直し後の新しい接続で証明書を確かめるため）
    HttpClient.newBuilder().sslContext(ssl).version(HttpClient.Version.HTTP_1_1).build()

  test("公開ポートの HTTPS：SEC1 の鍵を読み、証明書が更新されたら新しい接続から使う") {
    val dir = Files.createTempDirectory("public-tls")
    val (caKp, ca) = Pki.createCa("test ca", 1)
    val (chain1, key1, cert1) = issue(caKp.getPrivate, ca)
    val certFile = dir.resolve("fullchain.pem")
    val keyFile = dir.resolve("privkey.pem")
    Files.writeString(certFile, chain1)
    Files.writeString(keyFile, key1)
    val tls = PublicTls(certFile, keyFile, reloadEvery = 200.millis)
    def serial(addr: java.net.InetSocketAddress): IO[(String, BigInt)] = IO.blocking {
      val c = client(ca)
      try
        val r = c.send(HttpRequest.newBuilder(URI.create(s"https://localhost:${addr.getPort}/")).build(), HttpResponse.BodyHandlers.ofString())
        val peer = r.sslSession().get().getPeerCertificates.head.asInstanceOf[X509Certificate]
        (r.body(), BigInt(peer.getSerialNumber))
      finally c.close()
    }
    NettyHttpServer.resource(NettyHttpServer.Config("127.0.0.1", 0, tls = Some(tls)), app).use { addr =>
      for
        first <- serial(addr)
        (chain2, key2, cert2) = issue(caKp.getPrivate, ca)
        _ <- IO.blocking { Files.writeString(keyFile, key2); Files.writeString(certFile, chain2) }
        _ <- IO.sleep(100.millis)
        // 更新時刻が同じミリ秒にならないように少し進める
        _ <- IO.blocking(Files.setLastModifiedTime(certFile, java.nio.file.attribute.FileTime.fromMillis(System.currentTimeMillis() + 1000)))
        _ <- IO.sleep(1.second)
        second <- serial(addr)
        // 鍵だけ別のものにしても（更新の途中）、前の証明書で動き続ける
        (_, key3, _) = issue(caKp.getPrivate, ca)
        _ <- IO.blocking(Files.writeString(keyFile, key3))
        _ <- IO.blocking(Files.setLastModifiedTime(keyFile, java.nio.file.attribute.FileTime.fromMillis(System.currentTimeMillis() + 1500)))
        _ <- IO.sleep(1.second)
        mismatched <- serial(addr)
        // 壊れたファイルに置き換えても、前の証明書で動き続ける
        _ <- IO.blocking(Files.writeString(certFile, "garbage"))
        _ <- IO.blocking(Files.setLastModifiedTime(certFile, java.nio.file.attribute.FileTime.fromMillis(System.currentTimeMillis() + 2000)))
        _ <- IO.sleep(1.second)
        third <- serial(addr)
      yield
        assertEquals(first, ("secure=true", BigInt(cert1.getSerialNumber)))
        assertEquals(second, ("secure=true", BigInt(cert2.getSerialNumber)))
        assertEquals(mismatched._2, BigInt(cert2.getSerialNumber))
        assertEquals(third._2, BigInt(cert2.getSerialNumber))
    }
  }

  test("鍵の形式：PKCS#8 の EC 鍵も読める") {
    // OpenSSL が書く形（曲線のパラメーターと公開鍵を含む SEC1）にするため、Bouncy Castle で鍵を作る
    val kpg = java.security.KeyPairGenerator.getInstance("EC", org.bouncycastle.jce.provider.BouncyCastleProvider())
    kpg.initialize(java.security.spec.ECGenParameterSpec("secp256r1"))
    val kp = kpg.generateKeyPair()
    val k = Pki.readAnyKey(Pki.keyPem(kp.getPrivate))
    assertEquals(k.getEncoded.toSeq, kp.getPrivate.getEncoded.toSeq)
  }
