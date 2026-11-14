package scalas3.server

import cats.effect.{IO, Resource}
import io.netty.handler.ssl.{SslContext, SslContextBuilder, SslProtocols}
import scalas3.pki.Pki

import java.nio.file.{Files, Path}
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.*

/** 公開ポートの HTTPS の証明書と鍵（PEM）。証明書は鎖ごと（サーバーの証明書、中間 CA の順）。
  *
  * 鍵の形式は問わない（PKCS#8、RSA の PKCS#1、EC）。ファイルの更新時刻を `reloadEvery` ごとに見て、変わっていれば読み直す
  * （Let's Encrypt などの自動更新に合わせる。読み直した後の新しい接続から新しい証明書を使う）。読み直しに失敗したときは
  * 前の証明書を使い続け、警告を出す。
  */
final case class PublicTls(cert: Path, key: Path, reloadEvery: FiniteDuration = 1.minute)

object PublicTls:
  private val logger = org.slf4j.LoggerFactory.getLogger("scalas3.server.PublicTls")

  /** 証明書と鍵を読む。TLS 1.2 と 1.3 だけを許す。 */
  def load(t: PublicTls): SslContext =
    val chain = Pki.readCerts(Files.readString(t.cert))
    if chain.isEmpty then throw IllegalArgumentException(s"no certificate in ${t.cert}")
    val key = Pki.readAnyKey(Files.readString(t.key))
    // 更新の途中（証明書だけ新しい）で読んだ組を使わないよう、鍵と証明書が対になっているか確かめる
    if !matches(key, chain.head) then throw IllegalArgumentException(s"the private key in ${t.key} does not match the certificate in ${t.cert}")
    SslContextBuilder.forServer(key, chain*).protocols(SslProtocols.TLS_v1_3, SslProtocols.TLS_v1_2).build()

  private def matches(key: java.security.PrivateKey, cert: java.security.cert.X509Certificate): Boolean =
    val alg = key.getAlgorithm match
      case "RSA"         => "SHA256withRSA"
      case "EC" | "ECDSA" => "SHA256withECDSA"
      case "Ed25519"     => "Ed25519"
      case other         => throw IllegalArgumentException(s"unsupported key algorithm $other")
    val data = java.security.SecureRandom().generateSeed(32)
    val signer = java.security.Signature.getInstance(alg)
    signer.initSign(key)
    signer.update(data)
    val sig = signer.sign()
    val verifier = java.security.Signature.getInstance(alg)
    verifier.initVerify(cert.getPublicKey)
    verifier.update(data)
    verifier.verify(sig)

  private def stamp(t: PublicTls): (Long, Long) =
    (Files.getLastModifiedTime(t.cert).toMillis, Files.getLastModifiedTime(t.key).toMillis)

  /** 読んだ証明書と、ファイルが変わったら読み直す見張り。起動時に読めなければ失敗する。 */
  def reloading(t: PublicTls): Resource[IO, AtomicReference[SslContext]] =
    for
      initial <- Resource.eval(IO.blocking((load(t), stamp(t))))
      ref = AtomicReference(initial._1)
      last = AtomicReference(initial._2)
      check = IO.blocking {
        val now = stamp(t)
        if now != last.get then
          // 先に記録する（読めないファイルについて、変わるまで何度も警告しない）
          last.set(now)
          ref.set(load(t))
          logger.info(s"reloaded the public TLS certificate from ${t.cert}")
      }.handleError(e => logger.warn(s"could not reload the public TLS certificate (keeping the previous one): $e"))
      _ <- (IO.sleep(t.reloadEvery) >> check).foreverM.background
    yield ref
