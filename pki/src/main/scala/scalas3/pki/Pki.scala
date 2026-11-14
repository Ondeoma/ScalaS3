package scalas3.pki

import org.bouncycastle.asn1.x500.X500Name
import org.bouncycastle.asn1.x509.{BasicConstraints, ExtendedKeyUsage, Extension, GeneralName, GeneralNames, KeyPurposeId, KeyUsage}
import org.bouncycastle.cert.jcajce.{JcaX509CertificateConverter, JcaX509ExtensionUtils, JcaX509v3CertificateBuilder}
import org.bouncycastle.operator.jcajce.{JcaContentSignerBuilder, JcaContentVerifierProviderBuilder}
import org.bouncycastle.pkcs.PKCS10CertificationRequest
import org.bouncycastle.pkcs.jcajce.JcaPKCS10CertificationRequestBuilder

import java.io.{ByteArrayInputStream, FileOutputStream}
import java.math.BigInteger
import java.nio.charset.StandardCharsets.US_ASCII
import java.nio.file.attribute.PosixFilePermissions
import java.nio.file.{Files, Path, StandardCopyOption}
import java.security.cert.{CertificateFactory, X509Certificate}
import java.security.spec.{ECGenParameterSpec, PKCS8EncodedKeySpec, X509EncodedKeySpec}
import java.security.{KeyFactory, KeyPair, KeyPairGenerator, KeyStore, MessageDigest, PrivateKey, PublicKey, SecureRandom}
import java.time.Instant
import java.util.{Base64, Date, HexFormat}

/** 参加トークン。秘密（参加を受け付ける側が確かめる）と、内部 CA の証明書の SHA-256（参加する側が、相手が本物の
  * クラスタかを確かめる）を持つ。`S3J1.<秘密>.<CA の SHA-256>`。
  */
final case class JoinToken(secret: String, caSha256: String):
  def render: String = s"S3J1.$secret.$caSha256"

object JoinToken:
  def parse(s: String): Either[String, JoinToken] =
    s.trim.split('.') match
      case Array("S3J1", secret, hash) if secret.nonEmpty && hash.matches("[0-9a-f]{64}") => Right(JoinToken(secret, hash))
      case _                                                                            => Left("invalid join token (expected S3J1.<secret>.<sha256>)")

/** ノード間 TLS の証明書を作る（EC P-256、SHA256withECDSA）。 */
object Pki:
  private val random = SecureRandom()

  def newKeyPair(): KeyPair =
    val g = KeyPairGenerator.getInstance("EC")
    g.initialize(ECGenParameterSpec("secp256r1"), random)
    g.generateKeyPair()

  private def serial(): BigInteger = BigInteger(127, random)
  private def signer(key: PrivateKey) = JcaContentSignerBuilder("SHA256withECDSA").build(key)
  private val converter = JcaX509CertificateConverter()

  /** 証明書を作る。有効期間は、時計のずれを見込んで 5 分前から days 日後まで。 */
  private def certBuilder(issuer: X500Name, subject: X500Name, pub: PublicKey, days: Int): JcaX509v3CertificateBuilder =
    val now = Instant.now()
    JcaX509v3CertificateBuilder(issuer, serial(), Date.from(now.minusSeconds(300)), Date.from(now.plusSeconds(days.toLong * 86400)), subject, pub)

  /** 自己署名の内部 CA。 */
  def createCa(cn: String, days: Int): (KeyPair, X509Certificate) =
    val kp = newKeyPair()
    val name = X500Name(s"CN=$cn")
    val ext = JcaX509ExtensionUtils()
    val b = certBuilder(name, name, kp.getPublic, days)
      .addExtension(Extension.basicConstraints, true, BasicConstraints(0))
      .addExtension(Extension.keyUsage, true, KeyUsage(KeyUsage.keyCertSign | KeyUsage.cRLSign))
      .addExtension(Extension.subjectKeyIdentifier, false, ext.createSubjectKeyIdentifier(kp.getPublic))
    (kp, converter.getCertificate(b.build(signer(kp.getPrivate))))

  /** ノードの署名要求（PKCS#10、DER）。 */
  def csr(nodeId: String, kp: KeyPair): Array[Byte] =
    JcaPKCS10CertificationRequestBuilder(X500Name(s"CN=$nodeId"), kp.getPublic).build(signer(kp.getPrivate)).getEncoded

  /** 署名要求に CA で署名して、ノードの証明書を作る（TLS のサーバーとクライアントの両方に使える）。
    * 署名要求の自己署名を確かめ、名前は nodeId に置き換える（要求側の書いた名前は信用しない）。
    */
  def signCsr(csrDer: Array[Byte], nodeId: String, caKey: PrivateKey, caCert: X509Certificate, days: Int): X509Certificate =
    val req = PKCS10CertificationRequest(csrDer)
    val verifier = JcaContentVerifierProviderBuilder().build(req.getSubjectPublicKeyInfo)
    if !req.isSignatureValid(verifier) then throw IllegalArgumentException("invalid certificate request signature")
    val pub = KeyFactory.getInstance("EC").generatePublic(X509EncodedKeySpec(req.getSubjectPublicKeyInfo.getEncoded))
    val ext = JcaX509ExtensionUtils()
    val issuer = X500Name.getInstance(caCert.getSubjectX500Principal.getEncoded)
    val b = certBuilder(issuer, X500Name(s"CN=$nodeId"), pub, days)
      .addExtension(Extension.basicConstraints, true, BasicConstraints(false))
      .addExtension(Extension.keyUsage, true, KeyUsage(KeyUsage.digitalSignature | KeyUsage.keyAgreement))
      .addExtension(Extension.extendedKeyUsage, false, ExtendedKeyUsage(Array(KeyPurposeId.id_kp_serverAuth, KeyPurposeId.id_kp_clientAuth)))
      .addExtension(Extension.subjectAlternativeName, false, GeneralNames(GeneralName(GeneralName.dNSName, nodeId)))
      .addExtension(Extension.authorityKeyIdentifier, false, ext.createAuthorityKeyIdentifier(caCert))
    converter.getCertificate(b.build(signer(caKey)))

  def sha256Hex(cert: X509Certificate): String =
    HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(cert.getEncoded))

  def newSecret(): String =
    val b = new Array[Byte](24)
    random.nextBytes(b)
    Base64.getUrlEncoder.withoutPadding.encodeToString(b)

  /** 秘密の比較（かかる時間で中身が漏れないように）。 */
  def secretEquals(a: String, b: String): Boolean = MessageDigest.isEqual(a.getBytes(US_ASCII), b.getBytes(US_ASCII))

  // ---- PEM ----

  def certPem(c: X509Certificate): String = pem("CERTIFICATE", c.getEncoded)
  def keyPem(k: PrivateKey): String = pem("PRIVATE KEY", k.getEncoded)

  private def pem(kind: String, der: Array[Byte]): String =
    s"-----BEGIN $kind-----\n${Base64.getMimeEncoder(64, "\n".getBytes(US_ASCII)).encodeToString(der)}\n-----END $kind-----\n"

  def readCerts(pemText: String): Vector[X509Certificate] =
    import scala.jdk.CollectionConverters.*
    CertificateFactory.getInstance("X.509").generateCertificates(ByteArrayInputStream(pemText.getBytes(US_ASCII))).asScala.toVector.map(_.asInstanceOf[X509Certificate])

  def readKey(pemText: String): PrivateKey =
    val body = pemText.linesIterator.filterNot(_.startsWith("-----")).mkString
    KeyFactory.getInstance("EC").generatePrivate(PKCS8EncodedKeySpec(Base64.getDecoder.decode(body)))

  /** PEM の秘密鍵を読む。形式は問わない（PKCS#8、RSA の PKCS#1、EC の SEC1）。Let's Encrypt などが出す鍵をそのまま使うため。
    * 暗号化された鍵は扱わない。
    */
  def readAnyKey(pemText: String): PrivateKey =
    val parser = org.bouncycastle.openssl.PEMParser(java.io.StringReader(pemText))
    try
      val conv = org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter()
      Iterator.continually(parser.readObject()).takeWhile(_ != null).collectFirst {
        case kp: org.bouncycastle.openssl.PEMKeyPair              => conv.getPrivateKey(kp.getPrivateKeyInfo)
        case pk: org.bouncycastle.asn1.pkcs.PrivateKeyInfo        => conv.getPrivateKey(pk)
      }.getOrElse(throw IllegalArgumentException("no unencrypted private key found in the PEM text"))
    finally parser.close()

  // ---- 鍵ストア（PKCS12） ----

  def writeKeyStore(path: Path, password: String, key: PrivateKey, chain: Vector[X509Certificate]): Unit =
    val ks = emptyKeyStore()
    ks.setKeyEntry("node", key, password.toCharArray, chain.toArray)
    store(ks, path, password)

  def writeTrustStore(path: Path, password: String, ca: X509Certificate): Unit =
    val ks = emptyKeyStore()
    ks.setCertificateEntry("ca", ca)
    store(ks, path, password)

  private def emptyKeyStore(): KeyStore =
    val ks = KeyStore.getInstance("PKCS12")
    ks.load(null, null)
    ks

  private def createParentDirs(path: Path): Unit = Option(path.getParent).foreach(Files.createDirectories(_))

  private def store(ks: KeyStore, path: Path, password: String): Unit =
    createParentDirs(path)
    val tmp = path.resolveSibling(path.getFileName.toString + ".tmp")
    val out = FileOutputStream(tmp.toFile)
    try ks.store(out, password.toCharArray)
    finally out.close()
    Files.move(tmp, path, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING): Unit

  /** 秘密を含むファイルを、所有者だけが読めるように書く。 */
  def writePrivate(path: Path, text: String): Unit =
    createParentDirs(path)
    Files.writeString(path, text)
    try Files.setPosixFilePermissions(path, PosixFilePermissions.fromString("rw-------")): Unit
    catch case _: UnsupportedOperationException => ()
