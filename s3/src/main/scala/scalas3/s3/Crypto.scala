package scalas3.s3

import scodec.bits.ByteVector

import java.nio.charset.StandardCharsets.{US_ASCII, UTF_8}
import java.security.{MessageDigest, SecureRandom}
import java.util.Base64
import javax.crypto.Cipher
import javax.crypto.spec.{GCMParameterSpec, SecretKeySpec}

/** AES-256-GCM による暗号化。暗号文は [nonce 12][暗号文 + タグ 16]。 */
object Crypto:
  val Overhead = 28
  private val rnd = new SecureRandom()

  // 鍵の用途ごとの AAD（別の用途の暗号文を取り違えて復号できないように）
  private val DekAad = "scalas3-dek".getBytes(US_ASCII)
  private val SecretAad = "scalas3-secret".getBytes(US_ASCII)
  private val SealedPrefix = "enc:"

  /** 暗号論的な乱数で n バイト（ID や鍵、nonce に使う。SecureRandom は使い回す）。 */
  def randomBytes(n: Int): Array[Byte] =
    val b = new Array[Byte](n)
    rnd.nextBytes(b)
    b

  def randomKey(): Array[Byte] = randomBytes(32)

  private def cipher(mode: Int, key: Array[Byte], nonce: Array[Byte], aad: Array[Byte]): Cipher =
    val c = Cipher.getInstance("AES/GCM/NoPadding")
    c.init(mode, SecretKeySpec(key, "AES"), GCMParameterSpec(128, nonce))
    if aad.nonEmpty then c.updateAAD(aad)
    c

  def encrypt(key: Array[Byte], plain: ByteVector, aad: Array[Byte] = Array.emptyByteArray): ByteVector =
    val nonce = randomBytes(12)
    ByteVector.view(nonce) ++ ByteVector.view(cipher(Cipher.ENCRYPT_MODE, key, nonce, aad).doFinal(plain.toArrayUnsafe))

  def decrypt(key: Array[Byte], data: ByteVector, aad: Array[Byte] = Array.emptyByteArray): ByteVector =
    if data.size < Overhead then throw S3Error.internal
    ByteVector.view(cipher(Cipher.DECRYPT_MODE, key, data.take(12).toArray, aad).doFinal(data.drop(12).toArrayUnsafe))

  /** データ鍵をマスター鍵で包む / 取り出す。 */
  def wrap(master: Array[Byte], dataKey: Array[Byte]): ByteVector = encrypt(master, ByteVector.view(dataKey), DekAad)
  def unwrap(master: Array[Byte], wrapped: ByteVector): Array[Byte] = decrypt(master, wrapped, DekAad).toArray

  def md5Base64(b: Array[Byte]): String = Base64.getEncoder.encodeToString(MessageDigest.getInstance("MD5").digest(b))

  /** 保存するアクセスキーの秘密鍵（"enc:" で始まれば暗号化済み）。 */
  def sealSecret(master: Option[Array[Byte]], secret: String): String =
    master.fold(secret)(m => SealedPrefix + Base64.getEncoder.encodeToString(encrypt(m, ByteVector.view(secret.getBytes(UTF_8)), SecretAad).toArray))

  def openSecret(master: Option[Array[Byte]], stored: String): String =
    if !stored.startsWith(SealedPrefix) then stored
    else
      val m = master.getOrElse(throw IllegalStateException("access key secret is encrypted but no master key is configured"))
      new String(decrypt(m, ByteVector.view(Base64.getDecoder.decode(stored.drop(SealedPrefix.length))), SecretAad).toArray, UTF_8)
