package scalas3.s3

import scodec.bits.ByteVector

import java.nio.charset.StandardCharsets.UTF_8
import java.security.MessageDigest
import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDateTime, ZoneOffset}
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

/** AWS Signature Version 4 の計算。 */
object SigV4:
  val Algorithm = "AWS4-HMAC-SHA256"
  val UnsignedPayload = "UNSIGNED-PAYLOAD"
  val StreamingSigned = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
  val StreamingSignedTrailer = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER"
  val StreamingUnsignedTrailer = "STREAMING-UNSIGNED-PAYLOAD-TRAILER"
  val EmptySha256: String = sha256Hex(Array.emptyByteArray)

  final case class Scope(date: String, region: String, service: String):
    def render: String = s"$date/$region/$service/aws4_request"

  /** 署名に関する情報（ヘッダー認証と Presigned URL の共通部分）。 */
  final case class Auth(
      accessKey: String,
      scope: Scope,
      signedHeaders: List[String],
      signature: String,
      amzDate: String,
      presigned: Boolean,
      expiresSeconds: Option[Long]
  )

  private val amzDateFormat = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'")

  def parseAmzDate(s: String): Option[Long] =
    try Some(LocalDateTime.parse(s, amzDateFormat).toInstant(ZoneOffset.UTC).toEpochMilli)
    catch case _: Exception => None

  /** X-Amz-Date の形式（yyyyMMdd'T'HHmmss'Z'）。 */
  def formatAmzDate(millis: Long): String = amzDateFormat.format(Instant.ofEpochMilli(millis).atOffset(ZoneOffset.UTC))

  private def parseCredential(c: String): Option[(String, Scope)] =
    c.split('/') match
      case Array(ak, date, region, service, "aws4_request") => Some((ak, Scope(date, region, service)))
      case _                                                => None

  /** Authorization ヘッダーを読む。 */
  def parseHeader(value: String, amzDate: Option[String]): Either[S3Error, Auth] =
    if !value.startsWith(Algorithm + " ") then Left(S3Error.invalidArgument("Unsupported authorization type"))
    else
      val parts = value.drop(Algorithm.length + 1).split(',').map(_.trim).flatMap { p =>
        p.split("=", 2) match
          case Array(k, v) => Some(k.trim -> v.trim)
          case _           => None
      }.toMap
      for
        cred <- parts.get("Credential").flatMap(parseCredential).toRight(S3Error.invalidArgument("Bad Credential"))
        signed <- parts.get("SignedHeaders").toRight(S3Error.invalidArgument("Missing SignedHeaders"))
        sig <- parts.get("Signature").toRight(S3Error.invalidArgument("Missing Signature"))
        date <- amzDate.toRight(S3Error.accessDenied)
      yield Auth(cred._1, cred._2, signed.split(';').toList, sig, date, presigned = false, None)

  /** Presigned URL のクエリを読む。 */
  def parseQuery(q: Map[String, String]): Either[S3Error, Auth] =
    for
      _ <- q.get("X-Amz-Algorithm").filter(_ == Algorithm).toRight(S3Error.invalidArgument("Unsupported X-Amz-Algorithm"))
      cred <- q.get("X-Amz-Credential").flatMap(parseCredential).toRight(S3Error.invalidArgument("Bad X-Amz-Credential"))
      date <- q.get("X-Amz-Date").toRight(S3Error.invalidArgument("Missing X-Amz-Date"))
      signed <- q.get("X-Amz-SignedHeaders").toRight(S3Error.invalidArgument("Missing X-Amz-SignedHeaders"))
      sig <- q.get("X-Amz-Signature").toRight(S3Error.invalidArgument("Missing X-Amz-Signature"))
      exp <- q.get("X-Amz-Expires").flatMap(_.toLongOption).filter(e => e > 0 && e <= 604800)
        .toRight(S3Error.invalidArgument("X-Amz-Expires must be between 1 and 604800"))
    yield Auth(cred._1, cred._2, signed.split(';').toList, sig, date, presigned = true, Some(exp))

  private val HexDigits = "0123456789ABCDEF"

  private def unreserved(c: Char): Boolean =
    (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.' || c == '~'

  /** RFC 3986 の非予約文字以外を %XX にする（AWS の規則）。署名の検証で要求ごとに呼ぶので、書式化は使わない。 */
  def uriEncode(s: String, encodeSlash: Boolean): String =
    val bytes = s.getBytes(UTF_8)
    val sb = new java.lang.StringBuilder(bytes.length + 16)
    bytes.foreach { b =>
      val c = (b & 0xff).toChar
      if unreserved(c) || (c == '/' && !encodeSlash) then sb.append(c)
      else sb.append('%').append(HexDigits.charAt((b >> 4) & 0xf)).append(HexDigits.charAt(b & 0xf))
    }
    sb.toString

  /** %XX をデコードする（+ は空白にしない）。 */
  def percentDecode(s: String): String =
    if s.indexOf('%') < 0 then s
    else
      val out = new java.io.ByteArrayOutputStream(s.length)
      var i = 0
      while i < s.length do
        if s.charAt(i) == '%' && i + 2 < s.length && isHex(s.charAt(i + 1)) && isHex(s.charAt(i + 2)) then
          out.write(Character.digit(s.charAt(i + 1), 16) * 16 + Character.digit(s.charAt(i + 2), 16))
          i += 3
        else
          // 次の % までをまとめて UTF-8 にする（1 文字ずつだと、サロゲートペアの文字が壊れる）
          val next = s.indexOf('%', i + 1)
          val end = if next < 0 then s.length else next
          out.writeBytes(s.substring(i, end).getBytes(UTF_8))
          i = end
      new String(out.toByteArray, UTF_8)

  private def isHex(c: Char) = (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')

  /** 生のクエリ文字列を (デコード済みキー, デコード済み値) の列にする。 */
  def parseRawQuery(raw: String): List[(String, String)] =
    if raw.isEmpty then Nil
    else
      raw.split('&').toList.filter(_.nonEmpty).map { p =>
        p.split("=", 2) match
          case Array(k, v) => (percentDecode(k), percentDecode(v))
          case Array(k)    => (percentDecode(k), "")
          case _           => ("", "")
      }

  private val Whitespace = java.util.regex.Pattern.compile("\\s+")

  def canonicalRequest(
      method: String,
      decodedPath: String,
      query: List[(String, String)],
      headers: String => List[String],
      signedHeaders: List[String],
      payloadHash: String
  ): String =
    val canonicalUri = uriEncode(decodedPath, encodeSlash = false)
    val canonicalQuery = query
      .filterNot(_._1 == "X-Amz-Signature")
      .map((k, v) => (uriEncode(k, true), uriEncode(v, true)))
      .sorted
      .map((k, v) => s"$k=$v")
      .mkString("&")
    val canonicalHeaders = signedHeaders.map { h =>
      val v = headers(h).map(x => Whitespace.matcher(x.trim).replaceAll(" ")).mkString(",")
      s"$h:$v\n"
    }.mkString
    List(method, canonicalUri, canonicalQuery, canonicalHeaders, signedHeaders.mkString(";"), payloadHash).mkString("\n")

  def stringToSign(amzDate: String, scope: Scope, canonical: String): String =
    s"$Algorithm\n$amzDate\n${scope.render}\n${sha256Hex(canonical.getBytes(UTF_8))}"

  def signingKey(secret: String, scope: Scope): Array[Byte] =
    val kDate = hmac(("AWS4" + secret).getBytes(UTF_8), scope.date)
    val kRegion = hmac(kDate, scope.region)
    val kService = hmac(kRegion, scope.service)
    hmac(kService, "aws4_request")

  def sign(key: Array[Byte], data: String): String = ByteVector.view(hmac(key, data)).toHex

  def hmac(key: Array[Byte], data: String): Array[Byte] =
    val mac = Mac.getInstance("HmacSHA256")
    mac.init(SecretKeySpec(key, "HmacSHA256"))
    mac.doFinal(data.getBytes(UTF_8))

  def sha256Hex(data: Array[Byte]): String = ByteVector.view(MessageDigest.getInstance("SHA-256").digest(data)).toHex

  /** 定数時間の比較。 */
  def safeEquals(a: String, b: String): Boolean =
    MessageDigest.isEqual(a.getBytes(UTF_8), b.getBytes(UTF_8))

  /** aws-chunked の各チャンクの署名。 */
  def chunkStringToSign(amzDate: String, scope: Scope, previous: String, chunkSha256Hex: String): String =
    s"AWS4-HMAC-SHA256-PAYLOAD\n$amzDate\n${scope.render}\n$previous\n$EmptySha256\n$chunkSha256Hex"

  def trailerStringToSign(amzDate: String, scope: Scope, previous: String, trailerSha256Hex: String): String =
    s"AWS4-HMAC-SHA256-TRAILER\n$amzDate\n${scope.render}\n$previous\n$trailerSha256Hex"
