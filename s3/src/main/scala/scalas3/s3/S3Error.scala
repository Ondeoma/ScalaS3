package scalas3.s3

import org.http4s.Status

/** S3 の XML エラー応答になる例外。 */
final case class S3Error(code: String, message: String, status: Status, headers: Map[String, String] = Map.empty)
    extends RuntimeException(s"$code: $message"):
  def withHeaders(hs: (String, String)*): S3Error = copy(headers = headers ++ hs)

object S3Error:
  def noSuchBucket(b: String) = S3Error("NoSuchBucket", s"The specified bucket does not exist: $b", Status.NotFound)
  def noSuchKey(k: String) = S3Error("NoSuchKey", s"The specified key does not exist: $k", Status.NotFound)
  val noSuchUpload = S3Error("NoSuchUpload", "The specified multipart upload does not exist.", Status.NotFound)
  val bucketExists = S3Error("BucketAlreadyExists", "The requested bucket name is not available.", Status.Conflict)
  val bucketOwned = S3Error("BucketAlreadyOwnedByYou", "Your previous request to create the named bucket succeeded.", Status.Conflict)
  val bucketNotEmpty = S3Error("BucketNotEmpty", "The bucket you tried to delete is not empty.", Status.Conflict)
  val invalidBucketName = S3Error("InvalidBucketName", "The specified bucket is not valid.", Status.BadRequest)
  val accessDenied = S3Error("AccessDenied", "Access Denied", Status.Forbidden)
  val invalidAccessKey = S3Error("InvalidAccessKeyId", "The AWS access key ID you provided does not exist in our records.", Status.Forbidden)
  val signatureMismatch =
    S3Error("SignatureDoesNotMatch", "The request signature we calculated does not match the signature you provided.", Status.Forbidden)
  val timeSkewed = S3Error("RequestTimeTooSkewed", "The difference between the request time and the server's time is too large.", Status.Forbidden)
  val expired = S3Error("AccessDenied", "Request has expired", Status.Forbidden)
  val invalidRange = S3Error("InvalidRange", "The requested range is not satisfiable", Status.RangeNotSatisfiable)
  val preconditionFailed = S3Error("PreconditionFailed", "At least one of the preconditions you specified did not hold.", Status.PreconditionFailed)
  val notModified = S3Error("NotModified", "Not Modified", Status.NotModified)
  val invalidPart = S3Error("InvalidPart", "One or more of the specified parts could not be found.", Status.BadRequest)
  val invalidPartOrder = S3Error("InvalidPartOrder", "The list of parts was not in ascending order.", Status.BadRequest)
  val entityTooSmall = S3Error("EntityTooSmall", "Your proposed upload is smaller than the minimum allowed object size.", Status.BadRequest)
  val malformedXml = S3Error("MalformedXML", "The XML you provided was not well-formed.", Status.BadRequest)
  val badDigest = S3Error("BadDigest", "The Content-MD5 or checksum you specified did not match what we received.", Status.BadRequest)
  val sha256Mismatch =
    S3Error("XAmzContentSHA256Mismatch", "The provided 'x-amz-content-sha256' header does not match what was computed.", Status.BadRequest)
  val keyTooLong = S3Error("KeyTooLongError", "Your key is too long.", Status.BadRequest)
  def invalidArgument(msg: String) = S3Error("InvalidArgument", msg, Status.BadRequest)
  def invalidRequest(msg: String) = S3Error("InvalidRequest", msg, Status.BadRequest)
  def notImplemented(what: String) = S3Error("NotImplemented", s"$what is not implemented.", Status.NotImplemented)
  val methodNotAllowed = S3Error("MethodNotAllowed", "The specified method is not allowed against this resource.", Status.MethodNotAllowed)
  val internal = S3Error("InternalError", "We encountered an internal error. Please try again.", Status.InternalServerError)
  val unavailable = S3Error("ServiceUnavailable", "Please reduce your request rate.", Status.ServiceUnavailable)
  val noSuchVersion = S3Error("NoSuchVersion", "The specified version does not exist.", Status.NotFound)
  def notFoundConfig(code: String, what: String) = S3Error(code, s"The $what does not exist", Status.NotFound)
  val sseNotConfigured = S3Error("InvalidArgument", "Server-side encryption with Amazon S3 managed keys is not configured on this server.", Status.BadRequest)
  val sseCRequired = S3Error(
    "InvalidRequest",
    "The object was stored using a form of Server Side Encryption. The correct parameters must be provided to retrieve the object.",
    Status.BadRequest
  )
  val sseCKeyMismatch = S3Error("AccessDenied", "The provided customer key does not match the key used to encrypt the object.", Status.Forbidden)
  val incompleteBody = S3Error("IncompleteBody", "You did not provide the number of bytes specified by the Content-Length HTTP header.", Status.BadRequest)

/** 応答用の簡単な XML 生成（S3 の応答は浅く固定的なので、文字列で組み立てる）。 */
final class Xml:
  private val sb = new StringBuilder("""<?xml version="1.0" encoding="UTF-8"?>""")

  def open(tag: String, attrs: String = ""): this.type =
    sb.append('<').append(tag).append(attrs).append('>'); this
  def close(tag: String): this.type =
    sb.append("</").append(tag).append('>'); this
  def leaf(tag: String, value: String): this.type =
    open(tag); sb.append(Xml.escape(value)); close(tag)
  def elem(tag: String)(body: => Any): this.type =
    open(tag); body; close(tag)
  /** 所有者（ID と DisplayName）。 */
  def owner(id: String, name: String): this.type =
    elem("Owner") { leaf("ID", id); leaf("DisplayName", name) }
  def result: String = sb.toString

object Xml:
  val Ns = """ xmlns="http://s3.amazonaws.com/doc/2006-03-01/""""

  def escape(s: String): String =
    val b = new StringBuilder(s.length + 8)
    s.foreach {
      case '<'  => b.append("&lt;")
      case '>'  => b.append("&gt;")
      case '&'  => b.append("&amp;")
      case '"'  => b.append("&quot;")
      case '\'' => b.append("&apos;")
      case c if c < 0x20 && c != '\t' && c != '\n' && c != '\r' => b.append(f"&#x${c.toInt}%x;")
      case c    => b.append(c)
    }
    b.toString

  /** リクエストの XML を読む（DTD と外部実体は無効にする）。 */
  def parse(s: String): Either[S3Error, scala.xml.Elem] =
    try
      val f = javax.xml.parsers.SAXParserFactory.newInstance()
      f.setNamespaceAware(false)
      f.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true)
      f.setFeature("http://xml.org/sax/features/external-general-entities", false)
      f.setFeature("http://xml.org/sax/features/external-parameter-entities", false)
      Right(scala.xml.XML.withSAXParser(f.newSAXParser()).loadString(s))
    catch case _: Exception => Left(S3Error.malformedXml)
