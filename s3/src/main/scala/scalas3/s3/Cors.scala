package scalas3.s3

/** バケットの CORS 設定（PutBucketCors の XML）。 */
final case class CorsRule(
    origins: List[String],
    methods: List[String],
    headers: List[String],
    expose: List[String],
    maxAge: Option[Int]
):
  /** オリジンとヘッダーの名前は * を含められる（大文字小文字を区別しない）。 */
  def matches(origin: String, method: String, reqHeaders: List[String]): Boolean =
    origins.exists(Policy.glob(_, origin, questionMark = false)) && methods.exists(_.equalsIgnoreCase(method)) &&
      reqHeaders.forall(h => headers.exists(Policy.glob(_, h, questionMark = false)))

object Cors:
  def parse(xml: String): Either[S3Error, List[CorsRule]] =
    Xml.parse(xml).flatMap { doc =>
      val rules = (doc \ "CORSRule").toList.map { r =>
        CorsRule(
          (r \ "AllowedOrigin").map(_.text.trim).toList,
          (r \ "AllowedMethod").map(_.text.trim.toUpperCase).toList,
          (r \ "AllowedHeader").map(_.text.trim.toLowerCase).toList,
          (r \ "ExposeHeader").map(_.text.trim).toList,
          (r \ "MaxAgeSeconds").headOption.flatMap(_.text.trim.toIntOption)
        )
      }
      val valid = Set("GET", "PUT", "POST", "DELETE", "HEAD")
      if rules.isEmpty || rules.exists(r => r.origins.isEmpty || r.methods.isEmpty || !r.methods.forall(valid)) then Left(S3Error.malformedXml)
      else Right(rules)
    }

  /** 一致した最初のルール。 */
  def find(rules: List[CorsRule], origin: String, method: String, reqHeaders: List[String]): Option[CorsRule] =
    rules.find(_.matches(origin, method, reqHeaders.map(_.trim.toLowerCase).filter(_.nonEmpty)))
