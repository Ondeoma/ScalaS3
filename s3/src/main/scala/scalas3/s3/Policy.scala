package scalas3.s3

import com.comcast.ip4s.IpAddress
import io.circe.{Json, parser}

/** バケットポリシー（AWS のポリシー JSON のうち、よく使う部分）。
  *   - Effect: Allow / Deny
  *   - Principal: "*" か {"AWS": [アクセスキー ID または "*"]}
  *   - Action: "s3:GetObject" など（* を含められる）。NotAction は未対応
  *   - Resource: "arn:aws:s3:::bucket" / "arn:aws:s3:::bucket/prefix*"
  *   - Condition: StringEquals / StringNotEquals / StringLike / IpAddress / NotIpAddress / Bool
  *     （キーは aws:SourceIp、aws:SecureTransport、s3:prefix、s3:x-amz-server-side-encryption など）
  */
final case class Statement(
    allow: Boolean,
    principals: Option[List[String]], // None = すべて（"*"）
    actions: List[String],
    resources: List[String],
    conditions: List[(String, String, List[String])] // (演算子, キー, 値)
)

enum Decision:
  case Allow, Deny, NoOpinion

/** 認可の判断に使う、リクエストの情報。 */
final case class PolicyContext(principal: Option[String], action: String, resource: String, values: Map[String, String])

final case class Policy(statements: List[Statement]):

  def evaluate(ctx: PolicyContext): Decision =
    val hits = statements.filter(s => applies(s, ctx))
    if hits.exists(!_.allow) then Decision.Deny
    else if hits.exists(_.allow) then Decision.Allow
    else Decision.NoOpinion

  private def applies(s: Statement, ctx: PolicyContext): Boolean =
    s.principals.forall(ps => ps.contains("*") || ctx.principal.exists(ps.contains)) &&
      s.actions.exists(a => Policy.glob(a, ctx.action)) &&
      s.resources.exists(r => Policy.glob(r, ctx.resource)) &&
      s.conditions.forall((op, key, vals) => Policy.condition(op, ctx.values.get(key.toLowerCase), vals))

object Policy:

  /** * と ? のワイルドカード（大文字小文字を区別しない）。要求ごとに何度も呼ぶので、正規表現は作らずに照合する
    * （* は改行も含めて何にでも一致する）。questionMark が false なら ? はただの文字（CORS 用）。
    */
  def glob(pattern: String, s: String, questionMark: Boolean = true): Boolean =
    val p = pattern.toLowerCase
    val t = s.toLowerCase
    var pi = 0
    var ti = 0
    var star = -1 // 最後に見た * の位置
    var mark = 0 // その * が飲み込んだところまで
    var ok = true
    while ok && ti < t.length do
      // パターンの * は、照合する側に * があっても必ずワイルドカードとして扱う（先に見ないと、ただの文字として
      // 一致させてしまい、"b/*" が "b/*x" に一致しなくなる）
      if pi < p.length && p(pi) == '*' then
        star = pi; mark = ti; pi += 1
      else if pi < p.length && ((questionMark && p(pi) == '?') || p(pi) == t(ti)) then
        pi += 1; ti += 1
      else if star >= 0 then
        // * にもう 1 文字飲み込ませてやり直す
        pi = star + 1; mark += 1; ti = mark
      else ok = false
    while ok && pi < p.length && p(pi) == '*' do pi += 1
    ok && pi == p.length

  /** IP アドレスのリテラルだけを読む（InetAddress.getByName と違い、ホスト名を DNS で引かない）。 */
  private def ipBytes(s: String): Option[Array[Byte]] = IpAddress.fromString(s.trim).map(_.collapseMappedV4.toBytes)

  private def ipIn(ip: String, cidr: String): Boolean =
    val parsed = cidr.split('/') match
      case Array(n, b) => b.trim.toIntOption.map(n -> Some(_))
      case Array(n)    => Some(n -> None)
      case _           => None
    (for
      (net, bits) <- parsed
      a <- ipBytes(ip)
      b <- ipBytes(net)
      if a.length == b.length
      prefix = bits.getOrElse(a.length * 8)
      if prefix >= 0 && prefix <= a.length * 8
    yield
      val full = prefix / 8
      val rest = prefix % 8
      (0 until full).forall(i => a(i) == b(i)) &&
      (rest == 0 || ((a(full) ^ b(full)) & (0xff << (8 - rest)) & 0xff) == 0)
    ).getOrElse(false)

  def condition(op: String, value: Option[String], expected: List[String]): Boolean =
    op match
      case "StringEquals"    => value.exists(expected.contains)
      case "StringNotEquals" => !value.exists(expected.contains)
      case "StringLike"      => value.exists(v => expected.exists(glob(_, v)))
      case "StringNotLike"   => !value.exists(v => expected.exists(glob(_, v)))
      case "IpAddress"       => value.exists(v => expected.exists(ipIn(v, _)))
      case "NotIpAddress"    => !value.exists(v => expected.exists(ipIn(v, _)))
      case "Bool"            => value.exists(v => expected.exists(_.equalsIgnoreCase(v)))
      case _                 => false // 知らない演算子は一致しない（Allow なら効かない、Deny でも効かない）

  private def strings(j: Json): List[String] =
    j.asString.map(List(_)).orElse(j.asArray.map(_.toList.flatMap(_.asString))).getOrElse(Nil)

  def parse(text: String): Either[S3Error, Policy] =
    val bad = S3Error("MalformedPolicy", "Policies must be valid JSON and the first byte must be '{'", org.http4s.Status.BadRequest)
    parser.parse(text).left.map(_ => bad).flatMap { j =>
      val stmts = j.hcursor.downField("Statement").focus.toList.flatMap(s => s.asArray.map(_.toList).getOrElse(List(s)))
      val parsed = stmts.map { s =>
        val c = s.hcursor
        for
          effect <- c.get[String]("Effect").toOption.filter(e => e == "Allow" || e == "Deny")
          // None = すべての主体（"*"）
          principal <- c.downField("Principal").focus.map { p =>
            if p.asString.contains("*") then None
            else
              val l = p.hcursor.downField("AWS").focus.map(strings).getOrElse(Nil)
              if l.contains("*") then None else Some(l)
          }
          actions = c.downField("Action").focus.map(strings).getOrElse(Nil)
          resources = c.downField("Resource").focus.map(strings).getOrElse(Nil)
          if actions.nonEmpty && resources.nonEmpty
          conditions = c.downField("Condition").focus.flatMap(_.asObject).toList.flatMap { ops =>
            ops.toList.flatMap((op, body) => body.asObject.toList.flatMap(_.toList.map((k, v) => (op, k, strings(v)))))
          }
        yield Statement(effect == "Allow", principal, actions, resources, conditions)
      }
      if stmts.isEmpty || parsed.exists(_.isEmpty) then Left(bad) else Right(Policy(parsed.flatten))
    }

  /** 操作名（s3:GetObject など）。 */
  def action(method: String, hasKey: Boolean, q: Map[String, String]): String =
    def has(k: String) = q.contains(k)
    (method, hasKey) match
      case ("GET" | "HEAD", true) if has("uploadId") => "s3:ListMultipartUploadParts"
      case ("GET" | "HEAD", true) if has("versionId") => "s3:GetObjectVersion"
      case ("GET" | "HEAD", true)                    => "s3:GetObject"
      case ("PUT" | "POST", true)                    => "s3:PutObject"
      case ("DELETE", true) if has("uploadId")       => "s3:AbortMultipartUpload"
      case ("DELETE", true) if has("versionId")      => "s3:DeleteObjectVersion"
      case ("DELETE", true)                          => "s3:DeleteObject"
      case ("GET", false) if has("location")         => "s3:GetBucketLocation"
      case ("GET", false) if has("uploads")          => "s3:ListBucketMultipartUploads"
      case ("GET", false) if has("versions")         => "s3:ListBucketVersions"
      case ("GET", false) if has("policy")           => "s3:GetBucketPolicy"
      case ("PUT", false) if has("policy")           => "s3:PutBucketPolicy"
      case ("DELETE", false) if has("policy")        => "s3:DeleteBucketPolicy"
      case ("GET", false) if has("cors")             => "s3:GetBucketCORS"
      case ("PUT" | "DELETE", false) if has("cors")  => "s3:PutBucketCORS"
      case ("GET", false) if has("versioning")       => "s3:GetBucketVersioning"
      case ("PUT", false) if has("versioning")       => "s3:PutBucketVersioning"
      case ("POST", false) if has("delete")          => "s3:DeleteObject"
      case ("GET" | "HEAD", false)                   => "s3:ListBucket"
      case ("DELETE", false)                         => "s3:DeleteBucket"
      case (_, false)                                => "s3:PutBucketConfiguration"
      case _                                         => "s3:Unknown"
