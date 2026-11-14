package scalas3.s3

import cats.effect.{Deferred, IO}
import cats.syntax.all.*
import fs2.Stream
import org.http4s.*
import org.http4s.headers.`Content-Length`
import org.typelevel.ci.CIString
import scodec.bits.ByteVector

import java.nio.charset.StandardCharsets.UTF_8
import java.security.MessageDigest
import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneOffset, ZonedDateTime}
import java.util.{Base64, HexFormat, Locale}
import java.util.concurrent.ThreadLocalRandom
import scala.concurrent.duration.*

final case class S3Config(
    region: String = "us-east-1",
    /** 仮想ホスト形式のドメイン（例: s3.example.com なら bucket.s3.example.com）。 */
    domain: Option[String] = None,
    maxClockSkew: FiniteDuration = 15.minutes,
    blockSize: Int = 4 * 1024 * 1024,
    inlineThreshold: Int = 4096,
    minPartSize: Long = 5L * 1024 * 1024,
    /** ストレージクラス名（x-amz-storage-class）→ 冗長化の方式。無い名前は STANDARD 扱い。 */
    storageClasses: Map[String, Redundancy] = Map("STANDARD" -> Redundancy.Replicated),
    /** SSE-S3 とアクセスキーの秘密鍵の暗号化に使うマスター鍵（32 バイト）。 */
    masterKey: Option[Array[Byte]] = None,
    /** バケットとアクセスキーをキャッシュする時間（他のノードでの変更は、この時間だけ遅れて見える）。 */
    metadataCacheTtl: FiniteDuration = 1.second,
    /** 前段のリバースプロキシや CDN。None なら接続の相手をそのまま送信元とする（ClientAddress を参照）。 */
    trustedProxies: Option[TrustedProxies] = None
):
  def redundancyOf(storageClass: String): Redundancy =
    storageClasses.getOrElse(storageClass, storageClasses.getOrElse("STANDARD", Redundancy.Replicated))

/** S3 API の HTTP ハンドラー。 */
final class S3Service(store: ObjectStore, data: DataPath, config: S3Config):
  import S3Service.*

  private val policies = ParseCache(Policy.parse(_).toOption)
  private val corsConfigs = ParseCache(Cors.parse(_).toOption)

  val app: HttpApp[IO] = HttpApp { req =>
    val rid = requestId()
    IO.defer(parse(req, rid)).flatMap { ctx =>
      handle(ctx).handleErrorWith(e => IO.pure(errorResponse(e, ctx.decodedPath, rid, req.method))).flatMap(cors(ctx, _))
    }.handleErrorWith(e => IO.pure(errorResponse(e, req.uri.path.renderString, rid, req.method)))
      .map(_.putHeaders(hdr("x-amz-request-id", rid), hdr("Server", "scalas3")))
  }

  // ======================= 解析と認証 =======================

  private def parse(req: Request[IO], rid: String): IO[Ctx] = IO {
    val decodedPath = SigV4.percentDecode(req.uri.path.renderString)
    val query = SigV4.parseRawQuery(req.uri.query.renderString)
    val hostHeader = header(req, "host").map(_.split(':').head.toLowerCase)
    val vhostBucket = for
      d <- config.domain
      h <- hostHeader
      if h.endsWith("." + d)
    yield h.dropRight(d.length + 1)
    val (bucket, key) = vhostBucket match
      case Some(b) => (Some(b), Option(decodedPath.drop(1)).filter(_.nonEmpty))
      case None =>
        val p = decodedPath.drop(1)
        p.indexOf('/') match
          case -1 => (Option(p).filter(_.nonEmpty), None)
          case i  => (Some(p.take(i)), Option(p.drop(i + 1)).filter(_.nonEmpty))
    Ctx(req, rid, decodedPath, query, query.toMap, bucket, key)
  }

  /** 署名を検証する。署名が無ければ匿名（バケットポリシーで許されたときだけ通る）。 */
  private def authenticate(ctx: Ctx): IO[Authed] =
    val req = ctx.req
    val headerPayloadHash = header(req, "x-amz-content-sha256").getOrElse(SigV4.UnsignedPayload)
    val parsed: Option[Either[S3Error, SigV4.Auth]] =
      if ctx.q.contains("X-Amz-Signature") then Some(SigV4.parseQuery(ctx.q))
      else header(req, "authorization").map(a => SigV4.parseHeader(a, header(req, "x-amz-date").orElse(header(req, "date"))))
    parsed match
      case None => IO.pure(Authed(None, None, headerPayloadHash))
      case Some(p) =>
        IO.fromEither(p).flatMap { auth =>
          store.getAccessKey(auth.accessKey).flatMap {
            case None => IO.raiseError(S3Error.invalidAccessKey)
            case Some(key) =>
              IO.realTime.flatMap { now =>
                val skew = config.maxClockSkew.toMillis
                val timeOk = SigV4.parseAmzDate(auth.amzDate).orElse(parseHttpDate(auth.amzDate)) match
                  case None => Left(S3Error.accessDenied)
                  case Some(t) if auth.presigned =>
                    if now.toMillis > t + auth.expiresSeconds.getOrElse(0L) * 1000 then Left(S3Error.expired)
                    else if t - now.toMillis > skew then Left(S3Error.timeSkewed)
                    else Right(())
                  case Some(t) => if math.abs(now.toMillis - t) > skew then Left(S3Error.timeSkewed) else Right(())
                val payloadHash =
                  if auth.presigned then ctx.q.getOrElse("X-Amz-Content-Sha256", SigV4.UnsignedPayload) else headerPayloadHash
                val headerValues = (name: String) => req.headers.get(CIString(name)).map(_.toList.map(_.value)).getOrElse(Nil)
                val canonical = SigV4.canonicalRequest(req.method.name, ctx.decodedPath, ctx.query, headerValues, auth.signedHeaders, payloadHash)
                val signingKey = SigV4.signingKey(key.secret, auth.scope)
                val expected = SigV4.sign(signingKey, SigV4.stringToSign(auth.amzDate, auth.scope, canonical))
                IO.fromEither(timeOk) >>
                  IO.raiseUnless(SigV4.safeEquals(expected, auth.signature))(S3Error.signatureMismatch).as(
                    Authed(Some(key), Some(AwsChunked.Signing(signingKey, auth.amzDate, auth.scope, auth.signature)), payloadHash)
                  )
              }
          }
        }

  /** 署名方式に応じて本文を元に戻す。2 番目はトレーラー（完了後に得られる）。 */
  private def bodyOf(ctx: Ctx, a: Authed): IO[(Stream[IO, Byte], IO[Map[String, String]])] =
    a.payloadHash match
      case SigV4.StreamingSigned | SigV4.StreamingSignedTrailer => chunked(ctx, a.signing)
      case SigV4.StreamingUnsignedTrailer                       => chunked(ctx, None)
      case _                                                    => IO.pure((ctx.req.body, IO.pure(Map.empty)))

  private def chunked(ctx: Ctx, signing: Option[AwsChunked.Signing]): IO[(Stream[IO, Byte], IO[Map[String, String]])] =
    Deferred[IO, Map[String, String]].map(trailers => (ctx.req.body.through(AwsChunked.decode(signing, trailers)), trailers.get))

  /** メタデータをコミットする直前に呼ぶ。更新時刻を進め（GC との競合を防ぐ。spec/BlockGc.tla）、長くかかったアップロードは
    * ブロックが残っているかも確かめる。欠けていれば、壊れたオブジェクトを作らずに失敗させる（クライアントがやり直す）。
    */
  private def beforeCommit(ing: Ingested): IO[Unit] =
    data.touch(ing.body) >>
      data.missing(ing.body).flatMap { lost =>
        IO.raiseWhen(lost.nonEmpty)(S3Error.internal)
      }.whenA(ing.longUpload)

  /** 取り込んだ本文を参照するメタデータを書く。書く直前に beforeCommit を行い、どうなっても GC からの保護は外す。 */
  private def commit[A](ing: Ingested)(write: IO[A]): IO[A] =
    (beforeCommit(ing) >> write).guarantee(data.release(ing.body))

  /** 既存のブロックを新しいレコードでも参照する（書くまでの間 GC から守る）。 */
  private def share[A](body: Body)(write: IO[A]): IO[A] =
    (data.protect(body) >> write).guarantee(data.release(body))

  /** 本文を取り込み、長さ・SHA-256・Content-MD5・追加チェックサムを検証する。 */
  private def ingest(ctx: Ctx, a: Authed, storageClass: String, key: Option[Array[Byte]]): IO[Ingested] =
    val req = ctx.req
    val declaredChecksum = Checksummer.Algorithms.find(alg => header(req, s"x-amz-checksum-$alg").isDefined)
    val trailerChecksum = header(req, "x-amz-trailer").map(_.trim.toLowerCase).filter(_.startsWith("x-amz-checksum-")).map(_.stripPrefix("x-amz-checksum-"))
    val alg = declaredChecksum.orElse(trailerChecksum).filter(Checksummer.supported)
    val wantSha = a.payloadHash.length == 64
    for
      (body, trailers) <- bodyOf(ctx, a)
      ing <- data.ingest(body, wantSha, alg, config.redundancyOf(storageClass), key)
      tr <- trailers
      expectedLen = header(req, "x-amz-decoded-content-length").orElse(
        if a.payloadHash.startsWith("STREAMING-") then None else header(req, "content-length")
      ).flatMap(_.toLongOption)
      check = for
        _ <- Either.cond(expectedLen.forall(_ == ing.size), (), S3Error.incompleteBody)
        _ <- Either.cond(!wantSha || ing.sha256.contains(a.payloadHash.toLowerCase), (), S3Error.sha256Mismatch)
        _ <- header(req, "content-md5").fold(Right(()))(m =>
          Either.cond(m.trim == Base64.getEncoder.encodeToString(ing.md5.toArray), (), S3Error.badDigest)
        )
        _ <- alg.fold(Right(())) { al =>
          val expected = header(req, s"x-amz-checksum-$al").orElse(tr.get(s"x-amz-checksum-$al"))
          Either.cond(expected.forall(e => ing.checksum.exists(_._2 == e.trim)), (), S3Error.badDigest)
        }
      yield ()
      _ <- check.fold(e => data.release(ing.body) >> IO.raiseError(e), _ => IO.unit)
    yield ing

  // ======================= 暗号化 =======================

  /** SSE-C の鍵（ヘッダーが無ければ None）。 */
  private def customerKey(req: Request[IO], prefix: String): Either[S3Error, Option[(Array[Byte], String)]] =
    (header(req, prefix + "algorithm"), header(req, prefix + "key"), header(req, prefix + "key-md5")) match
      case (None, None, None) => Right(None)
      case (Some("AES256"), Some(k), Some(md5)) =>
        scala.util.Try(Base64.getDecoder.decode(k.trim)).toOption.filter(_.length == 32) match
          case None => Left(S3Error.invalidArgument("The secret key was invalid for the specified algorithm."))
          case Some(key) =>
            if Crypto.md5Base64(key) != md5.trim then Left(S3Error.invalidArgument("The calculated MD5 hash of the key did not match the hash that was provided."))
            else Right(Some((key, md5.trim)))
      case _ => Left(S3Error.invalidArgument("Requests specifying Server Side Encryption with Customer provided keys must provide the algorithm, key and key MD5."))

  private def master: IO[Array[Byte]] = IO.fromOption(config.masterKey)(S3Error.sseNotConfigured)

  /** 新しく書くデータの暗号化（ヘッダー、なければバケットの既定）。 */
  private def newEncryption(req: Request[IO], b: Bucket): IO[(Option[Encryption], Option[Array[Byte]])] =
    IO.fromEither(customerKey(req, SseC)).flatMap {
      case Some((key, md5)) => IO.pure((Some(Encryption.SseC(md5)), Some(key)))
      case None =>
        header(req, "x-amz-server-side-encryption").orElse(b.config.get("encryption")) match
          case None => IO.pure((None, None))
          case Some("AES256") =>
            master.map { m =>
              val dk = Crypto.randomKey()
              (Some(Encryption.SseS3(Crypto.wrap(m, dk))), Some(dk))
            }
          case Some(other) => IO.raiseError(S3Error.notImplemented(s"Server-side encryption $other"))
    }

  /** 暗号化されたデータを読むための鍵。SSE-C の鍵は prefix のヘッダー（コピー元なら CopySseC）から読む。 */
  private def readKey(req: Request[IO], enc: Option[Encryption], prefix: String = SseC): IO[Option[Array[Byte]]] =
    enc match
      case None                      => IO.pure(None)
      case Some(Encryption.SseS3(w)) => master.map(m => Some(Crypto.unwrap(m, w)))
      case Some(Encryption.SseC(md5)) =>
        IO.fromEither(customerKey(req, prefix)).flatMap {
          case None                                     => IO.raiseError(S3Error.sseCRequired)
          case Some((key, provided)) if provided == md5 => IO.pure(Some(key))
          case Some(_)                                  => IO.raiseError(S3Error.sseCKeyMismatch)
        }

  private def encHeaders(enc: Option[Encryption]): List[Header.Raw] = enc match
    case Some(Encryption.SseS3(_))  => List(hdr("x-amz-server-side-encryption", "AES256"))
    case Some(Encryption.SseC(md5)) => List(hdr(SseC + "algorithm", "AES256"), hdr(SseC + "key-MD5", md5))
    case None                       => Nil

  private def versionHeader(b: Bucket, o: ObjectRecord): List[Header.Raw] =
    if b.versioned || o.versionId != "null" then List(hdr("x-amz-version-id", o.versionId)) else Nil

  // ======================= 認可 =======================

  /** キーの権限とバケットポリシーで認可する。ポリシーの明示的な Deny は管理者以外に優先する。 */
  private def need(ctx: Ctx, a: Authed, b: Bucket, p: Perm, resourceKey: Option[String] = None): IO[Unit] =
    val key = resourceKey.orElse(ctx.key)
    val action = Policy.action(ctx.req.method.name, key.isDefined, ctx.q)
    val resource = "arn:aws:s3:::" + b.name + key.fold("")("/" + _)
    val client = ClientAddress.resolve(ctx.req, config.trustedProxies)
    val values = Map(
      // 送信元が分からない（プロキシが unknown などを伝えた）ときは空。IP の条件はどれにも当てはまらない
      "aws:sourceip" -> client.ip.fold("")(_.toString),
      "aws:securetransport" -> client.secure.toString,
      "s3:prefix" -> ctx.q.getOrElse("prefix", ""),
      "s3:x-amz-server-side-encryption" -> header(ctx.req, "x-amz-server-side-encryption").getOrElse("")
    )
    val decision = b.config.get("policy").flatMap(policies(_)).fold(Decision.NoOpinion)(
      _.evaluate(PolicyContext(a.key.map(_.id), action, resource, values))
    )
    val admin = a.key.exists(_.admin)
    val byKey = a.key.exists(k => k.can(b.name, p) || b.owner == k.id)
    decision match
      case Decision.Deny if !admin => IO.raiseError(S3Error.accessDenied)
      case Decision.Allow          => IO.unit
      case _                       => IO.raiseUnless(byKey)(S3Error.accessDenied)

  private def requireKey(a: Authed): IO[AccessKey] = IO.fromOption(a.key)(S3Error.accessDenied)

  // ======================= CORS =======================

  private def corsRules(b: Bucket): Option[List[CorsRule]] = b.config.get("cors").flatMap(corsConfigs(_))

  /** 応答に CORS のヘッダーを付ける（Origin があり、バケットのルールに一致したとき）。 */
  private def cors(ctx: Ctx, resp: Response[IO]): IO[Response[IO]] =
    (header(ctx.req, "origin"), ctx.bucket) match
      case (Some(origin), Some(name)) if ctx.req.method != Method.OPTIONS =>
        store.getBucket(name).attempt.map { b =>
          b.toOption.flatten.flatMap(corsRules).flatMap(Cors.find(_, origin, ctx.req.method.name, Nil))
            .fold(resp)(r => resp.putHeaders(corsHeaders(r, origin)))
        }
      case _ => IO.pure(resp)

  private def corsHeaders(r: CorsRule, origin: String): List[Header.Raw] =
    List(
      hdr("Access-Control-Allow-Origin", if r.origins.contains("*") then "*" else origin),
      hdr("Vary", "Origin")
    ) ++ Option.when(r.expose.nonEmpty)(hdr("Access-Control-Expose-Headers", r.expose.mkString(", ")))

  /** プリフライト（OPTIONS）。認証は要らない。 */
  private def preflight(ctx: Ctx): IO[Response[IO]] =
    val denied = S3Error("AccessForbidden", "CORSResponse: This CORS request is not allowed.", Status.Forbidden)
    (ctx.bucket, header(ctx.req, "origin"), header(ctx.req, "access-control-request-method")) match
      case (Some(name), Some(origin), Some(method)) =>
        withBucket(name) { b =>
          val reqHeaders = header(ctx.req, "access-control-request-headers").toList.flatMap(_.split(',').toList)
          corsRules(b).flatMap(Cors.find(_, origin, method, reqHeaders)) match
            case None => IO.raiseError(denied)
            case Some(r) =>
              val allow = List(
                hdr("Access-Control-Allow-Methods", r.methods.mkString(", ")),
                hdr("Access-Control-Allow-Headers", reqHeaders.map(_.trim).filter(_.nonEmpty).mkString(", "))
              ) ++ r.maxAge.map(m => hdr("Access-Control-Max-Age", m.toString))
              IO.pure(Response[IO](Status.Ok).putHeaders(corsHeaders(r, origin) ++ allow))
        }
      case _ => IO.raiseError(denied)

  // ======================= 振り分け =======================

  private def handle(ctx: Ctx): IO[Response[IO]] =
    val m = ctx.req.method
    if m == Method.OPTIONS then preflight(ctx)
    else
      authenticate(ctx).flatMap { a =>
        (ctx.bucket, ctx.key) match
          case (None, _) if m == Method.GET => listBuckets(a)
          case (None, _)                    => IO.raiseError(S3Error.methodNotAllowed)
          case (Some(b), None)              => bucketOp(ctx, a, b)
          case (Some(b), Some(k)) =>
            if k.getBytes(UTF_8).length > 1024 then IO.raiseError(S3Error.keyTooLong) else objectOp(ctx, a, b, k)
      }

  private def bucketOp(ctx: Ctx, a: Authed, name: String): IO[Response[IO]] =
    val m = ctx.req.method
    val q = ctx.q
    if m == Method.PUT && q.isEmpty then createBucket(a, name)
    else
      withBucket(name) { b =>
        def owner = need(ctx, a, b, Perm.Owner)
        def read = need(ctx, a, b, Perm.Read)
        val configKind = configKinds.keys.find(q.contains)
        m match
          case Method.HEAD                                    => read.as(Response[IO](Status.Ok))
          case Method.DELETE if q.isEmpty                     => owner >> store.deleteBucket(b).as(noContent)
          case Method.GET if q.contains("location")           => read.as(location)
          case Method.GET if q.contains("versioning")         => read.as(getVersioning(b))
          case Method.PUT if q.contains("versioning")         => owner >> putVersioning(ctx, a, b)
          case Method.GET if q.contains("versions")           => read >> listVersions(ctx, b)
          case Method.GET if q.contains("uploads")            => read >> listUploads(ctx, b)
          case Method.GET if q.contains("acl")                => read.as(acl(b))
          case _ if configKind.isDefined                      => owner >> bucketConfig(ctx, a, b, configKind.get)
          case Method.GET if q.get("list-type").contains("2") => read >> listObjects(ctx, b, v2 = true)
          case Method.GET if subresource(q).isEmpty           => read >> listObjects(ctx, b, v2 = false)
          case Method.POST if q.contains("delete")            => need(ctx, a, b, Perm.Write) >> deleteObjects(ctx, a, b)
          case _ => IO.raiseError(S3Error.notImplemented(subresource(q).headOption.getOrElse(m.name)))
      }

  private def objectOp(ctx: Ctx, a: Authed, name: String, key: String): IO[Response[IO]] =
    val m = ctx.req.method
    val q = ctx.q
    val copySource = header(ctx.req, "x-amz-copy-source")
    withBucket(name) { b =>
      def write = need(ctx, a, b, Perm.Write)
      def read = need(ctx, a, b, Perm.Read)
      m match
        case Method.PUT if q.contains("uploadId") && q.contains("partNumber") =>
          write >> (if copySource.isDefined then uploadPartCopy(ctx, a, b, key) else uploadPart(ctx, a, b, key))
        case Method.PUT if copySource.isDefined                  => write >> copyObject(ctx, a, b, key)
        case Method.PUT if subresource(q).isEmpty                => write >> putObject(ctx, a, b, key)
        case Method.GET if q.contains("uploadId")                => read >> listParts(ctx, b, key)
        case Method.GET if q.contains("acl")                     => read.as(acl(b))
        case Method.GET if subresource(q).isEmpty || q.contains("versionId") => read >> getObject(ctx, b, key, head = false)
        case Method.HEAD                                         => read >> getObject(ctx, b, key, head = true)
        case Method.DELETE if q.contains("uploadId")             => write >> abortUpload(ctx, b, key)
        case Method.DELETE if subresource(q).isEmpty || q.contains("versionId") => write >> deleteObject(ctx, b, key)
        case Method.POST if q.contains("uploads")                => write >> createUpload(ctx, b, key)
        case Method.POST if q.contains("uploadId")               => write >> completeUpload(ctx, a, b, key)
        case _ => IO.raiseError(S3Error.notImplemented(subresource(q).headOption.getOrElse(m.name)))
    }

  private def withBucket[A](name: String)(f: Bucket => IO[A]): IO[A] =
    store.getBucket(name).flatMap {
      case Some(b) => f(b)
      case None    => IO.raiseError(S3Error.noSuchBucket(name))
    }

  // ======================= バケット =======================

  private def listBuckets(a: Authed): IO[Response[IO]] =
    requireKey(a).flatMap { key =>
      store.listBuckets.map { bs =>
        val visible = bs.filter(b => key.admin || b.owner == key.id || key.grants.contains(b.name))
        xmlDoc("ListAllMyBucketsResult") { x =>
          x.owner(key.id, key.name)
          x.elem("Buckets") {
            visible.foreach(b => x.elem("Bucket") { x.leaf("Name", b.name).leaf("CreationDate", iso(b.createdMillis)) })
          }
        }
      }
    }

  private def createBucket(a: Authed, name: String): IO[Response[IO]] =
    requireKey(a).flatMap { key =>
      if !validBucketName(name) then IO.raiseError(S3Error.invalidBucketName)
      else
        IO.realTime.flatMap(now => store.createBucket(name, key, now.toMillis)).as(
          Response[IO](Status.Ok).putHeaders(hdr("Location", "/" + name))
        )
    }

  private def location: Response[IO] =
    val x = Xml()
    if config.region == "us-east-1" then x.open("LocationConstraint", Xml.Ns).close("LocationConstraint")
    else x.leaf("LocationConstraint", config.region)
    xmlResponse(Status.Ok, x)

  private def acl(b: Bucket): Response[IO] =
    xmlDoc("AccessControlPolicy") { x =>
      x.owner(b.owner, b.owner)
      x.elem("AccessControlList") {
        x.elem("Grant") {
          x.open("Grantee", """ xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser"""")
          x.leaf("ID", b.owner).leaf("DisplayName", b.owner).close("Grantee")
          x.leaf("Permission", "FULL_CONTROL")
        }
      }
    }

  private def getVersioning(b: Bucket): Response[IO] =
    xmlDoc("VersioningConfiguration")(x => b.versioning.foreach(x.leaf("Status", _)))

  private def putVersioning(ctx: Ctx, a: Authed, b: Bucket): IO[Response[IO]] =
    readXml(ctx, a).flatMap { doc =>
      (doc \ "Status").text.trim match
        case s @ ("Enabled" | "Suspended") => store.updateBucketConfig(b.name, "versioning", Some(s)).as(Response[IO](Status.Ok))
        case _                             => IO.raiseError(S3Error.malformedXml)
    }

  /** 設定の種類 → (見つからないときのエラーコード, 説明)。 */
  private val configKinds = Map(
    "cors" -> ("NoSuchCORSConfiguration", "CORS configuration"),
    "policy" -> ("NoSuchBucketPolicy", "bucket policy"),
    "encryption" -> ("ServerSideEncryptionConfigurationNotFoundError", "server side encryption configuration"),
    "lifecycle" -> ("NoSuchLifecycleConfiguration", "lifecycle configuration"),
    "replication" -> ("ReplicationConfigurationNotFoundError", "replication configuration")
  )

  /** バケット設定の取得・設定・削除（cors、policy、encryption、lifecycle、replication）。 */
  private def bucketConfig(ctx: Ctx, a: Authed, b: Bucket, kind: String): IO[Response[IO]] =
    val (code, what) = configKinds(kind)
    ctx.req.method match
      case Method.GET =>
        b.config.get(kind) match
          case None => IO.raiseError(S3Error.notFoundConfig(code, what))
          case Some(v) if kind == "encryption" =>
            IO.pure(xmlDoc("ServerSideEncryptionConfiguration") { x =>
              x.elem("Rule")(x.elem("ApplyServerSideEncryptionByDefault")(x.leaf("SSEAlgorithm", v)))
            })
          case Some(v) =>
            // 受け取った文書をそのまま返す
            IO.pure(bytesResponse(Status.Ok, if kind == "policy" then "application/json" else "application/xml", v.getBytes(UTF_8)))
      case Method.DELETE => store.updateBucketConfig(b.name, kind, None).as(noContent)
      case Method.PUT =>
        smallBody(ctx, a).flatMap { text =>
          val value: Either[S3Error, String] = kind match
            case "cors"   => Cors.parse(text).as(text)
            case "policy" => Policy.parse(text).as(text)
            case "encryption" =>
              Xml.parse(text).flatMap { doc =>
                (doc \\ "SSEAlgorithm").text.trim match
                  case "AES256"  => if config.masterKey.isEmpty then Left(S3Error.sseNotConfigured) else Right("AES256")
                  case "aws:kms" => Left(S3Error.notImplemented("SSE-KMS"))
                  case _         => Left(S3Error.malformedXml)
              }
            case "lifecycle"   => Lifecycle.parse(text).as(text)
            case "replication" => ReplicationConfig.parse(text).as(text)
            case _             => Left(S3Error.malformedXml)
          IO.fromEither(value).flatMap(v => store.updateBucketConfig(b.name, kind, Some(v))).as(
            if kind == "policy" then noContent else Response[IO](Status.Ok)
          )
        }
      case _ => IO.raiseError(S3Error.methodNotAllowed)

  private def listObjects(ctx: Ctx, b: Bucket, v2: Boolean): IO[Response[IO]] =
    val q = ctx.q
    val prefix = q.getOrElse("prefix", "")
    val delimiter = q.get("delimiter").filter(_.nonEmpty)
    val maxKeys = limit(q, "max-keys", min = 0)
    val urlEnc = q.get("encoding-type").contains("url")
    def enc(s: String) = if urlEnc then SigV4.uriEncode(s, encodeSlash = false) else s
    val after: Either[S3Error, Option[Marker]] =
      if v2 then
        q.get("continuation-token") match
          case Some(t) => Marker.fromToken(t).toRight(S3Error.invalidArgument("The continuation token provided is incorrect")).map(Some(_))
          case None    => Right(q.get("start-after").filter(_.nonEmpty).map(Marker.Key(_)))
      else Right(q.get("marker").filter(_.nonEmpty).map(Marker.Key(_)))
    IO.fromEither(after).flatMap(m => store.list(b, prefix, delimiter, m, maxKeys)).map { r =>
      xmlDoc("ListBucketResult") { x =>
        x.leaf("Name", b.name).leaf("Prefix", enc(prefix))
        if v2 then
          q.get("continuation-token").foreach(x.leaf("ContinuationToken", _))
          q.get("start-after").foreach(s => x.leaf("StartAfter", enc(s)))
          x.leaf("KeyCount", (r.contents.size + r.commonPrefixes.size).toString)
          r.next.foreach(n => x.leaf("NextContinuationToken", n.token))
        else
          x.leaf("Marker", enc(q.getOrElse("marker", "")))
          r.next.foreach {
            case Marker.Key(k)    => x.leaf("NextMarker", enc(k))
            case Marker.Prefix(p) => x.leaf("NextMarker", enc(p))
          }
        x.leaf("MaxKeys", maxKeys.toString)
        delimiter.foreach(d => x.leaf("Delimiter", enc(d)))
        if urlEnc then x.leaf("EncodingType", "url")
        x.leaf("IsTruncated", r.truncated.toString)
        val fetchOwner = !v2 || q.get("fetch-owner").contains("true")
        r.contents.foreach { (k, o) =>
          x.elem("Contents") {
            x.leaf("Key", enc(k)).leaf("LastModified", iso(o.lastModified)).leaf("ETag", quote(o.etag)).leaf("Size", o.size.toString)
            x.leaf("StorageClass", o.storageClass)
            if fetchOwner then x.owner(b.owner, b.owner)
          }
        }
        r.commonPrefixes.foreach(p => x.elem("CommonPrefixes")(x.leaf("Prefix", enc(p))))
      }
    }

  private def listVersions(ctx: Ctx, b: Bucket): IO[Response[IO]] =
    val q = ctx.q
    val prefix = q.getOrElse("prefix", "")
    val maxKeys = limit(q, "max-keys", min = 1)
    val km = q.get("key-marker").filter(_.nonEmpty)
    val vm = q.get("version-id-marker").filter(_.nonEmpty)
    store.listVersions(b, prefix, km, vm, maxKeys).map { (vs, truncated) =>
      xmlDoc("ListVersionsResult") { x =>
        x.leaf("Name", b.name).leaf("Prefix", prefix).leaf("KeyMarker", km.getOrElse("")).leaf("VersionIdMarker", vm.getOrElse(""))
        if truncated then vs.lastOption.foreach((k, r, _) => x.leaf("NextKeyMarker", k).leaf("NextVersionIdMarker", r.versionId))
        x.leaf("MaxKeys", maxKeys.toString).leaf("IsTruncated", truncated.toString)
        vs.foreach { (k, r, latest) =>
          x.elem(if r.deleteMarker then "DeleteMarker" else "Version") {
            x.leaf("Key", k).leaf("VersionId", r.versionId).leaf("IsLatest", latest.toString).leaf("LastModified", iso(r.lastModified))
            if !r.deleteMarker then x.leaf("ETag", quote(r.etag)).leaf("Size", r.size.toString).leaf("StorageClass", r.storageClass): Unit
            x.owner(b.owner, b.owner)
          }
        }
      }
    }

  private def deleteObjects(ctx: Ctx, a: Authed, b: Bucket): IO[Response[IO]] =
    readXml(ctx, a).flatMap { doc =>
      val quiet = (doc \ "Quiet").text.trim.equalsIgnoreCase("true")
      val objs = (doc \ "Object").map(o => ((o \ "Key").text, Option((o \ "VersionId").text.trim).filter(_.nonEmpty))).toVector
      if objs.size > 1000 then IO.raiseError(S3Error.malformedXml)
      else
        objs.traverse { (k, vid) =>
          vid.fold(store.deleteObject(b, k))(v => store.deleteVersion(b, k, v)).attempt.map(r => (k, vid, r))
        }.map { results =>
          xmlDoc("DeleteResult") { x =>
            results.foreach {
              case (k, vid, Right(rec)) =>
                if !quiet then
                  x.elem("Deleted") {
                    x.leaf("Key", k)
                    vid.foreach(x.leaf("VersionId", _))
                    rec.filter(_.deleteMarker).foreach { m =>
                      x.leaf("DeleteMarker", "true")
                      if vid.isEmpty then x.leaf("DeleteMarkerVersionId", m.versionId)
                    }
                  }
              case (k, _, Left(e)) =>
                // 失敗はキーごとに返す（一時的な失敗は ServiceUnavailable など、errorResponse と同じ分類にする）
                val err = toS3Error(e, ctx.rid)
                x.elem("Error") { x.leaf("Key", k).leaf("Code", err.code).leaf("Message", err.message) }
            }
          }
        }
    }

  private def listUploads(ctx: Ctx, b: Bucket): IO[Response[IO]] =
    val q = ctx.q
    val prefix = q.getOrElse("prefix", "")
    val max = limit(q, "max-uploads", min = 1)
    store.listUploads(b, prefix, q.get("key-marker").filter(_.nonEmpty), q.get("upload-id-marker").filter(_.nonEmpty), max).map {
      (us, truncated) =>
        xmlDoc("ListMultipartUploadsResult") { x =>
          x.leaf("Bucket", b.name).leaf("KeyMarker", q.getOrElse("key-marker", "")).leaf("UploadIdMarker", q.getOrElse("upload-id-marker", ""))
          if truncated then us.lastOption.foreach(u => x.leaf("NextKeyMarker", u.key).leaf("NextUploadIdMarker", u.uploadId))
          x.leaf("Prefix", prefix).leaf("MaxUploads", max.toString).leaf("IsTruncated", truncated.toString)
          us.foreach { u =>
            x.elem("Upload") {
              x.leaf("Key", u.key).leaf("UploadId", u.uploadId).leaf("Initiated", iso(u.initiated)).leaf("StorageClass", u.storageClass)
            }
          }
        }
    }

  // ======================= オブジェクト =======================

  private def putObject(ctx: Ctx, a: Authed, b: Bucket, key: String): IO[Response[IO]] =
    val req = ctx.req
    for
      (enc, dk) <- newEncryption(req, b)
      sc = header(req, "x-amz-storage-class").getOrElse("STANDARD")
      ing <- ingest(ctx, a, sc, dk)
      now <- IO.realTime.map(_.toMillis)
      rec = ObjectRecord("null", ing.size, ing.md5.toHex, now, storedHeaders(req), userMeta(req), sc, ing.checksum, ing.body, encryption = enc)
      written <- commit(ing)(
        store.putObject(b, key, rec, header(req, "if-none-match").exists(_.trim == "*"), header(req, "if-match").map(unquote))
      )
    yield Response[IO](Status.Ok).putHeaders(etagHeader(written.etag) :: checksumHeaders(written) ++ versionHeader(b, written) ++ encHeaders(enc))

  private def getObject(ctx: Ctx, b: Bucket, key: String, head: Boolean): IO[Response[IO]] =
    val req = ctx.req
    val vid = ctx.q.get("versionId").filter(_.nonEmpty)
    vid.fold(store.getHead(b, key))(v => store.getVersion(b, key, v)).flatMap {
      case None => IO.raiseError(if vid.isDefined then S3Error.noSuchVersion else S3Error.noSuchKey(key))
      case Some(o) if o.deleteMarker =>
        val e = if vid.isDefined then S3Error.methodNotAllowed else S3Error.noSuchKey(key)
        IO.raiseError(e.withHeaders("x-amz-delete-marker" -> "true", "x-amz-version-id" -> o.versionId))
      case Some(o) =>
        for
          _ <- IO.fromEither(checkConditions(req, o))
          range <- IO.fromEither(parseRange(header(req, "range"), o.size))
          dk <- readKey(req, o.encryption)
          repl <-
            if b.config.contains("replication") then
              store.replicationPending(b, key).map(p => List(hdr("x-amz-replication-status", if p then "PENDING" else "COMPLETED")))
            else IO.pure(Nil)
        yield
          val (status, from, until) = range.fold((Status.Ok, 0L, o.size))((f, u) => (Status.PartialContent, f, u))
          val hs = List(
            etagHeader(o.etag),
            hdr("Last-Modified", httpDate(o.lastModified)),
            hdr("Accept-Ranges", "bytes"),
            hdr("Content-Length", (until - from).toString)
          ) ++ range.map((f, u) => hdr("Content-Range", s"bytes $f-${u - 1}/${o.size}")) ++
            responseHeaders(ctx, o) ++
            o.userMeta.map((k, v) => hdr(s"x-amz-meta-$k", v)) ++
            Option.when(o.storageClass != "STANDARD")(hdr("x-amz-storage-class", o.storageClass)) ++
            (if header(req, "x-amz-checksum-mode").exists(_.equalsIgnoreCase("ENABLED")) && range.isEmpty then checksumHeaders(o) else Nil) ++
            versionHeader(b, o) ++ encHeaders(o.encryption) ++ repl
          val resp = Response[IO](status).putHeaders(hs)
          if head then resp
          else resp.withBodyStream(data.read(o.body, from, until, dk)).putHeaders(`Content-Length`.unsafeFromLong(until - from))
    }

  private def deleteObject(ctx: Ctx, b: Bucket, key: String): IO[Response[IO]] =
    ctx.q.get("versionId").filter(_.nonEmpty) match
      case Some(vid) =>
        store.deleteVersion(b, key, vid).map { removed =>
          noContent.putHeaders(hdr("x-amz-version-id", vid) :: removed.filter(_.deleteMarker).toList.map(_ => hdr("x-amz-delete-marker", "true")))
        }
      case None =>
        store.deleteObject(b, key).map {
          case Some(marker) => noContent.putHeaders(hdr("x-amz-delete-marker", "true"), hdr("x-amz-version-id", marker.versionId))
          case None         => noContent
        }

  /** コピー元とコピー先がどちらも暗号化されていなければブロックを共有する。そうでなければ読み直して書く。 */
  private def copyObject(ctx: Ctx, a: Authed, b: Bucket, key: String): IO[Response[IO]] =
    val req = ctx.req
    for
      (src, srcBucket, srcKey) <- copySource(ctx, a)
      _ <- IO.fromEither(copyConditions(req, src))
      replace = header(req, "x-amz-metadata-directive").exists(_.equalsIgnoreCase("REPLACE"))
      srcDk <- readKey(req, src.encryption, CopySseC)
      (enc, dk) <- newEncryption(req, b)
      _ <- IO.raiseWhen(srcBucket.id == b.id && srcKey == key && !replace && header(req, "x-amz-storage-class").isEmpty && enc.isEmpty && src.encryption.isEmpty)(
        S3Error.invalidRequest(
          "This copy request is illegal because it is trying to copy an object to itself without changing the object's metadata, storage class, website redirect location or encryption attributes."
        )
      )
      now <- IO.realTime.map(_.toMillis)
      sc = header(req, "x-amz-storage-class").getOrElse(src.storageClass)
      base = src.copy(
        versionId = "null",
        lastModified = now,
        headers = if replace then storedHeaders(req) else src.headers,
        userMeta = if replace then userMeta(req) else src.userMeta,
        storageClass = sc,
        encryption = enc
      )
      written <-
        if srcDk.isEmpty && dk.isEmpty then share(src.body)(store.putObject(b, key, base, false, None))
        else
          data.ingest(data.read(src.body, 0, src.size, srcDk), wantSha256 = false, None, config.redundancyOf(sc), dk).flatMap { ing =>
            commit(ing)(store.putObject(b, key, base.copy(body = ing.body, checksum = None), false, None))
          }
    yield xmlDoc("CopyObjectResult")(_.leaf("LastModified", iso(written.lastModified)).leaf("ETag", quote(written.etag))).putHeaders(
      versionHeader(b, written) ++ encHeaders(enc) ++
        Option.when(src.versionId != "null")(hdr("x-amz-copy-source-version-id", src.versionId))
    )

  /** x-amz-copy-source を読み、読み取り権限を確かめてコピー元を返す（?versionId= に対応）。 */
  private def copySource(ctx: Ctx, a: Authed): IO[(ObjectRecord, Bucket, String)] =
    val source = header(ctx.req, "x-amz-copy-source").getOrElse("")
    val (pathPart, queryPart) = source.indexOf('?') match
      case -1 => (source, "")
      case i  => (source.take(i), source.drop(i + 1))
    val vid = SigV4.parseRawQuery(queryPart).toMap.get("versionId").filter(_.nonEmpty)
    val decoded = SigV4.percentDecode(pathPart).stripPrefix("/")
    decoded.indexOf('/') match
      case -1 => IO.raiseError(S3Error.invalidArgument("Invalid copy source"))
      case i =>
        val (sb, sk) = (decoded.take(i), decoded.drop(i + 1))
        withBucket(sb) { srcB =>
          need(ctx, a, srcB, Perm.Read, Some(sk)) >> vid.fold(store.getObject(srcB, sk))(v => store.getVersion(srcB, sk, v)).flatMap {
            case Some(o) if !o.deleteMarker => IO.pure((o, srcB, sk))
            case _                          => IO.raiseError(if vid.isDefined then S3Error.noSuchVersion else S3Error.noSuchKey(sk))
          }
        }

  // ======================= マルチパート =======================

  private def createUpload(ctx: Ctx, b: Bucket, key: String): IO[Response[IO]] =
    val req = ctx.req
    for
      (enc, _) <- newEncryption(req, b)
      now <- IO.realTime.map(_.toMillis)
      u = UploadRecord(newUploadId(), key, now, storedHeaders(req), userMeta(req), header(req, "x-amz-storage-class").getOrElse("STANDARD"), enc)
      _ <- store.createUpload(b, u)
    yield xmlDoc("InitiateMultipartUploadResult")(_.leaf("Bucket", b.name).leaf("Key", key).leaf("UploadId", u.uploadId)).putHeaders(encHeaders(enc))

  private def partNumber(ctx: Ctx): IO[Int] =
    IO.fromOption(ctx.q.get("partNumber").flatMap(_.toIntOption).filter(n => n >= 1 && n <= 10000))(
      S3Error.invalidArgument("Part number must be an integer between 1 and 10000, inclusive")
    )

  private def requireUpload(b: Bucket, key: String, uploadId: String): IO[UploadRecord] =
    store.getUpload(b, key, uploadId).flatMap(IO.fromOption(_)(S3Error.noSuchUpload))

  private def uploadPart(ctx: Ctx, a: Authed, b: Bucket, key: String): IO[Response[IO]] =
    val uploadId = ctx.q("uploadId")
    for
      n <- partNumber(ctx)
      u <- requireUpload(b, key, uploadId)
      dk <- readKey(ctx.req, u.encryption)
      ing <- ingest(ctx, a, u.storageClass, dk)
      now <- IO.realTime.map(_.toMillis)
      p = PartRecord(n, ing.md5, ing.size, now, ing.body)
      _ <- commit(ing)(store.putPart(b, key, uploadId, p))
    yield Response[IO](Status.Ok).putHeaders(etagHeader(p.etag) :: encHeaders(u.encryption))

  private def uploadPartCopy(ctx: Ctx, a: Authed, b: Bucket, key: String): IO[Response[IO]] =
    val uploadId = ctx.q("uploadId")
    for
      n <- partNumber(ctx)
      u <- requireUpload(b, key, uploadId)
      (src, _, _) <- copySource(ctx, a)
      _ <- IO.fromEither(copyConditions(ctx.req, src))
      (from, until) <- IO.fromEither(header(ctx.req, "x-amz-copy-source-range") match
        case None => Right((0L, src.size))
        case Some(r) =>
          parseRange(Some(r), src.size).flatMap(_.toRight(S3Error.invalidArgument("Invalid copy source range")))
      )
      srcDk <- readKey(ctx.req, src.encryption, CopySseC)
      dk <- readKey(ctx.req, u.encryption)
      // 暗号化が絡まなければブロックを共有する。絡むなら読み直して書く
      (body, md5) <-
        if srcDk.isEmpty && dk.isEmpty then
          data.sliceBody(src.body, from, until).flatMap { body =>
            // 全体を写し、ETag がそのまま MD5 なら読み直さない
            val md5 =
              if from == 0 && until == src.size && !src.etag.contains('-') then IO.pure(ByteVector.fromValidHex(src.etag))
              else md5Of(data.read(body, 0, until - from))
            md5.tupleLeft(body).onError(_ => data.release(body)).onCancel(data.release(body))
          }
        else
          data.ingest(data.read(src.body, from, until, srcDk), wantSha256 = false, None, config.redundancyOf(u.storageClass), dk).map(i => (i.body, i.md5))
      now <- IO.realTime.map(_.toMillis)
      p = PartRecord(n, md5, until - from, now, body)
      _ <- (data.touch(body) >> store.putPart(b, key, uploadId, p)).guarantee(data.release(body))
    yield xmlDoc("CopyPartResult")(_.leaf("LastModified", iso(now)).leaf("ETag", quote(p.etag)))

  private def completeUpload(ctx: Ctx, a: Authed, b: Bucket, key: String): IO[Response[IO]] =
    val uploadId = ctx.q("uploadId")
    for
      u <- requireUpload(b, key, uploadId)
      doc <- readXml(ctx, a)
      requested = (doc \ "Part").map(p => ((p \ "PartNumber").text.trim.toIntOption, unquote((p \ "ETag").text.trim))).toVector
      _ <- IO.raiseWhen(requested.isEmpty || requested.exists(_._1.isEmpty))(S3Error.malformedXml)
      nums = requested.flatMap(_._1)
      _ <- IO.raiseUnless(nums.zip(nums.drop(1)).forall(_ < _))(S3Error.invalidPartOrder)
      stored <- store.allParts(b, key, uploadId)
      byNum = stored.map(p => p.partNumber -> p).toMap
      chosen <- requested.traverse { case (n, tag) =>
        IO.fromOption(n.flatMap(byNum.get).filter(_.etag.equalsIgnoreCase(tag)))(S3Error.invalidPart)
      }
      _ <- IO.raiseWhen(chosen.dropRight(1).exists(_.size < config.minPartSize))(S3Error.entityTooSmall)
      now <- IO.realTime.map(_.toMillis)
      md5s = chosen.foldLeft(ByteVector.empty)(_ ++ _.md5)
      etag = ByteVector.view(MessageDigest.getInstance("MD5").digest(md5s.toArray)).toHex + "-" + chosen.size
      // 小さなパートはインラインで保存されているので、ブロックとして書き直す（暗号化されていれば暗号文のまま）
      refs <- chosen.traverse { p =>
        p.body match
          case Body.Inline(d) if d.nonEmpty => data.putBlock(d).map(id => Vector(BlockRef(id, p.size.toInt)))
          case Body.Inline(_)               => IO.pure(Vector.empty)
          case Body.Blocks(rs)              => IO.pure(rs)
      }
      body = Body.Blocks(refs.flatten)
      rec = ObjectRecord("null", chosen.map(_.size).sum, etag, now, u.headers, u.userMeta, u.storageClass, None, body, encryption = u.encryption)
      written <- share(body)(store.completeUpload(b, key, uploadId, rec, stored))
    yield xmlDoc("CompleteMultipartUploadResult") { x =>
      x.leaf("Location", s"/${b.name}/$key").leaf("Bucket", b.name).leaf("Key", key).leaf("ETag", quote(etag))
    }.putHeaders(versionHeader(b, written) ++ encHeaders(u.encryption))

  private def abortUpload(ctx: Ctx, b: Bucket, key: String): IO[Response[IO]] =
    store.abortUpload(b, key, ctx.q("uploadId")).as(noContent)

  private def listParts(ctx: Ctx, b: Bucket, key: String): IO[Response[IO]] =
    val uploadId = ctx.q("uploadId")
    val marker = ctx.q.get("part-number-marker").flatMap(_.toIntOption).getOrElse(0)
    val max = limit(ctx.q, "max-parts", min = 1)
    for
      u <- requireUpload(b, key, uploadId)
      (parts, truncated) <- store.listParts(b, key, uploadId, marker, max)
    yield xmlDoc("ListPartsResult") { x =>
      x.leaf("Bucket", b.name).leaf("Key", key).leaf("UploadId", uploadId)
      x.leaf("PartNumberMarker", marker.toString)
      parts.lastOption.foreach(p => x.leaf("NextPartNumberMarker", p.partNumber.toString))
      x.leaf("MaxParts", max.toString).leaf("IsTruncated", truncated.toString).leaf("StorageClass", u.storageClass)
      parts.foreach { p =>
        x.elem("Part") {
          x.leaf("PartNumber", p.partNumber.toString).leaf("LastModified", iso(p.lastModified)).leaf("ETag", quote(p.etag)).leaf("Size", p.size.toString)
        }
      }
    }

  // ======================= 補助 =======================

  private def readXml(ctx: Ctx, a: Authed): IO[scala.xml.Elem] =
    smallBody(ctx, a).flatMap(s => IO.fromEither(Xml.parse(s)))

  /** 設定や一覧などの小さな本文（XML、JSON）を読む。大きさの上限を守り、署名した SHA-256、Content-MD5、追加チェックサム
    * （ヘッダーか trailer）が付いていれば確かめる。上限は DeleteObjects の 1,000 キー（キーは最大 1,024 バイト）が収まる大きさ。
    */
  private def smallBody(ctx: Ctx, a: Authed): IO[String] =
    val req = ctx.req
    for
      (stream, trailers) <- bodyOf(ctx, a)
      chunks <- stream.scanChunks(0L) { (n, c) =>
        val m = n + c.size
        if m > MaxSmallBody then throw S3Error.invalidRequest("The request body is too large.")
        (m, c)
      }.compile.to(fs2.Chunk)
      bytes = chunks.toArray
      tr <- trailers
      _ <- IO.raiseWhen(a.payloadHash.length == 64 && SigV4.sha256Hex(bytes) != a.payloadHash.toLowerCase)(S3Error.sha256Mismatch)
      _ <- IO.raiseWhen(header(req, "content-md5").exists(_.trim != Crypto.md5Base64(bytes)))(S3Error.badDigest)
      _ <- Checksummer.Algorithms.traverse_ { alg =>
        header(req, s"x-amz-checksum-$alg").orElse(tr.get(s"x-amz-checksum-$alg")).fold(IO.unit) { expected =>
          val c = Checksummer(alg).get
          c.update(bytes, 0, bytes.length)
          IO.raiseWhen(c.base64 != expected.trim)(S3Error.badDigest)
        }
      }
    yield String(bytes, UTF_8)

  private def responseHeaders(ctx: Ctx, o: ObjectRecord): List[Header.Raw] =
    val base = if o.headers.contains("content-type") then o.headers else o.headers.updated("content-type", "binary/octet-stream")
    val merged = responseOverrides.foldLeft(base)((m, kv) => ctx.q.get(kv._2).fold(m)(v => m.updated(kv._1, v)))
    merged.toList.map((k, v) => hdr(k, v))

object S3Service:

  /** 小さな本文（設定や DeleteObjects）の上限。 */
  private[s3] val MaxSmallBody: Long = 2L * 1024 * 1024

  private val SseC = "x-amz-server-side-encryption-customer-"
  private val CopySseC = "x-amz-copy-source-server-side-encryption-customer-"

  /** 応答のヘッダー → それを上書きするクエリ（GetObject の response-*）。 */
  private val responseOverrides = List(
    "content-type" -> "response-content-type",
    "content-language" -> "response-content-language",
    "expires" -> "response-expires",
    "cache-control" -> "response-cache-control",
    "content-disposition" -> "response-content-disposition",
    "content-encoding" -> "response-content-encoding"
  )

  private[s3] final case class Ctx(
      req: Request[IO],
      rid: String,
      decodedPath: String,
      query: List[(String, String)],
      q: Map[String, String],
      bucket: Option[String],
      key: Option[String]
  )

  /** 認証結果。key が None なら匿名（署名なし）。 */
  private[s3] final case class Authed(key: Option[AccessKey], signing: Option[AwsChunked.Signing], payloadHash: String)

  /** S3 のサブリソース（これがあるときは通常の GET/PUT ではない）。 */
  private val subresources = Set(
    "acl", "policy", "cors", "lifecycle", "encryption", "tagging", "website", "logging", "notification", "replication",
    "requestPayment", "versions", "versioning", "location", "uploads", "uploadId", "delete", "restore", "select",
    "object-lock", "retention", "legal-hold", "torrent", "accelerate", "analytics", "inventory", "metrics",
    "ownershipControls", "publicAccessBlock", "intelligent-tiering", "attributes"
  )
  def subresource(q: Map[String, String]): List[String] = q.keys.filter(subresources.contains).toList

  def header(req: Request[IO], name: String): Option[String] =
    req.headers.get(CIString(name)).map(_.head.value)

  def hdr(name: String, value: String): Header.Raw = Header.Raw(CIString(name), value)

  /** 一覧の件数の指定（max-keys など）。[min, 1000] に収める。無いか数でなければ 1000。 */
  def limit(q: Map[String, String], name: String, min: Int): Int =
    q.get(name).flatMap(_.toIntOption).map(n => math.max(min, math.min(n, 1000))).getOrElse(1000)

  private val storedHeaderNames = List("content-type", "content-encoding", "content-disposition", "content-language", "cache-control", "expires")

  def storedHeaders(req: Request[IO]): Map[String, String] =
    storedHeaderNames.flatMap(h => header(req, h).map(h -> _)).toMap

  def userMeta(req: Request[IO]): Map[String, String] =
    req.headers.headers.flatMap { h =>
      val name = h.name.toString.toLowerCase
      Option.when(name.startsWith("x-amz-meta-"))(name.stripPrefix("x-amz-meta-") -> h.value)
    }.toMap

  def quote(etag: String): String = "\"" + etag + "\""
  def unquote(s: String): String = s.trim.stripPrefix("\"").stripSuffix("\"")
  def etagHeader(etag: String): Header.Raw = hdr("ETag", quote(etag))

  def checksumHeaders(o: ObjectRecord): List[Header.Raw] =
    o.checksum.toList.map((alg, v) => hdr(s"x-amz-checksum-$alg", v))

  private val isoFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(ZoneOffset.UTC)
  private val httpFormat = DateTimeFormatter.ofPattern("EEE, dd MMM yyyy HH:mm:ss 'GMT'", Locale.US).withZone(ZoneOffset.UTC)

  def iso(millis: Long): String = isoFormat.format(Instant.ofEpochMilli(millis))
  def httpDate(millis: Long): String = httpFormat.format(Instant.ofEpochMilli(millis))

  def parseHttpDate(s: String): Option[Long] =
    scala.util.Try(ZonedDateTime.parse(s.trim, DateTimeFormatter.RFC_1123_DATE_TIME).toInstant.toEpochMilli).toOption

  def validBucketName(n: String): Boolean =
    n.length >= 3 && n.length <= 63 && n.matches("[a-z0-9][a-z0-9.-]*[a-z0-9]") && !n.contains("..") &&
      !n.matches("\\d+\\.\\d+\\.\\d+\\.\\d+") && !n.startsWith("xn--")

  /** Range ヘッダー（単一範囲のみ）。None なら全体。範囲外なら InvalidRange。 */
  def parseRange(h: Option[String], size: Long): Either[S3Error, Option[(Long, Long)]] =
    h.map(_.trim) match
      case None                               => Right(None)
      case Some(r) if !r.startsWith("bytes=") => Right(None)
      case Some(r) if r.contains(',')         => Right(None) // 複数範囲は全体を返す（S3 と同じ）
      case Some(r) =>
        r.stripPrefix("bytes=").split("-", -1) match
          case Array("", suffix) =>
            suffix.toLongOption match
              case Some(n) if n > 0 => Right(Some((math.max(0, size - n), size)))
              case _                => Left(S3Error.invalidRange)
          case Array(a, b) =>
            (a.toLongOption, if b.isEmpty then Some(size - 1) else b.toLongOption) match
              case (Some(f), Some(l)) if f <= l || b.isEmpty =>
                if f >= size then Left(S3Error.invalidRange) else Right(Some((f, math.min(l, size - 1) + 1)))
              case _ => Right(None)
          case _ => Right(None)

  /** 条件の比較に使う更新時刻（HTTP の日時は秒単位なので切り捨てる）。 */
  private def lastModifiedSeconds(o: ObjectRecord): Long = o.lastModified / 1000 * 1000

  def checkConditions(req: Request[IO], o: ObjectRecord): Either[S3Error, Unit] =
    val etag = o.etag
    val ifMatch = header(req, "if-match").map(_.split(',').map(unquote).toSet)
    val ifNoneMatch = header(req, "if-none-match").map(_.split(',').map(unquote).toSet)
    val ifModSince = header(req, "if-modified-since").flatMap(parseHttpDate)
    val ifUnmodSince = header(req, "if-unmodified-since").flatMap(parseHttpDate)
    val lastMod = lastModifiedSeconds(o)
    if ifMatch.exists(m => !m.contains("*") && !m.contains(etag)) then Left(S3Error.preconditionFailed)
    else if ifMatch.isEmpty && ifUnmodSince.exists(lastMod > _) then Left(S3Error.preconditionFailed)
    else if ifNoneMatch.exists(m => m.contains("*") || m.contains(etag)) then Left(S3Error.notModified)
    else if ifNoneMatch.isEmpty && ifModSince.exists(lastMod <= _) then Left(S3Error.notModified)
    else Right(())

  def copyConditions(req: Request[IO], o: ObjectRecord): Either[S3Error, Unit] =
    val lastMod = lastModifiedSeconds(o)
    val ifMatch = header(req, "x-amz-copy-source-if-match").map(unquote)
    val ifNoneMatch = header(req, "x-amz-copy-source-if-none-match").map(unquote)
    val ifUnmod = header(req, "x-amz-copy-source-if-unmodified-since").flatMap(parseHttpDate)
    val ifMod = header(req, "x-amz-copy-source-if-modified-since").flatMap(parseHttpDate)
    if ifMatch.exists(_ != o.etag) || ifNoneMatch.contains(o.etag) || ifUnmod.exists(lastMod > _) || ifMod.exists(lastMod <= _) then
      Left(S3Error.preconditionFailed)
    else Right(())

  private val UpperHex = HexFormat.of().withUpperCase()

  /** 要求の ID（大文字の 16 進数 16 桁）。 */
  def requestId(): String = UpperHex.toHexDigits(ThreadLocalRandom.current().nextLong())

  def newUploadId(): String = ByteVector.view(Crypto.randomBytes(16)).toHex

  /** 本文の MD5。 */
  private def md5Of(body: Stream[IO, Byte]): IO[ByteVector] =
    body.chunks.compile.fold(MessageDigest.getInstance("MD5")) { (md, c) =>
      val s = c.toArraySlice
      md.update(s.values, s.offset, s.size)
      md
    }.map(md => ByteVector.view(md.digest()))

  private val noContent: Response[IO] = Response[IO](Status.NoContent)

  def bytesResponse(status: Status, contentType: String, bytes: Array[Byte]): Response[IO] =
    Response[IO](status)
      .withBodyStream(Stream.chunk(fs2.Chunk.array(bytes)))
      .putHeaders(hdr("Content-Type", contentType), `Content-Length`.unsafeFromLong(bytes.length.toLong))

  def xmlResponse(status: Status, x: Xml): Response[IO] = bytesResponse(status, "application/xml", x.result.getBytes(UTF_8))

  /** S3 の名前空間を付けた root 要素 1 つの XML 応答（200）。 */
  def xmlDoc(root: String)(body: Xml => Any): Response[IO] =
    val x = Xml()
    x.open(root, Xml.Ns)
    body(x)
    x.close(root)
    xmlResponse(Status.Ok, x)

  /** 例外を S3 のエラーにする。想定していない失敗は、要求 ID を付けてログに残し InternalError にする。 */
  def toS3Error(e: Throwable, rid: String): S3Error = e match
    case s: S3Error                               => s
    case _: scalas3.meta.MetaUnavailableException => S3Error.unavailable
    // ノードの停止や入れ替えの間の一時的な失敗（クライアントにやり直してもらう）
    case _: scalas3.rpc.ConnectionClosedException | _: java.util.concurrent.TimeoutException | _: java.net.ConnectException |
        _: scalas3.cluster.NotEnoughReplicasException =>
      S3Error.unavailable
    case _: scalas3.block.BlockNotFoundException => S3Error.internal
    case other =>
      System.err.println(s"[scalas3] internal error ($rid): $other")
      other.printStackTrace()
      S3Error.internal

  def errorResponse(e: Throwable, resource: String, rid: String, method: Method): Response[IO] =
    val err = toS3Error(e, rid)
    val extra = err.headers.toList.map((k, v) => hdr(k, v))
    if err.status == Status.NotModified || method == Method.HEAD then Response[IO](err.status).putHeaders(extra)
    else
      val x = Xml()
      x.open("Error").leaf("Code", err.code).leaf("Message", err.message).leaf("Resource", resource).leaf("RequestId", rid).close("Error")
      xmlResponse(err.status, x).putHeaders(extra)
