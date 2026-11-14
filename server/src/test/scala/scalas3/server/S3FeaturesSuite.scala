package scalas3.server

import cats.effect.{IO, Resource}
import fs2.io.file.Files
import munit.CatsEffectSuite
import scalas3.runtime.NodeAddress
import scalas3.s3.*
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.model.*
import software.amazon.awssdk.services.s3.{S3Client, S3Configuration}

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.charset.StandardCharsets.UTF_8
import java.time.{ZoneOffset, ZonedDateTime}
import java.time.format.DateTimeFormatter
import java.util.Base64
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

/** M7 の機能（バージョニング、SSE、CORS、バケットポリシー、秘密鍵の暗号化）。 */
class S3FeaturesSuite extends CatsEffectSuite:

  override def munitIOTimeout: Duration = 3.minutes

  private val ak = "AKIDFEATURES0000001"
  private val sk = "featuressecret000000000000000000000000001"
  private val master = Array.tabulate[Byte](32)(i => (i * 7 + 3).toByte)

  private val node = ResourceSuiteLocalFixture(
    "node",
    for
      tmp <- Files[IO].tempDirectory
      n <- S3Node.resource(
        NodeConfig(
          dataDir = tmp,
          disks = Vector(tmp / "d0"),
          s3Host = "127.0.0.1",
          s3Port = 0,
          rpcListen = NodeAddress("127.0.0.1", 0),
          s3 = S3Config(blockSize = 64 * 1024, inlineThreshold = 1024, minPartSize = 1, masterKey = Some(master)),
          adminKey = Some((ak, sk)),
          gcInterval = 1000.days
        )
      )
    yield (n, tmp)
  )

  override def munitFixtures = List(node)

  private def endpoint = s"http://127.0.0.1:${node()._1.s3Address.port}"

  private def client(key: String = ak, secret: String = sk): S3Client =
    S3Client.builder().endpointOverride(URI.create(endpoint)).region(Region.US_EAST_1)
      .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(key, secret)))
      .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
      .build()

  private def withClient[A](f: S3Client => A): IO[A] = Resource.fromAutoCloseable(IO(client())).use(c => IO.blocking(f(c)))

  private def s3Code(f: => Any): String =
    try
      f
      "OK"
    catch case e: S3Exception => Option(e.awsErrorDetails()).flatMap(d => Option(d.errorCode())).getOrElse(e.statusCode().toString)

  private val http = HttpClient.newHttpClient()

  /** 自前の SigV4 署名でリクエストを送る（UNSIGNED-PAYLOAD）。 */
  private def signed(method: String, path: String, headers: Map[String, String] = Map.empty, body: Array[Byte] = Array.emptyByteArray, payloadHash: String = SigV4.UnsignedPayload): HttpResponse[Array[Byte]] =
    val now = ZonedDateTime.now(ZoneOffset.UTC)
    val amzDate = now.format(DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'"))
    val scope = SigV4.Scope(amzDate.take(8), "us-east-1", "s3")
    val host = s"127.0.0.1:${node()._1.s3Address.port}"
    val all = headers ++ Map("host" -> host, "x-amz-date" -> amzDate, "x-amz-content-sha256" -> payloadHash)
    val names = all.keys.map(_.toLowerCase).toList.sorted
    val (rawPath, rawQuery) = path.split("\\?", 2) match
      case Array(p, q) => (p, q.split("&").toList.map(kv => kv.split("=", 2) match { case Array(k, v) => (k, v); case Array(k) => (k, "") }))
      case Array(p)    => (p, Nil)
    val canonical = SigV4.canonicalRequest(method, SigV4.percentDecode(rawPath), rawQuery, n => all.find(_._1.equalsIgnoreCase(n)).map(_._2).toList, names, payloadHash)
    val sig = SigV4.sign(SigV4.signingKey(sk, scope), SigV4.stringToSign(amzDate, scope, canonical))
    val auth = s"AWS4-HMAC-SHA256 Credential=$ak/${scope.render}, SignedHeaders=${names.mkString(";")}, Signature=$sig"
    val b = HttpRequest.newBuilder(URI.create(endpoint + path)).method(method, HttpRequest.BodyPublishers.ofByteArray(body))
    (all - "host").foreach((k, v) => b.header(k, v))
    http.send(b.header("Authorization", auth).build(), HttpResponse.BodyHandlers.ofByteArray())

  private def anon(method: String, path: String, headers: Map[String, String] = Map.empty, body: Array[Byte] = Array.emptyByteArray) =
    val b = HttpRequest.newBuilder(URI.create(endpoint + path)).method(method, HttpRequest.BodyPublishers.ofByteArray(body))
    headers.foreach((k, v) => b.header(k, v))
    http.send(b.build(), HttpResponse.BodyHandlers.ofByteArray())

  private def put(c: S3Client, b: String, k: String, s: String) =
    c.putObject(PutObjectRequest.builder().bucket(b).key(k).build(), RequestBody.fromString(s))
  private def getS(c: S3Client, b: String, k: String, v: String = null) =
    c.getObjectAsBytes(GetObjectRequest.builder().bucket(b).key(k).versionId(v).build()).asUtf8String()

  test("DeleteObjects：Content-MD5・チェックサム・署名した SHA-256 が合わなければ消さない") {
    withClient { c =>
      c.createBucket(CreateBucketRequest.builder().bucket("delmd5").build())
      put(c, "delmd5", "a", "1")
      put(c, "delmd5", "b", "2")
      val body = "<Delete><Object><Key>a</Key></Object></Delete>".getBytes(UTF_8)
      val md5 = java.util.Base64.getEncoder.encodeToString(java.security.MessageDigest.getInstance("MD5").digest(body))
      val crc = { val c = java.util.zip.CRC32(); c.update(body); java.util.Base64.getEncoder.encodeToString(scodec.bits.ByteVector.fromLong(c.getValue, 4).toArray) }
      val wrongMd5 = signed("POST", "/delmd5?delete", Map("content-md5" -> "AAAAAAAAAAAAAAAAAAAAAA=="), body).statusCode()
      val wrongCrc = signed("POST", "/delmd5?delete", Map("x-amz-checksum-crc32" -> "AAAAAA=="), body).statusCode()
      val wrongSha = signed("POST", "/delmd5?delete", Map.empty, body, payloadHash = "0" * 64).statusCode()
      val stillThere = getS(c, "delmd5", "a")
      val okMd5 = signed("POST", "/delmd5?delete", Map("content-md5" -> md5, "x-amz-checksum-crc32" -> crc), body, payloadHash = SigV4.sha256Hex(body)).statusCode()
      val gone = s3Code(getS(c, "delmd5", "a"))
      val tooLarge = signed("POST", "/delmd5?delete", Map.empty, Array.fill(3 * 1024 * 1024)('x'.toByte)).statusCode()
      // SDK（Content-MD5 か CRC32 を付ける）でも消せる
      c.deleteObjects(DeleteObjectsRequest.builder().bucket("delmd5")
        .delete(Delete.builder().objects(ObjectIdentifier.builder().key("b").build()).build()).build())
      val sdkGone = s3Code(getS(c, "delmd5", "b"))
      assertEquals((wrongMd5, wrongCrc, wrongSha), (400, 400, 400))
      assertEquals(stillThere, "1")
      assertEquals(okMd5, 200)
      assertEquals(gone, "NoSuchKey")
      assertEquals(tooLarge, 400)
      assertEquals(sdkGone, "NoSuchKey")
    }
  }

  test("バージョニング：版の作成・取得・一覧、削除マーカー、版の削除") {
    withClient { c =>
      c.createBucket(CreateBucketRequest.builder().bucket("ver").build())
      put(c, "ver", "doc", "before-versioning")
      c.putBucketVersioning(PutBucketVersioningRequest.builder().bucket("ver")
        .versioningConfiguration(VersioningConfiguration.builder().status(BucketVersioningStatus.ENABLED).build()).build())
      val status = c.getBucketVersioning(GetBucketVersioningRequest.builder().bucket("ver").build()).statusAsString()
      val v1 = put(c, "ver", "doc", "one").versionId()
      val v2 = put(c, "ver", "doc", "two").versionId()
      val latest = getS(c, "ver", "doc")
      val old = getS(c, "ver", "doc", v1)
      val nullV = getS(c, "ver", "doc", "null")
      val versions = c.listObjectVersions(ListObjectVersionsRequest.builder().bucket("ver").build())
      val del = c.deleteObject(DeleteObjectRequest.builder().bucket("ver").key("doc").build())
      val afterDelete = s3Code(getS(c, "ver", "doc"))
      val listed = c.listObjectsV2(ListObjectsV2Request.builder().bucket("ver").build()).contents().asScala.map(_.key()).toList
      // 削除マーカーを消すと戻る
      c.deleteObject(DeleteObjectRequest.builder().bucket("ver").key("doc").versionId(del.versionId()).build())
      val restored = getS(c, "ver", "doc")
      // 最新版を消すと、ひとつ前が現在の版になる
      c.deleteObject(DeleteObjectRequest.builder().bucket("ver").key("doc").versionId(v2).build())
      val afterRemoveV2 = getS(c, "ver", "doc")
      assertEquals(status, "Enabled")
      assertNotEquals(v1, v2)
      assertEquals(latest, "two")
      assertEquals(old, "one")
      assertEquals(nullV, "before-versioning")
      assertEquals(versions.versions().asScala.map(_.versionId()).toList, List(v2, v1, "null"))
      assertEquals(versions.versions().asScala.filter(_.isLatest).map(_.versionId()).toList, List(v2))
      assert(del.deleteMarker())
      assertEquals(afterDelete, "NoSuchKey")
      assertEquals(listed, Nil)
      assertEquals(restored, "two")
      assertEquals(afterRemoveV2, "one")
    }
  }

  test("SSE-S3：保存されたブロックに平文が含まれない。バケットの既定の暗号化とマルチパートも") {
    withClient { c =>
      c.createBucket(CreateBucketRequest.builder().bucket("sse").build())
      val marker = "PLAINTEXT-MARKER-" * 5000
      c.putObject(PutObjectRequest.builder().bucket("sse").key("secret").serverSideEncryption(ServerSideEncryption.AES256).build(), RequestBody.fromString(marker))
      val head = c.headObject(HeadObjectRequest.builder().bucket("sse").key("secret").build())
      val back = getS(c, "sse", "secret")
      // 既定の暗号化
      c.putBucketEncryption(PutBucketEncryptionRequest.builder().bucket("sse").serverSideEncryptionConfiguration(
        ServerSideEncryptionConfiguration.builder().rules(ServerSideEncryptionRule.builder().applyServerSideEncryptionByDefault(
          ServerSideEncryptionByDefault.builder().sseAlgorithm(ServerSideEncryption.AES256).build()).build()).build()).build())
      put(c, "sse", "default", "x" * 3000)
      val head2 = c.headObject(HeadObjectRequest.builder().bucket("sse").key("default").build())
      // マルチパート
      val up = c.createMultipartUpload(CreateMultipartUploadRequest.builder().bucket("sse").key("mp").build()).uploadId()
      val e1 = c.uploadPart(UploadPartRequest.builder().bucket("sse").key("mp").uploadId(up).partNumber(1).build(), RequestBody.fromString("a" * 100000)).eTag()
      val e2 = c.uploadPart(UploadPartRequest.builder().bucket("sse").key("mp").uploadId(up).partNumber(2).build(), RequestBody.fromString("b" * 100)).eTag()
      c.completeMultipartUpload(CompleteMultipartUploadRequest.builder().bucket("sse").key("mp").uploadId(up).multipartUpload(
        CompletedMultipartUpload.builder().parts(CompletedPart.builder().partNumber(1).eTag(e1).build(), CompletedPart.builder().partNumber(2).eTag(e2).build()).build()).build())
      val mp = getS(c, "sse", "mp")
      // ディスク上に平文が無い
      val disk = node()._2 / "d0"
      val files = java.nio.file.Files.walk(disk.toNioPath).iterator().asScala.filter(java.nio.file.Files.isRegularFile(_)).toList
      val leaked = files.exists(f => new String(java.nio.file.Files.readAllBytes(f), UTF_8).contains("PLAINTEXT-MARKER"))
      assertEquals(head.serverSideEncryptionAsString(), "AES256")
      assertEquals(back, marker)
      assertEquals(head2.serverSideEncryptionAsString(), "AES256")
      assertEquals(mp, "a" * 100000 + "b" * 100)
      assert(files.nonEmpty)
      assert(!leaked, "ディスクに平文が残っている")
    }
  }

  test("SSE-C：鍵が無ければ 400、違えば 403、正しければ読める。コピーで SSE-S3 に変えられる") {
    IO.blocking {
      val c = client()
      c.createBucket(CreateBucketRequest.builder().bucket("ssec").build())
      c.close()
      val key = Array.tabulate[Byte](32)(i => (i + 1).toByte)
      val wrong = Array.fill[Byte](32)(9)
      def h(k: Array[Byte], prefix: String = "x-amz-server-side-encryption-customer-") = Map(
        prefix + "algorithm" -> "AES256",
        prefix + "key" -> Base64.getEncoder.encodeToString(k),
        prefix + "key-MD5" -> Crypto.md5Base64(k)
      )
      val body = ("customer-encrypted " * 10000).getBytes(UTF_8)
      val p = signed("PUT", "/ssec/obj", h(key), body)
      val noKey = signed("GET", "/ssec/obj")
      val bad = signed("GET", "/ssec/obj", h(wrong))
      val good = signed("GET", "/ssec/obj", h(key))
      val copy = signed("PUT", "/ssec/copied", h(key, "x-amz-copy-source-server-side-encryption-customer-") ++ Map(
        "x-amz-copy-source" -> "/ssec/obj", "x-amz-server-side-encryption" -> "AES256"))
      val copied = signed("GET", "/ssec/copied")
      assertEquals(p.statusCode(), 200)
      assertEquals(noKey.statusCode(), 400)
      assertEquals(bad.statusCode(), 403)
      assertEquals(good.statusCode(), 200)
      assert(java.util.Arrays.equals(good.body(), body))
      assertEquals(good.headers().firstValue("x-amz-server-side-encryption-customer-key-MD5").orElse(""), Crypto.md5Base64(key))
      assertEquals(copy.statusCode(), 200, new String(copy.body(), UTF_8))
      assertEquals(copied.statusCode(), 200)
      assert(java.util.Arrays.equals(copied.body(), body))
      assertEquals(copied.headers().firstValue("x-amz-server-side-encryption").orElse(""), "AES256")
    }
  }

  test("CORS：プリフライトと応答ヘッダー") {
    withClient { c =>
      c.createBucket(CreateBucketRequest.builder().bucket("web").build())
      put(c, "web", "page", "hello")
      c.putBucketCors(PutBucketCorsRequest.builder().bucket("web").corsConfiguration(CORSConfiguration.builder().corsRules(
        CORSRule.builder().allowedOrigins("https://app.example.com").allowedMethods("GET", "PUT").allowedHeaders("*").maxAgeSeconds(600).exposeHeaders("ETag").build()
      ).build()).build())
      val rules = c.getBucketCors(GetBucketCorsRequest.builder().bucket("web").build()).corsRules().asScala.toList
      val ok = anon("OPTIONS", "/web/page", Map("Origin" -> "https://app.example.com", "Access-Control-Request-Method" -> "PUT", "Access-Control-Request-Headers" -> "content-type"))
      val ng = anon("OPTIONS", "/web/page", Map("Origin" -> "https://evil.example.com", "Access-Control-Request-Method" -> "GET"))
      val actual = signed("GET", "/web/page", Map("Origin" -> "https://app.example.com"))
      assertEquals(rules.size, 1)
      assertEquals(ok.statusCode(), 200)
      assertEquals(ok.headers().firstValue("Access-Control-Allow-Origin").orElse(""), "https://app.example.com")
      assert(ok.headers().firstValue("Access-Control-Allow-Methods").orElse("").contains("PUT"))
      assertEquals(ok.headers().firstValue("Access-Control-Max-Age").orElse(""), "600")
      assertEquals(ng.statusCode(), 403)
      assertEquals(actual.headers().firstValue("Access-Control-Allow-Origin").orElse(""), "https://app.example.com")
    }
  }

  test("バケットポリシー：匿名の読み取りを許し、特定のキーの削除を拒否する") {
    for
      _ <- node()._1.store.putAccessKey(AccessKey("AKIDLIMITED00000001", "limitedsecret0000000000000000000000001", "limited", admin = false, Map("pub" -> (Perm.Read.bit | Perm.Write.bit))))
      r <- withClient { c =>
        c.createBucket(CreateBucketRequest.builder().bucket("pub").build())
        put(c, "pub", "readme.txt", "public!")
        val before = anon("GET", "/pub/readme.txt").statusCode()
        c.putBucketPolicy(PutBucketPolicyRequest.builder().bucket("pub").policy(
          """{"Version":"2012-10-17","Statement":[
            |{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::pub/*"},
            |{"Effect":"Deny","Principal":{"AWS":["AKIDLIMITED00000001"]},"Action":"s3:DeleteObject","Resource":"arn:aws:s3:::pub/*"}]}""".stripMargin
        ).build())
        val after = anon("GET", "/pub/readme.txt")
        val anonPut = anon("PUT", "/pub/hack.txt", body = "x".getBytes).statusCode()
        val anonList = anon("GET", "/pub/").statusCode()
        val limited = client("AKIDLIMITED00000001", "limitedsecret0000000000000000000000001")
        val limitedPut = s3Code(put(limited, "pub", "by-limited", "ok"))
        val limitedDelete = s3Code(limited.deleteObject(DeleteObjectRequest.builder().bucket("pub").key("by-limited").build()))
        val adminDelete = s3Code(c.deleteObject(DeleteObjectRequest.builder().bucket("pub").key("by-limited").build()))
        val policy = c.getBucketPolicy(GetBucketPolicyRequest.builder().bucket("pub").build()).policy()
        limited.close()
        (before, after.statusCode(), new String(after.body(), UTF_8), anonPut, anonList, limitedPut, limitedDelete, adminDelete, policy)
      }
    yield
      val (before, after, body, anonPut, anonList, limitedPut, limitedDelete, adminDelete, policy) = r
      assertEquals(before, 403)
      assertEquals(after, 200)
      assertEquals(body, "public!")
      assertEquals(anonPut, 403)
      assertEquals(anonList, 403)
      assertEquals(limitedPut, "OK")
      assertEquals(limitedDelete, "AccessDenied")
      assertEquals(adminDelete, "OK")
      assert(policy.contains("s3:GetObject"))
  }

  test("アクセスキーの秘密鍵はマスター鍵で暗号化して保存される") {
    node()._1.core.meta.client.get(S3Keys.accessKey(ak)).map { raw =>
      val stored = S3Codec.accessKey(raw.get).secret
      assert(stored.startsWith("enc:"), stored)
      assert(!stored.contains(sk))
    }
  }
