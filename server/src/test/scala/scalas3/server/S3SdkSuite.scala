package scalas3.server

import cats.effect.{IO, Resource}
import fs2.io.file.Files
import munit.CatsEffectSuite
import scalas3.runtime.NodeAddress
import scalas3.s3.{BlockGc, Redundancy, S3Config}
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.core.ResponseBytes
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.model.*
import software.amazon.awssdk.services.s3.presigner.S3Presigner
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest
import software.amazon.awssdk.services.s3.{S3Client, S3Configuration}

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

/** 本物のサーバーを立て、AWS SDK for Java v2 から操作する。 */
class S3SdkSuite extends CatsEffectSuite:

  override def munitIOTimeout: Duration = 3.minutes

  private val ak = "AKIDTESTKEY000000001"
  private val sk = "testsecretkey0000000000000000000000000001"

  private val node = ResourceSuiteLocalFixture(
    "node",
    for
      tmp <- Files[IO].tempDirectory
      n <- S3Node.resource(
        NodeConfig(
          dataDir = tmp,
          disks = Vector(tmp / "d0", tmp / "d1"),
          s3Host = "127.0.0.1",
          s3Port = 0,
          rpcListen = NodeAddress("127.0.0.1", 0),
          s3 = S3Config(
            blockSize = 64 * 1024,
            inlineThreshold = 1024,
            minPartSize = 1,
            storageClasses = Map("STANDARD" -> Redundancy.Replicated, "EC21" -> Redundancy.Erasure(2, 1))
          ),
          adminKey = Some((ak, sk)),
          gcInterval = 1000.days
        )
      )
    yield n
  )

  override def munitFixtures = List(node)

  private def endpoint = s"http://127.0.0.1:${node().s3Address.port}"

  private def client(secret: String = sk): S3Client =
    S3Client
      .builder()
      .endpointOverride(URI.create(endpoint))
      .region(Region.US_EAST_1)
      .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(ak, secret)))
      .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
      .build()

  private def withClient[A](f: S3Client => A): IO[A] = Resource.fromAutoCloseable(IO(client())).use(c => IO.blocking(f(c)))

  private def bytes(n: Int, seed: Int = 1): Array[Byte] =
    val r = new scala.util.Random(seed)
    Array.fill(n)(r.nextInt().toByte)

  private def put(c: S3Client, b: String, k: String, data: Array[Byte], ct: String = "application/octet-stream") =
    c.putObject(PutObjectRequest.builder().bucket(b).key(k).contentType(ct).build(), RequestBody.fromBytes(data))

  private def get(c: S3Client, b: String, k: String): ResponseBytes[GetObjectResponse] =
    c.getObjectAsBytes(GetObjectRequest.builder().bucket(b).key(k).build())

  private def s3Code(f: => Any): String =
    try
      f
      "OK"
    catch case e: S3Exception => Option(e.awsErrorDetails()).map(_.errorCode()).filter(_ != null).getOrElse(e.statusCode().toString)

  test("バケットの作成・一覧・確認・削除") {
    withClient { c =>
      c.createBucket(CreateBucketRequest.builder().bucket("alpha").build())
      val dup = s3Code(c.createBucket(CreateBucketRequest.builder().bucket("alpha").build()))
      val names = c.listBuckets().buckets().asScala.map(_.name()).toList
      c.headBucket(HeadBucketRequest.builder().bucket("alpha").build())
      val missing = s3Code(c.headBucket(HeadBucketRequest.builder().bucket("nope").build()))
      put(c, "alpha", "x", Array[Byte](1))
      val notEmpty = s3Code(c.deleteBucket(DeleteBucketRequest.builder().bucket("alpha").build()))
      c.deleteObject(DeleteObjectRequest.builder().bucket("alpha").key("x").build())
      c.deleteBucket(DeleteBucketRequest.builder().bucket("alpha").build())
      val after = c.listBuckets().buckets().asScala.map(_.name()).toList
      assertEquals(dup, "BucketAlreadyOwnedByYou")
      assert(names.contains("alpha"))
      assertEquals(missing, "NoSuchBucket")
      assertEquals(notEmpty, "BucketNotEmpty")
      assert(!after.contains("alpha"))
    }
  }

  test("小さいオブジェクト・複数ブロックのオブジェクトの読み書き、メタデータ、Range") {
    withClient { c =>
      c.createBucket(CreateBucketRequest.builder().bucket("data").build())
      val small = "こんにちは".getBytes("UTF-8")
      val big = bytes(300 * 1024 + 17)
      c.putObject(
        PutObjectRequest.builder().bucket("data").key("dir/small.txt").contentType("text/plain").metadata(Map("color" -> "blue").asJava).build(),
        RequestBody.fromBytes(small)
      )
      put(c, "data", "dir/big.bin", big)
      val gs = get(c, "data", "dir/small.txt")
      val gb = get(c, "data", "dir/big.bin")
      val head = c.headObject(HeadObjectRequest.builder().bucket("data").key("dir/big.bin").build())
      val range = c.getObjectAsBytes(GetObjectRequest.builder().bucket("data").key("dir/big.bin").range("bytes=65530-131080").build())
      val suffix = c.getObjectAsBytes(GetObjectRequest.builder().bucket("data").key("dir/big.bin").range("bytes=-10").build())
      val noKey = s3Code(get(c, "data", "nothing"))
      assertEquals(gs.asByteArray().toList, small.toList)
      assertEquals(gs.response().contentType(), "text/plain")
      assertEquals(gs.response().metadata().asScala.toMap, Map("color" -> "blue"))
      assertEquals(gb.asByteArray().toList, big.toList)
      assertEquals(head.contentLength().longValue, big.length.toLong)
      assertEquals(range.asByteArray().toList, big.slice(65530, 131081).toList)
      assertEquals(range.response().contentRange(), s"bytes 65530-131080/${big.length}")
      assertEquals(suffix.asByteArray().toList, big.takeRight(10).toList)
      assertEquals(noKey, "NoSuchKey")
    }
  }

  test("特殊な文字を含むキー") {
    withClient { c =>
      c.createBucket(CreateBucketRequest.builder().bucket("keys").build())
      val keys = List("a b/c+d=e&f", "日本語/ファイル名.txt", "x%2Fy", "tilde~!*'()", "semi;colon,comma", "a//double")
      keys.zipWithIndex.foreach((k, i) => put(c, "keys", k, Array(i.toByte)))
      val got = keys.map(k => get(c, "keys", k).asByteArray().toList)
      val listed = c.listObjectsV2(ListObjectsV2Request.builder().bucket("keys").build()).contents().asScala.map(_.key()).toSet
      assertEquals(got, keys.indices.map(i => List(i.toByte)).toList)
      assertEquals(listed, keys.toSet)
    }
  }

  test("ListObjects V1/V2：プレフィックス、区切り文字、ページング") {
    withClient { c =>
      c.createBucket(CreateBucketRequest.builder().bucket("list").build())
      val keys = List("a/1", "a/2", "a/b/3", "b/4", "c", "d/5", "d/6")
      keys.foreach(k => put(c, "list", k, Array[Byte](1)))
      val top = c.listObjectsV2(ListObjectsV2Request.builder().bucket("list").delimiter("/").build())
      val inA = c.listObjectsV2(ListObjectsV2Request.builder().bucket("list").prefix("a/").delimiter("/").build())
      // max-keys=2 で全部たどる
      var token: String = null
      val seen = scala.collection.mutable.ListBuffer.empty[String]
      var pages = 0
      while
        val r = c.listObjectsV2(ListObjectsV2Request.builder().bucket("list").maxKeys(2).continuationToken(token).build())
        seen ++= r.contents().asScala.map(_.key())
        token = r.nextContinuationToken()
        pages += 1
        r.isTruncated
      do ()
      val v1 = c.listObjects(ListObjectsRequest.builder().bucket("list").marker("b/4").build())
      val startAfter = c.listObjectsV2(ListObjectsV2Request.builder().bucket("list").startAfter("c").build())
      assertEquals(top.contents().asScala.map(_.key()).toList, List("c"))
      assertEquals(top.commonPrefixes().asScala.map(_.prefix()).toList, List("a/", "b/", "d/"))
      assertEquals(inA.contents().asScala.map(_.key()).toList, List("a/1", "a/2"))
      assertEquals(inA.commonPrefixes().asScala.map(_.prefix()).toList, List("a/b/"))
      assertEquals(seen.toList, keys)
      assertEquals(pages, 4)
      assertEquals(v1.contents().asScala.map(_.key()).toList, List("c", "d/5", "d/6"))
      assertEquals(startAfter.contents().asScala.map(_.key()).toList, List("d/5", "d/6"))
    }
  }

  test("マルチパートアップロード（UploadPartCopy、一覧、中止を含む）") {
    withClient { c =>
      c.createBucket(CreateBucketRequest.builder().bucket("mpu").build())
      val src = bytes(200 * 1024, 7)
      put(c, "mpu", "source", src)
      val p1 = bytes(150 * 1024, 2)
      val p3 = bytes(500, 3)
      val up = c.createMultipartUpload(CreateMultipartUploadRequest.builder().bucket("mpu").key("big").contentType("video/mp4").build())
      val id = up.uploadId()
      val e1 = c.uploadPart(UploadPartRequest.builder().bucket("mpu").key("big").uploadId(id).partNumber(1).build(), RequestBody.fromBytes(p1)).eTag()
      val e2 = c.uploadPartCopy(
        UploadPartCopyRequest.builder().sourceBucket("mpu").sourceKey("source").destinationBucket("mpu").destinationKey("big")
          .uploadId(id).partNumber(2).copySourceRange("bytes=1000-100999").build()
      ).copyPartResult().eTag()
      val e3 = c.uploadPart(UploadPartRequest.builder().bucket("mpu").key("big").uploadId(id).partNumber(3).build(), RequestBody.fromBytes(p3)).eTag()
      val parts = c.listParts(ListPartsRequest.builder().bucket("mpu").key("big").uploadId(id).build()).parts().asScala.map(_.partNumber().intValue).toList
      val uploads = c.listMultipartUploads(ListMultipartUploadsRequest.builder().bucket("mpu").build()).uploads().asScala.map(_.uploadId()).toList
      val done = c.completeMultipartUpload(
        CompleteMultipartUploadRequest.builder().bucket("mpu").key("big").uploadId(id).multipartUpload(
          CompletedMultipartUpload.builder().parts(
            CompletedPart.builder().partNumber(1).eTag(e1).build(),
            CompletedPart.builder().partNumber(2).eTag(e2).build(),
            CompletedPart.builder().partNumber(3).eTag(e3).build()
          ).build()
        ).build()
      )
      val got = get(c, "mpu", "big")
      val expected = p1 ++ src.slice(1000, 101000) ++ p3
      // 中止
      val id2 = c.createMultipartUpload(CreateMultipartUploadRequest.builder().bucket("mpu").key("aborted").build()).uploadId()
      c.uploadPart(UploadPartRequest.builder().bucket("mpu").key("aborted").uploadId(id2).partNumber(1).build(), RequestBody.fromBytes(p3))
      c.abortMultipartUpload(AbortMultipartUploadRequest.builder().bucket("mpu").key("aborted").uploadId(id2).build())
      val afterAbort = s3Code(c.uploadPart(UploadPartRequest.builder().bucket("mpu").key("aborted").uploadId(id2).partNumber(2).build(), RequestBody.fromBytes(p3)))
      val badPart = s3Code {
        val id3 = c.createMultipartUpload(CreateMultipartUploadRequest.builder().bucket("mpu").key("bad").build()).uploadId()
        c.completeMultipartUpload(
          CompleteMultipartUploadRequest.builder().bucket("mpu").key("bad").uploadId(id3).multipartUpload(
            CompletedMultipartUpload.builder().parts(CompletedPart.builder().partNumber(1).eTag("\"00\"").build()).build()
          ).build()
        )
      }
      assertEquals(parts, List(1, 2, 3))
      assert(uploads.contains(id))
      assert(done.eTag().endsWith("-3\""), done.eTag())
      assertEquals(got.asByteArray().length, expected.length)
      assert(java.util.Arrays.equals(got.asByteArray(), expected))
      assertEquals(got.response().contentType(), "video/mp4")
      assertEquals(afterAbort, "NoSuchUpload")
      assertEquals(badPart, "InvalidPart")
    }
  }

  test("コピー、一括削除、条件付き書き込み") {
    withClient { c =>
      c.createBucket(CreateBucketRequest.builder().bucket("ops").build())
      val data = bytes(100 * 1024, 9)
      put(c, "ops", "orig", data, "image/png")
      c.copyObject(CopyObjectRequest.builder().sourceBucket("ops").sourceKey("orig").destinationBucket("ops").destinationKey("copy").build())
      val copied = get(c, "ops", "copy")
      val self = s3Code(c.copyObject(CopyObjectRequest.builder().sourceBucket("ops").sourceKey("orig").destinationBucket("ops").destinationKey("orig").build()))
      val cond = s3Code(c.putObject(PutObjectRequest.builder().bucket("ops").key("orig").ifNoneMatch("*").build(), RequestBody.fromBytes(Array[Byte](1))))
      val condNew = s3Code(c.putObject(PutObjectRequest.builder().bucket("ops").key("fresh").ifNoneMatch("*").build(), RequestBody.fromBytes(Array[Byte](1))))
      val del = c.deleteObjects(
        DeleteObjectsRequest.builder().bucket("ops").delete(
          Delete.builder().objects(ObjectIdentifier.builder().key("orig").build(), ObjectIdentifier.builder().key("copy").build()).build()
        ).build()
      )
      val remaining = c.listObjectsV2(ListObjectsV2Request.builder().bucket("ops").build()).contents().asScala.map(_.key()).toList
      assert(java.util.Arrays.equals(copied.asByteArray(), data))
      assertEquals(copied.response().contentType(), "image/png")
      assertEquals(self, "InvalidRequest")
      assertEquals(cond, "PreconditionFailed")
      assertEquals(condNew, "OK")
      assertEquals(del.deleted().size, 2)
      assertEquals(remaining, List("fresh"))
    }
  }

  test("Presigned URL と、誤った秘密鍵の拒否") {
    IO.blocking {
      val c = client()
      c.createBucket(CreateBucketRequest.builder().bucket("pre").build())
      put(c, "pre", "file.txt", "presigned!".getBytes)
      val presigner = S3Presigner.builder()
        .endpointOverride(URI.create(endpoint))
        .region(Region.US_EAST_1)
        .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(ak, sk)))
        .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
        .build()
      val url = presigner.presignGetObject(
        GetObjectPresignRequest.builder().signatureDuration(java.time.Duration.ofMinutes(5))
          .getObjectRequest(GetObjectRequest.builder().bucket("pre").key("file.txt").build()).build()
      ).url()
      val http = HttpClient.newHttpClient()
      val ok = http.send(HttpRequest.newBuilder(url.toURI).build(), HttpResponse.BodyHandlers.ofString())
      val tampered = http.send(HttpRequest.newBuilder(URI.create(url.toString.replace("file.txt", "other.txt"))).build(), HttpResponse.BodyHandlers.ofString())
      val wrong = s3Code(client("wrong-secret").listBuckets())
      c.close()
      presigner.close()
      assertEquals(ok.statusCode(), 200)
      assertEquals(ok.body(), "presigned!")
      assertEquals(tampered.statusCode(), 403)
      assertEquals(wrong, "SignatureDoesNotMatch")
    }
  }

  test("ストレージクラス EC21 で書いたオブジェクトを読める") {
    withClient { c =>
      c.createBucket(CreateBucketRequest.builder().bucket("ecbucket").build())
      val data = bytes(500 * 1024 + 3, 21)
      c.putObject(PutObjectRequest.builder().bucket("ecbucket").key("ec.bin").storageClass("EC21").build(), RequestBody.fromBytes(data))
      val got = get(c, "ecbucket", "ec.bin")
      val head = c.headObject(HeadObjectRequest.builder().bucket("ecbucket").key("ec.bin").build())
      val range = c.getObjectAsBytes(GetObjectRequest.builder().bucket("ecbucket").key("ec.bin").range("bytes=100000-300000").build())
      assert(java.util.Arrays.equals(got.asByteArray(), data))
      assertEquals(head.storageClassAsString(), "EC21")
      assertEquals(range.asByteArray().toList, data.slice(100000, 300001).toList)
    }
  }

  test("バケット名の検証") {
    import scalas3.s3.S3Service.validBucketName
    assert(validBucketName("my-bucket.1"))
    assert(!validBucketName("Bad_Name"))
    assert(!validBucketName("ab"))
    assert(!validBucketName("a..b"))
    assert(!validBucketName("192.168.1.1"))
  }

  test("GC は参照されなくなったブロックだけを消す") {
    for
      _ <- withClient { c =>
        c.createBucket(CreateBucketRequest.builder().bucket("gcx").build())
        put(c, "gcx", "keep", bytes(200 * 1024, 11))
        put(c, "gcx", "drop", bytes(200 * 1024, 12))
        c.deleteObject(DeleteObjectRequest.builder().bucket("gcx").key("drop").build())
      }
      n = node()
      report <- BlockGc.run(n.store, n.blocks, 0.seconds)
      kept <- withClient(c => get(c, "gcx", "keep").asByteArray().toList)
    yield
      assert(report.deleted >= 4, s"削除したオブジェクトのブロックが消えるはず: $report")
      assertEquals(kept, bytes(200 * 1024, 11).toList)
  }
