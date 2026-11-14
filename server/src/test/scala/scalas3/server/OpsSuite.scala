package scalas3.server

import cats.effect.IO
import cats.syntax.all.*
import fs2.io.file.Files
import munit.CatsEffectSuite
import scalas3.runtime.NodeAddress
import scalas3.s3.{Lifecycle, S3Config}
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.model.*
import software.amazon.awssdk.services.s3.{S3Client, S3Configuration}

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

/** M8 の運用機能（ライフサイクル、クラスタ間複製、Admin API、メトリクス）。 */
class OpsSuite extends CatsEffectSuite:

  override def munitIOTimeout: Duration = 3.minutes

  private val token = "admin-token-for-test"

  private def nodeRes(ak: String, sk: String) =
    for
      tmp <- Files[IO].tempDirectory
      n <- S3Node.resource(
        NodeConfig(
          dataDir = tmp,
          s3Host = "127.0.0.1",
          s3Port = 0,
          rpcListen = NodeAddress("127.0.0.1", 0),
          s3 = S3Config(blockSize = 64 * 1024, inlineThreshold = 1024, minPartSize = 1),
          adminKey = Some((ak, sk)),
          adminListen = Some(NodeAddress("127.0.0.1", 0)),
          adminToken = Some(token),
          gcInterval = 1000.days,
          lifecycleInterval = 1000.days,
          replicationInterval = 1000.days
        )
      )
    yield n

  private val nodes = ResourceSuiteLocalFixture(
    "nodes",
    (nodeRes("AKIDOPSA00000000001", "opsasecret0000000000000000000000000001"), nodeRes("AKIDOPSB00000000001", "opsbsecret0000000000000000000000000001")).tupled
  )
  override def munitFixtures = List(nodes)

  private def client(n: RunningNode, ak: String, sk: String): S3Client =
    S3Client.builder().endpointOverride(URI.create(s"http://127.0.0.1:${n.s3Address.port}")).region(Region.US_EAST_1)
      .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(ak, sk)))
      .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build()).build()

  private def a = nodes()._1
  private def b = nodes()._2
  private def ca = client(a, "AKIDOPSA00000000001", "opsasecret0000000000000000000000000001")
  private def cb = client(b, "AKIDOPSB00000000001", "opsbsecret0000000000000000000000000001")

  private val http = HttpClient.newHttpClient()
  private def admin(n: RunningNode, method: String, path: String, body: String = "", auth: Boolean = true) =
    val r = HttpRequest.newBuilder(URI.create(s"http://127.0.0.1:${n.adminAddress.get.port}$path"))
      .method(method, if body.isEmpty then HttpRequest.BodyPublishers.noBody() else HttpRequest.BodyPublishers.ofString(body))
      .header("Content-Type", "application/json")
    if auth then r.header("Authorization", s"Bearer $token"): Unit
    http.send(r.build(), HttpResponse.BodyHandlers.ofString())

  private def keys(c: S3Client, bucket: String) =
    c.listObjectsV2(ListObjectsV2Request.builder().bucket(bucket).build()).contents().asScala.map(_.key()).toList.sorted

  test("ライフサイクル：期限切れ、古い版、残った削除マーカー、未完了のマルチパート") {
    for
      _ <- IO.blocking {
        val c = ca
        c.createBucket(CreateBucketRequest.builder().bucket("life").build())
        c.putBucketVersioning(PutBucketVersioningRequest.builder().bucket("life")
          .versioningConfiguration(VersioningConfiguration.builder().status(BucketVersioningStatus.ENABLED).build()).build())
        c.putObject(PutObjectRequest.builder().bucket("life").key("tmp/a").build(), RequestBody.fromString("a"))
        c.putObject(PutObjectRequest.builder().bucket("life").key("keep/b").build(), RequestBody.fromString("b1"))
        c.putObject(PutObjectRequest.builder().bucket("life").key("keep/b").build(), RequestBody.fromString("b2"))
        c.createMultipartUpload(CreateMultipartUploadRequest.builder().bucket("life").key("tmp/upload").build())
        c.putBucketLifecycleConfiguration(PutBucketLifecycleConfigurationRequest.builder().bucket("life").lifecycleConfiguration(
          BucketLifecycleConfiguration.builder().rules(
            LifecycleRule.builder().id("tmp").status(ExpirationStatus.ENABLED).filter(LifecycleRuleFilter.builder().prefix("tmp/").build())
              .expiration(LifecycleExpiration.builder().days(1).build())
              .abortIncompleteMultipartUpload(AbortIncompleteMultipartUpload.builder().daysAfterInitiation(1).build()).build(),
            LifecycleRule.builder().id("old").status(ExpirationStatus.ENABLED).filter(LifecycleRuleFilter.builder().prefix("").build())
              .noncurrentVersionExpiration(NoncurrentVersionExpiration.builder().noncurrentDays(1).build())
              .expiration(LifecycleExpiration.builder().expiredObjectDeleteMarker(true).build()).build()
          ).build()).build())
        c.close()
      }
      now <- IO.realTime.map(_.toMillis)
      r1 <- Lifecycle.run(a.store, now + 2L * 24 * 3600 * 1000)
      // 1 回目で tmp/a に削除マーカーが付き、古い版が消える。2 回目で古い版と残った削除マーカーが消える
      r2 <- Lifecycle.run(a.store, now + 4L * 24 * 3600 * 1000)
      state <- IO.blocking {
        val c = ca
        val ks = keys(c, "life")
        val versions = c.listObjectVersions(ListObjectVersionsRequest.builder().bucket("life").build())
        val vs = versions.versions().asScala.map(v => (v.key(), v.isLatest.booleanValue)).toList
        val markers = versions.deleteMarkers().asScala.map(_.key()).toList
        val uploads = c.listMultipartUploads(ListMultipartUploadsRequest.builder().bucket("life").build()).uploads().asScala.toList
        c.close()
        (ks, vs, markers, uploads)
      }
    yield
      val (ks, vs, markers, uploads) = state
      assertEquals(r1.expired, 1)
      assertEquals(r1.aborted, 1)
      assert(r1.noncurrent + r2.noncurrent >= 2, s"$r1 $r2")
      assertEquals(ks, List("keep/b"))
      assertEquals(vs, List(("keep/b", true)))
      assertEquals(markers, Nil)
      assertEquals(uploads, Nil)
  }

  test("クラスタ間の非同期複製：書き込みと削除が転送先に反映される") {
    for
      _ <- IO.blocking {
        cb.createBucket(CreateBucketRequest.builder().bucket("mirror").build())
        val reg = admin(a, "PUT", "/v1/remotes/backup",
          s"""{"endpoint":"http://127.0.0.1:${b.s3Address.port}","accessKey":"AKIDOPSB00000000001","secretKey":"opsbsecret0000000000000000000000000001"}""")
        assertEquals(reg.statusCode(), 204, reg.body())
        val c = ca
        c.createBucket(CreateBucketRequest.builder().bucket("origin").build())
        c.putBucketReplication(PutBucketReplicationRequest.builder().bucket("origin").replicationConfiguration(
          ReplicationConfiguration.builder().role("none").rules(ReplicationRule.builder().id("all").status(ReplicationRuleStatus.ENABLED)
            .priority(1).filter(ReplicationRuleFilter.builder().prefix("").build())
            .destination(Destination.builder().bucket("arn:aws:s3:::mirror").build()).build()).build()).build())
        c.close()
      }
      // SDK は独自要素 <Remote> を送れないので、設定は直接書き直す
      _ <- a.store.updateBucketConfig("origin", "replication",
        Some("""<ReplicationConfiguration><Rule><ID>all</ID><Status>Enabled</Status><Prefix></Prefix><Destination><Bucket>arn:aws:s3:::mirror</Bucket><Remote>backup</Remote></Destination></Rule></ReplicationConfiguration>"""))
      pending <- IO.blocking {
        val c = ca
        c.putObject(PutObjectRequest.builder().bucket("origin").key("doc.txt").contentType("text/plain").build(), RequestBody.fromString("hello mirror"))
        c.putObject(PutObjectRequest.builder().bucket("origin").key("big.bin").build(), RequestBody.fromBytes(Array.fill(300 * 1024)(7.toByte)))
        c.putObject(PutObjectRequest.builder().bucket("origin").key("gone").build(), RequestBody.fromString("x"))
        c.deleteObject(DeleteObjectRequest.builder().bucket("origin").key("gone").build())
        val st = c.headObject(HeadObjectRequest.builder().bucket("origin").key("doc.txt").build()).replicationStatusAsString()
        c.close()
        st
      }
      report <- a.jobs("replication")
      result <- IO.blocking {
        val c = cb
        val got = c.getObjectAsBytes(GetObjectRequest.builder().bucket("mirror").key("doc.txt").build())
        val big = c.getObjectAsBytes(GetObjectRequest.builder().bucket("mirror").key("big.bin").build()).asByteArray()
        val ks = keys(c, "mirror")
        c.close()
        val st = ca.headObject(HeadObjectRequest.builder().bucket("origin").key("doc.txt").build()).replicationStatusAsString()
        (got.asUtf8String(), got.response().contentType(), big.length, ks, st)
      }
    yield
      assertEquals(pending, "PENDING")
      assertEquals(result._1, "hello mirror")
      assertEquals(result._2, "text/plain")
      assertEquals(result._3, 300 * 1024)
      assertEquals(result._4, List("big.bin", "doc.txt"))
      assertEquals(result._5, "COMPLETED", report.toString)
  }

  test("Admin API：認証、キーの発行と権限、レイアウト、メトリクス") {
    IO.blocking {
      val unauth = admin(a, "GET", "/v1/status", auth = false)
      val status = admin(a, "GET", "/v1/status")
      val created = admin(a, "POST", "/v1/keys", """{"name":"app"}""")
      val json = io.circe.parser.parse(created.body()).toOption.get.hcursor
      val id = json.get[String]("id").toOption.get
      val secret = json.get[String]("secret").toOption.get
      val c = ca
      c.createBucket(CreateBucketRequest.builder().bucket("apps").build())
      c.close()
      val app = client(a, id, secret)
      val before = try { app.listObjectsV2(ListObjectsV2Request.builder().bucket("apps").build()); "OK" } catch case e: S3Exception => e.awsErrorDetails().errorCode()
      val grant = admin(a, "PUT", s"/v1/keys/$id/grants", """{"bucket":"apps","read":true,"write":true}""")
      app.putObject(PutObjectRequest.builder().bucket("apps").key("x").build(), RequestBody.fromString("ok"))
      val after = keys(app, "apps")
      app.close()
      val zone = admin(a, "PUT", s"/v1/layout/nodes/node1", """{"zone":"rack-9","weight":200}""")
      val removeActive = admin(a, "DELETE", "/v1/layout/nodes/node1")
      val metrics = admin(a, "GET", "/metrics", auth = false)
      val job = admin(a, "POST", "/v1/jobs/gc")
      assertEquals(unauth.statusCode(), 401)
      assertEquals(status.statusCode(), 200)
      assert(status.body().contains("\"ranges\""))
      assertEquals(created.statusCode(), 201)
      assertEquals(before, "AccessDenied")
      assertEquals(grant.statusCode(), 200)
      assertEquals(after, List("x"))
      assertEquals(zone.statusCode(), 200)
      assert(zone.body().contains("rack-9"))
      assertEquals(removeActive.statusCode(), 400)
      assert(metrics.body().contains("scalas3_s3_requests_total"), metrics.body().take(500))
      assert(metrics.body().contains("scalas3_ranges"))
      assertEquals(job.statusCode(), 200)
    }
  }

