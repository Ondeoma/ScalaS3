package scalas3.s3

import cats.effect.{IO, Resource}
import cats.syntax.all.*
import fs2.Stream
import org.http4s.client.Client
import org.http4s.ember.client.EmberClientBuilder
import org.http4s.{Header, Headers, Method, Request, Uri}
import org.typelevel.ci.CIString

import scala.concurrent.duration.*

/** 他の S3 互換エンドポイントへ書くための最小限のクライアント（SigV4、UNSIGNED-PAYLOAD）。 */
final class RemoteS3(client: Client[IO]):

  private def signed(r: Remote, method: Method, bucket: String, key: String, extra: Map[String, String], body: Stream[IO, Byte]): IO[Request[IO]] =
    IO.realTime.flatMap { now =>
      IO.fromEither(Uri.fromString(r.endpoint.stripSuffix("/"))).map { base =>
        val amzDate = SigV4.formatAmzDate(now.toMillis)
        val scope = SigV4.Scope(amzDate.take(8), r.region, "s3")
        val path = "/" + bucket + "/" + key
        val host = base.authority.fold("")(_.renderString)
        val hs = extra ++ Map("host" -> host, "x-amz-date" -> amzDate, "x-amz-content-sha256" -> SigV4.UnsignedPayload)
        val names = hs.keys.map(_.toLowerCase).toList.sorted
        val lower = hs.map((k, v) => k.toLowerCase -> v)
        val canonical = SigV4.canonicalRequest(method.name, path, Nil, n => lower.get(n).toList, names, SigV4.UnsignedPayload)
        val sig = SigV4.sign(SigV4.signingKey(r.secret, scope), SigV4.stringToSign(amzDate, scope, canonical))
        val auth = s"${SigV4.Algorithm} Credential=${r.accessKey}/${scope.render}, SignedHeaders=${names.mkString(";")}, Signature=$sig"
        val uri = Uri.unsafeFromString(base.renderString + "/" + SigV4.uriEncode(bucket, true) + "/" + SigV4.uriEncode(key, encodeSlash = false))
        Request[IO](method, uri, headers = Headers((hs - "host").toList.map((k, v) => Header.Raw(CIString(k), v)) :+ Header.Raw(CIString("Authorization"), auth)))
          .withBodyStream(body)
      }
    }

  /** 送って応答を読み捨てる。accept に当てはまらない状態なら失敗にする。 */
  private def send(req: Request[IO], accept: org.http4s.Status => Boolean): IO[Unit] =
    client.run(req).use(resp =>
      resp.body.compile.drain >> IO.raiseUnless(accept(resp.status))(new Exception(s"remote ${req.method} failed: ${resp.status}"))
    )

  def put(r: Remote, bucket: String, key: String, size: Long, headers: Map[String, String], body: Stream[IO, Byte]): IO[Unit] =
    signed(r, Method.PUT, bucket, key, headers ++ Map("content-length" -> size.toString), body).flatMap(send(_, _.isSuccess))

  def delete(r: Remote, bucket: String, key: String): IO[Unit] =
    signed(r, Method.DELETE, bucket, key, Map.empty, Stream.empty).flatMap(send(_, s => s.isSuccess || s.code == 404))

object RemoteS3:
  def resource: Resource[IO, RemoteS3] =
    EmberClientBuilder.default[IO].withTimeout(10.minutes).withIdleConnectionTime(1.minute).build.map(RemoteS3(_))

/** クラスタ間の非同期複製。複製待ちの印があるオブジェクトの「今の状態」を転送先に反映する
  * （何度変わっても最後の状態だけ送る）。SSE-C のオブジェクトは鍵が無いので送れない（印を残す）。
  */
object Replicator:
  final case class Report(replicated: Int, deleted: Int, failed: Int):
    def +(o: Report): Report = Report(replicated + o.replicated, deleted + o.deleted, failed + o.failed)

  private val Replicated = Report(1, 0, 0)
  private val Deleted = Report(0, 1, 0)
  private val Failed = Report(0, 0, 1)
  private val Empty = Report(0, 0, 0)

  def run(store: ObjectStore, data: DataPath, master: Option[Array[Byte]], remote: RemoteS3): IO[Report] =
    store.listBuckets.flatMap { buckets =>
      ReplicationConfig.configured(buckets).foldLeftM(Empty) {
        case (acc, (b, rules)) =>
          store.pendingReplication(b).evalMap { (key, seen) =>
            rules.find(r => key.startsWith(r.prefix)) match
              case None => store.clearReplication(b, key, seen).as(Empty)
              case Some(rule) =>
                store.getRemote(rule.remote).flatMap {
                  case None => IO.pure(Failed)
                  case Some(target) =>
                    val work = store.getObject(b, key).flatMap {
                      case None => remote.delete(target, rule.bucket, key).as(Deleted)
                      case Some(o) =>
                        plainKey(o.encryption, master).flatMap { dk =>
                          val hs = o.headers ++ o.userMeta.map((k, v) => s"x-amz-meta-$k" -> v)
                          remote.put(target, rule.bucket, key, o.size, hs, data.read(o.body, 0, o.size, dk)).as(Replicated)
                        }
                    }
                    work.flatTap(_ => store.clearReplication(b, key, seen)).handleError(_ => Failed)
                }
          }.compile.fold(acc)(_ + _)
      }
    }

  /** 平文に戻すための鍵。SSE-C は鍵が無いので送れない。SSE-S3 でマスター鍵が無ければ、暗号文を平文として送らないように失敗させる。 */
  private def plainKey(enc: Option[Encryption], master: Option[Array[Byte]]): IO[Option[Array[Byte]]] =
    enc match
      case None                     => IO.pure(None)
      case Some(Encryption.SseC(_)) => IO.raiseError(new Exception("SSE-C objects cannot be replicated"))
      case Some(Encryption.SseS3(w)) =>
        IO.fromOption(master)(new Exception("SSE-S3 objects cannot be replicated without the master key")).map(m => Some(Crypto.unwrap(m, w)))
