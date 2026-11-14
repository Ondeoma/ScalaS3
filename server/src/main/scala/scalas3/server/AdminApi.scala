package scalas3.server

import cats.effect.IO
import cats.syntax.all.*
import io.circe.syntax.*
import io.circe.{Json, JsonObject}
import org.http4s.*
import org.http4s.circe.*
import org.http4s.dsl.io.*
import org.http4s.headers.Authorization
import scalas3.cluster.{Layout, NodeInfo, NodeState}
import scalas3.s3.*

import java.nio.charset.StandardCharsets.UTF_8
import java.security.SecureRandom

/** 管理用の HTTP API（docs/admin-api.yaml）。/metrics と /v1/health 以外はベアラートークンが必要。 */
final class AdminApi(node: CoreNode, token: String, metrics: Metrics, jobs: AdminApi.Jobs):
  import AdminApi.*

  private def authorized(req: Request[IO]): Boolean =
    req.headers.get[Authorization].exists {
      case Authorization(Credentials.Token(AuthScheme.Bearer, t)) => java.security.MessageDigest.isEqual(t.getBytes(UTF_8), token.getBytes(UTF_8))
      case _                                                      => false
    }

  private def store = node.store

  val routes: HttpRoutes[IO] = HttpRoutes.of[IO] {
    case GET -> Root / "metrics" => Ok(metrics.render)
    case GET -> Root / "v1" / "health" => Ok(Json.obj("status" -> "ok".asJson))
    case req if !authorized(req) => IO.pure(Response[IO](Status.Unauthorized).withEntity(err("unauthorized")))

    case GET -> Root / "v1" / "status" =>
      for
        layout <- node.layout.get
        ranges <- node.meta.ranges.all.flatMap(_.traverse(r =>
          (r.node.status, r.sm.current).mapN((st, d) =>
            Json.obj(
              "id" -> r.rangeId.asJson,
              "role" -> st.role.toString.asJson,
              "term" -> st.term.asJson,
              "leader" -> st.leader.asJson,
              "commit" -> st.commit.asJson,
              "applied" -> st.applied.asJson,
              "voters" -> st.membership.voters.toList.sorted.asJson,
              "replicas" -> d.map(_.replicas.toList.sorted).asJson
            )
          )
        ))
        now <- IO.realTime.map(_.toMillis)
        health = node.health.snapshot.toList.sortBy(_._1).map((n, h) =>
          n -> Json.obj("up" -> h.up.asJson, "lastSeenSecondsAgo" -> ((now - h.lastSeen) / 1000).asJson, "features" -> h.features.asJson)
        )
        resp <- Ok(Json.obj("node" -> node.id.asJson, "layout" -> layoutJson(layout), "health" -> Json.obj(health*), "ranges" -> ranges.asJson))
      yield resp

    case GET -> Root / "v1" / "layout" => node.layout.get.flatMap(l => Ok(layoutJson(l)))

    case req @ PUT -> Root / "v1" / "layout" / "nodes" / id =>
      req.as[Json].flatMap { j =>
        val c = j.hcursor
        Layout.update(node.meta.client) { l =>
          val cur = l.nodes.get(id)
          val addr = c.get[String]("address").toOption.map(parseHostPort).orElse(cur.map(_.address))
          addr.toRight(s"node $id is unknown; give its address").map { a =>
            val info = NodeInfo(
              a,
              c.get[String]("zone").toOption.orElse(cur.map(_.zone)).getOrElse("default"),
              c.get[Int]("weight").toOption.orElse(cur.map(_.weight)).getOrElse(100),
              c.get[String]("state").toOption.map(s => NodeState.valueOf(s.capitalize)).orElse(cur.map(_.state)).getOrElse(NodeState.Active)
            )
            l.copy(nodes = l.nodes.updated(id, info))
          }
        }.flatMap(l => node.refreshLayout >> Ok(layoutJson(l)))
      }.handleErrorWith(e => BadRequest(err(e.getMessage)))

    case DELETE -> Root / "v1" / "layout" / "nodes" / id =>
      Layout.update(node.meta.client) { l =>
        l.nodes.get(id) match
          case None                                          => Left(s"node $id is unknown")
          case Some(n) if n.state != NodeState.Draining      => Left(s"node $id must be Draining before it is removed")
          case Some(_)                                       => Right(l.copy(nodes = l.nodes - id))
      }.flatMap(l => node.refreshLayout >> Ok(layoutJson(l))).handleErrorWith(e => BadRequest(err(e.getMessage)))

    case GET -> Root / "v1" / "keys" =>
      store.listAccessKeys.flatMap(ks => Ok(ks.map(keyJson).asJson))

    case req @ POST -> Root / "v1" / "keys" =>
      req.as[Json].flatMap { j =>
        val k = AccessKey(
          j.hcursor.get[String]("id").getOrElse("SK" + randomId(18).toUpperCase),
          randomId(40),
          j.hcursor.get[String]("name").getOrElse(""),
          j.hcursor.get[Boolean]("admin").getOrElse(false),
          Map.empty
        )
        store.putAccessKey(k) >> Created(keyJson(k).deepMerge(Json.obj("secret" -> k.secret.asJson)))
      }

    case DELETE -> Root / "v1" / "keys" / id => store.deleteAccessKey(id) >> NoContent()

    case req @ PUT -> Root / "v1" / "keys" / id / "grants" =>
      req.as[Json].flatMap { j =>
        val c = j.hcursor
        val bucket = c.get[String]("bucket").getOrElse("")
        val bits = (if c.get[Boolean]("read").getOrElse(false) then Perm.Read.bit else 0) |
          (if c.get[Boolean]("write").getOrElse(false) then Perm.Write.bit else 0) |
          (if c.get[Boolean]("owner").getOrElse(false) then Perm.Owner.bit else 0)
        store.updateGrants(id, g => if bits == 0 then g - bucket else g.updated(bucket, bits)).flatMap {
          case None    => NotFound(err(s"key $id not found"))
          case Some(k) => Ok(keyJson(k))
        }
      }

    case GET -> Root / "v1" / "buckets" =>
      store.listBuckets.flatMap(bs =>
        Ok(bs.map(b => Json.obj("name" -> b.name.asJson, "id" -> b.id.asJson, "owner" -> b.owner.asJson, "config" -> b.config.keys.toList.sorted.asJson)).asJson)
      )

    case GET -> Root / "v1" / "remotes" =>
      store.listRemotes.flatMap(rs => Ok(rs.map(r => Json.obj("name" -> r.name.asJson, "endpoint" -> r.endpoint.asJson, "region" -> r.region.asJson, "accessKey" -> r.accessKey.asJson)).asJson))

    case req @ PUT -> Root / "v1" / "remotes" / name =>
      req.as[Json].flatMap { j =>
        val c = j.hcursor
        (c.get[String]("endpoint"), c.get[String]("accessKey"), c.get[String]("secretKey")).mapN { (ep, ak, sk) =>
          store.putRemote(Remote(name, ep, c.get[String]("region").getOrElse("us-east-1"), ak, sk)) >> NoContent()
        }.getOrElse(BadRequest(err("endpoint, accessKey and secretKey are required")))
      }

    case DELETE -> Root / "v1" / "remotes" / name => store.deleteRemote(name) >> NoContent()

    case POST -> Root / "v1" / "jobs" / job =>
      jobs.get(job) match
        case None      => NotFound(err(s"unknown job $job (${jobs.keys.toList.sorted.mkString(", ")})"))
        case Some(run) => run.flatMap(r => Ok(Json.obj("job" -> job.asJson, "result" -> r.toString.asJson)))
  }

object AdminApi:
  type Jobs = Map[String, IO[Any]]

  private val rnd = new SecureRandom()
  private val alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
  def randomId(n: Int): String = Iterator.continually(alphabet(rnd.nextInt(alphabet.length))).take(n).mkString

  def err(msg: String): Json = Json.obj("error" -> msg.asJson)

  def keyJson(k: AccessKey): Json =
    Json.obj(
      "id" -> k.id.asJson,
      "name" -> k.name.asJson,
      "admin" -> k.admin.asJson,
      "grants" -> Json.fromJsonObject(JsonObject.fromIterable(k.grants.toList.sorted.map((b, g) =>
        b -> Json.obj("read" -> ((g & Perm.Read.bit) != 0).asJson, "write" -> ((g & Perm.Write.bit) != 0).asJson, "owner" -> ((g & Perm.Owner.bit) != 0).asJson)
      )))
    )

  def layoutJson(l: Layout): Json =
    Json.obj(
      "version" -> l.version.asJson,
      "nodes" -> Json.fromJsonObject(JsonObject.fromIterable(l.nodes.toList.sortBy(_._1).map((id, n) =>
        id -> Json.obj("address" -> n.address.toString.asJson, "zone" -> n.zone.asJson, "weight" -> n.weight.asJson, "state" -> n.state.toString.asJson)
      )))
    )

