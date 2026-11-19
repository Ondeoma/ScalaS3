package scalas3.server

import cats.effect.{IO, Ref}
import cats.syntax.all.*
import fs2.Stream
import fs2.io.file.Path
import munit.CatsEffectSuite
import scalas3.meta.MetaConfig
import scalas3.raft.{NodeId, RaftConfig, RaftNodeConfig}
import scalas3.runtime.NodeAddress
import scalas3.s3.*
import scalas3.sim.{SimFileSystem, SimNetwork}
import scodec.bits.ByteVector

import scala.concurrent.duration.*

/** 同じ種で同じシナリオを動かすと、ネットワーク上のやり取りがまったく同じになることを確かめる。 */
class DeterminismSuite extends CatsEffectSuite:

  override def munitIOTimeout: Duration = 5.minutes

  private def cfg(id: NodeId, peers: Set[NodeId], seed: Long) = NodeConfig(
    nodeId = id,
    dataDir = Path(s"/$id"),
    disks = Vector(Path(s"/$id/d0")),
    rpcListen = NodeAddress(id, 7000),
    peers = (peers - id).map(p => p -> NodeAddress(p, 7000)).toMap,
    zone = s"zone-$id",
    s3 = S3Config(blockSize = 1024, inlineThreshold = 64),
    repairInterval = 1000.days,
    scrubInterval = 1000.days,
    gcInterval = 1000.days,
    layoutRefresh = 300.millis,
    meta = MetaConfig(
      raft = RaftNodeConfig(raft = RaftConfig(electionTicks = 10), tickInterval = 50.millis, snapshotThreshold = 200),
      splitBytes = 4000,
      maintenanceInterval = 300.millis,
      mergeCheckInterval = Duration.Zero,
      requestTimeout = 5.seconds,
      splitBoundary = S3Keys.splitBoundary
    ),
    seed = seed
  )

  private val admin = AccessKey("admin", "secret", "admin", admin = true, Map.empty)

  private def retry[A](io: IO[A], n: Int = 100): IO[A] =
    io.handleErrorWith(e => if n <= 0 then IO.raiseError(e) else IO.sleep(200.millis) >> retry(io, n - 1))

  private def scenario: IO[Vector[String]] =
    val ids = Set("a", "b", "c")
    for
      net <- SimNetwork.create(5L, trace = true)
      running <- Ref.of[IO, Map[NodeId, (CoreNode, IO[Unit])]](Map.empty)
      disks <- ids.toList.sorted.traverse(id => SimFileSystem.create(id.hashCode.toLong).map(id -> _)).map(_.toMap)
      start = (id: NodeId, seed: Long) =>
        CoreNode.resource(cfg(id, ids, seed), disks(id), net.transport(id)).allocated.flatMap(r => running.update(_.updated(id, r)))
      _ <- ids.toList.sorted.zipWithIndex.traverse_((id, i) => start(id, i.toLong))
      a <- running.get.map(_("a")._1)
      bucket <- retry(a.store.createBucket("d", admin, 0L))
      put = (n: CoreNode, k: String) =>
        n.data.ingest(Stream.chunk(fs2.Chunk.byteVector(ByteVector.fill(3000)(k.length.toByte))), false, None).flatMap { ing =>
          n.store.putObject(bucket, k, ObjectRecord("null", ing.size, ing.md5.toHex, 0L, Map.empty, Map.empty, "STANDARD", None, ing.body), false, None)
            .guarantee(n.data.release(ing.body))
        }
      _ <- (0 until 5).toList.traverse_(i => retry(put(a, s"k$i")))
      _ <- running.modify(m => (m - "c", m.get("c"))).flatMap(_.traverse_(_._2)) >> disks("c").powerFail
      _ <- (5 until 8).toList.traverse_(i => retry(put(a, s"k$i")))
      _ <- start("c", 9L)
      _ <- IO.sleep(3.seconds)
      _ <- running.get.flatMap(_.values.toList.traverse_(_._2.attempt))
      log <- net.traceLog
    yield log

  test("同じシナリオを 2 回動かすと、ネットワーク上のやり取りが一致する") {
    for
      first <- scalas3.sim.Sim.run(scenario)
      second <- scalas3.sim.Sim.run(scenario)
    yield
      val diff = first.zip(second).indexWhere(_ != _)
      if diff >= 0 || first.size != second.size then
        val at = if diff >= 0 then diff else math.min(first.size, second.size)
        println(s"[determinism] diverged at $at of ${first.size}/${second.size}")
        (math.max(0, at - 5) until math.min(at + 5, math.min(first.size, second.size))).foreach(i => println(s"  $i: ${first(i)}  |  ${second(i)}"))
      assert(first.nonEmpty)
      assertEquals(first.size, second.size)
      assertEquals(diff, -1)
  }
