package scalas3.server

import cats.effect.{IO, Ref}
import cats.syntax.all.*
import fs2.Stream
import fs2.io.file.Path
import munit.CatsEffectSuite
import scalas3.cluster.{Layout, NodeState, Placement}
import scalas3.meta.{Keys, MetaConfig, RangeDescriptor}
import scalas3.raft.{NodeId, RaftConfig, RaftNodeConfig}
import scalas3.runtime.NodeAddress
import scalas3.s3.*
import scalas3.sim.{SimFileSystem, SimNetwork}
import scodec.bits.ByteVector

import scala.concurrent.duration.*

/** 複数ノードのクラスタを、シミュレーションのネットワークとディスクの上で動かす。 */
class ClusterSimSuite extends CatsEffectSuite:

  override def munitIOTimeout: Duration = 10.minutes

  private def addr(id: NodeId) = NodeAddress(id, 7000)
  private val admin = AccessKey("admin", "secret", "admin", admin = true, Map.empty)

  private def cfg(id: NodeId, peers: Set[NodeId], bootstrap: Boolean, seed: Long, replicas: Int = 3) = NodeConfig(
    nodeId = id,
    dataDir = Path(s"/$id"),
    disks = Vector(Path(s"/$id/d0"), Path(s"/$id/d1")),
    rpcListen = addr(id),
    peers = (peers - id).map(p => p -> addr(p)).toMap,
    bootstrap = bootstrap,
    zone = s"zone-$id",
    s3 = S3Config(blockSize = 1024, inlineThreshold = 64),
    copies = 3,
    replicas = replicas,
    repairInterval = 1000.days, // テストから明示的に呼ぶ
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

  private final class Cluster(net: SimNetwork, disks: Ref[IO, Map[NodeId, SimFileSystem]], running: Ref[IO, Map[NodeId, (CoreNode, IO[Unit])]]):
    def start(c: NodeConfig): IO[Unit] =
      for
        fs <- disks.get.map(_.get(c.nodeId)).flatMap(_.fold(SimFileSystem.create(c.seed).flatTap(f => disks.update(_.updated(c.nodeId, f))))(IO.pure))
        started <- CoreNode.resource(c, fs, net.transport(c.nodeId)).allocated
        _ <- running.update(_.updated(c.nodeId, started))
      yield ()
    def stop(id: NodeId): IO[Unit] = running.modify(m => (m - id, m.get(id))).flatMap(_.traverse_(_._2.attempt))
    def crash(id: NodeId): IO[Unit] = stop(id) >> disks.get.flatMap(_.get(id).traverse_(_.powerFail))
    def node(id: NodeId): IO[CoreNode] = running.get.map(_(id)._1)
    def nodes: IO[Map[NodeId, CoreNode]] = running.get.map(_.view.mapValues(_._1).toMap)
    def stopAll: IO[Unit] = running.get.flatMap(_.keys.toList.traverse_(stop))

  private def withCluster[A](seed: Long)(f: (SimNetwork, Cluster) => IO[A]): IO[A] =
    for
      net <- SimNetwork.create(seed)
      disks <- Ref.of[IO, Map[NodeId, SimFileSystem]](Map.empty)
      running <- Ref.of[IO, Map[NodeId, (CoreNode, IO[Unit])]](Map.empty)
      c = Cluster(net, disks, running)
      r <- f(net, c).onError(e => IO.println(s"[dump] failed: $e") >> dump(c).attempt.void).guarantee(c.stopAll)
    yield r

  private def retry[A](io: IO[A], n: Int = 100): IO[A] =
    io.handleErrorWith(e => if n <= 0 then IO.raiseError(e) else IO.sleep(200.millis) >> retry(io, n - 1))

  private def dump(c: Cluster): IO[Unit] =
    c.nodes.flatMap(_.toList.sortBy(_._1).traverse_ { (id, n) =>
      n.meta.ranges.all.flatMap(_.traverse(r => (r.node.status, r.sm.current).mapN((st, d) =>
        s"r${r.rangeId}:${st.role}/t${st.term}/c${st.commit}/a${st.applied}/v${st.membership.voters.toList.sorted.mkString}" +
          s"/l${st.membership.learners.toList.sorted.mkString}/o${st.membership.outgoing.toList.sorted.mkString}/d${d.map(_.replicas.toList.sorted.mkString)}"
      ))).flatMap(ss => IO.println(s"[dump] $id ${ss.mkString(" ")}"))
    })

  /** S3 の PUT と同じ手順でオブジェクトを書く。 */
  private def putObject(n: CoreNode, b: Bucket, key: String, data: ByteVector, r: Redundancy = Redundancy.Replicated): IO[Unit] =
    n.data.ingest(Stream.chunk(fs2.Chunk.byteVector(data)), wantSha256 = false, None, r).flatMap { ing =>
      val rec = ObjectRecord("null", ing.size, ing.md5.toHex, 0L, Map.empty, Map.empty, "STANDARD", None, ing.body)
      n.store.putObject(b, key, rec, ifNoneMatch = false, None).guarantee(n.data.release(ing.body)).void
    }

  private def getObject(n: CoreNode, b: Bucket, key: String): IO[ByteVector] =
    n.store.getObject(b, key).flatMap(IO.fromOption(_)(new Exception(s"missing $key"))).flatMap { o =>
      n.data.read(o.body, 0, o.size).compile.to(ByteVector)
    }

  private def payload(i: Int): ByteVector =
    val r = new scala.util.Random(i)
    ByteVector.view(Array.fill(500 + r.nextInt(4000))(r.nextInt().toByte))

  test("3 ノード → 1 台停止 → 復帰と修復 → 4 台目の追加と 1 台の取り外し") {
    val prog = withCluster(1L) { (net, c) =>
      val initial = Set("a", "b", "c")
      for
        _ <- initial.toList.sorted.zipWithIndex.traverse_((id, i) => c.start(cfg(id, initial, bootstrap = true, i.toLong + 1)))
        a <- c.node("a")
        bucket <- retry(a.store.createBucket("data", admin, 0L))
        // ---- 3 ノードで書く ----
        _ <- (0 until 20).toList.traverse_(i => retry(putObject(a, bucket, s"obj-$i", payload(i))))
        all <- c.nodes
        blockIds <- a.store.referencedBlocks.compile.toVector.map(_.distinct)
        copies1 <- blockIds.traverse(id => all.values.toList.traverse(_.local.has(id)).map(_.count(identity)))
        // ---- c を止める（電源断）。読み書きは続けられる ----
        _ <- c.crash("c")
        b <- c.node("b")
        readWhileDown <- (0 until 20).toList.traverse(i => retry(getObject(b, bucket, s"obj-$i")))
        _ <- (20 until 30).toList.traverse_(i => retry(putObject(b, bucket, s"obj-$i", payload(i))))
        // ---- c を戻し、修復で欠けたコピーを補う ----
        _ <- c.start(cfg("c", initial, bootstrap = true, 3L))
        _ <- IO.sleep(3.seconds)
        _ <- List("a", "b").traverse_(id => c.node(id).flatMap(_.repair))
        cNode <- c.node("c")
        blockIds2 <- a.store.referencedBlocks.compile.toVector.map(_.distinct)
        onC <- blockIds2.traverse(cNode.local.has)
        // ---- d を追加し、c を取り外し準備にする ----
        _ <- c.start(cfg("d", initial, bootstrap = false, 4L))
        _ <- retry(a.refreshLayout >> a.layout.get.flatMap(l => IO.raiseUnless(l.nodes.contains("d"))(new Exception("d not in layout yet"))))
        _ <- Layout.update(a.meta.client)(l => Right(l.copy(nodes = l.nodes.updatedWith("c")(_.map(_.copy(state = NodeState.Draining))))))
        // レンジが c から d へ移るのを待つ
        _ <- retry(
          a.meta.client.descriptors.flatMap { ds =>
            IO.raiseWhen(ds.exists(_.replicas.contains("c")) || !ds.forall(_.replicas.contains("d")))(new Exception(s"ranges not moved yet: $ds"))
          } >> a.meta.client.get(Keys.rangeDescriptor(0)).flatMap { raw =>
            IO.raiseWhen(raw.exists(r => RangeDescriptor.decode(r).replicas.contains("c")))(new Exception("system range not moved yet"))
          },
          n = 300
        )
        // ブロックを移す（全ノードで数回）
        _ <- c.nodes.flatMap(_.values.toList.traverse_(_.refreshLayout))
        // c → d の送信が失敗する状況で c が修復を走らせても、c はコピーを消してはいけない
        cBefore <- c.node("c").flatMap(_.local.list.compile.count)
        _ <- net.partition("c", "d")
        _ <- c.node("c").flatMap(_.repair)
        cDuring <- c.node("c").flatMap(_.local.list.compile.count)
        _ <- net.healAll
        _ <- (1 to 2).toList.traverse_(_ => c.nodes.flatMap(_.values.toList.traverse_(_.repair)))
        cLeft <- c.node("c").flatMap(_.local.list.compile.toVector)
        // ---- c を外す。残りの 3 台で全部読める ----
        _ <- c.stop("c")
        d <- c.node("d")
        layout <- d.layout.get
        readAfter <- (0 until 30).toList.traverse(i => retry(getObject(d, bucket, s"obj-$i")))
        _ <- retry(putObject(d, bucket, "after", payload(99)))
        remaining <- c.nodes
        placementOk <- blockIds2.traverse { id =>
          val targets = Placement.forBlock(id.hash, layout, 3)
          targets.traverse(t => remaining(t).local.has(id)).map(_.forall(identity))
        }
      yield
        assert(copies1.forall(_ == 3), s"全ブロックが 3 ノードにあるはず: ${copies1.groupBy(identity).view.mapValues(_.size).toMap}")
        assertEquals(readWhileDown, (0 until 20).map(payload).toList)
        assert(onC.forall(identity), s"修復後は c も全ブロックを持つはず（欠け ${onC.count(!_)}）")
        assertEquals(cDuring, cBefore, "移動先に届いたと確認できないうちは、コピーを消してはいけない")
        assertEquals(cLeft.size, 0, "取り外し準備のノードからはブロックが移されて空になるはず")
        assertEquals(readAfter, (0 until 30).map(payload).toList)
        assert(placementOk.forall(identity), "全ブロックが新しい置き場所にあるはず")
        assert(!layout.active.contains("c"))
    }
    scalas3.sim.Sim.run(prog)
  }

  test("分断：孤立した 1 台は書けず、多数側は書ける。分断を直すと孤立していたノードからも読める") {
    val prog = withCluster(2L) { (net, c) =>
      val ids = Set("a", "b", "c")
      for
        _ <- ids.toList.sorted.zipWithIndex.traverse_((id, i) => c.start(cfg(id, ids, bootstrap = true, i.toLong + 10)))
        a <- c.node("a")
        b <- c.node("b")
        bucket <- retry(a.store.createBucket("part", admin, 0L))
        _ <- retry(putObject(a, bucket, "before", payload(1)))
        _ <- net.isolate("a", ids)
        minority <- putObject(a, bucket, "minority", payload(2)).timeout(20.seconds).attempt
        _ <- (0 until 5).toList.traverse_(i => retry(putObject(b, bucket, s"majority-$i", payload(10 + i))))
        _ <- net.healAll
        _ <- IO.sleep(3.seconds)
        reads <- (0 until 5).toList.traverse(i => retry(getObject(a, bucket, s"majority-$i")))
        before <- retry(getObject(a, bucket, "before"))
      yield
        assert(minority.isLeft, "孤立したノードでの書き込みは失敗するはず")
        assertEquals(reads, (0 until 5).map(i => payload(10 + i)).toList)
        assertEquals(before, payload(1))
    }
    scalas3.sim.Sim.run(prog)
  }

  test("EC 4+2：断片は 6 ノードに 1 つずつ置かれ、2 台止まっても読め、失った断片は修復で作り直される") {
    val prog = withCluster(3L) { (_, c) =>
      val ids = Set("a", "b", "c", "d", "e", "f")
      val ec = Redundancy.Erasure(4, 2)
      for
        // 2 台の故障に耐えるには、メタデータも 5 つに複製する（2m+1）
        _ <- ids.toList.sorted.zipWithIndex.traverse_((id, i) => c.start(cfg(id, ids, bootstrap = true, i.toLong + 20, replicas = 5)))
        a <- c.node("a")
        bucket <- retry(a.store.createBucket("ec", admin, 0L))
        _ <- (0 until 8).toList.traverse_(i => retry(putObject(a, bucket, s"obj-$i", payload(100 + i), ec)))
        objs <- (0 until 8).toList.traverse(i => a.store.getObject(bucket, s"obj-$i").map(_.get))
        stripes = objs.flatMap(_.body.stripeIds).distinct
        all <- c.nodes
        // 各ストライプの断片は 6 ノードに 1 つずつ
        spread <- stripes.traverse(st => all.values.toList.traverse(_.localShards.list1(st)).map(_.map(_.size)))
        // 2 台を止めても読める
        _ <- c.crash("e") >> c.crash("f")
        b <- c.node("b")
        whileDown <- (0 until 8).toList.traverse(i => retry(getObject(b, bucket, s"obj-$i")))
        _ <- c.start(cfg("e", ids, bootstrap = true, 24L, replicas = 5)) >> c.start(cfg("f", ids, bootstrap = true, 25L, replicas = 5))
        _ <- IO.sleep(3.seconds)
        // 起動直後のレイアウトの変化（各ノードのゾーンの反映）でずれた断片を、修復で正しい置き場所へ移しておく
        _ <- c.nodes.flatMap(_.values.toList.traverse_(_.refreshLayout))
        _ <- c.nodes.flatMap(_.values.toList.traverse_(_.repair))
        // d の断片を全部失ったことにする
        d <- c.node("d")
        lost <- d.localShards.list.compile.toList
        _ <- lost.traverse_((st, i) => d.localShards.delete(st, i))
        _ <- c.nodes.flatMap(_.values.toList.traverse_(_.repair))
        restored <- lost.traverse((st, i) => d.localShards.has(st, i))
        readAfter <- (0 until 8).toList.traverse(i => retry(getObject(d, bucket, s"obj-$i")))
      yield
        assert(stripes.nonEmpty, "EC のストライプがあるはず")
        assert(spread.forall(_.sorted == List(1, 1, 1, 1, 1, 1)), s"断片の分布: $spread")
        assertEquals(whileDown, (0 until 8).map(i => payload(100 + i)).toList)
        assert(lost.nonEmpty)
        assert(restored.forall(identity), s"作り直された断片 ${restored.count(identity)}/${restored.size}")
        assertEquals(readAfter, (0 until 8).map(i => payload(100 + i)).toList)
    }
    scalas3.sim.Sim.run(prog)
  }
