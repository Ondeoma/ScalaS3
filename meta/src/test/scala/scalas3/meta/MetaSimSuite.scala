package scalas3.meta

import cats.effect.{IO, Ref}
import cats.syntax.all.*
import fs2.io.file.Path
import munit.CatsEffectSuite
import scalas3.core.DetRandom
import scalas3.lsm.LsmConfig
import scalas3.raft.{NodeId, RaftConfig, RaftNodeConfig}
import scalas3.rpc.{RpcClient, RpcServer}
import scalas3.runtime.NodeAddress
import scalas3.sim.{SimFileSystem, SimNetwork}
import scodec.bits.ByteVector

import scala.concurrent.duration.*

class MetaSimSuite extends CatsEffectSuite:

  override def munitIOTimeout: Duration = 10.minutes

  private def addr(id: NodeId) = NodeAddress(id, 7000)
  private val config = MetaConfig(
    raft = RaftNodeConfig(raft = RaftConfig(electionTicks = 10), tickInterval = 50.millis, snapshotThreshold = 50),
    splitBytes = 3000,
    maintenanceInterval = 300.millis,
    requestTimeout = 3.seconds
  )
  private val lsmConfig = LsmConfig(memtableBytes = 16 * 1024, blockBytes = 512, targetFileBytes = 8 * 1024)

  private def key(i: Long) = Keys.DataStart ++ ByteVector(f"k$i%08d".getBytes)
  private val counterKey = Keys.DataStart ++ ByteVector("counter".getBytes)

  private final case class Running(node: MetaNode, stop: IO[Unit])

  private final class Cluster(ids: Vector[NodeId], net: SimNetwork, disks: Map[NodeId, SimFileSystem], val running: Ref[IO, Map[NodeId, Running]]):
    def start(id: NodeId, seed: Long): IO[Unit] =
      val res =
        for
          rpc <- RpcClient(net.transport(id))
          node <- MetaNode.start(id, disks(id), Path("data"), rpc, n => Some(addr(n)), ids.toSet, bootstrap = true, IO.pure(ids.toSet), config, lsmConfig, seed)
          _ <- RpcServer.serve(net.transport(id), addr(id), node.rpcHandler)
        yield node
      res.allocated.flatMap((n, stop) => running.update(_.updated(id, Running(n, stop))))

    def crash(id: NodeId, seed: Long): IO[Unit] =
      running.modify(m => (m - id, m.get(id))).flatMap(_.traverse_(_.stop.attempt)) >> disks(id).powerFail >> start(id, seed)

    def client(i: Int): IO[Option[MetaClient]] = running.get.map(m => m.get(ids(i % ids.size)).map(_.node.client))
    def stopAll: IO[Unit] = running.get.flatMap(_.values.toList.traverse_(_.stop.attempt))

  private def withCluster[A](ids: Vector[NodeId], seed: Long)(f: (SimNetwork, Cluster) => IO[A]): IO[A] =
    for
      net <- SimNetwork.create(seed)
      disks <- ids.traverse(id => SimFileSystem.create(seed * 13 + id.hashCode).map(id -> _)).map(_.toMap)
      running <- Ref.of[IO, Map[NodeId, Running]](Map.empty)
      c = Cluster(ids, net, disks, running)
      _ <- ids.zipWithIndex.traverse_((id, i) => c.start(id, seed + i))
      r <- f(net, c).guarantee(c.stopAll)
    yield r

  test("1 ノード：読み書き、条件付き書き込み、分割をまたぐ scan") {
    val prog = withCluster(Vector("solo"), 1L) { (_, c) =>
      for
        _ <- IO.sleep(2.seconds)
        cl <- c.client(0).map(_.get)
        _ <- (1L to 300L).toList.traverse_(i => cl.write(Vector.empty, Vector(KvOp.Put(key(i), ByteVector.fromLong(i)))))
        r1 <- cl.write(Vector(Precondition.Absent(key(5))), Vector(KvOp.Put(key(5), ByteVector(0))))
        r2 <- cl.write(Vector(Precondition.Equals(key(5), ByteVector.fromLong(5))), Vector(KvOp.Delete(key(5))))
        _ <- IO.sleep(5.seconds) // 分割が進むのを待つ
        ds <- cl.descriptors
        all <- cl.scan(Keys.DataStart, None, 10000)
        page <- cl.scan(key(100), Some(key(110)), 5)
      yield
        assertEquals(r1, WriteResult.ConditionFailed(0))
        assertEquals(r2, WriteResult.Ok)
        assert(ds.size > 1, s"分割されているはず: $ds")
        assertEquals(all._1.map(_._1), (1L to 300L).filterNot(_ == 5).map(key).toVector)
        assertEquals(page._1.map(_._1), (100L to 104L).map(key).toVector)
        assert(page._2)
    }
    scalas3.sim.Sim.run(prog)
  }

  test("3 ノード：分割・分断・電源断を繰り返しても、成功した書き込みは失われず CAS も正しい") {
    val ids = Vector("a", "b", "c")
    def scenario(seed: Long): IO[(Int, Int, Int)] = withCluster(ids, seed) { (net, c) =>
      for
        _ <- IO.sleep(2.seconds)
        acked <- Ref.of[IO, Map[Long, Long]](Map.empty)
        attempted <- Ref.of[IO, Set[Long]](Set.empty)
        incAcked <- Ref.of[IO, Int](0)
        incAttempted <- Ref.of[IO, Int](0)
        rng <- Ref.of[IO, DetRandom](DetRandom(seed))
        rand = (n: Int) => rng.modify(_.nextInt(n))
        writer = (1L to 400L).toList.traverse_ { i =>
          rand(ids.size).flatMap(c.client).flatMap(_.traverse_ { cl =>
            attempted.update(_ + i) >>
              cl.write(Vector.empty, Vector(KvOp.Put(key(i), ByteVector.fromLong(i * 7)))).attempt.flatMap {
                case Right(WriteResult.Ok) => acked.update(_.updated(i, i * 7))
                case _                     => IO.unit
              }
          }) >> IO.sleep(15.millis)
        }
        // CAS で数を増やす：失われた更新（lost update）がないか
        incrementer = (1 to 80).toList.traverse_ { _ =>
          rand(ids.size).flatMap(c.client).flatMap(_.traverse_ { cl =>
            val once = cl.get(counterKey).flatMap { cur =>
              val n = cur.fold(0L)(_.toLong())
              val cond = cur.fold(Precondition.Absent(counterKey))(Precondition.Equals(counterKey, _))
              incAttempted.update(_ + 1) >>
                cl.write(Vector(cond), Vector(KvOp.Put(counterKey, ByteVector.fromLong(n + 1))))
            }
            once.attempt.flatMap {
              case Right(WriteResult.Ok) => incAcked.update(_ + 1)
              case _                     => IO.unit
            }
          }) >> IO.sleep(60.millis)
        }
        nemesis = (1 to 12).toList.traverse_ { _ =>
          for
            w <- rand(800)
            _ <- IO.sleep((300 + w).millis)
            action <- rand(10)
            a <- rand(ids.size)
            b <- rand(ids.size)
            _ <-
              if action < 4 then net.partition(ids(a), ids(b)).whenA(a != b)
              else if action < 7 then net.healAll
              else c.crash(ids(a), seed * 100 + a)
          yield ()
        }
        _ <- (writer, incrementer, nemesis).parTupled
        _ <- net.healAll
        _ <- IO.sleep(8.seconds)
        cl <- c.client(0).map(_.get)
        all <- cl.scan(Keys.DataStart, None, 100000).map(_._1.toMap)
        ack <- acked.get
        att <- attempted.get
        counter <- cl.get(counterKey).map(_.fold(0L)(_.toLong()))
        incA <- incAcked.get
        incT <- incAttempted.get
        ds <- cl.descriptors
        // 各ノードからも同じ値が読める
        views <- (0 until ids.size).toList.traverse(i => c.client(i).flatMap(_.get.get(key(ack.keys.headOption.getOrElse(1L)))))
      yield
        val missing = ack.filter((i, v) => !all.get(key(i)).contains(ByteVector.fromLong(v)))
        assert(missing.isEmpty, s"seed=$seed: 成功した書き込みが失われた: ${missing.keys.take(10)}")
        val unexpected = all.keySet.filterNot(k => k == counterKey || att.exists(i => key(i) == k))
        assert(unexpected.isEmpty, s"seed=$seed: 書いていないキーがある")
        assert(counter >= incA && counter <= incT, s"seed=$seed: カウンタ $counter が成功数 $incA〜試行数 $incT の範囲外")
        assert(views.distinct.size == 1, s"seed=$seed: ノードによって読める値が違う: $views")
        (ack.size, ds.size, incA)
    }
    scalas3.sim.Sim.run((1L to 4L).toList.traverse(scenario)).map { rs =>
      println(s"[meta-sim] acked=${rs.map(_._1).sum} ranges=${rs.map(_._2)} increments=${rs.map(_._3).sum}")
      assert(rs.exists(_._2 > 2), "分割が起きていない")
    }
  }


  test("3 ノード：大量に消すと隣のレンジが統合される。統合の最中に分断・電源断があっても、書き込みは失われず、再び分割もできる") {
    val ids = Vector("a", "b", "c")
    // キーごとの操作の記録：(書いた値、成功したか)。値 None は削除
    type Log = Map[Long, Vector[(Option[Long], Boolean)]]
    def scenario(seed: Long): IO[(Int, Int, Int)] = withCluster(ids, seed) { (net, c) =>
      def record(log: Ref[IO, Log], i: Long, v: Option[Long], ok: Boolean) = log.update(m => m.updated(i, m.getOrElse(i, Vector.empty) :+ (v, ok)))
      def anyClient(rand: Int => IO[Int]): IO[Option[MetaClient]] = rand(ids.size).flatMap(c.client)
      // 値を大きくして、レンジがたくさんできるようにする（先頭 8 バイトが値）
      def value(v: Long) = ByteVector.fromLong(v) ++ ByteVector.fill(40)(1)
      def descsOf(cl: MetaClient) = cl.descriptors.map(_.filter(d => scalas3.core.Bytes.compare(d.start, Keys.DataStart) >= 0))
      for
        _ <- IO.sleep(2.seconds)
        log <- Ref.of[IO, Log](Map.empty)
        rng <- Ref.of[IO, DetRandom](DetRandom(seed))
        rand = (n: Int) => rng.modify(_.nextInt(n))
        cl0 <- c.client(0).map(_.get)
        // 1. 書いて分割させる
        _ <- (1L to 300L).toList.traverse_(i =>
          cl0.write(Vector.empty, Vector(KvOp.Put(key(i), value(i)))).flatMap(r => record(log, i, Some(i), r == WriteResult.Ok))
        )
        _ <- IO.sleep(6.seconds)
        before <- descsOf(cl0).map(_.size)
        // 2. 20 個に 1 つを残して消す。同時に、残すキーの上書き、CAS の数え上げ、分断と電源断を起こす
        deleter = (1L to 300L).filter(_ % 20 != 0).toList.traverse_ { i =>
          anyClient(rand).flatMap(_.traverse_(cl =>
            cl.write(Vector.empty, Vector(KvOp.Delete(key(i)))).attempt.flatMap(r => record(log, i, None, r == Right(WriteResult.Ok)))
          )) >> IO.sleep(20.millis)
        }
        updater = (1 to 60).toList.traverse_ { j =>
          val i = 20L * (1 + j % 15)
          anyClient(rand).flatMap(_.traverse_(cl =>
            cl.write(Vector.empty, Vector(KvOp.Put(key(i), value(i * 1000 + j)))).attempt.flatMap(r =>
              record(log, i, Some(i * 1000 + j), r == Right(WriteResult.Ok))
            )
          )) >> IO.sleep(150.millis)
        }
        nemesis = (1 to 10).toList.traverse_ { _ =>
          for
            w <- rand(1500)
            _ <- IO.sleep((500 + w).millis)
            action <- rand(10)
            a <- rand(ids.size)
            b <- rand(ids.size)
            _ <-
              if action < 4 then net.partition(ids(a), ids(b)).whenA(a != b)
              else if action < 7 then net.healAll
              else c.crash(ids(a), seed * 100 + a)
          yield ()
        }
        _ <- (deleter, updater, nemesis).parTupled
        _ <- net.healAll
        _ <- IO.sleep(15.seconds) // 統合が進むのを待つ
        cl <- c.client(0).map(_.get)
        merged <- descsOf(cl)
        all1 <- cl.scan(Keys.DataStart, None, 100000).map(_._1.toMap)
        // 凍結したまま残ったレンジがない（統合が途中で止まっていない）
        frozenLeft <- c.running.get.flatMap(_.values.toList.flatTraverse(_.node.ranges.all.flatMap(_.toList.traverse(_.sm.frozen)))).map(_.count(identity))
        // 3. もう一度書いて、再び分割される
        _ <- (301L to 600L).toList.traverse_(i =>
          cl.write(Vector.empty, Vector(KvOp.Put(key(i), value(i)))).flatMap(r => record(log, i, Some(i), r == WriteResult.Ok))
        )
        _ <- IO.sleep(8.seconds)
        resplit <- descsOf(cl)
        all2 <- cl.scan(Keys.DataStart, None, 100000).map(_._1.toMap)
        views <- (0 until ids.size).toList.traverse(i => c.client(i).flatMap(_.get.get(key(20))))
        lg <- log.get
      yield
        // キーの値は、最後に成功した操作か、それより後に試した（結果の分からない）操作のどれかと一致する
        def check(all: Map[ByteVector, ByteVector], upTo: Long) =
          (1L to upTo).foreach { i =>
            val ops = lg.getOrElse(i, Vector.empty)
            val lastOk = ops.lastIndexWhere(_._2)
            val allowed = (if lastOk < 0 then Vector(None) else Vector.empty) ++ ops.drop(math.max(lastOk, 0)).map(_._1)
            val actual = all.get(key(i)).map(_.take(8).toLong())
            assert(allowed.contains(actual), s"seed=$seed: key $i is $actual, expected one of $allowed")
          }
        check(all1, 300)
        check(all2, 600)
        assertEquals(frozenLeft, 0, s"seed=$seed")
        assert(merged.size < before, s"seed=$seed: 統合されていない（前 $before、後 ${merged.size}）")
        assert(resplit.size > merged.size, s"seed=$seed: 再び分割されていない（${merged.size} → ${resplit.size}）")
        assert(views.distinct.size == 1, s"seed=$seed: ノードによって読める値が違う: $views")
        (before, merged.size, resplit.size)
    }
    scalas3.sim.Sim.run((1L to 4L).toList.traverse(scenario)).map(rs => println(s"[meta-sim] merge: ranges before/after/resplit = $rs"))
  }

  test("3 ノード：レンジのリーダーは 1 台に偏らない（受け持ちの差が 1 以内に収まる）") {
    val prog = withCluster(Vector("a", "b", "c"), 7L) { (_, c) =>
      for
        _ <- IO.sleep(3.seconds)
        cl <- c.client(0).map(_.get)
        _ <- (1L to 600L).toList.traverse_(i => cl.write(Vector.empty, Vector(KvOp.Put(key(i), ByteVector.fill(40)(i.toByte)))))
        _ <- IO.sleep(20.seconds) // 分割とリーダーの移譲が進むのを待つ
        statuses <- c.running.get.flatMap(_.values.toList.flatTraverse(_.node.ranges.all.flatMap(_.toList.traverse(_.node.status))))
      yield
        val leaders = statuses.filter(_.role == scalas3.raft.Role.Leader).groupMapReduce(_.id)(_ => 1)(_ + _)
        val counts = Vector("a", "b", "c").map(leaders.getOrElse(_, 0))
        assert(counts.sum >= 6, s"十分な数のレンジがあるはず: $counts")
        assert(counts.max - counts.min <= 1, s"リーダーの受け持ち: $counts")
    }
    scalas3.sim.Sim.run(prog)
  }
