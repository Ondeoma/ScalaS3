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
import scalas3.sim.Linearizability.{Event, Op}
import scalas3.sim.{Linearizability, Sim, SimFileSystem, SimNetwork}
import scodec.bits.ByteVector

import scala.concurrent.duration.*

/** Jepsen と同じ考え方の検査：分断と電源断を起こしながら複数のクライアントが読み・書き・CAS を行い、
  * キーごとの履歴が線形化可能かを調べる。
  */
class LinearizabilitySimSuite extends CatsEffectSuite:

  override def munitIOTimeout: Duration = 10.minutes

  private val ids = Vector("a", "b", "c")
  private def addr(id: NodeId) = NodeAddress(id, 7000)
  private val config = MetaConfig(
    raft = RaftNodeConfig(raft = RaftConfig(electionTicks = 10), tickInterval = 50.millis, snapshotThreshold = 30),
    splitBytes = 1500,
    maintenanceInterval = 300.millis,
    requestTimeout = 2.seconds
  )
  private val baseKeys = Vector("x", "y", "z", "w").map(k => Keys.DataStart ++ ByteVector(k.getBytes))
  /** churn のときは操作が多いので、キーを増やす（検査の計算量のため、1 キーあたり約 60 操作まで）。 */
  private val churnKeys = ("bcdfghjkmpqrstuv".map(_.toString) ++ Seq("x", "y", "z", "w")).toVector.map(k => Keys.DataStart ++ ByteVector(k.getBytes))

  /** 検査するキーの間に入るキー（分割と統合を起こすため）。 */
  private val fillers = for p <- Vector("a", "n", "x", "y", "z", "{"); i <- 0 until 12 yield Keys.DataStart ++ ByteVector(f"$p$i%03d".getBytes)

  /** churn が true なら、検査とは別に、間のキーを書いては消すことを繰り返し、分割と統合を起こし続ける。
    * 戻り値の 3 番目は、churn の間に見えたレンジの数の (最大, その後の最小)。
    */
  private def scenario(seed: Long, churn: Boolean = false): IO[(Vector[(Int, Vector[Event])], Int, (Int, Int))] =
    val keys = if churn then churnKeys else baseKeys
    for
      net <- SimNetwork.create(seed)
      disks <- ids.traverse(id => SimFileSystem.create(seed * 31 + id.hashCode).map(id -> _)).map(_.toMap)
      running <- Ref.of[IO, Map[NodeId, (MetaNode, IO[Unit])]](Map.empty)
      start = (id: NodeId, s: Long) =>
        (for
          rpc <- RpcClient(net.transport(id))
          n <- MetaNode.start(id, disks(id), Path("data"), rpc, x => Some(addr(x)), ids.toSet, true, IO.pure(ids.toSet), config,
            LsmConfig(memtableBytes = 8 * 1024, blockBytes = 512, targetFileBytes = 4096), s)
          _ <- RpcServer.serve(net.transport(id), addr(id), n.rpcHandler)
        yield n).allocated.flatMap(r => running.update(_.updated(id, r)))
      _ <- ids.zipWithIndex.traverse_((id, i) => start(id, seed + i))
      _ <- IO.sleep(2.seconds)
      rng <- Ref.of[IO, DetRandom](DetRandom(seed))
      rand = (n: Int) => rng.modify(_.nextInt(n))
      history <- Ref.of[IO, Vector[(Int, Event)]](Vector.empty)
      nextId <- Ref.of[IO, Int](0)
      crashes <- Ref.of[IO, Int](0)
      client = (c: Int) =>
        (1 to (if churn then 120 else 30)).toList.traverse_ { j =>
          for
            k <- rand(keys.size)
            kind <- rand(10)
            nodeIdx <- rand(ids.size)
            node <- running.get.map(_.get(ids(nodeIdx)).map(_._1))
            id <- nextId.getAndUpdate(_ + 1)
            value = (c * 1000 + j).toLong
            t0 <- IO.monotonic.map(_.toMicros)
            _ <- node.traverse_ { n =>
              val key = keys(k)
              val run: IO[(Boolean, Op)] =
                if kind < 4 then
                  n.client.get(key).map(v => (true, Op.Read(v.map(_.toLong()))))
                else if kind < 7 then
                  n.client.write(Vector.empty, Vector(KvOp.Put(key, ByteVector.fromLong(value)))).map {
                    case WriteResult.Ok => (true, Op.Write(value))
                    case _              => (false, Op.Write(value))
                  }
                else
                  // 直前に読んだ値を期待値にした CAS
                  n.client.get(key).flatMap { cur =>
                    val cond = cur.fold(Precondition.Absent(key))(Precondition.Equals(key, _))
                    n.client.write(Vector(cond), Vector(KvOp.Put(key, ByteVector.fromLong(value)))).map {
                      case WriteResult.Ok                 => (true, Op.Cas(cur.map(_.toLong()), value, succeeded = true))
                      case WriteResult.ConditionFailed(_) => (true, Op.Cas(cur.map(_.toLong()), value, succeeded = false))
                      case _                              => (false, Op.Cas(cur.map(_.toLong()), value, succeeded = true))
                    }
                  }
              run.timeout(5.seconds).attempt.flatMap { r =>
                IO.monotonic.map(_.toMicros).flatMap { t1 =>
                  val ev = r match
                    case Right((true, op)) => Event(id, t0, t1, completed = true, op)
                    // 失敗やタイムアウトは「効果があったかもしれない」。CAS の期待値は分からないので、成功しうる操作として扱う
                    case Right((false, op)) => Event(id, t0, 0, completed = false, op)
                    case Left(_) =>
                      if kind < 4 then Event(id, t0, 0, completed = false, Op.Read(None))
                      else if kind < 7 then Event(id, t0, 0, completed = false, Op.Write(value))
                      else Event(id, t0, 0, completed = false, Op.Write(value))
                  history.update(_ :+ (k, ev))
                }
              }
            }
            _ <- IO.sleep(30.millis)
          yield ()
        }
      nemesis = (1 to 10).toList.traverse_ { _ =>
        for
          w <- rand(700)
          _ <- IO.sleep((200 + w).millis)
          action <- rand(10)
          a <- rand(ids.size)
          b <- rand(ids.size)
          _ <-
            if action < 4 then net.partition(ids(a), ids(b)).whenA(a != b)
            else if action < 7 then net.healAll
            else
              crashes.update(_ + 1) >> running.modify(m => (m - ids(a), m.get(ids(a)))).flatMap(_.traverse_(_._2.attempt)) >>
                disks(ids(a)).powerFail >> start(ids(a), seed * 100 + a)
        yield ()
      }
      seen <- Ref.of[IO, Vector[Int]](Vector.empty)
      churner = (1 to 4).toList.traverse_ { _ =>
        def onSome(f: MetaClient => IO[Unit]) = rand(ids.size).flatMap(i => running.get.map(_.get(ids(i)).map(_._1.client))).flatMap(_.traverse_(f))
        def observe = onSome(_.descriptors.flatMap(ds => seen.update(_ :+ ds.size)).attempt.void)
        fillers.toList.traverse_(k => onSome(_.write(Vector.empty, Vector(KvOp.Put(k, ByteVector.fill(40)(7)))).attempt.void)) >>
          IO.sleep(2.seconds) >> observe >>
          fillers.toList.traverse_(k => onSome(_.write(Vector.empty, Vector(KvOp.Delete(k))).attempt.void)) >>
          (1 to 4).toList.traverse_(_ => IO.sleep(1.second) >> observe)
      }
      _ <- ((0 until 6).toList.parTraverse_(client), nemesis, churner.whenA(churn)).parTupled
      _ <- net.healAll
      _ <- running.get.flatMap(_.values.toList.traverse_(_._2.attempt))
      h <- history.get
      n <- crashes.get
      sizes <- seen.get
    yield
      val peak = sizes.maxOption.getOrElse(0)
      val low = sizes.drop(sizes.indexOf(peak)).minOption.getOrElse(0)
      (h.groupMap(_._1)(_._2).toVector, n, (peak, low))

  test("分断と電源断の下でも、読み・書き・CAS の履歴は線形化可能") {
    (1L to 5L).toList.traverse { seed =>
      Sim.run(scenario(seed)).map { (perKey, crashes, _) =>
        val results = perKey.map((k, h) => (k, h.size, Linearizability.check(h)))
        results.foreach { (k, size, r) =>
          assert(r.ok, s"seed=$seed key=$k: ${size} 件の履歴が線形化可能でない")
        }
        (results.map(_._2).sum, crashes, perKey.flatMap(_._2).count(!_.completed))
      }
    }.map { stats =>
      println(s"[linearizability] ops=${stats.map(_._1).sum} crashes=${stats.map(_._2).sum} indeterminate=${stats.map(_._3).sum}")
    }
  }

  test("分割と統合を繰り返している最中も、読み・書き・CAS の履歴は線形化可能") {
    (11L to 14L).toList.traverse { seed =>
      Sim.run(scenario(seed, churn = true)).map { (perKey, crashes, range) =>
        val (peak, low) = range
        perKey.foreach { (k, h) =>
          assert(Linearizability.check(h).ok, s"seed=$seed key=$k: ${h.size} 件の履歴が線形化可能でない")
        }
        (perKey.map(_._2.size).sum, crashes, peak, low)
      }
    }.map { stats =>
      println(s"[linearizability] churn: ops=${stats.map(_._1).sum} crashes=${stats.map(_._2).sum} ranges(peak→low)=${stats.map(s => s"${s._3}→${s._4}")}")
      assert(stats.exists(s => s._3 > s._4), s"統合が起きていない: $stats")
    }
  }
