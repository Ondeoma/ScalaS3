package scalas3.raft

import cats.effect.{IO, Ref, Resource}
import cats.syntax.all.*
import fs2.io.file.Path
import munit.CatsEffectSuite
import scalas3.core.DetRandom
import scalas3.rpc.{RpcClient, RpcServer}
import scalas3.runtime.NodeAddress
import scalas3.sim.{SimFileSystem, SimNetwork}
import scodec.bits.ByteVector

import scala.concurrent.duration.*

/** メモリ上の状態マシン：適用されたコマンドを順に並べるだけ。 */
final class ListSm(state: Ref[IO, (Long, Vector[Long])]) extends StateMachine:
  def appliedIndex: IO[Long] = IO.pure(0L)
  def apply(index: Long, command: Option[ByteVector]): IO[ByteVector] =
    state.update((_, xs) => (index, xs ++ command.map(_.toLong()))).as(ByteVector.fromLong(index))
  def snapshot: IO[ByteVector] = state.get.map(_._2.foldLeft(ByteVector.empty)(_ ++ ByteVector.fromLong(_)))
  def restore(index: Long, data: ByteVector): IO[Unit] = state.set((index, data.grouped(8).map(_.toLong()).toVector))
  def values: IO[Vector[Long]] = state.get.map(_._2)

object ListSm:
  def create: IO[ListSm] = Ref.of[IO, (Long, Vector[Long])]((0L, Vector.empty)).map(ListSm(_))

/** 本物のドライバ・RPC・ストレージを、シミュレーションのネットワークとディスクの上で動かす。 */
class RaftNodeSimSuite extends CatsEffectSuite:

  override def munitIOTimeout: Duration = 5.minutes

  private val ids = Vector("n1", "n2", "n3")
  private val members = Membership.of(ids*)
  private def addr(id: NodeId) = NodeAddress(id, 7000)
  private val nodeConfig = RaftNodeConfig(tickInterval = 50.millis, snapshotThreshold = 40)

  private final case class Running(node: RaftNode, sm: ListSm, stop: IO[Unit])

  private final class Cluster(net: SimNetwork, disks: Map[NodeId, SimFileSystem], running: Ref[IO, Map[NodeId, Running]]):

    def start(id: NodeId, seed: Long): IO[Unit] =
      val res =
        for
          storage <- FileRaftStorage.open(disks(id), Path("raft"))
          sm <- Resource.eval(ListSm.create)
          client <- RpcClient(net.transport(id))
          node <- RaftNode.start(id, 1L, SnapshotMeta(0, 0, members), storage, sm, RaftRpc.transport(client, n => Some(addr(n))), nodeConfig, seed)
          _ <- RpcServer.serve(net.transport(id), addr(id), RaftRpc.handler(g => IO.pure(Option.when(g == 1L)(node))))
        yield (node, sm)
      res.allocated.flatMap { case ((node, sm), stop) => running.update(_.updated(id, Running(node, sm, stop))) }

    /** プロセスを止め、電源断を起こしてから再起動する。 */
    def crash(id: NodeId, seed: Long): IO[Unit] =
      running.modify(m => (m - id, m.get(id))).flatMap(_.traverse_(_.stop)) >>
        disks(id).powerFail >> start(id, seed)

    def get: IO[Map[NodeId, Running]] = running.get

    def leader: IO[Option[RaftNode]] =
      get.flatMap(_.values.toList.traverse(r => r.node.status.map(s => (r.node, s)))).map { ss =>
        ss.filter(_._2.role == Role.Leader).sortBy(-_._2.term).headOption.map(_._1)
      }

    def stopAll: IO[Unit] = get.flatMap(_.values.toList.traverse_(_.stop))

  private def withCluster[A](seed: Long)(f: (SimNetwork, Cluster) => IO[A]): IO[A] =
    for
      net <- SimNetwork.create(seed, SimNetwork.Config(baseLatency = 2.millis, jitter = 3.millis))
      disks <- ids.traverse(id => SimFileSystem.create(seed * 7 + id.hashCode).map(id -> _)).map(_.toMap)
      running <- Ref.of[IO, Map[NodeId, Running]](Map.empty)
      c = Cluster(net, disks, running)
      _ <- ids.zipWithIndex.traverse_((id, i) => c.start(id, seed + i))
      r <- f(net, c).guarantee(c.stopAll)
    yield r

  /** リーダーを探して提案する。成功したら true。 */
  private def proposeAnywhere(c: Cluster, v: Long): IO[Boolean] =
    c.leader.flatMap {
      case None    => IO.pure(false)
      case Some(l) => l.propose(ByteVector.fromLong(v)).timeout(1.second).attempt.map(_.isRight)
    }

  test("3 ノードで書き込みと線形化可能な読み込みができる") {
    val prog = withCluster(1L) { (_, c) =>
      for
        _ <- IO.sleep(2.seconds)
        oks <- (1L to 50L).toList.traverse(v => proposeAnywhere(c, v))
        l <- c.leader.flatMap(IO.fromOption(_)(new Exception("no leader")))
        _ <- l.readBarrier
        sms <- c.get.map(_.values.toList)
        _ <- IO.sleep(1.second)
        vals <- sms.traverse(_.sm.values)
      yield
        assert(oks.forall(identity), "安定した状態では全部成功するはず")
        vals.foreach(v => assertEquals(v, (1L to 50L).toVector))
    }
    scalas3.sim.Sim.run(prog)
  }

  test("分断・クラッシュ（電源断）・再起動を繰り返しても、成功した書き込みは失われず全ノードが一致する") {
    def scenario(seed: Long): IO[(Int, Int)] = withCluster(seed) { (net, c) =>
      for
        acked <- Ref.of[IO, Vector[Long]](Vector.empty)
        rng <- Ref.of[IO, DetRandom](DetRandom(seed))
        rand = (n: Int) => rng.modify(r => r.nextInt(n))
        crashes <- Ref.of[IO, Int](0)
        client = (1L to 400L).toList.traverse_ { v =>
          proposeAnywhere(c, v).flatMap(ok => acked.update(_ :+ v).whenA(ok)) >> IO.sleep(20.millis)
        }
        // 読み込みの検査：開始時点で成功済みの書き込みが、障壁のあとのローカル状態に全部あること
        reader = (1 to 60).toList.traverse_ { _ =>
          IO.sleep(130.millis) >> acked.get.flatMap { before =>
            c.leader.flatMap(_.traverse_ { l =>
              c.get.flatMap(_.values.find(_.node eq l).traverse_ { r =>
                l.readBarrier.timeout(1.second).attempt.flatMap {
                  case Right(_) =>
                    r.sm.values.map(vs => assert((before.toSet -- vs).isEmpty, s"seed=$seed: 読み込みで成功済みの書き込みが見えない"))
                  case Left(_) => IO.unit
                }
              })
            })
          }
        }
        nemesis = (1 to 25).toList.traverse_ { _ =>
          for
            wait <- rand(600)
            _ <- IO.sleep((200 + wait).millis)
            action <- rand(10)
            a <- rand(ids.size)
            b <- rand(ids.size)
            _ <-
              if action < 4 then net.partition(ids(a), ids(b)).whenA(a != b)
              else if action < 7 then net.healAll
              else crashes.update(_ + 1) >> c.crash(ids(a), seed * 100 + a)
          yield ()
        }
        _ <- (client, reader, nemesis).parTupled
        _ <- net.healAll
        _ <- IO.sleep(3.seconds)
        // 活性：分断を直せば書き込みが通る
        finalOk <- (100000L to 100004L).toList.traverse(v =>
          (proposeAnywhere(c, v), IO.sleep(200.millis)).tupled.map(_._1).iterateUntil(identity).timeout(20.seconds).attempt
        )
        _ <- acked.update(_ ++ (100000L to 100004L))
        _ <- IO.sleep(3.seconds)
        all <- c.get.flatMap(_.values.toList.traverse(_.sm.values))
        a <- acked.get
        n <- crashes.get
      yield
        assert(finalOk.forall(_.isRight), s"seed=$seed: 分断を直しても書き込みが通らない")
        all.foreach(vs => assertEquals(vs, all.head, s"seed=$seed: ノード間で状態が一致しない"))
        val missing = a.toSet -- all.head
        assert(missing.isEmpty, s"seed=$seed: 成功した書き込みが失われた: ${missing.take(10)}")
        (a.size, n)
    }

    scalas3.sim.Sim.run((1L to 6L).toList.traverse(scenario)).map { rs =>
      assert(rs.map(_._2).sum > 0, "クラッシュが一度も起きていない")
      println(s"[raft-node-sim] acked=${rs.map(_._1).sum} crashes=${rs.map(_._2).sum}")
    }
  }
