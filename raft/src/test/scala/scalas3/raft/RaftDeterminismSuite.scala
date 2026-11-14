package scalas3.raft

import cats.effect.IO
import cats.syntax.all.*
import fs2.io.file.Path
import munit.CatsEffectSuite
import scalas3.rpc.{RpcClient, RpcServer}
import scalas3.runtime.NodeAddress
import scalas3.sim.{SimFileSystem, SimNetwork}
import scodec.bits.ByteVector

import scala.concurrent.duration.*

class RaftDeterminismSuite extends CatsEffectSuite:

  private val ids = Vector("n1", "n2", "n3")

  private def scenario(withRpcOnly: Boolean): IO[Vector[String]] =
    for
      net <- SimNetwork.create(3L, trace = true)
      stops <- ids.traverse { id =>
        SimFileSystem.create(id.hashCode.toLong).flatMap { fs =>
          (for
            client <- RpcClient(net.transport(id))
            node <-
              if withRpcOnly then cats.effect.Resource.pure[IO, Option[RaftNode]](None)
              else
                for
                  storage <- FileRaftStorage.open(fs, Path("raft"))
                  sm <- cats.effect.Resource.eval(ListSm.create)
                  n <- RaftNode.start(id, 1L, SnapshotMeta(0, 0, Membership.of(ids*)), storage, sm,
                    RaftRpc.transport(client, x => Some(NodeAddress(x, 7000))), RaftNodeConfig(tickInterval = 50.millis), 1L)
                yield Some(n)
            _ <- RpcServer.serve(net.transport(id), NodeAddress(id, 7000), RaftRpc.handler(g => IO.pure(node.filter(_ => g == 1L))))
            // RPC だけの場合は、各ノードが他のノードへ定期的に送る
            _ <- (if withRpcOnly then
                    (IO.sleep(50.millis) >> ids.filterNot(_ == id).traverse_(to =>
                      client.call(NodeAddress(to, 7000), RaftRpc.DeliverControl, (1L, id, Message.TimeoutNow(0))).attempt
                    )).foreverM.background.void
                  else cats.effect.Resource.unit[IO])
          yield node).allocated
        }
      }
      _ <- IO.sleep(3.seconds)
      _ <- stops.flatMap(_._1).headOption.traverse_(n => n.propose(ByteVector(1)).attempt)
      _ <- IO.sleep(1.second)
      _ <- stops.traverse_(_._2)
      log <- net.traceLog
    yield log

  private def compare(rpcOnly: Boolean): IO[Unit] =
    for
      r1 <- scalas3.sim.Sim.run(scenario(rpcOnly))
      r2 <- scalas3.sim.Sim.run(scenario(rpcOnly))
      r3 <- scalas3.sim.Sim.run(scenario(rpcOnly))
    yield
      def d(x: Vector[String], y: Vector[String]) = x.zip(y).indexWhere(_ != _)
      println(s"[determinism-raft rpcOnly=$rpcOnly] 1-2:${d(r1, r2)} 2-3:${d(r2, r3)} sizes ${r1.size} ${r2.size} ${r3.size}")
      val at = d(r1, r2)
      if at >= 0 then (math.max(0, at - 3) until math.min(r1.size, at + 4)).foreach(i => println(s"  $i: ${r1(i).take(80)} | ${r2.lift(i).getOrElse("").take(80)}"))
      assertEquals((d(r1, r2), d(r2, r3), r1.size, r2.size), (-1, -1, r2.size, r3.size))

  test("RPC だけなら決定的")(compare(rpcOnly = true))
  test("Raft のドライバでも決定的")(compare(rpcOnly = false))
