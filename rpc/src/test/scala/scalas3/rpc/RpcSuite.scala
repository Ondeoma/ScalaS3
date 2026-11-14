package scalas3.rpc

import cats.effect.{Deferred, IO, Ref, Resource}
import cats.syntax.all.*
import munit.CatsEffectSuite
import scalas3.runtime.{NodeAddress, TcpTransport, Transport}
import scalas3.sim.SimNetwork
import scodec.codecs.*

import scala.concurrent.duration.*

class RpcSuite extends CatsEffectSuite:

  private val Echo = Method(1, "echo", utf8_32, utf8_32)
  private val Add = Method(2, "add", int32 :: int32, int32)
  private val Fail = Method(3, "fail", utf8_32, utf8_32)
  private val Slow = Method(4, "slow", int32, int32)
  private val Unknown = Method(99, "unknown", utf8_32, utf8_32)

  private val addr = NodeAddress("server", 7000)

  private def handler(slowStarted: Deferred[IO, Unit], slowCanceled: Ref[IO, Boolean]) = RpcHandler.of(
    Echo.handledBy(IO.pure),
    Add.handledBy((a, b) => IO.pure(a + b)),
    Fail.handledBy(msg => IO.raiseError(RemoteException(RemoteException.BadRequest, msg))),
    Slow.handledBy(n => (slowStarted.complete(()) >> IO.sleep(n.seconds)).onCancel(slowCanceled.set(true)).as(n))
  )

  private def withSim[A](f: (SimNetwork, Transport, Deferred[IO, Unit], Ref[IO, Boolean]) => IO[A]): IO[A] =
    scalas3.sim.Sim.run(
      for
        net <- SimNetwork.create(7L)
        started <- Deferred[IO, Unit]
        canceled <- Ref.of[IO, Boolean](false)
        r <- RpcServer.serve(net.transport("server"), addr, handler(started, canceled)).surround(
          f(net, net.transport("client"), started, canceled)
        )
      yield r
    )

  test("基本的な呼び出しとエラー") {
    withSim { (_, t, _, _) =>
      RpcClient(t).use { c =>
        for
          e <- c.call(addr, Echo, "こんにちは")
          s <- c.call(addr, Add, (40, 2))
          f <- c.call(addr, Fail, "bad").attempt
          u <- c.call(addr, Unknown, "?").attempt
        yield
          assertEquals(e, "こんにちは")
          assertEquals(s, 42)
          assertEquals(f, Left(RemoteException(RemoteException.BadRequest, "bad")))
          assert(u.left.exists { case RemoteException(code, _) => code == RemoteException.UnknownMethod; case _ => false })
      }
    }
  }

  test("同じ接続の上で並行に呼び出せる") {
    withSim { (_, t, _, _) =>
      RpcClient(t).use { c =>
        (1 to 200).toList.parTraverse(i => c.call(addr, Add, (i, i))).map(rs => assertEquals(rs, (1 to 200).map(_ * 2).toList))
      }
    }
  }

  test("呼び出しをキャンセルすると、相手側の処理もキャンセルされる") {
    withSim { (_, t, started, canceled) =>
      RpcClient(t).use { c =>
        for
          fib <- c.call(addr, Slow, 60).start
          _ <- started.get
          _ <- fib.cancel
          _ <- IO.sleep(1.second)
          wasCanceled <- canceled.get
        yield assert(wasCanceled)
      }
    }
  }

  test("接続が切れると待機中の呼び出しは失敗し、次の呼び出しで再接続する") {
    withSim { (net, t, started, _) =>
      RpcClient(t).use { c =>
        for
          fib <- c.call(addr, Slow, 60).start
          _ <- started.get
          _ <- net.partition("client", "server")
          r <- fib.joinWithNever.attempt
          _ <- net.healAll
          e <- c.call(addr, Echo, "again")
        yield
          assert(r.left.exists(_.isInstanceOf[ConnectionClosedException]), r.toString)
          assertEquals(e, "again")
      }
    }
  }

  test("相手がいないと接続エラー") {
    withSim { (_, t, _, _) =>
      RpcClient(t).use(c => c.call(NodeAddress("nobody", 1), Echo, "x").attempt).map(r => assert(r.isLeft))
    }
  }

  test("本物の TCP でも動く") {
    val server = Resource.eval((Deferred[IO, Unit], Ref.of[IO, Boolean](false)).tupled).flatMap { (s, c) =>
      RpcServer.serve(TcpTransport, NodeAddress("127.0.0.1", 0), handler(s, c))
    }
    (server, RpcClient(TcpTransport)).tupled.use { (bound, c) =>
      (1 to 50).toList.parTraverse(i => c.call(bound, Add, (i, 1))).map(rs => assertEquals(rs, (2 to 51).toList))
    }
  }
