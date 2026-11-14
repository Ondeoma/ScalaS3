package scalas3.cluster

import cats.effect.IO
import cats.syntax.all.*
import munit.CatsEffectSuite
import scalas3.rpc.{RpcClient, RpcServer}
import scalas3.runtime.NodeAddress
import scalas3.sim.{Sim, SimNetwork}

import scala.concurrent.duration.*

class HealthSuite extends CatsEffectSuite:

  private val ids = Vector("a", "b", "c")
  private val layout = Layout(1, ids.map(id => id -> NodeInfo(NodeAddress(id, 7000), "z", 100, NodeState.Active)).toMap)
  private val config = HealthConfig(probeInterval = 1.second, probeTimeout = 1.second, downAfter = 10.seconds)

  test("応答のないノードは downAfter の後で Down になり、戻れば Up に戻る") {
    val prog =
      for
        net <- SimNetwork.create(1L)
        r <- (ids.traverse(id => RpcServer.serve(net.transport(id), NodeAddress(id, 7000), Health.handler)) >>
          RpcClient(net.transport("a")).flatMap(rpc => Health.start("a", rpc, IO.pure(layout), config))).use { h =>
          for
            _ <- IO.sleep(5.seconds)
            initially = (h.isDown("b"), h.isDown("c"))
            _ <- net.isolate("c", ids.filter(_ != "c"))
            _ <- IO.sleep(5.seconds)
            early = h.isDown("c") // まだ downAfter に達していない
            _ <- IO.sleep(10.seconds)
            down = (h.isDown("b"), h.isDown("c"), h.isDown("a"))
            _ <- net.healAll
            _ <- IO.sleep(3.seconds)
            recovered = h.isDown("c")
          yield (initially, early, down, recovered)
        }
      yield r
    Sim.run(prog).map { (initially, early, down, recovered) =>
      assertEquals(initially, (false, false))
      assertEquals(early, false)
      assertEquals(down, (false, true, false))
      assertEquals(recovered, false)
    }
  }

  test("古い版（health.ping2 を知らない）は機能の水準 0 とみなし、全ノードが新しくなるまで supports は false") {
    // 古い版の応答：health.ping だけ
    val oldHandler = scalas3.rpc.RpcHandler.of(Health.Ping.handledBy(_ => IO.unit))
    val prog =
      for
        net <- SimNetwork.create(2L)
        r <- (RpcServer.serve(net.transport("b"), NodeAddress("b", 7000), Health.handler) >>
          RpcServer.serve(net.transport("c"), NodeAddress("c", 7000), oldHandler) >>
          RpcClient(net.transport("a")).flatMap(rpc => Health.start("a", rpc, IO.pure(layout), config))).use { h =>
          IO.sleep(3.seconds) >> IO((h.snapshot.view.mapValues(_.features).toMap, h.supports(ids.toSet, Health.MergeFeature), h.supports(Set("a", "b"), Health.MergeFeature), h.isDown("c")))
        }
        // 見張りを始めた直後（まだ応答がない）は分からないので false
        unknown <- RpcClient(net.transport("a")).flatMap(rpc => Health.start("a", rpc, IO.pure(layout), config)).use(h => IO(h.supports(Set("b"), Health.MergeFeature)))
      yield (r, unknown)
    Sim.run(prog).map { case ((features, all, ab, cDown), unknown) =>
      assertEquals(features, Map("b" -> Health.Features, "c" -> 0))
      assert(!all, "an old node must block the feature")
      assert(ab, "the feature is allowed when every listed node supports it")
      assert(!cDown, "an old node is still up")
      assert(!unknown, "unknown nodes must not count as supporting")
    }
  }

