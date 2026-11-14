package scalas3.cluster

import munit.FunSuite
import scalas3.runtime.NodeAddress
import scodec.bits.ByteVector

class PlacementSuite extends FunSuite:

  private def node(zone: String, weight: Int = 100) = NodeInfo(NodeAddress("h", 1), zone, weight, NodeState.Active)
  private def key(i: Int) = ByteVector.fromInt(i)

  test("コピーはなるべく別のゾーンに置く") {
    val nodes = Map("a1" -> node("a"), "a2" -> node("a"), "a3" -> node("a"), "b1" -> node("b"), "c1" -> node("c"))
    (0 until 500).foreach { i =>
      val picked = Placement.choose(key(i), nodes, 3)
      assertEquals(picked.size, 3)
      assertEquals(picked.map(nodes(_).zone).toSet, Set("a", "b", "c"), s"key $i: $picked")
    }
  }

  test("ノードが足りなければ、あるだけ返す") {
    assertEquals(Placement.choose(key(1), Map("x" -> node("z")), 3), Vector("x"))
  }

  test("ノードを 1 台足しても、動くのはおよそ 1/(n+1) の割合だけ") {
    val before = (1 to 4).map(i => s"n$i" -> node(s"z$i")).toMap
    val after = before + ("n5" -> node("z5"))
    val n = 5000
    val moved = (0 until n).count(i => Placement.choose(key(i), before, 1) != Placement.choose(key(i), after, 1))
    val ratio = moved.toDouble / n
    assert(ratio > 0.15 && ratio < 0.25, s"移動の割合 $ratio は 0.2 前後のはず")
  }

  test("重みに比例して選ばれる") {
    val nodes = Map("big" -> node("z1", 300), "small" -> node("z2", 100))
    val n = 8000
    val big = (0 until n).count(i => Placement.choose(key(i), nodes, 1).head == "big")
    val ratio = big.toDouble / n
    assert(ratio > 0.70 && ratio < 0.80, s"重み 3:1 なら 0.75 前後のはず（$ratio）")
  }

  test("レイアウトの符号化は往復で元に戻る") {
    val l = Layout(7, Map("a" -> node("z1"), "b" -> NodeInfo(NodeAddress("10.0.0.2", 3901), "z2", 50, NodeState.Draining)))
    assertEquals(Layout.decode(Layout.encode(l)), l)
  }
