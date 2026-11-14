package scalas3.block

import munit.ScalaCheckSuite
import org.scalacheck.Gen
import org.scalacheck.Prop.*

class ReedSolomonSuite extends ScalaCheckSuite:

  private val params = Gen.oneOf((2, 1), (4, 2), (6, 3), (3, 0), (1, 2), (10, 4))

  property("どの k 個の断片からでも元のデータに戻せる") {
    forAll(params, Gen.choose(0, 5000), Gen.long) { case ((k, m), size, seed) =>
      val rnd = new scala.util.Random(seed)
      val data = Array.fill(size)(rnd.nextInt().toByte)
      val rs = ReedSolomon(k, m)
      val shards = rs.encode(data)
      assertEquals(shards.length, k + m)
      // ランダムに m 個（まで）を失う
      val lost = rnd.shuffle((0 until k + m).toList).take(m).toSet
      val present = shards.indices.filterNot(lost).map(i => i -> shards(i)).toMap
      assert(java.util.Arrays.equals(rs.decode(present, size), data))
    }
  }

  test("データ断片は元のデータをそのまま分けたもの（組織符号）") {
    val data = (0 until 100).map(_.toByte).toArray
    val shards = ReedSolomon(4, 2).encode(data)
    assertEquals(shards.take(4).flatten.take(100).toList, data.toList)
  }

  test("断片が足りなければ失敗する") {
    val rs = ReedSolomon(4, 2)
    val shards = rs.encode(Array.fill(40)(1.toByte))
    intercept[IllegalArgumentException](rs.decode(Map(0 -> shards(0), 5 -> shards(5), 3 -> shards(3)), 40))
  }
