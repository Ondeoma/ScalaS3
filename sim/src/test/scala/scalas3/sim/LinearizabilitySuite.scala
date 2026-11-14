package scalas3.sim

import munit.FunSuite
import scalas3.sim.Linearizability.*

class LinearizabilitySuite extends FunSuite:

  private def ev(id: Int, inv: Long, comp: Long, op: Op, completed: Boolean = true) = Event(id, inv, comp, completed, op)

  test("逐次的な履歴は線形化可能") {
    val h = Vector(ev(0, 0, 1, Op.Write(1)), ev(1, 2, 3, Op.Read(Some(1))), ev(2, 4, 5, Op.Cas(Some(1), 2, true)), ev(3, 6, 7, Op.Read(Some(2))))
    assert(check(h).ok)
  }

  test("書き込みの完了後に古い値を読んだら線形化不可能") {
    val h = Vector(ev(0, 0, 1, Op.Write(1)), ev(1, 2, 3, Op.Write(2)), ev(2, 4, 5, Op.Read(Some(1))))
    assert(!check(h).ok)
  }

  test("重なった操作はどちらの順にもできる") {
    val h = Vector(ev(0, 0, 10, Op.Write(1)), ev(1, 1, 9, Op.Write(2)), ev(2, 11, 12, Op.Read(Some(1))))
    assert(check(h).ok)
  }

  test("結果が不明な書き込みは、効果があってもなくてもよい") {
    val a = Vector(ev(0, 0, 1, Op.Write(1)), ev(1, 2, 0, Op.Write(2), completed = false), ev(2, 5, 6, Op.Read(Some(2))))
    val b = Vector(ev(0, 0, 1, Op.Write(1)), ev(1, 2, 0, Op.Write(2), completed = false), ev(2, 5, 6, Op.Read(Some(1))))
    assert(check(a).ok)
    assert(check(b).ok)
  }

  test("同じ値から 2 回の CAS が両方成功したら線形化不可能") {
    val h = Vector(ev(0, 0, 1, Op.Write(1)), ev(1, 2, 5, Op.Cas(Some(1), 2, true)), ev(2, 3, 6, Op.Cas(Some(1), 3, true)))
    assert(!check(h).ok)
  }
