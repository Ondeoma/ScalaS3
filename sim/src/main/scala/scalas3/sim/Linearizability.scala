package scalas3.sim

import scala.collection.mutable

/** レジスタ（読み・書き・CAS）の履歴が線形化可能かを調べる（Wing-Gong-Lowe の探索）。
  *
  * 操作は呼び出し時刻と完了時刻を持つ。完了が分からない操作（タイムアウトなど）は完了時刻を無限大とし、
  * 効果があったともなかったとも扱える（線形化の順序に入れても入れなくてもよい）。
  */
object Linearizability:

  enum Op:
    case Write(value: Long)
    case Read(result: Option[Long]) // None は「値がない」
    case Cas(expected: Option[Long], value: Long, succeeded: Boolean)

  /** completed が false の操作は結果が不明（Read なら結果は無視、Write/Cas は効果があったかもしれない）。 */
  final case class Event(id: Int, invoke: Long, complete: Long, completed: Boolean, op: Op)

  final case class Result(ok: Boolean, explored: Int)

  def check(history: Vector[Event], initial: Option[Long] = None, maxStates: Int = 2_000_000): Result =
    // 結果が不明な読み込みは何の制約にもならないので除く
    val ops = history.filterNot(e => !e.completed && e.op.isInstanceOf[Op.Read]).sortBy(_.invoke)
    val n = ops.size
    require(n <= 62, s"too many operations for one key: $n")
    val required = ops.indices.filter(i => ops(i).completed).foldLeft(0L)((m, i) => m | (1L << i))
    val visited = mutable.HashSet.empty[(Long, Option[Long])]
    var explored = 0

    // done の中に入っていない操作のうち、最も早い完了時刻（これより後に呼ばれた操作は、まだ線形化できない）
    def minComplete(done: Long): Long =
      var m = Long.MaxValue
      var i = 0
      while i < n do
        if (done & (1L << i)) == 0 && ops(i).completed && ops(i).complete < m then m = ops(i).complete
        i += 1
      m

    def step(e: Event, value: Option[Long]): Option[Option[Long]] = e.op match
      case Op.Write(v)                    => Some(Some(v))
      case Op.Read(r)                     => Option.when(r == value)(value)
      case Op.Cas(exp, v, true)           => Option.when(value == exp)(Some(v))
      case Op.Cas(exp, _, false)          => Option.when(value != exp)(value)

    def search(done: Long, value: Option[Long]): Boolean =
      if (done & required) == required then true
      else if !visited.add((done, value)) then false
      else
        explored += 1
        if explored > maxStates then throw IllegalStateException(s"search space too large ($explored states)")
        val limit = minComplete(done)
        (0 until n).exists { i =>
          (done & (1L << i)) == 0 && ops(i).invoke <= limit && step(ops(i), value).exists(v => search(done | (1L << i), v))
        }

    val ok = search(0L, initial)
    Result(ok, explored)
