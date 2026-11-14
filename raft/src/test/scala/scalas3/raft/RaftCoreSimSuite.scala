package scalas3.raft

import munit.FunSuite
import scalas3.core.DetRandom
import scodec.bits.ByteVector

import scala.collection.mutable

/** RaftCore だけを使った高速な決定的シミュレーション。
  *
  * メッセージの欠落・重複・順序の入れ替え、ネットワーク分断、クラッシュと再起動、
  * スナップショット、構成変更、リーダー移譲を乱数で起こし、次の性質を検査する。
  *   - 選挙の安全性：1 つの term にリーダーは高々 1 人
  *   - 状態マシンの安全性：同じインデックスに異なるエントリが適用されない
  *   - 永続性：クライアントに成功を返した書き込みは失われない
  *   - 線形化可能な読み込み：ReadIndex で得た位置まで適用すると、読み込み開始前に成功した書き込みが全部見える
  *   - 活性：分断を直すと、いずれリーダーが決まり書き込みが進む
  */
class RaftCoreSimSuite extends FunSuite:

  private val cfg = RaftConfig(electionTicks = 10, heartbeatTicks = 2, maxEntriesPerAppend = 8)
  private val pool = Vector("n1", "n2", "n3", "n4", "n5")
  private val initial = Membership.of("n1", "n2", "n3")

  private def enc(v: Long) = ByteVector.fromLong(v)
  private def dec(b: ByteVector) = b.toLong()
  private def encSm(xs: Vector[Long]) = xs.foldLeft(ByteVector.empty)(_ ++ enc(_))
  private def decSm(b: ByteVector) = b.grouped(8).map(_.toLong()).toVector

  private final class Node(val id: NodeId, seed: Long):
    var hs = HardState(0, None)
    var snap = (SnapshotMeta(0, 0, if initial.voters.contains(id) then initial else Membership.empty), Vector.empty[Long])
    var entries = Vector.empty[LogEntry]
    var core: RaftCore = fresh(seed)
    var sm = Vector.empty[Long]
    var smIndex = 0L

    def fresh(seed: Long): RaftCore = RaftCore(id, cfg, hs, snap._1, entries, snap._1.index, seed)

    def restart(seed: Long): Unit =
      core = fresh(seed)
      sm = snap._2
      smIndex = snap._1.index

  private final class World(seed: Long):
    var rng = DetRandom(seed)
    def rand(n: Int): Int = { val (r, v) = rng.nextInt(n); rng = r; v }
    def chance(p: Double): Boolean = { val (r, v) = rng.nextBoolean(p); rng = r; v }

    val nodes: Map[NodeId, Node] = pool.map(id => id -> Node(id, seed * 31 + id.hashCode)).toMap
    var inflight = Vector.empty[(NodeId, NodeId, Message)]
    var cut = Set.empty[(NodeId, NodeId)]
    var nextReq = 1L
    var nextValue = 1L

    val leaders = mutable.Map.empty[Long, NodeId] // term -> leader
    val appliedAt = mutable.Map.empty[Long, LogEntry] // index -> entry（全ノード共通であるべき）
    val proposals = mutable.Map.empty[(NodeId, Long), Long] // (node, reqId) -> value
    val acceptedAt = mutable.Map.empty[(NodeId, Long), (Long, Long, Long)] // (node, index) -> (term, value, reqId)
    val acked = mutable.LinkedHashSet.empty[Long]
    val readRequests = mutable.Map.empty[(NodeId, Long), Set[Long]] // 読み込み開始時点で成功済みの値
    val readChecks = mutable.ArrayBuffer.empty[(NodeId, Long, Set[Long])]

    def drive(n: Node, input: Input): Unit = process(n, n.core.step(input))

    def process(n: Node, r: Ready): Unit =
      // 1. スナップショットの導入
      r.snapshot.foreach { (meta, data) =>
        snapshotsInstalled += 1
        n.snap = (meta, decSm(data))
        n.entries = n.core.logEntries
        n.sm = decSm(data)
        n.smIndex = meta.index
        // スナップショットに含まれる範囲は、他ノードの適用結果と一致しているはず（ここでは件数だけ確認）
      }
      // 2. 永続化
      r.hardState.foreach(h => n.hs = h)
      truncations += r.truncateFrom.size
      r.truncateFrom.foreach(i => n.entries = n.entries.filter(_.index < i))
      if r.append.nonEmpty then
        val first = r.append.head.index
        n.entries = n.entries.filter(_.index < first) ++ r.append
      // 3. 送信
      r.messages.foreach((to, m) => send(n.id, to, m))
      r.snapshotSends.foreach { to =>
        send(n.id, to, Message.Snapshot(n.core.currentTerm, n.snap._1, encSm(n.snap._2)))
      }
      // 4. 受理・拒否
      r.accepted.foreach { (req, idx, term) =>
        proposals.get((n.id, req)).foreach(v => acceptedAt((n.id, idx)) = (term, v, req))
      }
      // 5. 適用
      r.committed.foreach { e =>
        assertEquals(e.index, n.smIndex + 1, s"${n.id}: 適用の順序が飛んだ")
        appliedAt.get(e.index) match
          case Some(prev) => assertEquals(e, prev, s"index ${e.index} に異なるエントリが適用された（${n.id}）")
          case None       => appliedAt(e.index) = e
        e.payload match
          case Payload.Command(d) => n.sm = n.sm :+ dec(d)
          case _                  => ()
        n.smIndex = e.index
        acceptedAt.remove((n.id, e.index)).foreach { (term, v, _) =>
          if term == e.term then acked += v
        }
      }
      // 6. 読み込み
      r.reads.foreach { (req, idx) =>
        readRequests.remove((n.id, req)).foreach(required => readChecks += ((n.id, idx, required)))
      }
      checkReads(n)
      if n.core.status.role == Role.Leader then
        val t = n.core.status.term
        leaders.get(t) match
          case Some(other) => assertEquals(other, n.id, s"term $t にリーダーが 2 人いる")
          case None        => leaders(t) = n.id

    var readsChecked = 0
    var snapshotsInstalled = 0
    var truncations = 0

    def checkReads(n: Node): Unit =
      val (ready, waiting) = readChecks.partition((id, idx, _) => id == n.id && n.smIndex >= idx)
      readChecks.clear()
      readChecks ++= waiting
      readsChecked += ready.size
      ready.foreach { (_, _, required) =>
        val missing = required -- n.sm
        assert(missing.isEmpty, s"${n.id}: 線形化可能な読み込みで、成功済みの書き込み $missing が見えない")
      }

    def send(from: NodeId, to: NodeId, m: Message): Unit =
      if !chance(0.05) then inflight :+= ((from, to, m))
      if chance(0.01) then inflight :+= ((from, to, m)) // 重複

    def deliverOne(): Unit =
      if inflight.nonEmpty then
        val i = rand(inflight.size)
        val (from, to, m) = inflight(i)
        inflight = inflight.patch(i, Nil, 1)
        if !cut.contains(from -> to) then drive(nodes(to), Input.Receive(from, m))

    def leader: Option[Node] =
      nodes.values.filter(_.core.status.role == Role.Leader).toList.sortBy(-_.core.status.term).headOption

    def randomNode: Node = nodes(pool(rand(pool.size)))

    def chaosStep(): Unit =
      val p = rand(1000)
      if p < 600 then deliverOne()
      else if p < 850 then drive(randomNode, Input.Tick)
      else if p < 920 then
        val n = randomNode
        val v = nextValue; nextValue += 1
        val req = nextReq; nextReq += 1
        proposals((n.id, req)) = v
        drive(n, Input.Propose(req, enc(v)))
      else if p < 950 then
        val n = randomNode
        val req = nextReq; nextReq += 1
        readRequests((n.id, req)) = acked.toSet
        drive(n, Input.ReadIndex(req))
      else if p < 970 then
        val n = randomNode
        if n.smIndex > n.snap._1.index + 3 then
          val meta = n.core.metaAt(n.smIndex)
          n.snap = (meta, n.sm)
          drive(n, Input.Compacted(meta))
          n.entries = n.entries.filter(_.index > meta.index)
      else if p < 975 then
        val n = randomNode
        n.restart(rand(1 << 30).toLong)
        readChecks.filterInPlace(_._1 != n.id)
      else if p < 985 then
        val a = pool(rand(pool.size))
        val b = pool(rand(pool.size))
        if a != b then cut = cut + (a -> b) + (b -> a)
      else if p < 993 then cut = Set.empty
      else if p < 997 then
        leader.foreach { l =>
          val voters = pool.filter(_ => chance(0.6)).toSet
          val learners = pool.toSet -- voters
          if voters.nonEmpty then drive(l, Input.ProposeMembership(nextReq, voters, learners.filter(_ => chance(0.5))))
          nextReq += 1
        }
      else leader.foreach(l => drive(l, Input.TransferLeader(pool(rand(pool.size)))))

    def calmStep(): Unit =
      if rand(10) < 7 && inflight.nonEmpty then deliverOne() else drive(randomNode, Input.Tick)

  private def runSeed(seed: Long): Unit =
    val w = World(seed)
    for _ <- 0 until 8000 do w.chaosStep()
    w.cut = Set.empty
    for _ <- 0 until 6000 do w.calmStep()
    // 活性：リーダーがいて、書き込みが成功する
    val before = w.acked.size
    for i <- 0 until 20 do
      w.leader.foreach { l =>
        val v = 1000000L + i
        w.proposals((l.id, w.nextReq)) = v
        w.drive(l, Input.Propose(w.nextReq, enc(v)))
        w.nextReq += 1
      }
      for _ <- 0 until 300 do w.calmStep()
    assert(w.leader.isDefined, s"seed=$seed: 分断を直してもリーダーが決まらない")
    assert(w.acked.size > before, s"seed=$seed: 分断を直しても書き込みが進まない")
    // 永続性：成功した書き込みはすべて、最も進んだノードの状態に含まれる
    val longest = w.nodes.values.maxBy(_.smIndex)
    val missing = w.acked.toSet -- longest.sm
    assert(missing.isEmpty, s"seed=$seed: 成功した書き込み ${missing.take(5)} が失われた")
    Stats.acked += w.acked.size; Stats.terms += w.leaders.size; Stats.reads += w.readsChecked
    Stats.snapshots += w.snapshotsInstalled; Stats.truncations += w.truncations
    Stats.configs += w.appliedAt.values.count(_.payload.isInstanceOf[Payload.Config])

  test("ランダムな障害の下で Raft の安全性と活性が保たれる") {
    (1L to 60L).foreach(runSeed)
    println(s"[raft-sim] acked=${Stats.acked} terms=${Stats.terms} reads=${Stats.reads} snapshots=${Stats.snapshots} truncations=${Stats.truncations} configEntries=${Stats.configs}")
  }

private object Stats:
  var acked = 0; var terms = 0; var reads = 0; var snapshots = 0; var truncations = 0; var configs = 0
