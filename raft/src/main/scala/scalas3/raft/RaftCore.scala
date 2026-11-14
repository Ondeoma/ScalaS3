package scalas3.raft

import scala.collection.mutable

/** メモリ上のログ（スナップショット以降のエントリだけを持つ）。 */
final class MemLog(var snapIndex: Long, var snapTerm: Long, initial: Iterable[LogEntry]):
  private val buf = mutable.ArrayBuffer.from(initial.filter(_.index > snapIndex))
  require(buf.zipWithIndex.forall((e, i) => e.index == snapIndex + 1 + i), "log entries must be contiguous")

  def lastIndex: Long = snapIndex + buf.size
  def lastTerm: Long = if buf.isEmpty then snapTerm else buf.last.term

  def term(i: Long): Option[Long] =
    if i == snapIndex then Some(snapTerm)
    else if i < snapIndex || i > lastIndex then None
    else Some(buf((i - snapIndex - 1).toInt).term)

  def entry(i: Long): LogEntry = buf((i - snapIndex - 1).toInt)

  /** [from, to] の範囲（スナップショット以降に限る）。 */
  def slice(from: Long, to: Long): Vector[LogEntry] =
    if from > to then Vector.empty
    else buf.view.slice((from - snapIndex - 1).toInt, (to - snapIndex).toInt).toVector // 途中の ArrayBuffer を作らない

  def entries: Vector[LogEntry] = buf.toVector

  def truncateFrom(i: Long): Unit = buf.dropRightInPlace((lastIndex - i + 1).toInt)

  def append(e: LogEntry): Unit =
    require(e.index == lastIndex + 1, s"non-contiguous append: ${e.index} after $lastIndex")
    buf += e

  /** index 以前を捨てる。 */
  def compact(index: Long, term: Long): Unit =
    if index > snapIndex then
      val drop = math.min(buf.size.toLong, index - snapIndex).toInt
      buf.remove(0, drop)
      snapIndex = index
      snapTerm = term

  /** スナップショットで置き換える。一致する後続があれば残す。 */
  def restore(index: Long, t: Long): Unit =
    if term(index).contains(t) then compact(index, t)
    else
      buf.clear()
      snapIndex = index
      snapTerm = t

/** Raft のコア。I/O を一切行わず、入力を受けて Ready（やるべきこと）を返す。
  *
  * 1 つのファイバーからだけ使うこと（内部は可変だが、同じ入力列に対して常に同じ出力を返す決定的な実装）。
  * 乱数（選挙タイムアウトの揺らぎ）も seed から決まる。
  */
final class RaftCore(
    val id: NodeId,
    config: RaftConfig,
    initialHard: HardState,
    snapshot: SnapshotMeta,
    entries: Iterable[LogEntry],
    appliedIndex: Long,
    seed: Long
):
  import Message.*

  private var rng = scalas3.core.DetRandom(seed)

  // ---- 永続状態 ----
  private var term = initialHard.term
  private var vote = initialHard.vote
  private val log = MemLog(snapshot.index, snapshot.term, entries)
  private var snapMembership = snapshot.membership

  // ---- 揮発状態 ----
  private var role = Role.Follower
  private var leader: Option[NodeId] = None
  private var commit = math.max(snapshot.index, appliedIndex)
  private var applied = commit
  private var membership = Membership.empty
  private var membershipIndex = 0L
  private var electionElapsed = 0
  private var heartbeatElapsed = 0
  private var randomizedTimeout = config.electionTicks
  private val votes = mutable.Map.empty[NodeId, Boolean]

  private final class Progress(var next: Long, var matched: Long, var probing: Boolean, var active: Boolean, var snapshotPending: Boolean)
  private val progress = mutable.Map.empty[NodeId, Progress]
  private var readSeq = 0L
  private val acks = mutable.Map.empty[NodeId, Long]
  private val pendingReads = mutable.ArrayBuffer.empty[(Long, Long, Long)] // (reqId, index, seq)
  private val readsAwaitingCommit = mutable.ArrayBuffer.empty[Long]
  private var transferee: Option[NodeId] = None
  private var transferElapsed = 0
  private var pendingConfigIndex = 0L

  // ---- この step の出力 ----
  private var out = Ready()
  private var hardDirty = false

  recomputeMembership()
  resetElectionTimer()

  // ================= 公開 API =================

  def step(input: Input): Ready =
    out = Ready()
    hardDirty = false
    input match
      case Input.Tick                          => tick()
      case Input.Receive(from, msg)            => receive(from, msg)
      case Input.Propose(reqId, data)          => propose(reqId, Payload.Command(data))
      case Input.ProposeMembership(r, v, l)    => proposeMembership(r, v, l)
      case Input.ReadIndex(reqId)              => readIndex(reqId)
      case Input.TransferLeader(to)            => transferLeader(to)
      case Input.Compacted(meta)               => compacted(meta)
    collectCommitted()
    if hardDirty then out = out.copy(hardState = Some(HardState(term, vote)))
    out

  def status: RaftStatus =
    RaftStatus(id, role, term, leader, commit, applied, log.lastIndex, membership, progress.view.mapValues(_.matched).toMap)
  def currentTerm: Long = term
  def logEntries: Vector[LogEntry] = log.entries
  def lastIndex: Long = log.lastIndex

  /** リーダーなら、最近応答のあったフォロワー（Learner を含む）が受け取り済みの位置の最小値。ログをこれより先まで
    * 切り詰めると、そのフォロワーはスナップショットでしか追いつけなくなる。止まっているフォロワーのためには残さない
    * （残しても使われず、止まっている間リーダーのメモリを占め続ける）。
    */
  def followerMatchFloor: Option[Long] =
    val active = progress.values.filter(_.active)
    if role == Role.Leader && active.nonEmpty then Some(active.map(_.matched).min) else None
  def hardState: HardState = HardState(term, vote)

  /** index 時点での構成とターム（スナップショットを作るとき用）。index は適用済みであること。 */
  def metaAt(index: Long): SnapshotMeta =
    val t = log.term(index).getOrElse(throw IllegalArgumentException(s"no term for index $index"))
    val m = (index until log.snapIndex by -1).iterator
      .map(log.entry)
      .collectFirst { case LogEntry(_, _, Payload.Config(m)) => m }
      .getOrElse(snapMembership)
    SnapshotMeta(index, t, m)

  // ================= 内部 =================

  private def send(to: NodeId, m: Message): Unit = out = out.copy(messages = out.messages :+ (to -> m))
  private def reject(reqId: Long, e: RaftError): Unit = out = out.copy(rejected = out.rejected :+ (reqId -> e))

  private def resetElectionTimer(): Unit =
    electionElapsed = 0
    val (r, extra) = rng.nextInt(config.electionTicks)
    rng = r
    randomizedTimeout = config.electionTicks + extra

  private def recomputeMembership(): Unit =
    val found = (log.lastIndex until log.snapIndex by -1).iterator
      .map(log.entry)
      .collectFirst { case LogEntry(i, _, Payload.Config(m)) => (i, m) }
    found match
      case Some((i, m)) => membership = m; membershipIndex = i
      case None         => membership = snapMembership; membershipIndex = log.snapIndex
    if role == Role.Leader then
      membership.members.filterNot(_ == id).foreach { p =>
        if !progress.contains(p) then progress(p) = Progress(log.lastIndex + 1, 0, true, true, false)
      }
      progress.keys.filterNot(membership.members.contains).toList.foreach(progress.remove)

  private def otherVoters: Set[NodeId] = (membership.voters ++ membership.outgoing) - id
  private def others: Set[NodeId] = membership.members - id

  private def setTerm(t: Long): Unit =
    if t != term then
      term = t
      vote = None
      hardDirty = true

  private def becomeFollower(t: Long, lead: Option[NodeId]): Unit =
    val wasLeader = role == Role.Leader
    setTerm(t)
    role = Role.Follower
    leader = lead
    resetElectionTimer()
    transferee = None
    if wasLeader then
      progress.clear()
      pendingReads.foreach((r, _, _) => reject(r, RaftError.NotLeader(lead)))
      pendingReads.clear()
      readsAwaitingCommit.foreach(r => reject(r, RaftError.NotLeader(lead)))
      readsAwaitingCommit.clear()

  private def becomeLeader(): Unit =
    role = Role.Leader
    leader = Some(id)
    transferee = None
    heartbeatElapsed = 0
    electionElapsed = 0
    progress.clear()
    acks.clear()
    others.foreach(p => progress(p) = Progress(log.lastIndex + 1, 0, true, true, false))
    pendingConfigIndex =
      (log.lastIndex until commit by -1).iterator.map(log.entry).collectFirst { case LogEntry(i, _, Payload.Config(_)) => i }.getOrElse(0L)
    appendLocal(Payload.Noop)
    broadcastAppend()
    maybeCommit(): Unit

  private def campaign(pre: Boolean, force: Boolean): Unit =
    if !membership.isVoter(id) then ()
    else
      votes.clear()
      votes(id) = true
      if pre then
        role = Role.PreCandidate
        leader = None
      else
        setTerm(term + 1)
        vote = Some(id)
        hardDirty = true
        role = Role.Candidate
        leader = None
      resetElectionTimer()
      if membership.hasQuorum(p => votes.getOrElse(p, false)) then
        if pre then campaign(pre = false, force = false) else becomeLeader()
      else
        val t = if pre then term + 1 else term
        otherVoters.foreach(p => send(p, Vote(t, pre, log.lastIndex, log.lastTerm, force)))

  private def tick(): Unit =
    if role == Role.Leader then
      heartbeatElapsed += 1
      electionElapsed += 1
      if transferee.isDefined then
        transferElapsed += 1
        if transferElapsed >= config.electionTicks then transferee = None
      if electionElapsed >= config.electionTicks then
        electionElapsed = 0
        if config.checkQuorum then
          val ok = membership.hasQuorum(p => p == id || progress.get(p).exists(_.active))
          progress.values.foreach(_.active = false)
          if !ok then becomeFollower(term, None)
        progress.values.foreach(_.snapshotPending = false)
      if role == Role.Leader && heartbeatElapsed >= config.heartbeatTicks then
        heartbeatElapsed = 0
        broadcastAppend()
    else
      electionElapsed += 1
      if electionElapsed >= randomizedTimeout && membership.isVoter(id) then campaign(config.preVote, force = false)

  private def receive(from: NodeId, msg: Message): Unit =
    // ---- term の比較 ----
    if msg.term > term then
      msg match
        case Vote(_, true, _, _, _)     => () // PreVote では自分の term を変えない
        case VoteResp(_, true, true)    => ()
        case Vote(_, false, _, _, force) =>
          val inLease = config.checkQuorum && leader.isDefined && electionElapsed < config.electionTicks
          if inLease && !force then return // 有効なリーダーがいるので無視（揺さぶり防止）
          becomeFollower(msg.term, None)
        case _: Append | _: Snapshot | _: TimeoutNow => becomeFollower(msg.term, Some(from))
        case _                                       => becomeFollower(msg.term, None)
    else if msg.term < term then
      msg match
        case _: Append | _: Snapshot => send(from, AppendResp(term, success = false, 0, 0)) // 古いリーダーに知らせる
        case Vote(_, pre, _, _, _)   => send(from, VoteResp(term, pre, granted = false))
        case _                       => ()
      return

    msg match
      case Vote(t, pre, li, lt, force) =>
        val upToDate = lt > log.lastTerm || (lt == log.lastTerm && li >= log.lastIndex)
        val inLease = leader.isDefined && electionElapsed < config.electionTicks && !force
        val canVote =
          if pre then t > term && !inLease
          else vote.contains(from) || (vote.isEmpty && leader.isEmpty)
        val granted = canVote && upToDate
        if granted && !pre then
          vote = Some(from)
          hardDirty = true
          electionElapsed = 0
        send(from, VoteResp(if pre && granted then t else term, pre, granted))

      case VoteResp(_, pre, granted) =>
        val expected = if pre then Role.PreCandidate else Role.Candidate
        if role == expected then
          votes(from) = granted
          if membership.hasQuorum(p => votes.getOrElse(p, false)) then
            if pre then campaign(pre = false, force = false) else becomeLeader()
          else if membership.hasQuorum(p => votes.get(p).contains(false)) then becomeFollower(term, None)

      case a: Append =>
        if role != Role.Follower then becomeFollower(term, Some(from))
        leader = Some(from)
        electionElapsed = 0
        handleAppend(from, a)

      case AppendResp(_, success, li, ctx) if role == Role.Leader =>
        progress.get(from).foreach { pr =>
          pr.active = true
          acks(from) = math.max(acks.getOrElse(from, 0L), ctx)
          if success then
            pr.snapshotPending = false
            if li > pr.matched then pr.matched = li
            pr.next = math.max(pr.next, li + 1)
            pr.probing = false
            if maybeCommit() then broadcastAppend()
            else if pr.next <= log.lastIndex then sendAppend(from)
            if transferee.contains(from) && pr.matched == log.lastIndex then send(from, TimeoutNow(term))
          else if !pr.snapshotPending then
            // 一致する位置を探して戻る
            pr.next = math.max(1L, math.min(pr.next - 1, li + 1))
            pr.matched = math.min(pr.matched, pr.next - 1)
            pr.probing = true
            sendAppend(from)
          checkReads()
        }

      case Snapshot(_, meta, data) =>
        if role != Role.Follower then becomeFollower(term, Some(from))
        leader = Some(from)
        electionElapsed = 0
        if meta.index <= commit then send(from, SnapshotResp(term, commit))
        else
          log.restore(meta.index, meta.term)
          snapMembership = meta.membership
          commit = meta.index
          applied = meta.index
          recomputeMembership()
          out = out.copy(snapshot = Some((meta, data)))
          send(from, SnapshotResp(term, meta.index))

      case SnapshotResp(_, li) if role == Role.Leader =>
        progress.get(from).foreach { pr =>
          pr.active = true
          pr.snapshotPending = false
          pr.matched = math.max(pr.matched, li)
          pr.next = pr.matched + 1
          pr.probing = false
          sendAppend(from)
        }

      case TimeoutNow(_) =>
        if membership.isVoter(id) then campaign(pre = false, force = true)

      case _ => ()

  private def handleAppend(from: NodeId, a: Append): Unit =
    if a.prevIndex < commit then
      // 既にコミット済みの範囲。スナップショットより前でもここに来る
      val usable = a.entries.filter(_.index > commit)
      if usable.isEmpty then
        send(from, AppendResp(term, success = true, commit, a.ctx))
        return
      // prev はリーダーが送ってきたエントリの中にある（commit 以上なので）。リーダー側の term を使う
      val prev = usable.head.index - 1
      val prevTerm = a.entries.find(_.index == prev).map(_.term).getOrElse(a.prevTerm)
      handleAppend(from, a.copy(prevIndex = prev, prevTerm = prevTerm, entries = usable))
      return
    log.term(a.prevIndex) match
      case Some(t) if t == a.prevTerm =>
        var truncated: Option[Long] = None
        val toAppend = Vector.newBuilder[LogEntry]
        var conflict = false
        a.entries.foreach { e =>
          if conflict then toAppend += e
          else
            log.term(e.index) match
              case Some(t2) if t2 == e.term => ()
              case Some(_) =>
                require(e.index > commit, s"attempt to overwrite committed entry ${e.index}")
                truncated = Some(e.index)
                conflict = true
                toAppend += e
              case None =>
                conflict = true
                toAppend += e
        }
        val news = toAppend.result()
        truncated.foreach { i =>
          log.truncateFrom(i)
          out = out.copy(truncateFrom = Some(out.truncateFrom.fold(i)(math.min(_, i))), append = out.append.filter(_.index < i))
        }
        news.foreach(log.append)
        out = out.copy(append = out.append ++ news)
        if news.exists(_.payload.isInstanceOf[Payload.Config]) || truncated.isDefined then recomputeMembership()
        val lastNew = a.prevIndex + a.entries.size
        if a.commit > commit then commit = math.min(a.commit, lastNew)
        send(from, AppendResp(term, success = true, lastNew, a.ctx))
      case _ =>
        val hint = if a.prevIndex > log.lastIndex then log.lastIndex else a.prevIndex - 1
        send(from, AppendResp(term, success = false, hint, a.ctx))

  private def appendLocal(p: Payload): LogEntry =
    val e = LogEntry(log.lastIndex + 1, term, p)
    log.append(e)
    out = out.copy(append = out.append :+ e)
    if p.isInstanceOf[Payload.Config] then
      pendingConfigIndex = e.index
      recomputeMembership()
    e

  private def sendAppend(to: NodeId): Unit =
    progress.get(to).foreach { pr =>
      if pr.next <= log.snapIndex then
        if !pr.snapshotPending then
          pr.snapshotPending = true
          out = out.copy(snapshotSends = out.snapshotSends :+ to)
      else
        val prev = pr.next - 1
        val prevTerm = log.term(prev).get
        val last = if pr.probing then math.min(log.lastIndex, pr.next) else math.min(log.lastIndex, pr.next + config.maxEntriesPerAppend - 1)
        val ents = log.slice(pr.next, last)
        send(to, Append(term, prev, prevTerm, ents, commit, readSeq))
        if !pr.probing && ents.nonEmpty then pr.next = ents.last.index + 1
    }

  private def broadcastAppend(): Unit = others.foreach(sendAppend)

  private def maybeCommit(): Boolean =
    val n = membership.committedIndex(p => if p == id then log.lastIndex else progress.get(p).map(_.matched).getOrElse(0L))
    if n > commit && n <= log.lastIndex && log.term(n).contains(term) then
      commit = n
      onCommitAdvanced()
      true
    else false

  private def onCommitAdvanced(): Unit =
    if role == Role.Leader then
      // 現在の term で初めてコミットできたら、保留していた読み込みを開始できる
      if readsAwaitingCommit.nonEmpty && log.term(commit).contains(term) then
        val rs = readsAwaitingCommit.toList
        readsAwaitingCommit.clear()
        rs.foreach(readIndex)
      // Joint 構成がコミットされたら、新構成だけの構成に移る
      if membership.isJoint && commit >= membershipIndex then
        appendLocal(Payload.Config(membership.copy(outgoing = Set.empty)))
        broadcastAppend()
      else if !membership.isVoter(id) && commit >= membershipIndex then
        becomeFollower(term, None) // 自分が外された構成がコミットされた

  private def collectCommitted(): Unit =
    if commit > applied then
      val ents = log.slice(applied + 1, commit)
      out = out.copy(committed = out.committed ++ ents)
      applied = commit

  private def propose(reqId: Long, p: Payload): Unit =
    if role != Role.Leader then reject(reqId, RaftError.NotLeader(leader))
    else if transferee.isDefined then reject(reqId, RaftError.Busy("leadership transfer in progress"))
    else
      val e = appendLocal(p)
      out = out.copy(accepted = out.accepted :+ (reqId, e.index, e.term))
      broadcastAppend()
      maybeCommit(): Unit

  private def proposeMembership(reqId: Long, voters: Set[NodeId], learners: Set[NodeId]): Unit =
    if role != Role.Leader then reject(reqId, RaftError.NotLeader(leader))
    else if pendingConfigIndex > commit || membership.isJoint then reject(reqId, RaftError.Busy("membership change in progress"))
    else if voters.isEmpty then reject(reqId, RaftError.Busy("empty voter set"))
    else
      val next =
        if voters == membership.voters then Membership(voters, learners -- voters, Set.empty)
        else Membership(voters, learners -- voters, membership.voters)
      propose(reqId, Payload.Config(next))

  private def readIndex(reqId: Long): Unit =
    if role != Role.Leader then reject(reqId, RaftError.NotLeader(leader))
    else if !log.term(commit).contains(term) then readsAwaitingCommit += reqId
    else if otherVoters.isEmpty then out = out.copy(reads = out.reads :+ (reqId -> commit))
    else
      readSeq += 1
      pendingReads += ((reqId, commit, readSeq))
      broadcastAppend()

  private def checkReads(): Unit =
    if pendingReads.nonEmpty then
      val done = pendingReads.filter((_, _, seq) => membership.hasQuorum(p => p == id || acks.getOrElse(p, 0L) >= seq))
      if done.nonEmpty then
        pendingReads --= done
        out = out.copy(reads = out.reads ++ done.map((r, i, _) => r -> i))

  private def transferLeader(to: NodeId): Unit =
    if role == Role.Leader && to != id && membership.isVoter(to) && progress.contains(to) then
      transferee = Some(to)
      transferElapsed = 0
      if progress(to).matched == log.lastIndex then send(to, TimeoutNow(term)) else sendAppend(to)

  private def compacted(meta: SnapshotMeta): Unit =
    if meta.index > log.snapIndex && meta.index <= applied then
      log.compact(meta.index, meta.term)
      snapMembership = meta.membership
