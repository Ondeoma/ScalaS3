package scalas3.raft

import scodec.bits.ByteVector

type NodeId = String

/** クラスタ構成。outgoing が空でなければ Joint Consensus の途中（新旧両方の過半数が必要）。 */
final case class Membership(voters: Set[NodeId], learners: Set[NodeId], outgoing: Set[NodeId]):
  def isJoint: Boolean = outgoing.nonEmpty
  def members: Set[NodeId] = voters ++ learners ++ outgoing
  def isVoter(id: NodeId): Boolean = voters.contains(id) || outgoing.contains(id)

  /** 各構成で過半数が acked を満たすか。 */
  def hasQuorum(acked: NodeId => Boolean): Boolean =
    Membership.majority(voters, acked) && (outgoing.isEmpty || Membership.majority(outgoing, acked))

  /** 各構成で過半数が一致しているインデックスのうち、小さいほう。 */
  def committedIndex(matchOf: NodeId => Long): Long =
    val a = Membership.quorumIndex(voters, matchOf)
    if outgoing.isEmpty then a else math.min(a, Membership.quorumIndex(outgoing, matchOf))

object Membership:
  val empty: Membership = Membership(Set.empty, Set.empty, Set.empty)
  def of(voters: NodeId*): Membership = Membership(voters.toSet, Set.empty, Set.empty)

  private def majority(set: Set[NodeId], acked: NodeId => Boolean): Boolean =
    set.isEmpty || set.count(acked) > set.size / 2

  private def quorumIndex(set: Set[NodeId], matchOf: NodeId => Long): Long =
    if set.isEmpty then Long.MaxValue
    else
      val sorted = set.toVector.map(matchOf).sorted(using Ordering[Long].reverse)
      sorted(set.size / 2)

enum Payload:
  case Noop
  case Command(data: ByteVector)
  case Config(membership: Membership)

final case class LogEntry(index: Long, term: Long, payload: Payload)

final case class SnapshotMeta(index: Long, term: Long, membership: Membership)

/** 永続化が必要な状態。 */
final case class HardState(term: Long, vote: Option[NodeId])

enum Message:
  def term: Long

  /** 投票依頼。preVote なら term は「上げるつもりの term」で、受け手は自分の term を変えない。 */
  case Vote(term: Long, preVote: Boolean, lastIndex: Long, lastTerm: Long, force: Boolean)
  case VoteResp(term: Long, preVote: Boolean, granted: Boolean)

  /** ログの複製（entries が空ならハートビート）。ctx は ReadIndex 用の通し番号。 */
  case Append(term: Long, prevIndex: Long, prevTerm: Long, entries: Vector[LogEntry], commit: Long, ctx: Long)

  /** success なら lastIndex は一致した最後のインデックス、失敗ならリーダーが次に試すべき位置のヒント。 */
  case AppendResp(term: Long, success: Boolean, lastIndex: Long, ctx: Long)
  case Snapshot(term: Long, meta: SnapshotMeta, data: ByteVector)
  case SnapshotResp(term: Long, lastIndex: Long)

  /** リーダー移譲：受け手はすぐに選挙を始める。 */
  case TimeoutNow(term: Long)

enum Input:
  case Tick
  case Receive(from: NodeId, msg: Message)
  case Propose(reqId: Long, data: ByteVector)
  case ProposeMembership(reqId: Long, voters: Set[NodeId], learners: Set[NodeId])
  case ReadIndex(reqId: Long)
  case TransferLeader(to: NodeId)

  /** ドライバがスナップショットを取った。index 以前のログを捨ててよい。 */
  case Compacted(meta: SnapshotMeta)

enum Role:
  case Follower, PreCandidate, Candidate, Leader

enum RaftError:
  case NotLeader(leaderHint: Option[NodeId])
  case Busy(reason: String)

/** 1 回の step の結果。ドライバはこの順に処理すること:
  *   1. snapshot を永続化して状態マシンに復元する
  *   2. hardState・truncateFrom・append を永続化する（fsync まで）
  *   3. messages と snapshotSends を送る
  *   4. accepted / rejected を記録する
  *   5. committed を順に状態マシンへ適用する
  *   6. reads は、適用済みインデックスが追いついたら完了させる
  */
final case class Ready(
    hardState: Option[HardState] = None,
    truncateFrom: Option[Long] = None,
    append: Vector[LogEntry] = Vector.empty,
    snapshot: Option[(SnapshotMeta, ByteVector)] = None,
    messages: Vector[(NodeId, Message)] = Vector.empty,
    snapshotSends: Vector[NodeId] = Vector.empty,
    committed: Vector[LogEntry] = Vector.empty,
    accepted: Vector[(Long, Long, Long)] = Vector.empty, // (reqId, index, term)
    rejected: Vector[(Long, RaftError)] = Vector.empty,
    reads: Vector[(Long, Long)] = Vector.empty // (reqId, readIndex)
):
  def isEmpty: Boolean = this == Ready.empty

object Ready:
  val empty: Ready = Ready()

final case class RaftConfig(
    electionTicks: Int = 10,
    heartbeatTicks: Int = 1,
    preVote: Boolean = true,
    checkQuorum: Boolean = true,
    maxEntriesPerAppend: Int = 256
)

final case class RaftStatus(
    id: NodeId,
    role: Role,
    term: Long,
    leader: Option[NodeId],
    commit: Long,
    applied: Long,
    lastIndex: Long,
    membership: Membership,
    /** リーダーのとき、各フォロワーと一致している最後のインデックス。 */
    matched: Map[NodeId, Long] = Map.empty
)
