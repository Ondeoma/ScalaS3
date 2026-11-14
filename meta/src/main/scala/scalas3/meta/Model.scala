package scalas3.meta

import scalas3.core.{ByteReader, ByteWriter, Bytes, DecodeError}
import scalas3.raft.{Membership, NodeId, RaftCodec, SnapshotMeta}
import scodec.bits.ByteVector


/** メタデータのキー空間の区分（設計書 §5.2）。 */
object Keys:
  val RangeLocalTag: Byte = 0x01
  val SystemTag: Byte = 0x02

  /** システムレンジ（レンジ 0）が受け持つ区間。 */
  val SystemStart: ByteVector = ByteVector(SystemTag)
  val SystemEnd: ByteVector = ByteVector(0x03)

  /** データ（オブジェクトなど）の先頭。 */
  val DataStart: ByteVector = ByteVector(0x03)

  // レンジ固有のキー（そのレンジの Raft で複製し、スナップショットにも含める）
  val Applied = 1
  val Descriptor = 2
  val RaftInit = 3
  val Stats = 4
  /** スナップショットの切り替え中の印（預かりの id、スナップショットの位置、記述子）。起動時に残っていれば切り替えをやり直す。 */
  val Installing = 5
  /** 最後に取り込んだスナップショットの位置（RaftNode が起動時に使う）。 */
  val SnapMeta = 6
  /** 統合のために凍結された印（値は空）。凍結されたレンジは読み書きと分割を拒否し、左隣のレンジに取り込まれるのを待つ。 */
  val Frozen = 7

  /** 届いたスナップショットの預かり。どのレンジの区間にも入らないよう 0x00 から始める。 */
  val StagingTag: ByteVector = ByteVector(0x00, 'S'.toByte)
  def stagingRange(rangeId: Long): ByteVector = StagingTag ++ ByteVector.fromLong(rangeId)
  def staging(rangeId: Long, id: Long): ByteVector = stagingRange(rangeId) ++ ByteVector.fromLong(id)

  def rangeLocalPrefix(rangeId: Long): ByteVector = ByteVector(RangeLocalTag) ++ ByteVector.fromLong(rangeId)
  def rangeLocal(rangeId: Long, kind: Int): ByteVector = rangeLocalPrefix(rangeId) :+ kind.toByte

  // システムレンジのキー
  val RangeDescriptorPrefix: ByteVector = ByteVector(SystemTag, 'r'.toByte)
  def rangeDescriptor(rangeId: Long): ByteVector = RangeDescriptorPrefix ++ ByteVector.fromLong(rangeId)
  val NextRangeId: ByteVector = ByteVector(SystemTag, 'n'.toByte)

/** レンジの記述子。区間は [start, end)、end が None なら末尾まで。 */
final case class RangeDescriptor(id: Long, start: ByteVector, end: Option[ByteVector], replicas: Set[NodeId], generation: Long):
  def contains(key: ByteVector): Boolean =
    Bytes.compare(start, key) <= 0 && end.forall(e => Bytes.compare(key, e) < 0)

  /** [from, until) と重なるか。 */
  def overlaps(from: ByteVector, until: Option[ByteVector]): Boolean =
    end.forall(e => Bytes.compare(from, e) < 0) && until.forall(u => Bytes.compare(start, u) < 0)

object RangeDescriptor:
  def encode(d: RangeDescriptor): ByteVector =
    val w = ByteWriter()
    w.u64(d.id).lenBytes(d.start)
    d.end match
      case Some(e) => w.u8(1).lenBytes(e)
      case None    => w.u8(0)
    writeNodes(w, d.replicas)
    w.u64(d.generation)
    w.toByteVector

  def decode(b: ByteVector): RangeDescriptor = decode(ByteReader(b.toArray))

  def decode(r: ByteReader): RangeDescriptor =
    val id = r.u64()
    val start = ByteVector.view(r.lenBytes())
    val end = if r.u8() == 1 then Some(ByteVector.view(r.lenBytes())) else None
    RangeDescriptor(id, start, end, readNodes(r), r.u64())

  /** ノードの集合：[個数 varInt]([名前 lenBytes])*（名前の順。どのノードでも同じバイト列になる）。 */
  private[meta] def writeNodes(w: ByteWriter, nodes: Set[NodeId]): Unit =
    w.varInt(nodes.size)
    nodes.toVector.sorted.foreach(n => w.string(n))

  private[meta] def readNodes(r: ByteReader): Set[NodeId] = Vector.fill(r.varInt())(r.string()).toSet

enum KvOp:
  case Put(key: ByteVector, value: ByteVector)
  case Delete(key: ByteVector)
  def key: ByteVector

/** 書き込みの前提条件（適用時に評価するので、アトミックな CAS になる）。 */
enum Precondition:
  case Absent(key: ByteVector)
  case Exists(key: ByteVector)
  case Equals(key: ByteVector, value: ByteVector)
  def key: ByteVector

object Precondition:
  /** key の値が今読んだ current のまま（current が None なら、無いまま）であること。読んでから書く CAS に使う。 */
  def unchanged(key: ByteVector, current: Option[ByteVector]): Precondition = current.fold(Absent(key))(Equals(key, _))

/** レンジの Raft に提案するコマンド。 */
enum Command:
  case Write(conditions: Vector[Precondition], ops: Vector[KvOp])

  /** splitKey で 2 つに分ける。右側が newRangeId になる。generation が一致しなければ拒否。 */
  case Split(splitKey: ByteVector, newRangeId: Long, generation: Long)

  /** Raft の構成変更が終わったあと、記述子のレプリカを合わせる。 */
  case SetReplicas(replicas: Set[NodeId], generation: Long)

  /** 統合の第 1 段：このレンジを凍結する（右側のレンジに送る）。取り消しはない。generation が一致しなければ拒否。 */
  case Freeze(generation: Long)

  /** 統合の第 2 段：凍結した右隣のレンジ（記述子と中身）を取り込み、区間を right.end まで広げる（左側のレンジに送る）。
    * 中身をコマンドに含めるので、各レプリカは手元の右のレプリカの状態によらず同じ結果になる。
    * leftGeneration が一致しない、区間が隣り合っていない、自分が凍結されている、のどれかなら拒否。
    * bytes は統合後のおおよその量（分割の判断に使う）。
    */
  case Merge(right: RangeDescriptor, entries: Vector[(ByteVector, ByteVector)], leftGeneration: Long, bytes: Long)

enum WriteResult:
  case Ok
  case ConditionFailed(index: Int)
  case NotInRange(descriptor: RangeDescriptor)
  case Rejected(reason: String)

/** 読み込みの要求と結果。 */
enum ReadRequest:
  case Get(key: ByteVector)
  case Scan(from: ByteVector, until: Option[ByteVector], limit: Int)
  /** 統合の状態。Value(Some(凍結されているか 1 バイト ++ 記述子)) を返す。 */
  case MergeState
  /** 凍結したレンジの中身を最大 limit 件（統合に使う）。凍結されていなければ Value(None)。 */
  case Export(limit: Int)

enum ReadResult:
  case Value(value: Option[ByteVector])
  case Entries(entries: Vector[(ByteVector, ByteVector)], more: Boolean)
  case NotInRange(descriptor: RangeDescriptor)

object MetaCodec:
  private def opt(w: ByteWriter, b: Option[ByteVector]): Unit = b match
    case Some(v) => w.u8(1).lenBytes(v): Unit
    case None    => w.u8(0): Unit
  private def opt(r: ByteReader): Option[ByteVector] = if r.u8() == 1 then Some(ByteVector.view(r.lenBytes())) else None
  private def bv(r: ByteReader): ByteVector = ByteVector.view(r.lenBytes())

  def command(c: Command): ByteVector =
    val w = ByteWriter()
    c match
      case Command.Write(conds, ops) =>
        w.u8(1).varInt(conds.size)
        conds.foreach {
          case Precondition.Absent(k)    => w.u8(1).lenBytes(k)
          case Precondition.Exists(k)    => w.u8(2).lenBytes(k)
          case Precondition.Equals(k, v) => w.u8(3).lenBytes(k).lenBytes(v)
        }
        w.varInt(ops.size)
        ops.foreach {
          case KvOp.Put(k, v) => w.u8(1).lenBytes(k).lenBytes(v)
          case KvOp.Delete(k) => w.u8(2).lenBytes(k)
        }
      case Command.Split(k, id, gen) => w.u8(2).lenBytes(k).u64(id).u64(gen)
      case Command.SetReplicas(rs, gen) =>
        w.u8(3)
        RangeDescriptor.writeNodes(w, rs)
        w.u64(gen)
      case Command.Freeze(gen) => w.u8(4).u64(gen)
      case Command.Merge(right, entries, gen, bytes) =>
        w.u8(5).lenBytes(RangeDescriptor.encode(right)).u64(gen).u64(bytes).varInt(entries.size)
        entries.foreach((k, v) => w.lenBytes(k).lenBytes(v))
    w.toByteVector

  def command(b: ByteVector): Command =
    val r = ByteReader(b.toArray)
    r.u8() match
      case 1 =>
        val conds = Vector.fill(r.varInt())(r.u8() match
          case 1 => Precondition.Absent(bv(r))
          case 2 => Precondition.Exists(bv(r))
          case 3 => Precondition.Equals(bv(r), bv(r))
          case k => throw DecodeError(s"condition $k"))
        val ops = Vector.fill(r.varInt())(r.u8() match
          case 1 => KvOp.Put(bv(r), bv(r))
          case 2 => KvOp.Delete(bv(r))
          case k => throw DecodeError(s"op $k"))
        Command.Write(conds, ops)
      case 2 => Command.Split(bv(r), r.u64(), r.u64())
      case 3 => Command.SetReplicas(RangeDescriptor.readNodes(r), r.u64())
      case 4 => Command.Freeze(r.u64())
      case 5 =>
        val right = RangeDescriptor.decode(bv(r))
        val gen = r.u64()
        val bytes = r.u64()
        Command.Merge(right, Vector.fill(r.varInt())((bv(r), bv(r))), gen, bytes)
      case k => throw DecodeError(s"command $k")

  def writeResult(res: WriteResult): ByteVector =
    val w = ByteWriter()
    res match
      case WriteResult.Ok                 => w.u8(0)
      case WriteResult.ConditionFailed(i) => w.u8(1).varInt(i)
      case WriteResult.NotInRange(d)      => w.u8(2).lenBytes(RangeDescriptor.encode(d))
      case WriteResult.Rejected(why)      => w.u8(3).string(why)
    w.toByteVector

  def writeResult(b: ByteVector): WriteResult =
    val r = ByteReader(b.toArray)
    r.u8() match
      case 0 => WriteResult.Ok
      case 1 => WriteResult.ConditionFailed(r.varInt())
      case 2 => WriteResult.NotInRange(RangeDescriptor.decode(bv(r)))
      case 3 => WriteResult.Rejected(r.string())
      case k => throw DecodeError(s"write result $k")

  def readRequest(q: ReadRequest): ByteVector =
    val w = ByteWriter()
    q match
      case ReadRequest.Get(k) => w.u8(1).lenBytes(k)
      case ReadRequest.Scan(from, until, limit) =>
        w.u8(2).lenBytes(from); opt(w, until); w.varInt(limit)
      case ReadRequest.MergeState    => w.u8(3)
      case ReadRequest.Export(limit) => w.u8(4).varInt(limit)
    w.toByteVector

  def readRequest(b: ByteVector): ReadRequest =
    val r = ByteReader(b.toArray)
    r.u8() match
      case 1 => ReadRequest.Get(bv(r))
      case 2 => ReadRequest.Scan(bv(r), opt(r), r.varInt())
      case 3 => ReadRequest.MergeState
      case 4 => ReadRequest.Export(r.varInt())
      case k => throw DecodeError(s"read request $k")

  def readResult(res: ReadResult): ByteVector =
    val w = ByteWriter()
    res match
      case ReadResult.Value(v) => w.u8(1); opt(w, v)
      case ReadResult.Entries(es, more) =>
        w.u8(2).varInt(es.size)
        es.foreach((k, v) => w.lenBytes(k).lenBytes(v))
        w.u8(if more then 1 else 0)
      case ReadResult.NotInRange(d) => w.u8(3).lenBytes(RangeDescriptor.encode(d))
    w.toByteVector

  def readResult(b: ByteVector): ReadResult =
    val r = ByteReader(b.toArray)
    r.u8() match
      case 1 => ReadResult.Value(opt(r))
      case 2 => ReadResult.Entries(Vector.fill(r.varInt())((bv(r), bv(r))), r.u8() == 1)
      case 3 => ReadResult.NotInRange(RangeDescriptor.decode(bv(r)))
      case k => throw DecodeError(s"read result $k")

  def snapshotMeta(m: SnapshotMeta): ByteVector =
    val w = ByteWriter()
    RaftCodec.meta(w, m)
    w.toByteVector

  def snapshotMeta(b: ByteVector): SnapshotMeta = RaftCodec.meta(ByteReader(b.toArray))

  /** 分割で生まれたレンジの Raft の初期位置。ログが空のレプリカがスナップショットを必要とするよう、0 より大きくする。 */
  def splitInitial(members: Set[NodeId]): SnapshotMeta = SnapshotMeta(5, 5, Membership(members, Set.empty, Set.empty))

  /** ReadRequest.MergeState の値。 */
  def mergeState(frozen: Boolean, d: RangeDescriptor): ByteVector =
    ByteVector(if frozen then 1.toByte else 0.toByte) ++ RangeDescriptor.encode(d)

  def mergeState(b: ByteVector): (Boolean, RangeDescriptor) = (b(0) == 1, RangeDescriptor.decode(b.drop(1)))
