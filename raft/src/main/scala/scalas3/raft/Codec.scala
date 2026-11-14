package scalas3.raft

import scalas3.core.{ByteReader, ByteWriter, DecodeError}
import scodec.bits.ByteVector


/** Raft の型のバイナリ表現（ディスクとネットワークで共通）。 */
object RaftCodec:


  extension (w: ByteWriter) private def bool(b: Boolean): ByteWriter = w.u8(if b then 1 else 0)
  extension (r: ByteReader) private def bool(): Boolean = r.u8() == 1

  private def set(w: ByteWriter, s: Set[NodeId]): Unit =
    w.varInt(s.size)
    s.toVector.sorted.foreach(w.string(_))
  private def set(r: ByteReader): Set[NodeId] = Vector.fill(r.varInt())(r.string()).toSet

  def membership(w: ByteWriter, m: Membership): Unit =
    set(w, m.voters); set(w, m.learners); set(w, m.outgoing)
  def membership(r: ByteReader): Membership = Membership(set(r), set(r), set(r))

  def entry(w: ByteWriter, e: LogEntry): Unit =
    w.u64(e.index).u64(e.term)
    e.payload match
      case Payload.Noop       => w.u8(0): Unit
      case Payload.Command(d) => w.u8(1).lenBytes(d): Unit
      case Payload.Config(m)  => w.u8(2); membership(w, m)

  def entry(r: ByteReader): LogEntry =
    val index = r.u64()
    val term = r.u64()
    val p = r.u8() match
      case 0 => Payload.Noop
      case 1 => Payload.Command(ByteVector.view(r.lenBytes()))
      case 2 => Payload.Config(membership(r))
      case k => throw DecodeError(s"unknown payload kind $k")
    LogEntry(index, term, p)

  def meta(w: ByteWriter, m: SnapshotMeta): Unit =
    w.u64(m.index).u64(m.term): Unit; membership(w, m.membership)
  def meta(r: ByteReader): SnapshotMeta = SnapshotMeta(r.u64(), r.u64(), membership(r))

  def hardState(w: ByteWriter, h: HardState): Unit =
    w.u64(h.term)
    h.vote match
      case Some(v) => w.u8(1); w.string(v)
      case None    => w.u8(0)
  def hardState(r: ByteReader): HardState =
    val t = r.u64()
    HardState(t, if r.bool() then Some(r.string()) else None)

  def message(w: ByteWriter, m: Message): Unit = m match
    case Message.Vote(t, pre, li, lt, force) =>
      w.u8(1).u64(t).bool(pre).u64(li).u64(lt).bool(force): Unit
    case Message.VoteResp(t, pre, granted) =>
      w.u8(2).u64(t).bool(pre).bool(granted): Unit
    case Message.Append(t, pi, pt, ents, commit, ctx) =>
      w.u8(3).u64(t).u64(pi).u64(pt).u64(commit).u64(ctx).varInt(ents.size)
      ents.foreach(entry(w, _))
    case Message.AppendResp(t, ok, li, ctx) =>
      w.u8(4).u64(t).bool(ok).u64(li).u64(ctx): Unit
    case Message.Snapshot(t, m, data) =>
      w.u8(5).u64(t); meta(w, m); w.lenBytes(data): Unit
    case Message.SnapshotResp(t, li) => w.u8(6).u64(t).u64(li): Unit
    case Message.TimeoutNow(t)       => w.u8(7).u64(t): Unit

  def message(r: ByteReader): Message = r.u8() match
    case 1 => Message.Vote(r.u64(), r.bool(), r.u64(), r.u64(), r.bool())
    case 2 => Message.VoteResp(r.u64(), r.bool(), r.bool())
    case 3 =>
      val t = r.u64(); val pi = r.u64(); val pt = r.u64(); val c = r.u64(); val ctx = r.u64()
      Message.Append(t, pi, pt, Vector.fill(r.varInt())(entry(r)), c, ctx)
    case 4 => Message.AppendResp(r.u64(), r.bool(), r.u64(), r.u64())
    case 5 =>
      val t = r.u64()
      val m = meta(r)
      Message.Snapshot(t, m, ByteVector.view(r.lenBytes()))
    case 6 => Message.SnapshotResp(r.u64(), r.u64())
    case 7 => Message.TimeoutNow(r.u64())
    case k => throw DecodeError(s"unknown message kind $k")

  /** 手書きの形式を scodec の Codec にする（RPC 用）。sizeHint は書き込みバッファの初期の大きさ。 */
  private def binary[A](decode: ByteReader => A)(encode: (ByteWriter, A) => Unit, sizeHint: A => Int = (_: A) => 256): scodec.Codec[A] =
    scodec.codecs.bytes.exmap(
      b =>
        scodec.Attempt.fromEither(
          // 読むだけなので、コピーせずに済むならそうする
          try Right(decode(ByteReader(b.toArrayUnsafe)))
          catch case e: DecodeError => Left(scodec.Err(e.getMessage))
        ),
      a =>
        val w = ByteWriter(sizeHint(a))
        encode(w, a)
        scodec.Attempt.successful(w.result())
    )

  /** RPC 用：(グループ, 送信元, メッセージ) */
  val envelope: scodec.Codec[(Long, NodeId, Message)] =
    binary(r => (r.u64(), r.string(), message(r))) { case (w, (group, from, m)) =>
      w.u64(group)
      w.string(from)
      message(w, m)
    }

  /** スナップショットのかたまり。 */
  final case class SnapshotChunk(group: Long, from: NodeId, term: Long, meta: SnapshotMeta, id: Long, seq: Int, last: Boolean, data: ByteVector)

  val snapshotChunk: scodec.Codec[SnapshotChunk] =
    binary(r => SnapshotChunk(r.u64(), r.string(), r.u64(), meta(r), r.u64(), r.varInt(), r.bool(), ByteVector.view(r.lenBytes())))(
      (w, c) =>
        w.u64(c.group)
        w.string(c.from)
        w.u64(c.term)
        meta(w, c.meta)
        w.u64(c.id).varInt(c.seq).bool(c.last).lenBytes(c.data): Unit,
      c => c.data.size.toInt + 128
    )

  /** RPC 用：(グループ, 送信元, メッセージの列)。同じ相手に溜まったメッセージを 1 回で送る。 */
  val batchEnvelope: scodec.Codec[(Long, NodeId, Vector[Message])] =
    binary { r =>
      val group = r.u64()
      val from = r.string()
      (group, from, Vector.fill(r.varInt())(message(r)))
    } { case (w, (group, from, ms)) =>
      w.u64(group)
      w.string(from)
      w.varInt(ms.size)
      ms.foreach(message(w, _))
    }
