package scalas3.lsm

import scalas3.core.Bytes
import scodec.bits.ByteVector

import scala.collection.immutable.TreeMap

/** LSM 内部のエントリ。削除は墓石（Del）として残し、コンパクションで消す。 */
enum Entry:
  case Put(value: ByteVector)
  case Del

  /** 読み手に見える値（墓石なら None）。 */
  def toOption: Option[ByteVector] = this match
    case Put(v) => Some(v)
    case Del    => None

/** 公開 API の書き込み操作。 */
enum WriteOp:
  case Put(key: ByteVector, value: ByteVector)
  case Delete(key: ByteVector)

  def key: ByteVector

/** メモリ上の表。不変なので、そのままスナップショットとして使える。 */
final case class Memtable(seq: Long, map: TreeMap[ByteVector, Entry], bytes: Long):
  def isEmpty: Boolean = map.isEmpty

  def apply(ops: Seq[WriteOp]): Memtable =
    // 1 操作ごとに Memtable を作り直さない
    var m = map
    var n = bytes
    ops.foreach {
      case WriteOp.Put(k, v) => m = m.updated(k, Entry.Put(v)); n += k.size + v.size + 32
      case WriteOp.Delete(k) => m = m.updated(k, Entry.Del); n += k.size + 32
    }
    copy(map = m, bytes = n)

object Memtable:
  given Ordering[ByteVector] = Bytes.unsignedOrdering
  def empty(seq: Long): Memtable = Memtable(seq, TreeMap.empty, 0L)

  /** 新しい順に並んだ memtable から key を探す。 */
  def lookup(mems: List[Memtable], key: ByteVector): Option[Entry] =
    mems.iterator.flatMap(_.map.get(key)).nextOption()
