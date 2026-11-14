package scalas3.cluster

import cats.effect.IO
import scalas3.core.{ByteReader, ByteWriter}
import scalas3.meta.{KvOp, MetaClient, Precondition, WriteResult}
import scalas3.raft.NodeId
import scalas3.runtime.NodeAddress
import scodec.bits.ByteVector

import java.nio.charset.StandardCharsets.UTF_8
import java.security.MessageDigest

enum NodeState:
  /** データを受け持つ。 */
  case Active

  /** 新しいデータを置かない。持っているデータは他へ移す（取り外しの準備）。 */
  case Draining

final case class NodeInfo(address: NodeAddress, zone: String, weight: Int, state: NodeState)

/** クラスタの構成。システムレンジに保存し、全ノードが同じものから同じ配置を計算する。 */
final case class Layout(version: Long, nodes: Map[NodeId, NodeInfo]):
  def active: Map[NodeId, NodeInfo] = nodes.filter(_._2.state == NodeState.Active)

object Layout:
  val Key: ByteVector = ByteVector(0x02, 'L'.toByte)

  def encode(l: Layout): ByteVector =
    val w = ByteWriter()
    w.u8(1).u64(l.version).varInt(l.nodes.size)
    l.nodes.toVector.sortBy(_._1).foreach { (id, n) =>
      w.string(id).string(n.address.host).u32(n.address.port)
      w.string(n.zone).varInt(n.weight).u8(n.state.ordinal)
    }
    w.toByteVector

  def decode(b: ByteVector): Layout =
    val r = ByteReader(b.toArray)
    r.u8()
    val version = r.u64()
    val nodes = Vector.fill(r.varInt()) {
      val id = r.string()
      val addr = NodeAddress(r.string(), r.u32())
      id -> NodeInfo(addr, r.string(), r.varInt(), NodeState.fromOrdinal(r.u8()))
    }.toMap
    Layout(version, nodes)

  def get(meta: MetaClient): IO[Option[Layout]] = meta.get(Key).map(_.map(decode))

  /** レイアウトを書き換える（CAS で競合したらやり直す）。 */
  def update(meta: MetaClient)(f: Layout => Either[String, Layout]): IO[Layout] =
    meta.get(Key).flatMap { raw =>
      val cur = raw.map(decode).getOrElse(Layout(0, Map.empty))
      f(cur) match
        case Left(err) => IO.raiseError(IllegalArgumentException(err))
        case Right(next0) =>
          val next = next0.copy(version = cur.version + 1)
          val cond = Precondition.unchanged(Key, raw)
          meta.write(Vector(cond), Vector(KvOp.Put(Key, encode(next)))).flatMap {
            case WriteResult.Ok                 => IO.pure(next)
            case WriteResult.ConditionFailed(_) => update(meta)(f)
            case other                          => IO.raiseError(new Exception(s"layout update failed: $other"))
          }
    }

/** 配置の計算（ランデブーハッシュ。重み付きで、ゾーンが分散するように選ぶ）。 */
object Placement:

  private def score(key: ByteVector, id: NodeId, weight: Int): Double =
    val md = MessageDigest.getInstance("SHA-256")
    md.update(key.toArray)
    md.update(id.getBytes(UTF_8))
    val h = ByteVector.view(md.digest()).take(8).toLong() >>> 11 // 53 ビット
    val u = (h.toDouble + 0.5) / (1L << 53).toDouble // (0, 1)
    -math.max(1, weight).toDouble / math.log(u)

  /** key を置くノードを優先順に n 個選ぶ。まず異なるゾーンから 1 つずつ、足りなければ残りから。 */
  def choose(key: ByteVector, nodes: Map[NodeId, NodeInfo], n: Int): Vector[NodeId] =
    val ranked = nodes.toVector.map((id, info) => (id, info.zone, score(key, id, info.weight))).sortBy(-_._3)
    val firstPerZone = ranked.groupBy(_._2).values.map(_.head).toVector.sortBy(-_._3)
    val picked = firstPerZone.take(n).map(_._1)
    (picked ++ ranked.map(_._1).filterNot(picked.contains)).take(math.min(n, nodes.size))

  def forRange(rangeId: Long, layout: Layout, replicas: Int): Set[NodeId] =
    choose(ByteVector('r'.toByte) ++ ByteVector.fromLong(rangeId), layout.active, replicas).toSet

  def forBlock(hash: ByteVector, layout: Layout, copies: Int): Vector[NodeId] =
    choose(ByteVector('b'.toByte) ++ hash, layout.active, copies)
