package scalas3.cluster

import cats.effect.{IO, Ref}
import cats.syntax.all.*
import scalas3.block.{BlockClient, BlockId, BlockNotFoundException, BlockStore}
import scalas3.raft.NodeId
import scalas3.rpc.{Method, Priority, RpcClient, RpcHandler}
import scodec.Codec
import scodec.bits.ByteVector
import scodec.codecs.*

import scala.concurrent.duration.*

/** ノード間のブロック操作。 */
object BlockRpc:
  private val id: Codec[BlockId] = bytes(32).xmap(BlockId(_), _.hash)
  private val ids: Codec[Vector[BlockId]] = vectorOfN(int32, id)

  val Put: Method[ByteVector, Unit] = Method(300, "block.put", bytes, provide(()), Priority.Bulk)
  val Get: Method[BlockId, Option[ByteVector]] = Method(301, "block.get", id, optional(bool(8), bytes), Priority.Bulk)
  val Has: Method[Vector[BlockId], Vector[Boolean]] = Method(302, "block.has", ids, vectorOfN(int32, bool(8)))
  val Touch: Method[Vector[BlockId], Unit] = Method(303, "block.touch", ids, provide(()))
  val Protect: Method[Vector[BlockId], Unit] = Method(304, "block.protect", ids, provide(()))
  val Release: Method[Vector[BlockId], Unit] = Method(305, "block.release", ids, provide(()))

  def handler(local: BlockStore): RpcHandler =
    RpcHandler.of(
      Put.handledBy(d => local.put(d).void),
      Get.handledBy(local.getLocal),
      Has.handledBy(_.traverse(local.has)),
      Touch.handledBy(local.touch),
      Protect.handledBy(local.protect),
      Release.handledBy(local.release)
    )

final class NotEnoughReplicasException(msg: String) extends java.io.IOException(msg)

/** クラスタ全体のブロック。置き場所はレイアウトから決め、書き込みは過半数のコピーが書けたら成功とする。
  * 書けなかったコピーは、持っているノードの修復処理（[[Repairer]]）が後で補う。
  */
final class ClusterBlocks(
    self: NodeId,
    local: BlockStore,
    rpc: RpcClient,
    layout: IO[Layout],
    copies: Int,
    timeout: FiniteDuration = 30.seconds,
    /** Down と分かっているノード（Health）。書き込みでは飛ばし（後で修復が補う）、読み込みでは最後に回す。 */
    isDown: NodeId => Boolean = _ => false
) extends BlockClient:

  private val peers = Peers(self, rpc, timeout, isDown)

  def targets(id: BlockId, l: Layout): Vector[NodeId] = Placement.forBlock(id.hash, l, copies)

  def put(data: ByteVector): IO[BlockId] =
    val id = BlockId.of(data)
    layout.flatMap { l =>
      val ts = targets(id, l)
      val need = if ts.isEmpty then 1 else ts.size / 2 + 1
      val writes =
        if ts.isEmpty then List(local.put(data).void.attempt)
        else ts.toList.map(n => peers.write(n)(push(l, n, data)))
      peers.quorum(writes, need)(ok => s"block ${id.hex.take(16)}: only $ok of ${ts.size} copies written (need $need)").as(id)
    }

  /** 置き場所（自分を先に）→ 他の全ノードの順に探す（配置替えの途中でも読めるように）。 */
  def get(id: BlockId): IO[ByteVector] =
    layout.flatMap { l =>
      val ts = targets(id, l)
      val order = peers.readOrder(l, ts.filter(_ == self) ++ ts.filterNot(_ == self))
      order
        .collectFirstSomeM(n => peers.on(l, n, BlockRpc.Get, id)(local.getLocal(id)).attempt.map(_.toOption.flatten.filter(BlockId.of(_) == id)))
        .flatMap {
          case Some(d) => IO.pure(d)
          // レイアウトに自分がいなくても、手元にあれば返す
          case None => local.getLocal(id).flatMap(IO.fromOption(_)(BlockNotFoundException(id)))
        }
    }

  private def byTarget(ids: Vector[BlockId], l: Layout): Map[NodeId, Vector[BlockId]] =
    ids.flatMap(id => targets(id, l).map(_ -> id)).groupMap(_._1)(_._2)

  /** 置き場所のノードに送る。自分が置き場所でないブロックも、手元にあるかもしれないので自分にも適用する。 */
  private def broadcast(ids: Iterable[BlockId], m: Method[Vector[BlockId], Unit], localOp: Iterable[BlockId] => IO[Unit]): IO[Unit] =
    layout.flatMap { l =>
      val all = ids.toVector
      val byNode = byTarget(all, l)
      byNode.toList.parTraverse_((n, xs) => peers.on(l, n, m, xs)(localOp(xs)).attempt.void) >>
        localOp(all).whenA(!byNode.contains(self))
    }

  def protect(ids: Iterable[BlockId]): IO[Unit] = broadcast(ids, BlockRpc.Protect, local.protect)
  def release(ids: Iterable[BlockId]): IO[Unit] = broadcast(ids, BlockRpc.Release, local.release)
  def touch(ids: Iterable[BlockId]): IO[Unit] = broadcast(ids, BlockRpc.Touch, local.touch)

  /** 置き場所のどのノードにも無いブロック。答えなかったノードは「持っていない」とみなす。 */
  def missing(ids: Iterable[BlockId]): IO[Set[BlockId]] =
    layout.flatMap { l =>
      val wanted = ids.toVector.distinct
      peers.found(byTarget(wanted, l))(has(l, _, _)).map(found => wanted.toSet -- found)
    }

  /** 修復用：ノード n が ids を持っているか。 */
  private[cluster] def has(l: Layout, n: NodeId, ids: Vector[BlockId]): IO[Vector[Boolean]] =
    peers.on(l, n, BlockRpc.Has, ids)(ids.traverse(local.has))

  private[cluster] def push(l: Layout, n: NodeId, data: ByteVector): IO[Unit] =
    peers.on(l, n, BlockRpc.Put, data)(local.put(data).void)

/** 修復とリバランス。自分のディスクにあるブロックについて、
  *   - 置き場所のノードに無ければ送る（コピー不足の修復、ノード追加時の移動）
  *   - 自分が置き場所でなく、置き場所の全ノードが持っていれば、自分のコピーを消す（移動の完了）
  */
object Repairer:
  final case class Report(scanned: Int, pushed: Int, removed: Int, failed: Int):
    private[cluster] def +(o: Report): Report = Report(scanned + o.scanned, pushed + o.pushed, removed + o.removed, failed + o.failed)

  def run(self: NodeId, local: BlockStore, cluster: ClusterBlocks, layout: IO[Layout], batch: Int = 256): IO[Report] =
    Ref.of[IO, Report](Report(0, 0, 0, 0)).flatMap { report =>
      local.list
        .map(_._1)
        .zipWithPrevious
        .collect { case (prev, id) if !prev.contains(id) => id } // 複数ディスクの重複を（隣接分だけ）除く
        .chunkN(batch)
        .evalMap(chunk => layout.flatMap(l => repairChunk(self, local, cluster, l, chunk.toVector.distinct)).flatMap(r => report.update(_ + r)))
        .compile
        .drain >> report.get
    }

  private def repairChunk(self: NodeId, local: BlockStore, cluster: ClusterBlocks, l: Layout, ids: Vector[BlockId]): IO[Report] =
    val wanted = ids.map(id => id -> cluster.targets(id, l)).toMap
    val peers = (wanted.values.flatten.toSet - self).toList
    for
      // 各ノードが持っているかをまとめて（ノードごとに並行に）聞く
      holdings <- peers.parTraverse { p =>
        val ask = ids.filter(id => wanted(id).contains(p))
        cluster.has(l, p, ask).attempt.map(r => p -> r.map(ask.zip(_).collect { case (id, true) => id }.toSet))
      }.map(_.toMap)
      results <- ids.traverse { id =>
        val ts = wanted(id)
        val missing = ts.filter(n => n != self && holdings.get(n).exists(_.exists(!_.contains(id))))
        val unknown = ts.exists(n => n != self && holdings.get(n).exists(_.isLeft))
        val pushIt =
          if missing.isEmpty then IO.pure((0, 0))
          else
            local.getLocal(id).flatMap {
              case None => IO.pure((0, 1))
              case Some(d) =>
                missing.traverse(n => cluster.push(l, n, d).attempt).map(rs => (rs.count(_.isRight), rs.count(_.isLeft)))
            }
        pushIt.flatMap { (pushed, failed) =>
          // 自分が置き場所でなく、全員に行き渡ったなら消す
          val allHave = ts.nonEmpty && !unknown && failed == 0
          val remove = if !ts.contains(self) && allHave then local.deleteLocal(id) else IO.pure(false)
          remove.map(r => Report(0, pushed, if r then 1 else 0, failed))
        }
      }
    yield results.foldLeft(Report(ids.size, 0, 0, 0))(_ + _)
