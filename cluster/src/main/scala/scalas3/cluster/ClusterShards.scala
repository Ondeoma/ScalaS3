package scalas3.cluster

import cats.effect.{IO, Ref}
import cats.syntax.all.*
import scalas3.block.*
import scalas3.raft.NodeId
import scalas3.rpc.{Method, Priority, RpcClient, RpcHandler}
import scodec.Codec
import scodec.bits.ByteVector
import scodec.codecs.*

import scala.concurrent.duration.*

object ShardRpc:
  private val id: Codec[BlockId] = bytes(32).xmap(BlockId(_), _.hash)
  private val shard: Codec[Shard] = (id :: uint8 :: uint8 :: uint8 :: int32 :: bytes).as[Shard]
  private val ref: Codec[(BlockId, Int)] = id :: uint8
  private val ids: Codec[Vector[BlockId]] = vectorOfN(int32, id)

  val Put: Method[Shard, Unit] = Method(310, "shard.put", shard, provide(()), Priority.Bulk)
  val Get: Method[(BlockId, Int), Option[Shard]] = Method(311, "shard.get", ref, optional(bool(8), shard), Priority.Bulk)
  val Has: Method[Vector[(BlockId, Int)], Vector[Boolean]] = Method(312, "shard.has", vectorOfN(int32, ref), vectorOfN(int32, bool(8)))
  val Touch: Method[Vector[BlockId], Unit] = Method(313, "shard.touch", ids, provide(()))
  val Protect: Method[Vector[BlockId], Unit] = Method(314, "shard.protect", ids, provide(()))
  val Release: Method[Vector[BlockId], Unit] = Method(315, "shard.release", ids, provide(()))

  def handler(local: ShardStore): RpcHandler =
    RpcHandler.of(
      Put.handledBy(local.put),
      Get.handledBy((s, i) => local.get(s, i)),
      Has.handledBy(_.traverse((s, i) => local.has(s, i))),
      Touch.handledBy(local.touchAll),
      Protect.handledBy(local.protect),
      Release.handledBy(local.release)
    )

/** イレイジャーコーディングのストライプをクラスタに置く。断片 i は、ストライプ ID から決まるノード列の i 番目に置く。 */
final class ClusterShards(
    self: NodeId,
    local: ShardStore,
    rpc: RpcClient,
    layout: IO[Layout],
    timeout: FiniteDuration = 30.seconds,
    /** Down と分かっているノード（ClusterBlocks と同じ扱い）。 */
    isDown: NodeId => Boolean = _ => false
) extends StripeClient:

  private val peers = Peers(self, rpc, timeout, isDown)

  /** 断片 0..n-1 の置き場所。ノードが n 台より少なければ、順に重ねて置く。 */
  def targets(stripe: BlockId, n: Int, l: Layout): Vector[NodeId] =
    val nodes = Placement.choose(ByteVector('s'.toByte) ++ stripe.hash, l.active, n)
    if nodes.isEmpty then Vector.fill(n)(self) else Vector.tabulate(n)(i => nodes(i % nodes.size))

  private[cluster] def putShard(l: Layout, n: NodeId, s: Shard): IO[Unit] = peers.on(l, n, ShardRpc.Put, s)(local.put(s))
  private[cluster] def getShardFrom(l: Layout, n: NodeId, stripe: BlockId, i: Int): IO[Option[Shard]] =
    peers.on(l, n, ShardRpc.Get, (stripe, i))(local.get(stripe, i))
  private[cluster] def hasShards(l: Layout, n: NodeId, refs: Vector[(BlockId, Int)]): IO[Vector[Boolean]] =
    peers.on(l, n, ShardRpc.Has, refs)(refs.traverse((s, i) => local.has(s, i)))

  /** 書き込みの過半数：k + ceil(m/2)（4+2 なら 5）。 */
  def writeQuorum(k: Int, m: Int): Int = k + (m + 1) / 2

  def putStripe(data: ByteVector, k: Int, m: Int): IO[BlockId] =
    val stripe = BlockId.of(data)
    layout.flatMap { l =>
      val shards = ReedSolomon(k, m).encode(data.toArray)
      val ts = targets(stripe, k + m, l)
      val writes = shards.toList.zipWithIndex.map { (p, i) =>
        peers.write(ts(i))(putShard(l, ts(i), Shard(stripe, i, k, m, data.size.toInt, ByteVector.view(p))))
      }
      peers.quorum(writes, writeQuorum(k, m))(ok => s"stripe ${stripe.hex.take(16)}: only $ok of ${k + m} shards written").as(stripe)
    }

  /** 断片 i を探す：置き場所 → 他の全ノード。 */
  private def findShard(l: Layout, stripe: BlockId, i: Int, home: NodeId): IO[Option[Shard]] =
    peers.readOrder(l, Vector(home)).collectFirstSomeM(n => getShardFrom(l, n, stripe, i).attempt.map(_.toOption.flatten))

  def getStripe(id: BlockId, size: Int, k: Int, m: Int): IO[ByteVector] =
    layout.flatMap { l =>
      val ts = targets(id, k + m, l)
      // まずデータ断片を並行に取り、足りなければパリティを取る
      (0 until k).toList.parTraverse(i => findShard(l, id, i, ts(i)).map(i -> _)).flatMap { first =>
        val got = first.collect { case (i, Some(s)) => i -> s.payload.toArray }.toMap
        val more =
          if got.size >= k then IO.pure(got)
          else
            (k until k + m).toList.foldLeftM(got) { (acc, i) =>
              if acc.size >= k then IO.pure(acc) else findShard(l, id, i, ts(i)).map(_.fold(acc)(s => acc.updated(i, s.payload.toArray)))
            }
        more.flatMap { present =>
          if present.size < k then IO.raiseError(BlockNotFoundException(id))
          else
            IO(ByteVector.view(ReedSolomon(k, m).decode(present, size))).flatTap(d =>
              IO.raiseUnless(BlockId.of(d) == id)(BlockNotFoundException(id))
            )
        }
      }
    }

  /** 断片はどのノードにもありうるので、全ノードに送る。 */
  private def broadcast(ids: Iterable[BlockId], m: Method[Vector[BlockId], Unit], localOp: Vector[BlockId] => IO[Unit]): IO[Unit] =
    layout.flatMap { l =>
      val xs = ids.toVector
      l.nodes.keys.toList.parTraverse_(n => peers.on(l, n, m, xs)(localOp(xs)).attempt.void)
    }

  def protect(ids: Iterable[BlockId]): IO[Unit] = broadcast(ids, ShardRpc.Protect, local.protect)
  def release(ids: Iterable[BlockId]): IO[Unit] = broadcast(ids, ShardRpc.Release, local.release)
  def touch(ids: Iterable[BlockId]): IO[Unit] = broadcast(ids, ShardRpc.Touch, local.touchAll)

  def lost(stripes: Iterable[(BlockId, Int, Int)]): IO[Set[BlockId]] =
    layout.flatMap { l =>
      val all = stripes.toVector.distinctBy(_._1)
      // 断片 i はノード列の i 番目にある
      val refsByNode = all.flatMap((s, k, m) => targets(s, k + m, l).zipWithIndex.map((n, i) => n -> (s, i))).groupMap(_._1)(_._2)
      peers.found(refsByNode)(hasShards(l, _, _)).map { found =>
        val counts = found.groupMapReduce(_._1)(_ => 1)(_ + _)
        all.collect { case (s, k, _) if counts.getOrElse(s, 0) < k => s }.toSet
      }
    }

/** 断片の修復とリバランス。自分が持つ断片のストライプごとに、
  *   - 置き場所に無い断片は、持っていれば送る。届いたと確認でき、自分が置き場所でなければ自分のコピーを消す
  *   - どこにも無い断片は、残りの断片から作り直して置き場所に送る（ストライプの断片を持つノードのうち、
  *     いちばん小さい番号を持つノードが担当する）
  */
object ShardRepairer:
  final case class Report(stripes: Int, pushed: Int, rebuilt: Int, removed: Int, failed: Int)

  def run(self: NodeId, local: ShardStore, cluster: ClusterShards, layout: IO[Layout]): IO[Report] =
    Ref.of[IO, Report](Report(0, 0, 0, 0, 0)).flatMap { report =>
      local.list
        .map(_._1)
        .changes(using cats.kernel.Eq.fromUniversalEquals)
        .evalMap(stripe => repairStripe(self, local, cluster, layout, stripe).attempt.flatMap {
          case Right((pushed, rebuilt, removed)) =>
            report.update(a => a.copy(stripes = a.stripes + 1, pushed = a.pushed + pushed, rebuilt = a.rebuilt + rebuilt, removed = a.removed + removed))
          case Left(_) => report.update(a => a.copy(stripes = a.stripes + 1, failed = a.failed + 1))
        })
        .compile
        .drain >> report.get
    }

  /** (送った数, 作り直した数, 消した数) */
  private def repairStripe(self: NodeId, local: ShardStore, cluster: ClusterShards, layout: IO[Layout], stripe: BlockId): IO[(Int, Int, Int)] =
    for
      l <- layout
      mine <- local.list1(stripe)
      sample <- mine.collectFirstSomeM(i => local.get(stripe, i))
      res <- sample match
        case None => IO.pure((0, 0, 0))
        case Some(s) =>
          val n = s.k + s.m
          val ts = cluster.targets(stripe, n, l)
          for
            // 置き場所のノードに、それぞれの断片があるか（答えなかったノードの断片は None。ノードごとに並行に聞く）
            presence <- ts.zipWithIndex.groupMap(_._1)(_._2).toList.parTraverse { (node, idxs) =>
              cluster.hasShards(l, node, idxs.map((stripe, _))).attempt.map(r => idxs.zip(r.fold(_ => Vector.fill(idxs.size)(None), _.map(Some(_)))))
            }.map(_.flatten.toMap)
            present = (i: Int) => presence.get(i).flatten.contains(true)
            // 自分の断片を置き場所へ
            pushed <- mine.traverse { i =>
              if ts(i) == self || present(i) then IO.pure(0)
              else local.get(stripe, i).flatMap(_.fold(IO.pure(0))(sh => cluster.putShard(l, ts(i), sh).as(1)))
            }
            // どこにも無い断片を作り直す
            known = (0 until n).filter(i => present(i) || mine.contains(i))
            missing = (0 until n).filter(i => presence.get(i).flatten.contains(false) && !mine.contains(i))
            leader = known.minOption.exists(i => mine.contains(i) || ts(i) == self)
            rebuilt <-
              if missing.isEmpty || !leader || known.size < s.k then IO.pure(0)
              else
                cluster.getStripe(stripe, s.size, s.k, s.m).flatMap { data =>
                  val shards = ReedSolomon(s.k, s.m).encode(data.toArray)
                  missing.toList.traverse_(i => cluster.putShard(l, ts(i), Shard(stripe, i, s.k, s.m, s.size, ByteVector.view(shards(i))))).as(missing.size)
                }
            // 置き場所でない自分の断片は、置き場所にあると確認できたら消す
            removed <- mine.filter(i => ts(i) != self).traverse { i =>
              cluster.hasShards(l, ts(i), Vector((stripe, i))).attempt.flatMap {
                case Right(Vector(true)) => local.delete(stripe, i).map(if _ then 1 else 0)
                case _                   => IO.pure(0)
              }
            }
          yield (pushed.sum, rebuilt, removed.sum)
    yield res
