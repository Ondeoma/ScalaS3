package scalas3.block

import cats.effect.{IO, Resource}
import cats.syntax.all.*
import fs2.Stream
import fs2.io.file.Path
import scalas3.runtime.FileSystem
import scodec.bits.ByteVector

import java.security.MessageDigest

/** 内容のハッシュ（SHA-256）で識別するブロック。 */
final case class BlockId(hash: ByteVector):
  require(hash.size == 32, "block id must be 32 bytes")
  def hex: String = hash.toHex
  override def toString: String = s"Block(${hex.take(16)}…)"

object BlockId:
  def of(data: ByteVector): BlockId =
    val md = MessageDigest.getInstance("SHA-256")
    md.update(data.toByteBuffer)
    BlockId(ByteVector.view(md.digest()))

  def fromHex(s: String): Option[BlockId] = ByteVector.fromHex(s).filter(_.size == 32).map(BlockId(_))

final class BlockNotFoundException(id: BlockId) extends java.io.IOException(s"block not found or corrupted on all disks: ${id.hex}")

/** ブロックの読み書き（ローカルのディスク群でも、クラスタ全体でもよい）。 */
trait BlockClient:
  def put(data: ByteVector): IO[BlockId]
  def get(id: BlockId): IO[ByteVector]

  /** GC から守る（書き込み中のアップロードなど）。 */
  def protect(ids: Iterable[BlockId]): IO[Unit]
  def release(ids: Iterable[BlockId]): IO[Unit]

  /** 「今使われた」ことにする（共有したとき）。 */
  def touch(ids: Iterable[BlockId]): IO[Unit]

  /** どこにも残っていないブロック（長いアップロードのコミット前の確認に使う）。 */
  def missing(ids: Iterable[BlockId]): IO[Set[BlockId]]

final case class BlockStoreConfig(
    /** 1 ノード内で何台のディスクに複製するか（ディスク数より多ければディスク数に丸める）。 */
    copies: Int = 2,
    volume: VolumeConfig = VolumeConfig()
)

/** 1 ノードのディスク群（JBOD）にブロックを保存する。
  *
  *   - 置き場所は、ディスクごとのスコア（ブロック ID とディスク番号のハッシュ）が高い順に copies 台（ランデブーハッシュ）。
  *     ディスクを足しても動くブロックが少ない。
  *   - 中身はディスクごとの追記型のボリューム（VolumeStore、`<disk>/volumes/blocks`）に置く。既にあれば最後に使われた
  *     時刻だけ進める（GC が「古くて参照のないブロック」を消すので、再利用されたことを示す）。
  *   - 読み込みはハッシュを検証し、壊れていれば別のディスクを試す。
  *   - 書き込み中のアップロードが使うブロックは「使用中」として登録し、GC から守る。
  *   - 起動してから GC の猶予期間が過ぎるまでは、GC で消さない（ボリュームの索引は落ちると時刻の更新を失いうるので）。
  */
final class BlockStore private (
    disks: Vector[Path],
    vols: Vector[VolumeStore],
    copies: Int,
    guard: GcGuard,
    startedAt: Long
) extends BlockClient:

  def diskCount: Int = disks.size

  /** ブロックを置くディスク（優先順）。スコアはディスクごとに 1 回だけ計算する（sortBy は比較のたびにキーを計算する）。 */
  def placement(id: BlockId): Vector[Int] =
    val key = id.hash.toArray
    disks.indices.toVector
      .map { i =>
        val md = MessageDigest.getInstance("SHA-256")
        md.update(key)
        md.update(BigInt(i).toByteArray)
        i -> -ByteVector.view(md.digest()).take(8).toLong()
      }
      .sortBy(_._2)
      .map(_._1)
      .take(math.max(1, math.min(copies, disks.size)))

  /** ブロックを書き、ID を返す。 */
  def put(data: ByteVector): IO[BlockId] =
    val id = BlockId.of(data)
    placement(id).parTraverse_(i => vols(i).put(id.hash, data)).as(id)

  /** ブロックを読む。置き場所 → 他のディスクの順に探し、ハッシュが一致したものを返す。 */
  def get(id: BlockId): IO[ByteVector] =
    val preferred = placement(id)
    val order = preferred ++ disks.indices.filterNot(preferred.contains)
    def attempt(rest: List[Int]): IO[ByteVector] = rest match
      case Nil => IO.raiseError(BlockNotFoundException(id))
      case i :: tail =>
        vols(i).get(id.hash).flatMap {
          case Some(data) if BlockId.of(data) == id => IO.pure(data)
          case _                                    => attempt(tail)
        }
    attempt(order.toList)

  /** 既存のブロックを「今使われた」ことにする（コピーなどで共有するとき。GC の猶予期間を延ばす）。 */
  def touch(ids: Iterable[BlockId]): IO[Unit] =
    ids.toList.traverse_(id => disks.indices.toList.traverse_(i => vols(i).touch(id.hash).attempt.void))

  /** このノードのどこかのディスクに正しいコピーがあれば返す。 */
  def getLocal(id: BlockId): IO[Option[ByteVector]] = get(id).map(Some(_)).recover { case _: BlockNotFoundException => None }

  def missing(ids: Iterable[BlockId]): IO[Set[BlockId]] = ids.toList.distinct.filterA(id => has(id).map(!_)).map(_.toSet)

  /** このノードのどこかのディスクにあるか（内容は検証しない）。 */
  def has(id: BlockId): IO[Boolean] = vols.toList.existsM(_.contains(id.hash))

  /** このノードのすべてのディスクから消す（別のノードへ移したあと）。使用中なら消さない。 */
  def deleteLocal(id: BlockId): IO[Boolean] =
    guard.deleteUnlessInUse(id)(vols.toList.traverse(_.delete(id.hash)).map(_.exists(identity)))

  /** スクラブ：全ディスクのブロックを読んでハッシュを確かめ、壊れたコピーを消す。消した数を返す。 */
  def scrub: IO[Int] =
    list.evalMap { (id, i) =>
      guard.locked(id)(vols(i).get(id.hash).flatMap {
        case Some(data) if BlockId.of(data) == id => IO.pure(false)
        case _                                    => vols(i).delete(id.hash)
      })
    }.filter(identity).compile.count.map(_.toInt)

  /** 書き込み中のアップロードが使うブロックを GC から守る。 */
  def protect(ids: Iterable[BlockId]): IO[Unit] = guard.protect(ids)

  def release(ids: Iterable[BlockId]): IO[Unit] = guard.release(ids)

  /** 全ディスクのブロック（ID, ディスク番号）。 */
  def list: Stream[IO, (BlockId, Int)] =
    Stream.emits(vols.zipWithIndex).flatMap((v, i) => v.entries().map((k, _) => BlockId(k) -> i))

  /** cutoff より古く、使用中でもなく、referenced が false なら消す。消したら true。 */
  def deleteIfUnused(id: BlockId, disk: Int, cutoffMillis: Long, referenced: BlockId => Boolean): IO[Boolean] =
    if startedAt > cutoffMillis then IO.pure(false)
    else
      guard.deleteUnlessInUse(id)(vols(disk).deleteIf(id.hash)(e => e.touched < cutoffMillis && !referenced(id)))

  /** ゴミの多いボリュームを詰め直す（GC の後に呼ぶ）。詰め直したボリュームの数。 */
  def compact: IO[Int] = vols.traverse(_.compact).map(_.sum)

object BlockStore:
  def apply(fs: FileSystem, disks: Vector[Path], config: BlockStoreConfig = BlockStoreConfig()): Resource[IO, BlockStore] =
    for
      _ <- Resource.eval(IO.raiseWhen(disks.isEmpty)(IllegalArgumentException("at least one disk is required")))
      vols <- disks.traverse(d => VolumeStore.open(fs, d / "volumes" / "blocks", config.volume))
      _ <- Resource.eval(disks.zip(vols).traverse_((d, v) => migrateLegacy(fs, d, v)))
      guard <- Resource.eval(GcGuard.create)
      now <- Resource.eval(IO.realTime.map(_.toMillis))
    yield new BlockStore(disks, vols, config.copies, guard, now)

  /** 以前の形式（ブロックごとのファイル `<disk>/blocks/xx/yy/<id>`）を、ボリュームに移して消す。 */
  private def migrateLegacy(fs: FileSystem, disk: Path, vol: VolumeStore): IO[Unit] =
    Legacy
      .files(fs, disk / "blocks")
      .evalMap { p =>
        BlockId.fromHex(p.fileName.toString) match
          case None => fs.deleteIfExists(p).void // 書きかけの一時ファイルなど
          case Some(id) =>
            fs.readAll(p).attempt.flatMap {
              case Right(data) if BlockId.of(data) == id => vol.put(id.hash, data).void
              case _                                     => IO.unit // 壊れたコピーは移さない
            } >> fs.deleteIfExists(p).void
      }
      .compile
      .drain
