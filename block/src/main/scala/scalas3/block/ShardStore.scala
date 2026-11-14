package scalas3.block

import cats.effect.{IO, Resource}
import cats.syntax.all.*
import fs2.Stream
import fs2.io.file.Path
import scalas3.core.{ByteReader, ByteWriter, Crc32c}
import scalas3.runtime.FileSystem
import scodec.bits.ByteVector

/** イレイジャーコーディングの断片。stripe は元のブロック（平文または暗号文）の SHA-256。 */
final case class Shard(stripe: BlockId, index: Int, k: Int, m: Int, size: Int, payload: ByteVector)

/** 断片を 1 ノードのディスク群に保存する。中身はディスクごとの追記型のボリューム（`<disk>/volumes/shards`）に、
  * キー「ストライプ ID ＋番号」で置く。
  * {{{
  * [magic "SHD1"][k u8][m u8][index u8][元の大きさ u32][crc32c(payload) u32][payload]
  * }}}
  * 同じストライプの断片は別のノードに置くので、ノード内では 1 コピーだけ持つ（ディスクはハッシュで選ぶ）。
  * 起動してから GC の猶予期間が過ぎるまでは、GC で消さない（BlockStore と同じ理由）。
  */
final class ShardStore private (vols: Vector[VolumeStore], guard: GcGuard, startedAt: Long):
  private val Magic = 0x53484431 // "SHD1"

  private def key(stripe: BlockId, index: Int): ByteVector = stripe.hash :+ index.toByte
  // Long で足す（Int だと、ハッシュの先頭が Int.MaxValue に近いときに桁あふれして負の番号になった）
  private def disk(stripe: BlockId, index: Int): Int = (((stripe.hash.take(4).toInt() & Int.MaxValue).toLong + index) % vols.size).toInt
  private def vol(stripe: BlockId, index: Int): VolumeStore = vols(disk(stripe, index))

  def put(s: Shard): IO[Unit] =
    val w = ByteWriter(s.payload.size.toInt + 32)
    w.u32(Magic).u8(s.k).u8(s.m).u8(s.index).u32(s.size).u32(Crc32c(s.payload)).bytes(s.payload)
    vol(s.stripe, s.index).put(key(s.stripe, s.index), w.result()).void

  /** 断片を読む。無いか壊れていれば None。 */
  def get(stripe: BlockId, index: Int): IO[Option[Shard]] =
    vol(stripe, index).get(key(stripe, index)).map(_.flatMap(parse(stripe, _)))

  private def parse(stripe: BlockId, b: ByteVector): Option[Shard] =
    try
      val r = ByteReader(b.toArray)
      if r.u32() != Magic then None
      else
        val k = r.u8(); val m = r.u8(); val idx = r.u8(); val size = r.u32(); val crc = r.u32()
        val payload = ByteVector.view(r.bytes(r.remaining))
        Option.when(Crc32c(payload) == crc)(Shard(stripe, idx, k, m, size, payload))
    catch case _: Exception => None

  def has(stripe: BlockId, index: Int): IO[Boolean] = vol(stripe, index).contains(key(stripe, index))

  def touch(stripe: BlockId): IO[Unit] =
    list1(stripe).flatMap(_.traverse_(i => vol(stripe, i).touch(key(stripe, i)).attempt.void))

  def touchAll(stripes: Iterable[BlockId]): IO[Unit] = stripes.toList.traverse_(touch)

  /** このノードが持っている、そのストライプの断片の番号。 */
  def list1(stripe: BlockId): IO[List[Int]] =
    vols.toList.flatTraverse(_.entries(stripe.hash).map((k, _) => k.last & 0xff).compile.toList).map(_.distinct.sorted)

  /** 全ディスクの断片 (ストライプ, 番号)。 */
  def list: Stream[IO, (BlockId, Int)] =
    Stream.emits(vols).flatMap(_.entries().collect { case (k, _) if k.size == 33 => (BlockId(k.take(32)), k.last & 0xff) })

  def delete(stripe: BlockId, index: Int): IO[Boolean] =
    guard.deleteUnlessInUse(stripe)(vol(stripe, index).delete(key(stripe, index)))

  def deleteIfUnused(stripe: BlockId, index: Int, cutoffMillis: Long, referenced: BlockId => Boolean): IO[Boolean] =
    if startedAt > cutoffMillis then IO.pure(false)
    else
      guard.deleteUnlessInUse(stripe)(vol(stripe, index).deleteIf(key(stripe, index))(e => e.touched < cutoffMillis && !referenced(stripe)))

  def protect(ids: Iterable[BlockId]): IO[Unit] = guard.protect(ids)
  def release(ids: Iterable[BlockId]): IO[Unit] = guard.release(ids)

  /** 壊れた断片を消す（修復で作り直される）。 */
  def scrub: IO[Int] =
    list.evalMap((s, i) => get(s, i).flatMap { case Some(_) => IO.pure(false); case None => delete(s, i) })
      .filter(identity).compile.count.map(_.toInt)

  /** ゴミの多いボリュームを詰め直す（GC の後に呼ぶ）。 */
  def compact: IO[Int] = vols.traverse(_.compact).map(_.sum)

object ShardStore:
  def apply(fs: FileSystem, disks: Vector[Path], volume: VolumeConfig = VolumeConfig()): Resource[IO, ShardStore] =
    for
      // ディスクが無いと、断片を置くディスクを選べない（BlockStore と同じく、開くときに断る）
      _ <- Resource.eval(IO.raiseWhen(disks.isEmpty)(IllegalArgumentException("at least one disk is required")))
      vols <- disks.traverse(d => VolumeStore.open(fs, d / "volumes" / "shards", volume))
      store <- Resource.eval((GcGuard.create, IO.realTime.map(_.toMillis)).mapN(new ShardStore(vols, _, _)))
      _ <- Resource.eval(disks.traverse_(d => migrateLegacy(fs, d, store)))
    yield store

  /** 以前の形式（断片ごとのファイル `<disk>/shards/xx/yy/<stripe>.<番号>`）を、ボリュームに移して消す。 */
  private def migrateLegacy(fs: FileSystem, disk: Path, store: ShardStore): IO[Unit] =
    Legacy
      .files(fs, disk / "shards")
      .evalMap { p =>
        val parsed = p.fileName.toString.split('.') match
          case Array(h, i) => (BlockId.fromHex(h), i.toIntOption).tupled
          case _           => None
        parsed.fold(IO.unit) { (stripe, i) =>
          fs.readAll(p).attempt.flatMap {
            case Right(b) => store.parse(stripe, b).filter(_.index == i).traverse_(store.put)
            case Left(_)  => IO.unit
          }
        } >> fs.deleteIfExists(p).void
      }
      .compile
      .drain

/** イレイジャーコーディングでブロックを読み書きする（クラスタ全体）。 */
trait StripeClient:
  /** data を k+m 個の断片にして保存し、ストライプ ID（data の SHA-256）を返す。 */
  def putStripe(data: ByteVector, k: Int, m: Int): IO[BlockId]
  def getStripe(id: BlockId, size: Int, k: Int, m: Int): IO[ByteVector]
  def protect(ids: Iterable[BlockId]): IO[Unit]
  def release(ids: Iterable[BlockId]): IO[Unit]
  def touch(ids: Iterable[BlockId]): IO[Unit]

  /** 復元できなくなったストライプ（残っている断片が k 個未満）。引数は (ストライプ, k, m)。 */
  def lost(stripes: Iterable[(BlockId, Int, Int)]): IO[Set[BlockId]]
