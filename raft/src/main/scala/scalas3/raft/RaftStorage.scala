package scalas3.raft

import cats.effect.{IO, Ref, Resource}
import cats.syntax.all.*
import fs2.io.file.Path
import scalas3.core.ByteWriter
import scalas3.runtime.{FileHandle, FileSystem, OpenMode}
import scodec.bits.ByteVector

/** Raft の永続状態（ハードステート、ログ、スナップショット）を保存する。 */
trait RaftStorage:
  def load: IO[RaftStorage.Loaded]

  /** 書き込む（まだ fsync しない）。 */
  def append(hs: Option[HardState], truncateFrom: Option[Long], entries: Vector[LogEntry]): IO[Unit] =
    appendAll(Vector((hs, truncateFrom, entries)))

  /** 複数の書き込みを 1 回のファイル書き込みにまとめる（まだ fsync しない）。 */
  def appendAll(items: Vector[(Option[HardState], Option[Long], Vector[LogEntry])]): IO[Unit]

  /** ここまでの append を永続化する。 */
  def sync: IO[Unit]

  /** スナップショットを保存し、ログをその後のエントリだけで作り直す。 */
  def rewrite(snapshot: SnapshotMeta, data: ByteVector, hs: HardState, entries: Vector[LogEntry]): IO[Unit]

  def latestSnapshot: IO[Option[(SnapshotMeta, ByteVector)]]

object RaftStorage:
  final case class Loaded(hs: HardState, snapshot: Option[(SnapshotMeta, ByteVector)], entries: Vector[LogEntry])

/** ディレクトリに 1 本の追記ログとスナップショットを置く実装（形式は RaftLogFormat）。
  *
  * ノードでは SharedRaftLog を使う。これは以前の形式のデータの移行と、単一グループのテストで使う。
  * 起動時に、壊れた末尾（書きかけのレコード）を切り詰める。
  */
final class FileRaftStorage private (
    fs: FileSystem,
    dir: Path,
    handle: Ref[IO, (FileHandle, IO[Unit], Long)], // (ハンドル, 解放, 書き込み位置)
    snapshotCache: Ref[IO, Option[(SnapshotMeta, ByteVector)]]
) extends RaftStorage:
  import FileRaftStorage.*
  import RaftLogFormat.*

  def load: IO[RaftStorage.Loaded] =
    for
      snap <- snapshotCache.get
      (h, _, _) <- handle.get
      size <- h.size
      bytes <- h.read(0, size.toInt)
      (hs, entries, validEnd) = replay(bytes.toArrayUnsafe)
      _ <- (h.truncate(validEnd) >> h.sync).whenA(validEnd < size)
      _ <- handle.update((hh, rel, _) => (hh, rel, validEnd))
    yield
      val snapIndex = snap.map(_._1.index).getOrElse(0L)
      RaftStorage.Loaded(hs, snap, entries.filter(_.index > snapIndex))

  def appendAll(items: Vector[(Option[HardState], Option[Long], Vector[LogEntry])]): IO[Unit] =
    val w = ByteWriter()
    items.foreach((hs, truncateFrom, es) => writeItem(hs, truncateFrom, es)(record(w)))
    if w.size == 0 then IO.unit
    else
      handle.get.flatMap { (h, rel, pos) =>
        h.write(pos, w.result()) >> handle.set((h, rel, pos + w.size))
      }

  def sync: IO[Unit] = handle.get.flatMap(_._1.sync)

  def rewrite(snapshot: SnapshotMeta, data: ByteVector, hs: HardState, es: Vector[LogEntry]): IO[Unit] =
    val lw = ByteWriter()
    record(lw)(hard(_, hs))
    if es.nonEmpty then record(lw)(entries(_, es))
    for
      // スナップショットを先に置く。ログの差し替え前に落ちても、古いログの冗長な部分は読み込み時に捨てられる
      _ <- writeSnapshot(fs, dir / SnapshotFile, snapshot, data)
      _ <- snapshotCache.set(Some((snapshot, data)))
      (_, oldRel, _) <- handle.get
      _ <- oldRel
      _ <- fs.writeAtomically(dir / LogFile, lw.toByteVector)
      opened <- fs.open(dir / LogFile, OpenMode.ReadWrite).allocated
      _ <- handle.set((opened._1, opened._2, lw.size.toLong))
    yield ()

  def latestSnapshot: IO[Option[(SnapshotMeta, ByteVector)]] = snapshotCache.get

  private def close: IO[Unit] = handle.get.flatMap(_._2)

object FileRaftStorage:
  import RaftLogFormat.*

  private val LogFile = "raft.log"
  private val SnapshotFile = "snapshot"

  /** (ハードステート, エントリ, 正しく読めた終端) */
  private def replay(arr: Array[Byte]): (HardState, Vector[LogEntry], Long) =
    val st = LogState(HardState(0, None), Vector.empty)
    val end = scan(arr)(r => st.replay(r.u8(), r))
    (st.hs, st.entries, end.toLong)

  def open(fs: FileSystem, dir: Path): Resource[IO, FileRaftStorage] =
    val init =
      for
        _ <- fs.createDirectories(dir)
        snap <- readSnapshot(fs, dir / SnapshotFile)
        existed <- fs.exists(dir / LogFile)
        opened <- fs.open(dir / LogFile, OpenMode.ReadWrite).allocated
        _ <- fs.syncDir(dir).whenA(!existed)
        h <- Ref.of[IO, (FileHandle, IO[Unit], Long)]((opened._1, opened._2, 0L))
        sc <- Ref.of[IO, Option[(SnapshotMeta, ByteVector)]](snap)
        st = new FileRaftStorage(fs, dir, h, sc)
        _ <- st.load // 書き込み位置を確定させ、壊れた末尾を切る
      yield st
    Resource.make(init)(_.close.attempt.void)
