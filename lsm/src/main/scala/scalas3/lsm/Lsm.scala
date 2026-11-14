package scalas3.lsm

import cats.effect.std.{Mutex, Queue}
import cats.effect.{IO, Ref, Resource}
import cats.syntax.all.*
import fs2.Stream
import fs2.concurrent.SignallingRef
import fs2.io.file.Path
import scalas3.core.Bytes
import scalas3.runtime.{FileSystem, OpenMode}
import scodec.bits.ByteVector

final case class LsmConfig(
    memtableBytes: Long = 64L * 1024 * 1024,
    maxImmutables: Int = 4,
    blockBytes: Int = 4096,
    targetFileBytes: Long = 16L * 1024 * 1024,
    l0CompactionTrigger: Int = 4,
    baseLevelBytes: Long = 128L * 1024 * 1024,
    levelMultiplier: Int = 10,
    maxLevels: Int = 7,
    bloomBitsPerKey: Int = 10
)

object LsmConfig:
  /** ヒープの大きさに合わせた設定。memtable は書き込み中の 1 枚と flush 待ちの 4 枚を合わせてヒープの約 1/5 に
    * 収める（固定の 64 MiB × 5 枚だと、小さなヒープではそれだけで使い切る）。flush 待ちを 2 枚にすると、
    * 並行数 128 の小さな PUT で書き込みが flush を待ち、p99 が 550 ms になった（4 枚では 180 ms）。
    */
  def forHeap(maxHeapBytes: Long = Runtime.getRuntime.maxMemory): LsmConfig =
    val mem = math.max(4L * 1024 * 1024, math.min(64L * 1024 * 1024, maxHeapBytes / 24))
    LsmConfig(memtableBytes = mem, maxImmutables = 4)

/** ある時点の一貫したビュー。解放するまで、参照しているファイルは消されない。 */
final class LsmSnapshot private[lsm] (
    mems: List[Memtable], // 新しい順
    version: Version,
    readers: Map[Long, Sst.Reader]
):

  def get(key: ByteVector): IO[Option[ByteVector]] =
    Memtable.lookup(mems, key) match
      case Some(e) => IO.pure(e.toOption)
      case None    => Lsm.search(version.filesFor(key).iterator.map(f => readers(f.id)).toList, key)

  /** [from, until) のエントリを昇順に返す。 */
  def scan(from: ByteVector, until: Option[ByteVector]): Stream[IO, (ByteVector, ByteVector)] =
    Stream.eval(merged(from)).flatMap { c =>
      def inRange = c.valid && until.forall(u => Bytes.compare(c.key, u) < 0)
      Stream.unfoldChunkEval(()) { _ =>
        IO.defer {
          val buf = Vector.newBuilder[(ByteVector, ByteVector)]
          var n = 0
          def loop: IO[Unit] =
            if n >= 256 || !inRange then IO.unit
            else
              c.entry match
                case Entry.Put(v) => buf += ((c.key, v)); n += 1; c.next >> loop
                case Entry.Del    => c.next >> loop
          loop.map { _ =>
            val out = buf.result()
            if out.isEmpty && !inRange then None else Some((fs2.Chunk.from(out), ()))
          }
        }
      }
    }

  /** 全エントリ（墓石を含む）をマージしたカーソル。 */
  private[lsm] def merged(from: ByteVector): IO[Cursor] =
    val memCursors = mems.map(m => Cursor.Mem(m.map, from): Cursor)
    for
      l0c <- version.levels(0).traverse(f => Cursor.SstFile.open(readers(f.id), from))
      lc <- version.levels.drop(1).filter(_.nonEmpty).traverse(fs => Cursor.Level.open(fs.map(f => readers(f.id)), from))
    yield Cursor.Merge(memCursors.toVector ++ l0c ++ lc)

/** 自前の LSM-Tree。
  *
  * WAL を持たないのが特徴で、書き込みはメモリ（memtable）に入り、`flush` かバックグラウンドの
  * フラッシュで SST に書き出された時点で永続化される。電源断のあと再度開くと、
  * 最後に永続化されたバッチまでの状態に戻る（バッチの途中で切れることはない）。
  * 失われた分は、上位（Raft の状態マシン）がログを再生して埋める前提。
  */
final class Lsm private (
    fs: FileSystem,
    dir: Path,
    config: LsmConfig,
    private[lsm] val state: Ref[IO, Lsm.State],
    progress: SignallingRef[IO, Lsm.Progress],
    private[lsm] val wake: Queue[IO, Unit],
    writeLock: Mutex[IO]
):
  import Lsm.*

  /** バッチをアトミックに適用する。 */
  def write(ops: Seq[WriteOp]): IO[Unit] =
    if ops.isEmpty then IO.unit
    else
      writeLock.lock.surround {
        for
          _ <- awaitRoom
          sealedNow <- state.modify { s =>
            val mem = s.mem.apply(ops)
            if mem.bytes >= config.memtableBytes then
              (s.copy(mem = Memtable.empty(mem.seq + 1), imms = mem :: s.imms), true)
            else (s.copy(mem = mem), false)
          }
          _ <- wake.tryOffer(()).whenA(sealedNow)
        yield ()
      }

  def put(key: ByteVector, value: ByteVector): IO[Unit] = write(Seq(WriteOp.Put(key, value)))
  def delete(key: ByteVector): IO[Unit] = write(Seq(WriteOp.Delete(key)))

  /** 1 キーの読み込み。全ファイルのスナップショットは取らず、そのキーを含みうるファイルだけを参照中にする
    * （ファイル数が多いと、全ファイルの参照カウントの増減が重い）。
    */
  def get(key: ByteVector): IO[Option[ByteVector]] =
    state.modify { s =>
      Memtable.lookup(s.mem :: s.imms, key) match
        case Some(e) => (s, Left(e))
        case None =>
          val files = s.version.filesFor(key).toList
          (s.copy(refs = retain(s.refs, files.map(_.id))), Right(files.map(f => s.readers(f.id))))
    }.flatMap {
      case Left(e)        => IO.pure(e.toOption)
      case Right(readers) => search(readers, key).guarantee(releaseFiles(readers.map(_.meta.id)))
    }

  def scan(from: ByteVector, until: Option[ByteVector]): Stream[IO, (ByteVector, ByteVector)] =
    Stream.resource(snapshot).flatMap(_.scan(from, until))

  /** ここまでの書き込みをすべて永続化する。 */
  def flush: IO[Unit] =
    writeLock.lock.surround {
      state.modify { s =>
        if s.mem.isEmpty then (s, s.imms.headOption.map(_.seq).getOrElse(0L))
        else (s.copy(mem = Memtable.empty(s.mem.seq + 1), imms = s.mem :: s.imms), s.mem.seq)
      }
    }.flatMap(target => wake.tryOffer(()) >> awaitFlushed(target))

  def snapshot: Resource[IO, LsmSnapshot] =
    Resource.make(state.modify { s =>
      val ids = s.version.fileIds
      (s.copy(refs = retain(s.refs, ids)), (LsmSnapshot(s.mem :: s.imms, s.version, s.readers), ids))
    })((_, ids) => releaseFiles(ids)).map(_._1)

  /** 現在のファイル構成（テストと監視用）。 */
  def version: IO[Version] = state.get.map(_.version)

  // ---- 内部 ----

  private def awaitRoom: IO[Unit] =
    state.get.flatMap { s =>
      val sealedSeq = s.imms.headOption.map(_.seq).getOrElse(0L)
      val need = sealedSeq - config.maxImmutables + 1
      if s.imms.size < config.maxImmutables then IO.unit else awaitFlushed(need)
    }

  private def awaitFlushed(seq: Long): IO[Unit] =
    progress.waitUntil(p => p.flushedSeq >= seq || p.error.isDefined) >>
      progress.get.flatMap(p => p.error.fold(IO.unit)(e => IO.raiseError(LsmFailedException(e))))

  private def fileName(id: Long): Path = sstPath(dir, id)

  /** 参照を外し、参照されなくなったファイルを消す。 */
  private def releaseFiles(ids: Iterable[Long]): IO[Unit] =
    state.modify(s => collectGarbage(s.copy(refs = release(s.refs, ids)))).flatMap(deleteFiles)

  /** 参照されなくなったファイルを状態から外す。実際の削除は deleteFiles で行う。 */
  private def collectGarbage(s: State): (State, List[Sst.Reader]) =
    val live = s.version.fileIds
    val dead = s.readers.values.filter(r => !live.contains(r.meta.id) && !s.refs.contains(r.meta.id)).toList
    (s.copy(readers = s.readers -- dead.map(_.meta.id)), dead)

  private def deleteFiles(rs: List[Sst.Reader]): IO[Unit] =
    rs.traverse_(r => r.close.attempt >> fs.deleteIfExists(fileName(r.meta.id)).attempt.void)

  private def nextId: IO[Long] =
    state.modify(s => (s.copy(nextFileId = s.nextFileId + 1), s.nextFileId))

  /** エントリ列を targetFileBytes ごとに SST に書き、ディレクトリを sync する。 */
  private def writeFiles(level: Int, entries: Cursor, dropTombstones: Boolean): IO[Vector[SstMeta]] =
    def writeOne(b: Sst.Builder): IO[SstMeta] =
      for
        id <- nextId
        (bytes, smallest, largest, count) = b.finish()
        _ <- fs.open(fileName(id), OpenMode.CreateNew).use(h => h.write(0, bytes) >> h.sync)
      yield SstMeta(id, level, smallest, largest, bytes.size, count)

    def loop(b: Sst.Builder, acc: Vector[SstMeta]): IO[Vector[SstMeta]] =
      if !entries.valid then if b.isEmpty then IO.pure(acc) else writeOne(b).map(acc :+ _)
      else
        val k = entries.key
        val e = entries.entry
        val skip = dropTombstones && e == Entry.Del
        IO(if !skip then b.add(k, e)).flatMap { _ =>
          if b.estimatedSize >= config.targetFileBytes then
            writeOne(b).flatMap(m => entries.next >> loop(newBuilder, acc :+ m))
          else entries.next >> loop(b, acc)
        }

    loop(newBuilder, Vector.empty).flatTap(fsMetas => fs.syncDir(dir).whenA(fsMetas.nonEmpty))

  // ファイルは targetFileBytes を少し超えたところで切るので、その分の余裕を見て確保する
  private def newBuilder =
    Sst.Builder(config.blockBytes, config.bloomBitsPerKey, math.min(Int.MaxValue - 1024L, config.targetFileBytes + config.targetFileBytes / 8 + 64 * 1024).toInt)

  /** 新しい版をマニフェストに書き、状態に反映する。 */
  private def install(update: State => (Version, State => State), added: Vector[SstMeta]): IO[Unit] =
    for
      newReaders <- added.traverse(m => openReader(fs, dir, m).map(m.id -> _))
      s0 <- state.get
      (newVersion, finish) = update(s0)
      seq = s0.manifestSeq + 1
      _ <- fs.writeAtomically(dir / f"MANIFEST-$seq%016d", Manifest.encode(Manifest(s0.nextFileId, newVersion)))
      _ <- fs.deleteIfExists(dir / f"MANIFEST-${s0.manifestSeq}%016d").whenA(s0.manifestSeq > 0)
      dead <- state.modify { s =>
        collectGarbage(finish(s.copy(version = newVersion, manifestSeq = seq, readers = s.readers ++ newReaders)))
      }
      _ <- deleteFiles(dead)
    yield ()

  /** 最も古い immutable memtable を L0 に書き出す。 */
  private def flushOne: IO[Boolean] =
    state.get.map(_.imms.lastOption).flatMap {
      case None => IO.pure(false)
      case Some(mem) =>
        for
          metas <- writeFiles(0, Cursor.Mem(mem.map, ByteVector.empty), dropTombstones = false)
          _ <- install(
            s => (s.version.copy(levels = s.version.levels.updated(0, metas.sortBy(-_.id) ++ s.version.levels(0))),
              // フラッシュ中に新しい immutable が先頭に積まれていても、末尾が今回の分
              cur => cur.copy(imms = cur.imms.dropRight(1))),
            metas
          )
          _ <- progress.update(_.copy(flushedSeq = mem.seq))
        yield true
    }

  /** 必要ならコンパクションを 1 回行う。行ったら true。 */
  private def compactOnce: IO[Boolean] =
    state.get.flatMap { s =>
      pickCompaction(s.version) match
        case None => IO.pure(false)
        case Some(c) =>
          val inputs = c.upper ++ c.lower
          val inputIds = inputs.map(_.id).toSet
          val upperReaders = c.upper.map(m => s.readers(m.id))
          val lowerReaders = c.lower.map(m => s.readers(m.id))
          for
            upperCursors <-
              if c.level == 0 then upperReaders.traverse(r => Cursor.SstFile.open(r, ByteVector.empty))
              else Cursor.Level.open(upperReaders, ByteVector.empty).map(Vector(_))
            lowerCursor <- Cursor.Level.open(lowerReaders, ByteVector.empty)
            merged = Cursor.Merge(upperCursors :+ lowerCursor)
            out <- writeFiles(c.level + 1, merged, dropTombstones = s.version.isBottom(c.level + 1))
            _ <- install(
              cur => {
                val lv = cur.version.levels
                val remainingUpper = lv(c.level).filterNot(f => inputIds.contains(f.id))
                val newLower = (lv(c.level + 1).filterNot(f => inputIds.contains(f.id)) ++ out).sorted(using SstMeta.bySmallest)
                (Version(lv.updated(c.level, remainingUpper).updated(c.level + 1, newLower)), identity)
              },
              out
            )
          yield true
    }

  private def pickCompaction(v: Version): Option[Compaction] =
    def overlapping(level: Int, lo: ByteVector, hi: ByteVector) = v.levels(level).filter(_.overlaps(lo, hi))
    def range(fs: Vector[SstMeta]) =
      (fs.map(_.smallest).min(using Bytes.unsignedOrdering), fs.map(_.largest).max(using Bytes.unsignedOrdering))
    if v.levels(0).size >= config.l0CompactionTrigger then
      val upper = v.levels(0)
      val (lo, hi) = range(upper)
      Some(Compaction(0, upper, overlapping(1, lo, hi)))
    else
      (1 until config.maxLevels - 1).iterator
        .map { lv =>
          val target = config.baseLevelBytes * math.pow(config.levelMultiplier.toDouble, (lv - 1).toDouble)
          (lv, v.levelBytes(lv).toDouble / target)
        }
        .filter(_._2 > 1.0)
        .maxByOption(_._2)
        .map { (lv, _) =>
          // 最も大きいファイルを 1 つ下へ押し出す
          val f = v.levels(lv).maxBy(_.size)
          Compaction(lv, Vector(f), overlapping(lv + 1, f.smallest, f.largest))
        }

  /** バックグラウンド処理：フラッシュを優先し、なければコンパクション。 */
  private[lsm] def worker: IO[Nothing] =
    def drain: IO[Unit] =
      flushOne.flatMap {
        case true  => drain
        case false => compactOnce.flatMap(did => drain.whenA(did))
      }
    (wake.take >> drain).foreverM.handleErrorWith { e =>
      progress.update(_.copy(error = Some(e))) >> IO.never
    }

object Lsm:

  final class LsmFailedException(cause: Throwable) extends java.io.IOException("lsm background work failed", cause)

  private[lsm] final case class Compaction(level: Int, upper: Vector[SstMeta], lower: Vector[SstMeta])

  private[lsm] final case class State(
      mem: Memtable,
      imms: List[Memtable], // 新しい順
      version: Version,
      readers: Map[Long, Sst.Reader],
      refs: Map[Long, Int],
      nextFileId: Long,
      manifestSeq: Long
  )

  private[lsm] final case class Progress(flushedSeq: Long, error: Option[Throwable])

  private def sstPath(dir: Path, id: Long): Path = dir / f"$id%016d.sst"

  private def openReader(fs: FileSystem, dir: Path, m: SstMeta): IO[Sst.Reader] =
    fs.open(sstPath(dir, m.id), OpenMode.Read).allocated.flatMap { (h, release) =>
      Sst.Reader.open(m, h, release).onError(_ => release)
    }

  /** 新しい順に並んだファイルから key を探し、最初に見つかったエントリの値を返す。 */
  private[lsm] def search(readers: List[Sst.Reader], key: ByteVector): IO[Option[ByteVector]] =
    def loop(rs: List[Sst.Reader]): IO[Option[Entry]] = rs match
      case Nil    => IO.pure(None)
      case h :: t => h.get(key).flatMap(_.fold(loop(t))(e => IO.pure(Some(e))))
    loop(readers).map(_.flatMap(_.toOption))

  private def retain(refs: Map[Long, Int], ids: Iterable[Long]): Map[Long, Int] =
    ids.foldLeft(refs)((m, id) => m.updated(id, m.getOrElse(id, 0) + 1))

  private def release(refs: Map[Long, Int], ids: Iterable[Long]): Map[Long, Int] =
    ids.foldLeft(refs)((m, id) => if m(id) <= 1 then m - id else m.updated(id, m(id) - 1))

  /** ディレクトリを開く（なければ作る）。前回の電源断で残った不要なファイルは消す。 */
  def open(fs: FileSystem, dir: Path, config: LsmConfig = LsmConfig()): Resource[IO, Lsm] =
    val init =
      for
        _ <- fs.createDirectories(dir)
        names <- fs.list(dir).map(_.map(_.fileName.toString))
        manifests = names.filter(n => n.startsWith("MANIFEST-") && !n.endsWith(".tmp")).sorted.reverse
        loaded <- manifests.collectFirstSomeM(n => fs.readAll(dir / n).map(b => Manifest.decode(b).toOption.map(n -> _)))
        (manifestName, manifest) = loaded.getOrElse(("", Manifest(1L, Version.empty(config.maxLevels))))
        seq = if manifestName.isEmpty then 0L else manifestName.stripPrefix("MANIFEST-").toLong
        live = manifest.version.fileIds
        // 参照されていない SST、古いマニフェスト、一時ファイルを消す
        garbage = names.filter { n =>
          (n.endsWith(".sst") && !live.contains(n.stripSuffix(".sst").toLongOption.getOrElse(-1L))) ||
          n.endsWith(".tmp") || (n.startsWith("MANIFEST-") && n != manifestName)
        }
        _ <- garbage.traverse_(n => fs.deleteIfExists(dir / n))
        readers <- manifest.version.files.toVector.traverse(m => openReader(fs, dir, m).map(m.id -> _))
        version = manifest.version.copy(levels =
          manifest.version.levels.padTo(config.maxLevels, Vector.empty)
        )
        st <- Ref.of[IO, State](State(Memtable.empty(1L), Nil, version, readers.toMap, Map.empty, manifest.nextFileId, seq))
        progress <- SignallingRef.of[IO, Progress](Progress(0L, None))
        wake <- Queue.dropping[IO, Unit](1)
        lock <- Mutex[IO]
      yield new Lsm(fs, dir, config, st, progress, wake, lock)

    Resource
      .make(init)(lsm => lsm.state.get.flatMap(_.readers.values.toList.traverse_(_.close.attempt)))
      .flatTap(lsm => lsm.worker.background.void)
      .evalTap(lsm => lsm.wake.offer(())) // 起動時に溜まっているコンパクションを処理する
