package scalas3.raft

import cats.effect.std.{Mutex, Queue}
import cats.effect.{IO, Ref, Resource}
import cats.syntax.all.*
import fs2.concurrent.SignallingRef
import fs2.io.file.Path
import scalas3.core.{ByteWriter, DecodeError}
import scalas3.runtime.{FileHandle, FileSystem, OpenMode}
import scodec.bits.ByteVector

import scala.collection.mutable

/** ノード内のすべての Raft グループで共有する追記ログ（設計 §5.4）。
  *
  * グループごとにログを持つと、レンジの数だけ fsync が走り、まとめ書きの効果が薄れる（小さな PUT が
  * レンジに散ると、fsync 1 回あたりのエントリが 1〜2 件になる）。ここでは 1 本のファイバーが全グループの
  * 書き込みを集めて書き、fsync を 1 回で済ませる（グループコミット）。
  *
  * {{{
  * <dir>/log/<id>.seg = record*   （一定の大きさで次のセグメントに切り替える。payload の先頭にグループ ID）
  * <dir>/snap/<group>             （writeAtomically。これがスナップショットの正）
  * }}}
  * レコードとスナップショットの形式は RaftLogFormat。Reset はそれまでのエントリを捨てて置き換える（スナップショット後と
  * 古いセグメントの回収で使う）、Drop はグループを消したことを表す。
  *
  * 古いセグメントの回収: セグメントが maxSegments を超えたら、最古のセグメントに記録が残っているグループの
  * 現在の状態（ハードステートとエントリ）を Reset として書き直し、それが永続化されてから最古のセグメントを消す。
  * そのため各グループのハードステートとエントリをメモリにも持つ（エントリは RaftCore と同じオブジェクトを指す）。
  */
final class SharedRaftLog private (
    fs: FileSystem,
    dir: Path,
    config: SharedRaftLog.Config,
    lock: Mutex[IO],
    durable: SignallingRef[IO, SharedRaftLog.Durable],
    wake: Queue[IO, Unit],
    segHandle: Ref[IO, (FileHandle, IO[Unit], Long)] // 書き込み中のセグメント (ハンドル, 解放, 書き込み位置)
):
  import SharedRaftLog.*

  // 以下は lock の中でだけ触る
  private val groups = mutable.Map.empty[Long, GroupState]
  private var segments = Vector.empty[Long] // 存在するセグメント（昇順）。末尾が書き込み中
  private var buf = ByteWriter()
  private var appendedSeq = 0L
  private var pendingDelete = Vector.empty[(Long, Long)] // (消せるようになる seq, セグメント)
  // 書き込み中のセグメントを切り替える大きさ。切り替えのときの書き直しの分だけ segmentBytes より大きくする。
  // 書き直しを数えると、全グループのエントリの合計が segmentBytes * maxSegments を超えたとき、書き直しだけで
  // セグメントが埋まって切り替わり、また書き直す、を新しい書き込みが無くても繰り返す（ディスクと GC を使い切る）
  private var segLimit = config.segmentBytes

  private def currentSeg: Long = segments.last

  /** グループのストレージ。初めてのグループなら空の状態から始まる。 */
  def storage(group: Long): IO[RaftStorage] =
    knows(group).flatMap {
      case true => IO.unit
      case false =>
        // ログに記録がなくても、スナップショットのファイルだけはあることがある（Reset を書く前に落ちた）
        RaftLogFormat.readSnapshot(fs, snapPath(group)).flatMap { snap =>
          lock.lock.surround(IO {
            val st = groups.getOrElseUpdate(group, GroupState.empty(currentSeg))
            if st.snapshot.isEmpty then st.snapshot = snap
          })
        }
    }.as(new GroupStorage(group))

  def knows(group: Long): IO[Boolean] = lock.lock.surround(IO(groups.contains(group)))

  /** グループを消す（ログには Drop を残し、スナップショットのファイルを消す）。 */
  def drop(group: Long): IO[Unit] =
    lock.lock.surround(IO.defer {
      if groups.remove(group).isEmpty then IO.pure(None)
      else IO { record(group)(_.u8(RaftLogFormat.KindDrop)); Some(appendedSeq) }
    }).flatMap(_.traverse_(awaitDurable)) >> fs.deleteIfExists(snapPath(group)).void

  private def snapPath(group: Long): Path = dir / "snap" / group.toString

  /** lock の中で呼ぶ。グループ ID を先頭に置いたレコードを buf に足す。 */
  private def record(group: Long)(body: ByteWriter => Unit): Unit =
    RaftLogFormat.record(buf) { p => p.varLong(group); body(p) }
    appendedSeq += 1

  private def awaitDurable(seq: Long): IO[Unit] =
    wake.offer(()) >> durable.waitUntil(d => d.seq >= seq || d.error.isDefined) >>
      durable.get.flatMap(d => d.error.fold(IO.unit)(e => IO.raiseError(java.io.IOException("raft log failed", e))))

  /** 書き込み担当のファイバー。溜まった分をまとめて書いて fsync し、必要ならセグメントを切り替えて回収する。 */
  private def writer: IO[Nothing] =
    (wake.take >> wake.tryTakeN(None) >> drain).foreverM

  private def drain: IO[Unit] =
    lock.lock.surround(IO {
      val bytes = buf.result()
      buf = ByteWriter()
      (bytes, appendedSeq)
    }).flatMap { (bytes, seq) =>
      val write =
        if bytes.isEmpty then IO.unit
        else
          segHandle.get.flatMap { (h, rel, pos) =>
            h.write(pos, bytes) >> h.sync >> segHandle.set((h, rel, pos + bytes.size))
          }
      (write >> durable.update(_.copy(seq = seq)) >> rotateIfNeeded >> collect(seq))
        .handleErrorWith(e => durable.update(_.copy(error = Some(e))))
    }

  private def rotateIfNeeded: IO[Unit] =
    (segHandle.get, lock.lock.surround(IO(segLimit))).tupled.flatMap { case ((_, rel, pos), limit) =>
      if pos < limit then IO.unit
      else
        val next = currentSeg + 1
        for
          opened <- fs.open(segPath(dir, next), OpenMode.CreateNew).allocated
          _ <- fs.syncDir(dir / "log")
          _ <- rel
          _ <- segHandle.set((opened._1, opened._2, 0L))
          _ <- lock.lock.surround(IO {
            segments :+= next
            // 上限を超えた古いセグメントに記録が残っているグループを Reset で書き直す
            val toFree = segments.dropRight(config.maxSegments).filterNot(s => pendingDelete.exists(_._2 == s))
            val before = buf.size
            if toFree.nonEmpty then
              val upTo = toFree.last
              groups.toList.sortBy(_._1).foreach { (g, st) =>
                if st.firstSeg <= upTo then
                  record(g)(RaftLogFormat.reset(_, st.hs, st.entries))
                  st.firstSeg = next
              }
              pendingDelete ++= toFree.map(s => (appendedSeq, s))
            // 書き直しは新しいセグメントに入るので、その分は切り替えの判定に数えない
            segLimit = config.segmentBytes + (buf.size - before)
          })
          _ <- wake.offer(()) // 書き直した分を次の周で書く
        yield ()
    }

  /** 書き直しが永続化されたセグメントを消す。 */
  private def collect(durableSeq: Long): IO[Unit] =
    lock.lock.surround(IO {
      val (ready, rest) = pendingDelete.partition(_._1 <= durableSeq)
      pendingDelete = rest
      segments = segments.filterNot(s => ready.exists(_._2 == s))
      ready.map(_._2)
    }).flatMap(_.traverse_(s => fs.deleteIfExists(segPath(dir, s))))

  private def close: IO[Unit] = segHandle.get.flatMap(_._2)

  private final class GroupStorage(group: Long) extends RaftStorage:
    private def state: GroupState = groups(group)

    def load: IO[RaftStorage.Loaded] =
      lock.lock.surround(IO {
        val st = state
        val snapIndex = st.snapshot.map(_._1.index).getOrElse(0L)
        RaftStorage.Loaded(st.hs, st.snapshot, st.entries.filter(_.index > snapIndex))
      })

    def appendAll(items: Vector[(Option[HardState], Option[Long], Vector[LogEntry])]): IO[Unit] =
      lock.lock.surround(IO {
        val st = state
        items.foreach { (hs, truncateFrom, entries) =>
          RaftLogFormat.writeItem(hs, truncateFrom, entries)(record(group))
          st.applyItem(hs, truncateFrom, entries)
        }
        st.lastSeq = appendedSeq
      })

    def sync: IO[Unit] = lock.lock.surround(IO(state.lastSeq)).flatMap(seq => durable.get.flatMap(d => if d.seq >= seq then IO.unit else awaitDurable(seq)))

    def rewrite(snapshot: SnapshotMeta, data: ByteVector, hs: HardState, entries: Vector[LogEntry]): IO[Unit] =
      for
        // スナップショットを先に置く。Reset が永続化される前に落ちても、古いエントリは読み込み時に捨てられる
        _ <- RaftLogFormat.writeSnapshot(fs, snapPath(group), snapshot, data)
        seq <- lock.lock.surround(IO {
          val st = state
          record(group)(RaftLogFormat.reset(_, hs, entries))
          st.hs = hs
          st.entries = entries
          st.snapshot = Some((snapshot, data))
          st.firstSeg = currentSeg
          st.lastSeq = appendedSeq
          appendedSeq
        })
        _ <- awaitDurable(seq)
      yield ()

    def latestSnapshot: IO[Option[(SnapshotMeta, ByteVector)]] = lock.lock.surround(IO(state.snapshot))

object SharedRaftLog:
  import RaftLogFormat.*
  final case class Config(segmentBytes: Long = 64L * 1024 * 1024, maxSegments: Int = 4)

  private[raft] final case class Durable(seq: Long, error: Option[Throwable])

  private final class GroupState(
      hs0: HardState,
      entries0: Vector[LogEntry],
      var snapshot: Option[(SnapshotMeta, ByteVector)],
      var firstSeg: Long, // このグループの有効な記録が残る最古のセグメント
      var lastSeq: Long
  ) extends LogState(hs0, entries0)
  private object GroupState:
    def empty(seg: Long) = GroupState(HardState(0, None), Vector.empty, None, seg, 0L)

  private def segPath(dir: Path, id: Long): Path = dir / "log" / f"$id%016d.seg"

  def open(fs: FileSystem, dir: Path, config: Config = Config()): Resource[IO, SharedRaftLog] =
    val init =
      for
        _ <- fs.createDirectories(dir / "log")
        _ <- fs.createDirectories(dir / "snap")
        segFiles <- fs.list(dir / "log").map(_.filter(_.fileName.toString.endsWith(".seg")))
        ids = segFiles.map(_.fileName.toString.stripSuffix(".seg").toLong).sorted.toVector
        replayed <- replay(fs, dir, ids)
        (states, lastEnd) = replayed
        // 書き込み中のセグメントを開く（なければ作る）
        cur = ids.lastOption.getOrElse(1L)
        opened <- fs.open(segPath(dir, cur), OpenMode.ReadWrite).allocated
        (h, rel) = opened
        size <- h.size
        // 壊れた末尾（書きかけのレコード）を切る
        end = if ids.isEmpty then 0L else lastEnd
        _ <- (h.truncate(end) >> h.sync).whenA(end < size)
        _ <- fs.syncDir(dir / "log").whenA(ids.isEmpty)
        // ログに記録のあるグループのスナップショットを読む
        snaps <- states.keys.toList.sorted.traverse(g => readSnapshot(fs, dir / "snap" / g.toString).map(g -> _))
        lock <- Mutex[IO]
        durable <- SignallingRef[IO, Durable](Durable(0L, None))
        wake <- Queue.unbounded[IO, Unit]
        segRef <- Ref.of[IO, (FileHandle, IO[Unit], Long)]((h, rel, end))
      yield
        val log = new SharedRaftLog(fs, dir, config, lock, durable, wake, segRef)
        log.segments = if ids.isEmpty then Vector(cur) else ids
        snaps.foreach((g, s) => states(g).snapshot = s)
        log.groups ++= states
        log
    Resource.make(init)(_.close.attempt.void).flatTap(log => log.writer.background)

  /** 全セグメントを順に読み、グループごとの状態と、最後のセグメントの正しい終端を返す。 */
  private def replay(fs: FileSystem, dir: Path, ids: Vector[Long]): IO[(mutable.Map[Long, GroupState], Long)] =
    val states = mutable.Map.empty[Long, GroupState]
    ids.zipWithIndex
      .foldLeftM(0L) { case (_, (id, i)) =>
        fs.readAll(segPath(dir, id)).flatMap { bytes =>
          IO {
            val arr = bytes.toArrayUnsafe // 読むだけなので、コピーせずに済むならそうする
            val end = scan(arr) { r =>
              val g = r.varLong()
              def st = states.getOrElseUpdate(g, GroupState.empty(id))
              r.u8() match
                case KindReset =>
                  val s = st
                  s.hs = RaftCodec.hardState(r)
                  s.entries = readEntries(r)
                  s.firstSeg = id
                case KindDrop                 => states.remove(g): Unit
                case k if LogState.handles(k) => st.replay(k, r)
                case k                        => throw DecodeError(s"unknown record $k")
            }
            // 書きかけで終わってよいのは最後のセグメントだけ
            if end < arr.length && i < ids.size - 1 then throw java.io.IOException(s"raft log segment $id is corrupted at $end")
            end.toLong
          }
        }
      }
      .map(end => (states, end))
