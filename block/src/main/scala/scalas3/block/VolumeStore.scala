package scalas3.block

import cats.effect.std.{Mutex, Queue}
import cats.effect.{Deferred, IO, Ref, Resource}
import cats.syntax.all.*
import fs2.Stream
import fs2.io.file.Path
import scalas3.core.{ByteReader, ByteWriter, Bytes, Crc32c}
import scalas3.lsm.{Lsm, LsmConfig, WriteOp}
import scalas3.runtime.{FileHandle, FileSystem, OpenMode}
import scodec.bits.ByteVector

import scala.concurrent.duration.*

final case class VolumeConfig(
    /** これを超えたら次のボリュームに書く。 */
    volumeBytes: Long = 1L << 30,
    /** ゴミ（消したものや重複）の割合がこれ以上の、書き終えたボリュームを詰め直す。 */
    compactGarbageRatio: Double = 0.5,
    /** 索引を定期的に永続化する間隔（起動時に読み直すボリュームの量を抑える）。 */
    indexFlushInterval: FiniteDuration = 30.seconds
)

/** 値の置き場所と、最後に使われた時刻（GC の猶予期間の判断に使う）。 */
final case class VolumeEntry(vid: Long, offset: Long, length: Int, touched: Long)

/** 追記型のボリューム（1 つのディスクの 1 種類の値）。設計 D6。
  *
  * {{{
  * <dir>/<vid>.vol = record*
  * record          = [magic u32][キーの長さ u8][キー][値の長さ u32][crc32c(値) u32][値]
  * <dir>/index/    = LSM：e+キー → (vid, offset, 長さ, 最後に使われた時刻)、c+vid → 索引に入れ終えた位置
  * }}}
  *
  *   - 書き込みは 1 本のファイバーがまとめて追記し、fsync を 1 回で済ませてから索引を更新する（グループコミット）。
  *     値ごとにファイルを作る方式（一時ファイル → fsync → rename → ディレクトリの fsync）より fsync がずっと少ない
  *   - 削除は索引から外すだけ。ゴミの多いボリュームは、生きている値を新しいボリュームに写してから消す（compact）
  *   - 索引は LSM なので、落ちると最後の flush 以降が失われる。そこで「どこまで索引に入れたか」（c+vid）を索引と同じ
  *     書き込みで記録しておき、起動時にそこから先のボリュームを読み直して索引に入れる。読み直した値の「最後に使われた
  *     時刻」は起動時刻にする（GC に早く消されないよう、安全側に倒す）
  *   - 落ちたときに失われうるのは、索引の削除と時刻の更新だけ。削除が戻っても、GC がもう一度消すだけ
  */
final class VolumeStore private (
    fs: FileSystem,
    dir: Path,
    config: VolumeConfig,
    index: Lsm,
    lock: Mutex[IO],
    writes: Queue[IO, VolumeStore.Write],
    active: Ref[IO, VolumeStore.Active],
    readers: Ref[IO, Map[Long, (FileHandle, IO[Unit])]]
):
  import VolumeStore.*

  /** 値を書く。既にあれば最後に使われた時刻だけ進める。永続化してから返る。新しく書いたら true。 */
  def put(key: ByteVector, data: ByteVector): IO[Boolean] = submit(key, data, None)

  /** 書き込み担当のファイバーに渡し、索引の更新まで終わるのを待つ。 */
  private def submit(key: ByteVector, data: ByteVector, move: Option[VolumeEntry]): IO[Boolean] =
    Deferred[IO, Either[Throwable, Boolean]].flatMap(d => writes.offer(Write(key, data, move, d)) >> d.get.rethrow)

  def entry(key: ByteVector): IO[Option[VolumeEntry]] = index.get(entryKey(key)).map(_.map(decodeEntry))

  def contains(key: ByteVector): IO[Boolean] = index.get(entryKey(key)).map(_.isDefined)

  /** 値を読む。無いか、壊れていれば（crc が合わない）None。 */
  def get(key: ByteVector): IO[Option[ByteVector]] =
    entry(key).flatMap(_.flatTraverse { e =>
      // 詰め直しで元のボリュームが消された直後なら、索引は新しい場所を指しているので読み直す
      read(key, e).flatMap {
        case None => entry(key).flatMap(_.filter(_ != e).flatTraverse(read(key, _)))
        case some => IO.pure(some)
      }
    })

  private def read(key: ByteVector, e: VolumeEntry): IO[Option[ByteVector]] =
    reader(e.vid).flatMap(_.traverse(h => h.read(e.offset, headerSize(key.size) + e.length))).map(_.flatMap { b =>
      parseRecord(b).collect { case (k, v) if Bytes.equal(k, key) && v.size == e.length => v }
    }).handleError(_ => None)

  /** 最後に使われた時刻を now にする（無ければ何もしない）。 */
  def touch(key: ByteVector): IO[Unit] =
    IO.realTime.flatMap { now =>
      lock.lock.surround(entry(key).flatMap(_.traverse_(e => index.write(Seq(WriteOp.Put(entryKey(key), encodeEntry(e.copy(touched = now.toMillis))))))))
    }

  /** cond が真なら索引から外す。外したら true（領域は compact で回収する）。 */
  def deleteIf(key: ByteVector)(cond: VolumeEntry => Boolean): IO[Boolean] =
    lock.lock.surround(entry(key).flatMap {
      case Some(e) if cond(e) => index.write(Seq(WriteOp.Delete(entryKey(key)))).as(true)
      case _                  => IO.pure(false)
    })

  def delete(key: ByteVector): IO[Boolean] = deleteIf(key)(_ => true)

  /** prefix で始まるキーと置き場所。 */
  def entries(prefix: ByteVector = ByteVector.empty): Stream[IO, (ByteVector, VolumeEntry)] =
    val from = entryKey(prefix)
    index.scan(from, Bytes.prefixEnd(from)).map((k, v) => (k.drop(1), decodeEntry(v)))

  /** ゴミの多い、書き終えたボリュームを詰め直す。詰め直した数を返す。 */
  def compact: IO[Int] =
    for
      // 書き込みのバッチが終わるのを待ってから今のボリュームを見る。バッチの途中でボリュームを切り替えた直後に見ると、
      // 古いボリュームに書いたばかりの値がまだ索引に入っておらず、写さずにボリュームを消してしまう
      act <- lock.lock.surround(active.get)
      vids <- volumeIds(fs, dir)
      sizes <- vids.filter(_ != act.vid).traverse(v => fs.open(volumePath(dir, v), OpenMode.Read).use(_.size).map(v -> _))
      live <- entries().compile.fold(Map.empty[Long, Long])((m, ke) => m.updated(ke._2.vid, m.getOrElse(ke._2.vid, 0L) + headerSize(ke._1.size) + ke._2.length))
      targets = sizes.collect { case (v, size) if size > 0 && (size - live.getOrElse(v, 0L)).toDouble / size >= config.compactGarbageRatio => v }
      _ <- targets.traverse_(compactOne)
    yield targets.size

  private def compactOne(vid: Long): IO[Unit] =
    for
      moving <- entries().filter(_._2.vid == vid).compile.toVector
      // 生きている値を、元の「最後に使われた時刻」のまま今のボリュームに写す。写している間に消されたり書き直された
      // ものは写さない（索引が元の場所を指しているときだけ差し替える）
      _ <- moving.traverse_ { (key, e) =>
        read(key, e).flatMap(_.traverse_(submit(key, _, Some(e))))
      }
      _ <- index.flush
      _ <- readers.modify(m => (m - vid, m.get(vid))).flatMap(_.traverse_(_._2))
      _ <- fs.deleteIfExists(volumePath(dir, vid))
      // 消したことを永続化してから「どこまで読んだか」を消す。逆だと、落ちた後にファイルだけが戻り、記録がないので
      // 頭から読み直して、消した値を生き返らせてしまう
      _ <- fs.syncDir(dir)
      _ <- index.write(Seq(WriteOp.Delete(ckptKey(vid))))
    yield ()

  private def reader(vid: Long): IO[Option[FileHandle]] =
    readers.get.map(_.get(vid).map(_._1)).flatMap {
      case some @ Some(_) => IO.pure(some)
      case None =>
        fs.exists(volumePath(dir, vid)).flatMap {
          case false => IO.pure(None)
          case true =>
            fs.open(volumePath(dir, vid), OpenMode.Read).allocated.flatMap { (h, rel) =>
              readers.modify(m => m.get(vid).fold((m.updated(vid, (h, rel)), (h, IO.unit)))(existing => (m, (existing._1, rel)))).flatMap((use, extra) => extra.as(Some(use)))
            }
        }
    }

  /** 書き込み担当のファイバー。溜まった分を追記し、fsync を 1 回にまとめてから索引を更新する。 */
  private def writer: IO[Nothing] =
    (writes.take.flatMap(first => writes.tryTakeN(Some(255)).map(first :: _)).flatMap(writeBatch)).foreverM

  private def writeBatch(ws: List[Write]): IO[Unit] =
    lock.lock.surround {
      IO.realTime.map(_.toMillis).flatMap { now =>
        // 同じバッチの同じキーは 1 回だけ書く。rotated は、このバッチで書き終えたボリュームとその末尾
        ws.foldLeftM((Vector.empty[WriteOp], Map.empty[ByteVector, Boolean], Map.empty[Long, Long])) { case ((ops, results, rotated), w) =>
          if results.contains(w.key) && w.move.isEmpty then IO.pure((ops, results, rotated))
          else
            entry(w.key).flatMap { current =>
              (current, w.move) match
                // 既にある：時刻だけ進める
                case (Some(e), None) =>
                  IO.pure((ops :+ WriteOp.Put(entryKey(w.key), encodeEntry(e.copy(touched = now))), results.updated(w.key, false), rotated))
                // 詰め直し：索引が元の場所を指していなければ写さない（消されたか、書き直された）。場所だけを比べる。
                // 時刻まで比べると、写している間に重複排除や touch で時刻だけ進んだ値を写さずに元のボリュームを消してしまう
                case (cur, Some(expected)) if !cur.exists(c => c.vid == expected.vid && c.offset == expected.offset) =>
                  IO.pure((ops, results.updated(w.key, false), rotated))
                case (cur, move) =>
                  append(w.key, w.data).map { (vid, off, rotatedFrom) =>
                    // 写すときは、元の「最後に使われた時刻」と、その後に進んだ時刻の新しい方を残す
                    val touched = move.fold(now)(m => math.max(m.touched, cur.fold(m.touched)(_.touched)))
                    val e = VolumeEntry(vid, off, w.data.size.toInt, touched)
                    (ops :+ WriteOp.Put(entryKey(w.key), encodeEntry(e)), results.updated(w.key, move.isEmpty), rotated ++ rotatedFrom)
                }
            }
        }.flatMap { case (ops, results, rotated) =>
          // 書き終えたボリュームの「どこまで索引に入れたか」も、その索引の変更と同じ書き込みで末尾まで進める
          // （でないと、起動時に切り替え前の記録を読み直し、その後に消した値を索引に戻してしまう）
          val ckpts = rotated.toVector.sorted.map((v, end) => WriteOp.Put(ckptKey(v), ByteVector.fromLong(end)))
          for
            act <- active.get
            _ <- act.handle.sync.whenA(ops.nonEmpty)
            _ <- index.write(ops ++ ckpts :+ WriteOp.Put(ckptKey(act.vid), ByteVector.fromLong(act.offset))).whenA(ops.nonEmpty)
          yield results
        }
      }
    }.attempt.flatMap { r =>
      ws.traverse_(w => w.done.complete(r.map(_.getOrElse(w.key, false))).void)
    }

  /** 今のボリュームの末尾に 1 件書く（いっぱいなら次のボリュームに切り替える）。(vid, offset, 切り替えた古いボリュームの (vid, 末尾)) */
  private def append(key: ByteVector, data: ByteVector): IO[(Long, Long, Option[(Long, Long)])] =
    val size = headerSize(key.size).toLong + data.size
    active.get.flatMap { act =>
      val rotate = act.offset > 0 && act.offset + size > config.volumeBytes
      val target: IO[(Active, Option[(Long, Long)])] =
        if !rotate then IO.pure((act, None))
        else
          // 古いボリュームを永続化してから切り替える（「どこまで入れたか」はバッチの索引の更新で進める）
          for
            _ <- act.handle.sync
            next = act.vid + 1
            opened <- fs.open(volumePath(dir, next), OpenMode.CreateNew).allocated
            _ <- fs.syncDir(dir)
            _ <- act.release
            fresh = Active(next, opened._1, opened._2, 0L)
            _ <- active.set(fresh)
          yield (fresh, Some((act.vid, act.offset)))
      target.flatMap { (a, rotatedFrom) =>
        val w = ByteWriter(size.toInt)
        w.u32(Magic).u8(key.size.toInt).bytes(key).u32(data.size.toInt).u32(Crc32c(data)).bytes(data)
        a.handle.write(a.offset, w.result()) >> active.set(a.copy(offset = a.offset + size)).as((a.vid, a.offset, rotatedFrom))
      }
    }

  /** 止めるときは索引も書き出す（次の起動で、ボリュームを頭から読み直さなくて済むように）。 */
  private def close: IO[Unit] =
    lock.lock.surround(index.flush).attempt.void >>
      active.get.flatMap(_.release) >> readers.get.flatMap(_.values.toList.traverse_(_._2))

object VolumeStore:
  private val Magic = 0x564f4c31 // "VOL1"

  private final case class Write(key: ByteVector, data: ByteVector, move: Option[VolumeEntry], done: Deferred[IO, Either[Throwable, Boolean]])
  private final case class Active(vid: Long, handle: FileHandle, release: IO[Unit], offset: Long)

  private def volumePath(dir: Path, vid: Long): Path = dir / f"$vid%016d.vol"

  /** ディレクトリにあるボリュームの番号（昇順）。 */
  private def volumeIds(fs: FileSystem, dir: Path): IO[Vector[Long]] =
    fs.list(dir).map(_.map(_.fileName.toString).collect { case n if n.endsWith(".vol") => n.stripSuffix(".vol").toLongOption }.flatten.sorted.toVector)

  private def headerSize(keyLen: Long): Int = 4 + 1 + keyLen.toInt + 4 + 4
  private def entryKey(key: ByteVector): ByteVector = 'e'.toByte +: key
  private def ckptKey(vid: Long): ByteVector = 'c'.toByte +: ByteVector.fromLong(vid)

  private def encodeEntry(e: VolumeEntry): ByteVector =
    ByteWriter(28).u64(e.vid).u64(e.offset).u32(e.length).u64(e.touched).result()
  private def decodeEntry(b: ByteVector): VolumeEntry =
    val r = ByteReader(b.toArray)
    VolumeEntry(r.u64(), r.u64(), r.u32(), r.u64())

  /** レコードを読む（キー, 値）。壊れていれば None。値はコピーせずに切り出す（大きな値の読み込みで、コピーが目立った）。 */
  private def parseRecord(b: ByteVector): Option[(ByteVector, ByteVector)] =
    if b.size < 5 || b.take(4).toInt() != Magic then None
    else
      val keyLen = b(4) & 0xff
      val head = headerSize(keyLen)
      if b.size < head then None
      else
        val len = b.slice(5L + keyLen, 9L + keyLen).toInt()
        val crc = b.slice(9L + keyLen, 13L + keyLen).toInt()
        if len < 0 || b.size < head.toLong + len then None
        else
          val data = b.slice(head.toLong, head.toLong + len)
          Option.when(Crc32c(data) == crc)((b.slice(5, 5L + keyLen), data))

  private val indexConfig = LsmConfig(memtableBytes = 4L * 1024 * 1024, maxImmutables = 2, targetFileBytes = 8L * 1024 * 1024)

  def open(fs: FileSystem, dir: Path, config: VolumeConfig = VolumeConfig()): Resource[IO, VolumeStore] =
    for
      _ <- Resource.eval(fs.createDirectories(dir))
      index <- Lsm.open(fs, dir / "index", indexConfig)
      lock <- Resource.eval(Mutex[IO])
      writes <- Resource.eval(Queue.unbounded[IO, Write])
      readers <- Resource.eval(Ref.of[IO, Map[Long, (FileHandle, IO[Unit])]](Map.empty))
      act <- Resource.eval(recover(fs, dir, index))
      active <- Resource.eval(Ref.of[IO, Active](act))
      store <- Resource.make(IO(new VolumeStore(fs, dir, config, index, lock, writes, active, readers)))(_.close)
      _ <- store.writer.background
      _ <- (IO.sleep(config.indexFlushInterval) >> index.flush.attempt).foreverM.background
    yield store

  /** 起動時：各ボリュームを「どこまで索引に入れたか」の先から読み直して索引に入れ、書き込み先のボリュームを開く。 */
  private def recover(fs: FileSystem, dir: Path, index: Lsm): IO[Active] =
    for
      now <- IO.realTime.map(_.toMillis)
      vids <- volumeIds(fs, dir)
      _ <- vids.zipWithIndex.traverse_ { (vid, i) =>
        val path = volumePath(dir, vid)
        val last = i == vids.size - 1
        index.get(ckptKey(vid)).map(_.fold(0L)(_.toLong())).flatMap { from =>
          fs.open(path, OpenMode.ReadWrite).use { h =>
            h.size.flatMap { size =>
              // 1 件ずつ読む（壊れた記録で止まる）
              def loop(off: Long, ops: Vector[WriteOp]): IO[(Long, Vector[WriteOp])] =
                if off + headerSize(0) > size then IO.pure((off, ops))
                else
                  h.read(off, 5).flatMap { pre =>
                    val keyLen = if pre.size == 5 then pre(4) & 0xff else -1
                    if keyLen < 0 || off + headerSize(keyLen) > size then IO.pure((off, ops))
                    else
                      h.read(off + 5 + keyLen, 4).flatMap { lenB =>
                        val len = if lenB.size == 4 then lenB.toInt() else -1
                        val total = headerSize(keyLen).toLong + len
                        if len < 0 || off + total > size then IO.pure((off, ops))
                        else
                          h.read(off, total.toInt).flatMap { rec =>
                            parseRecord(rec) match
                              case None => IO.pure((off, ops))
                              case Some((slice, data)) =>
                                // キーは記録全体のバッファからの切り出しなので、コピーしてから索引の操作に入れる。でないと、
                                // ボリュームを読み終えるまで全記録のバッファ（1 件最大 4 MiB）がヒープに残る
                                val key = slice.copy
                                index.get(entryKey(key)).flatMap { cur =>
                                  // 新しい方（vid が大きい、同じなら後ろ）を残す
                                  val newer = cur.map(decodeEntry).forall(e => e.vid < vid || (e.vid == vid && e.offset < off))
                                  val op = Option.when(newer)(WriteOp.Put(entryKey(key), encodeEntry(VolumeEntry(vid, off, data.size.toInt, now))))
                                  loop(off + total, ops ++ op)
                                }
                          }
                      }
                  }
              loop(from, Vector.empty).flatMap { (end, ops) =>
                // 書きかけで終わってよいのは最後のボリュームだけ（そこで切る）
                (h.truncate(end) >> h.sync).whenA(last && end < size) >>
                  index.write(ops :+ WriteOp.Put(ckptKey(vid), ByteVector.fromLong(end)))
              }
            }
          }
        }
      }
      _ <- index.flush
      act <- vids.lastOption match
        case Some(v) =>
          fs.open(volumePath(dir, v), OpenMode.ReadWrite).allocated.flatMap { (h, rel) =>
            h.size.map(size => Active(v, h, rel, size))
          }
        case None =>
          fs.open(volumePath(dir, 1L), OpenMode.CreateNew).allocated.flatMap((h, rel) => fs.syncDir(dir).as(Active(1L, h, rel, 0L)))
    yield act
