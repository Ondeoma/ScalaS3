package scalas3.raft

import cats.effect.IO
import fs2.io.file.Path
import scalas3.core.{ByteReader, ByteWriter, Crc32c, DecodeError}
import scalas3.runtime.FileSystem
import scodec.bits.ByteVector

/** Raft のログとスナップショットのファイル形式（FileRaftStorage と SharedRaftLog で共通）。
  * {{{
  * record   = [len u32][crc32c u32][payload]
  * payload  = kind ...    （SharedRaftLog は先頭にグループ ID を置く）
  *   1 HardState | 2 Entries(先頭以降を置き換え) | 3 Truncate(from) | 4 Reset(hs, entries) | 5 Drop
  * snapshot = [meta][data lenBytes][crc32c u32]
  * }}}
  */
private[raft] object RaftLogFormat:
  val KindHard = 1
  val KindEntries = 2
  val KindTruncate = 3
  val KindReset = 4
  val KindDrop = 5

  /** payload を組み立てて、長さとチェックサムをつけて w に足す。 */
  def record(w: ByteWriter)(body: ByteWriter => Unit): Unit =
    val p = ByteWriter()
    body(p)
    val payload = p.result() // p はこの後使わないので、コピーせずに読む
    w.u32(payload.size.toInt).u32(Crc32c(payload)).bytes(payload): Unit

  def hard(w: ByteWriter, hs: HardState): Unit =
    w.u8(KindHard); RaftCodec.hardState(w, hs)

  def entries(w: ByteWriter, es: Vector[LogEntry]): Unit =
    w.u8(KindEntries); writeEntries(w, es)

  def truncate(w: ByteWriter, from: Long): Unit =
    w.u8(KindTruncate).u64(from): Unit

  def reset(w: ByteWriter, hs: HardState, es: Vector[LogEntry]): Unit =
    w.u8(KindReset); RaftCodec.hardState(w, hs); writeEntries(w, es)

  private def writeEntries(w: ByteWriter, es: Vector[LogEntry]): Unit =
    w.varInt(es.size); es.foreach(RaftCodec.entry(w, _))

  def readEntries(r: ByteReader): Vector[LogEntry] = Vector.fill(r.varInt())(RaftCodec.entry(r))

  /** RaftStorage.appendAll の 1 件分のレコードを rec で書く。 */
  def writeItem(hs: Option[HardState], truncateFrom: Option[Long], es: Vector[LogEntry])(rec: (ByteWriter => Unit) => Unit): Unit =
    hs.foreach(h => rec(hard(_, h)))
    // Entries レコードは先頭以降を置き換えるので、truncateFrom が先頭と一致すれば省略できる
    truncateFrom.filterNot(i => es.headOption.exists(_.index == i)).foreach(i => rec(truncate(_, i)))
    if es.nonEmpty then rec(entries(_, es))

  /** レコードを先頭から順に f に渡し、正しく読めた終端の位置を返す。長さやチェックサムが合わないところ、
    * f が DecodeError を投げたところで止まる（書きかけの末尾）。
    */
  def scan(arr: Array[Byte])(f: ByteReader => Unit): Int =
    var pos = 0
    var ok = true
    while ok && pos + 8 <= arr.length do
      val r = ByteReader(arr, pos, arr.length)
      val len = r.u32()
      val crc = r.u32()
      if len < 0 || pos + 8 + len > arr.length || Crc32c(arr, pos + 8, len) != crc then ok = false
      else
        try
          f(ByteReader(arr, pos + 8, pos + 8 + len))
          pos += 8 + len
        catch case _: DecodeError => ok = false
    pos

  /** エントリ列 es の、index 未満だけを残す（エントリは index の昇順で連続している）。 */
  def keepBelow(es: Vector[LogEntry], index: Long): Vector[LogEntry] =
    if es.isEmpty || es.head.index >= index then Vector.empty
    else es.take((index - es.head.index).toInt)

  /** 1 グループ分のハードステートとエントリ。書き込みやレコードを順に当てはめて組み立てる。 */
  class LogState(var hs: HardState, var entries: Vector[LogEntry]):
    /** got の先頭以降を置き換える。 */
    def appendEntries(got: Vector[LogEntry]): Unit =
      if got.nonEmpty then entries = keepBelow(entries, got.head.index) ++ got

    def truncate(from: Long): Unit = entries = keepBelow(entries, from)

    /** RaftStorage.appendAll の 1 件分を当てはめる（writeItem で書いた内容と同じ結果になる）。 */
    def applyItem(h: Option[HardState], truncateFrom: Option[Long], es: Vector[LogEntry]): Unit =
      h.foreach(hs = _)
      truncateFrom.foreach(truncate)
      appendEntries(es)

    /** HardState / Entries / Truncate のレコードを当てはめる。それ以外の種類は DecodeError。 */
    def replay(kind: Int, r: ByteReader): Unit =
      kind match
        case KindHard     => hs = RaftCodec.hardState(r)
        case KindEntries  => appendEntries(readEntries(r))
        case KindTruncate => truncate(r.u64())
        case k            => throw DecodeError(s"unknown record $k")

  object LogState:
    /** LogState.replay で扱える種類か。 */
    def handles(kind: Int): Boolean = kind == KindHard || kind == KindEntries || kind == KindTruncate

  def writeSnapshot(fs: FileSystem, path: Path, meta: SnapshotMeta, data: ByteVector): IO[Unit] =
    // データは大きいことがあるので、最初から足りる大きさで確保し、書いた内容はコピーせずに使う
    val w = ByteWriter(data.size.toInt + 256)
    RaftCodec.meta(w, meta)
    w.lenBytes(data)
    val body = w.result()
    fs.writeAtomically(path, body ++ ByteVector.fromInt(Crc32c(body)))

  def readSnapshot(fs: FileSystem, path: Path): IO[Option[(SnapshotMeta, ByteVector)]] =
    fs.exists(path).flatMap {
      case false => IO.pure(None)
      case true =>
        fs.readAll(path).flatMap { b =>
          val arr = b.toArrayUnsafe // 読むだけなので、コピーせずに済むならそうする
          val n = arr.length - 4
          if n < 0 || Crc32c(arr, 0, n) != ByteReader(arr, n, arr.length).u32() then
            IO.raiseError(java.io.IOException(s"raft snapshot $path is corrupted"))
          else
            IO {
              val r = ByteReader(arr, 0, n)
              val m = RaftCodec.meta(r)
              val len = r.varInt()
              val off = r.position
              r.skip(len)
              Some((m, ByteVector.view(arr, off, len)))
            }
        }
    }
