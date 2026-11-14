package scalas3.lsm

import cats.effect.IO
import cats.syntax.all.*
import scalas3.core.Bytes
import scodec.bits.ByteVector

import scala.collection.immutable.TreeMap

/** 昇順に並んだエントリを 1 つずつ読むカーソル。1 つのファイバーからだけ使うこと。 */
private[lsm] trait Cursor:
  def key: ByteVector // valid のときだけ
  def entry: Entry
  def valid: Boolean
  def next: IO[Unit]

private[lsm] object Cursor:

  final class Mem(map: TreeMap[ByteVector, Entry], from: ByteVector) extends Cursor:
    private val it = map.iteratorFrom(from)
    private var cur: (ByteVector, Entry) = if it.hasNext then it.next() else null
    def valid: Boolean = cur != null
    def key: ByteVector = cur._1
    def entry: Entry = cur._2
    def next: IO[Unit] = IO { cur = if it.hasNext then it.next() else null }

  /** 1 つの SST ファイルを読む。 */
  final class SstFile private (r: Sst.Reader, private var bi: Int, private var block: Sst.Block, private var pos: Int)
      extends Cursor:
    def valid: Boolean = block != null && pos < block.keys.length
    def key: ByteVector = block.keys(pos)
    def entry: Entry = block.entries(pos)
    def next: IO[Unit] =
      IO(pos += 1) >> settle

    /** 現在のブロックを読み終えていたら、次のブロックへ進む。 */
    def settle: IO[Unit] =
      if block == null || pos < block.keys.length then IO.unit
      else if bi + 1 >= r.blockCount then IO { block = null }
      else r.readBlock(bi + 1).flatMap(b => IO { bi += 1; block = b; pos = 0 }) >> settle

  object SstFile:
    def open(r: Sst.Reader, from: ByteVector): IO[Cursor] =
      val bi = r.blockFor(from)
      if bi >= r.blockCount then IO.pure(Empty)
      else r.readBlock(bi).flatMap(b => SstFile(r, bi, b, b.seek(from)).let(c => c.settle.as(c)))

  /** 1 つのレベル（重なりのないファイル列）を連結して読む。 */
  final class Level private (files: Vector[Sst.Reader], private var fi: Int, private var cur: Cursor) extends Cursor:
    def valid: Boolean = cur.valid
    def key: ByteVector = cur.key
    def entry: Entry = cur.entry
    def next: IO[Unit] = cur.next >> advanceFile

    private def advanceFile: IO[Unit] = IO.defer {
      if cur.valid || fi + 1 >= files.size then IO.unit
      else SstFile.open(files(fi + 1), ByteVector.empty).flatMap(c => IO { fi += 1; cur = c }) >> advanceFile
    }

  object Level:
    def open(files: Vector[Sst.Reader], from: ByteVector): IO[Cursor] =
      // 重なりがなく昇順なので、最大キーが from 以上の最初のファイルを二分探索で探せる
      val start = lowerBound(files.length, from)(files(_).meta.largest)
      if start >= files.length then IO.pure(Empty)
      else SstFile.open(files(start), from).flatMap(c => Level(files, start, c).let(l => l.advanceFile.as(l)))

  object Empty extends Cursor:
    def valid = false
    def key: ByteVector = throw java.util.NoSuchElementException()
    def entry: Entry = throw java.util.NoSuchElementException()
    def next: IO[Unit] = IO.unit

  /** 複数のカーソルをマージする。同じキーは、リストの先頭に近い（新しい）カーソルの値を採る。 */
  final class Merge(cursors: Vector[Cursor]) extends Cursor:
    private var winner: Int = pick()

    private def pick(): Int =
      var best = -1
      var i = 0
      while i < cursors.length do
        val c = cursors(i)
        if c.valid && (best < 0 || Bytes.compare(c.key, cursors(best).key) < 0) then best = i
        i += 1
      best

    def valid: Boolean = winner >= 0
    def key: ByteVector = cursors(winner).key
    def entry: Entry = cursors(winner).entry
    def next: IO[Unit] =
      if winner < 0 then IO.unit
      else
        val k = key
        cursors.traverse_(c => c.next.whenA(c.valid && Bytes.equal(c.key, k))) >> IO { winner = pick() }

  extension [A](a: A) private inline def let[B](f: A => B): B = f(a)
