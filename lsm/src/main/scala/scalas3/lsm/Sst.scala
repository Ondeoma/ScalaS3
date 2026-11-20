package scalas3.lsm

import cats.effect.IO
import scalas3.core.{ByteReader, ByteWriter, Bytes, Crc32c, DecodeError}
import scalas3.runtime.FileHandle
import scodec.bits.ByteVector

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/** SST ファイルの情報（マニフェストに記録する）。deletes は墓石の件数（古い形式のマニフェストから読んだときは
  * 分からないので -1。バックグラウンドで数え直す）。
  */
final case class SstMeta(id: Long, level: Int, smallest: ByteVector, largest: ByteVector, size: Long, entries: Long, deletes: Long = -1L):
  def overlaps(lo: ByteVector, hi: ByteVector): Boolean =
    Bytes.compare(largest, lo) >= 0 && Bytes.compare(smallest, hi) <= 0

  def contains(key: ByteVector): Boolean =
    Bytes.compare(smallest, key) <= 0 && Bytes.compare(key, largest) <= 0

object SstMeta:
  /** L1 以降のレベル内の並び（最小キーの昇順）。 */
  val bySmallest: Ordering[SstMeta] = (a, b) => Bytes.compare(a.smallest, b.smallest)

  /** 重なりのないレベル（最小キーの昇順）から、key を含むファイルを二分探索で探す。 */
  def find[A](files: IndexedSeq[A], key: ByteVector)(meta: A => SstMeta): Option[A] =
    val i = lowerBound(files.length, key)(i => meta(files(i)).largest)
    if i < files.length && meta(files(i)).contains(key) then Some(files(i)) else None

/** 昇順に並んだ n 個のキー（keyAt）のうち、key 以上の最初の位置（なければ n）。 */
private[lsm] inline def lowerBound(n: Int, key: ByteVector)(inline keyAt: Int => ByteVector): Int =
  var lo = 0
  var hi = n
  while lo < hi do
    val mid = (lo + hi) >>> 1
    if Bytes.compare(keyAt(mid), key) < 0 then lo = mid + 1 else hi = mid
  lo

/** 末尾 4 バイトの crc32c を確かめ、それより前（本体）を読む ByteReader を返す。本体はコピーしない。 */
private[lsm] def verifyCrc(bytes: ByteVector): Either[String, ByteReader] =
  val arr = bytes.toArrayUnsafe // 読むだけなので、コピーせずに済むならそうする
  val n = arr.length - 4
  if n < 0 then Left("too short")
  else if Crc32c(arr, 0, n) != ByteReader(arr, n, arr.length).u32() then Left("checksum mismatch")
  else Right(ByteReader(arr, 0, n))

final class CorruptionException(msg: String) extends java.io.IOException(msg)

/** SST ファイルの形式。
  * {{{
  * [data block]* [index block] [bloom block] [footer]
  * data block  = ([keyLen varint][key][kind u8][valLen varint][val])* [crc32c u32]
  * index block = [count varint] ([lastKey lenBytes][offset varLong][size varInt])* [crc32c u32]
  * bloom block = [k u8][bits lenBytes] [crc32c u32]
  * footer      = [indexOff u64][indexLen u32][bloomOff u64][bloomLen u32][entries u64][magic u64][crc32c u32]
  * }}}
  * ブロックの size には末尾の crc を含む。
  */
object Sst:
  val Magic: Long = 0x5343414c41535354L // "SCALASST"
  val FooterSize = 44

  private val KindPut = 0
  private val KindDel = 1

  final case class IndexEntry(lastKey: ByteVector, offset: Long, size: Int)

  /** 復号済みのデータブロック。 */
  final class Block(val keys: Array[ByteVector], val entries: Array[Entry]):
    /** key 以上の最初の位置（なければ keys.length）。 */
    def seek(key: ByteVector): Int = lowerBound(keys.length, key)(keys(_))

  /** ファイル全体をメモリ上に組み立てる。キーは昇順に追加すること。
    *
    * @param expectedBytes 出力の大きさの見込み。最初からこの大きさで確保する（倍々に伸ばすと、16 MiB の
    *                      ファイルで合計 32 MiB ほどコピーしていた）
    */
  final class Builder(blockBytes: Int, bloomBitsPerKey: Int, expectedBytes: Int = 64 * 1024):
    private val out = ByteWriter(expectedBytes)
    private val block = ByteWriter(blockBytes + 256)
    private val index = ArrayBuffer.empty[IndexEntry]
    private val bloomHashes = mutable.ArrayBuilder.ofLong() // キーのコピーは持たず、ハッシュだけを持つ
    private var lastKey: ByteVector = null
    private var first: ByteVector = null
    private var count = 0L
    private var dels = 0L

    /** これまでに加えた墓石の件数。 */
    def deletes: Long = dels

    def isEmpty: Boolean = count == 0
    def estimatedSize: Long = out.size.toLong + block.size

    def add(key: ByteVector, e: Entry): Unit =
      if lastKey != null && Bytes.compare(lastKey, key) >= 0 then
        throw IllegalArgumentException("keys must be added in strictly increasing order")
      if first == null then first = key
      block.lenBytes(key)
      e match
        case Entry.Put(v) => block.u8(KindPut).lenBytes(v)
        case Entry.Del    => block.u8(KindDel); dels += 1
      bloomHashes += Bloom.hash(key.toArrayUnsafe)
      lastKey = key
      count += 1
      if block.size >= blockBytes then finishBlock()

    private def finishBlock(): Unit =
      if block.size > 0 then
        val data = block.toArray
        val off = out.size.toLong
        out.bytes(data).u32(Crc32c(data))
        index += IndexEntry(lastKey, off, data.length + 4)
        block.reset()

    /** (ファイルの内容, 最小キー, 最大キー, 件数) */
    def finish(): (ByteVector, ByteVector, ByteVector, Long) =
      require(count > 0, "empty sst")
      finishBlock()
      val idx = ByteWriter()
      idx.varInt(index.size)
      index.foreach(e => idx.lenBytes(e.lastKey).varLong(e.offset).varInt(e.size))
      val idxArr = idx.toArray
      val idxOff = out.size.toLong
      out.bytes(idxArr).u32(Crc32c(idxArr))
      val bw = ByteWriter()
      Bloom.fromHashes(bloomHashes.result(), bloomBitsPerKey).encode(bw)
      val bloomArr = bw.toArray
      val bloomOff = out.size.toLong
      out.bytes(bloomArr).u32(Crc32c(bloomArr))
      val footer = ByteWriter()
      footer.u64(idxOff).u32(idxArr.length + 4).u64(bloomOff).u32(bloomArr.length + 4).u64(count).u64(Magic)
      val f = footer.toArray
      out.bytes(f).u32(Crc32c(f))
      (out.result(), first, lastKey, count)

  private def checked(bytes: ByteVector, what: String): ByteReader =
    verifyCrc(bytes).fold(e => throw CorruptionException(s"$what: $e"), identity)

  def decodeBlock(bytes: ByteVector): Block =
    val r = checked(bytes, "data block")
    val keys = ArrayBuffer.empty[ByteVector]
    val entries = ArrayBuffer.empty[Entry]
    while r.hasRemaining do
      keys += ByteVector.view(r.lenBytes())
      entries += (r.u8() match
        case KindPut => Entry.Put(ByteVector.view(r.lenBytes()))
        case KindDel => Entry.Del
        case k       => throw CorruptionException(s"unknown entry kind $k"))
    Block(keys.toArray, entries.toArray)

  /** 開いた SST ファイル。インデックスとブルームフィルタはメモリに置く。 */
  final class Reader private (
      val meta: SstMeta,
      handle: FileHandle,
      index: Array[IndexEntry],
      bloom: Bloom,
      val close: IO[Unit]
  ):
    def blockCount: Int = index.length

    def readBlock(i: Int): IO[Block] =
      val e = index(i)
      handle.read(e.offset, e.size).flatMap { b =>
        if b.size != e.size then IO.raiseError(CorruptionException(s"sst ${meta.id}: short read"))
        else IO(decodeBlock(b))
      }

    /** [from, until) にかかるブロックの大きさの合計（メモリのインデックスだけから求める。ディスクは読まない）。
      * 範囲の端のブロックは丸ごと数えるので、実際より少し大きめになる。
      */
    def approximateBytes(from: ByteVector, until: Option[ByteVector]): Long =
      if Bytes.compare(meta.largest, from) < 0 || until.exists(u => Bytes.compare(meta.smallest, u) >= 0) then 0L
      else
        val start = blockFor(from)
        // until を含みうるブロックまで（そのブロックは until より前のキーを含みうる）
        val end = until.fold(index.length)(u => math.min(index.length, blockFor(u) + 1))
        var n = 0L
        var i = start
        while i < end do
          n += index(i).size
          i += 1
        n

    /** key を含みうる最初のブロック（なければ blockCount）。 */
    def blockFor(key: ByteVector): Int = lowerBound(index.length, key)(index(_).lastKey)

    def get(key: ByteVector): IO[Option[Entry]] =
      if !meta.contains(key) || !bloom.mayContain(key.toArrayUnsafe) then IO.pure(None)
      else
        val bi = blockFor(key)
        if bi >= index.length then IO.pure(None)
        else
          readBlock(bi).map { b =>
            val i = b.seek(key)
            if i < b.keys.length && Bytes.equal(b.keys(i), key) then Some(b.entries(i)) else None
          }

  object Reader:
    def open(meta: SstMeta, handle: FileHandle, close: IO[Unit]): IO[Reader] =
      for
        size <- handle.size
        _ <- IO.raiseWhen(size < FooterSize)(CorruptionException(s"sst ${meta.id}: file too small"))
        footerBytes <- handle.read(size - FooterSize, FooterSize)
        f <- IO {
          val r = checked(footerBytes, s"sst ${meta.id} footer")
          val idxOff = r.u64(); val idxLen = r.u32(); val bloomOff = r.u64(); val bloomLen = r.u32()
          r.u64()
          if r.u64() != Magic then throw CorruptionException(s"sst ${meta.id}: bad magic")
          (idxOff, idxLen, bloomOff, bloomLen)
        }
        (idxOff, idxLen, bloomOff, bloomLen) = f
        idxBytes <- handle.read(idxOff, idxLen)
        bloomBytes <- handle.read(bloomOff, bloomLen)
        reader <- IO {
          val ir = checked(idxBytes, s"sst ${meta.id} index")
          val n = ir.varInt()
          val index = Array.fill(n)(IndexEntry(ByteVector.view(ir.lenBytes()), ir.varLong(), ir.varInt()))
          val bloom = Bloom.decode(checked(bloomBytes, s"sst ${meta.id} bloom"))
          Reader(meta, handle, index, bloom, close)
        }.adaptError { case e: DecodeError => CorruptionException(s"sst ${meta.id}: ${e.getMessage}") }
      yield reader
