package scalas3.lsm

import scalas3.core.{ByteReader, ByteWriter}

import scala.util.hashing.MurmurHash3

/** ブルームフィルタ。ダブルハッシュで k 個の位置を決める。 */
final class Bloom private (bits: Array[Byte], k: Int):
  private val nbits = bits.length.toLong * 8

  def mayContain(key: Array[Byte]): Boolean =
    nbits == 0 || {
      val h = Bloom.hash(key)
      var i = 0
      while i < k && Bloom.isSet(bits, Bloom.bitIndex(h, i, nbits)) do i += 1
      i == k
    }

  def encode(w: ByteWriter): Unit =
    w.u8(k).lenBytes(bits): Unit

object Bloom:
  /** キーの 2 つのハッシュを 1 つの Long に詰めたもの（上位 32 ビットが h1、下位が h2）。
    * 照会のたびにタプルを作らず、組み立て中はキーのコピーの代わりにこれだけを持つ。
    */
  private[lsm] def hash(key: Array[Byte]): Long =
    val h1 = MurmurHash3.bytesHash(key, 0x5bd1e995)
    val h2 = MurmurHash3.bytesHash(key, 0x1b873593) | 1
    (h1.toLong << 32) | (h2.toLong & 0xffffffffL)

  /** i 番目の位置。h1 + i * h2（どちらも符号付きの Int を広げたもの）を符号なしで nbits で割った余り。 */
  private inline def bitIndex(h: Long, i: Int, nbits: Long): Long =
    java.lang.Long.remainderUnsigned((h >> 32) + i.toLong * h.toInt.toLong, nbits)

  private inline def isSet(bits: Array[Byte], idx: Long): Boolean =
    (bits((idx >>> 3).toInt) & (1 << (idx & 7).toInt)) != 0

  /** hash で求めた値の列から組み立てる。 */
  def fromHashes(hashes: Array[Long], bitsPerKey: Int): Bloom =
    val n = math.max(1, hashes.length)
    val nbytes = math.max(8, (n.toLong * bitsPerKey + 7) / 8).toInt
    val k = math.max(1, math.min(30, math.round(bitsPerKey * 0.69).toInt))
    val bits = new Array[Byte](nbytes)
    val nbits = nbytes.toLong * 8
    hashes.foreach { h =>
      var i = 0
      while i < k do
        val idx = bitIndex(h, i, nbits)
        bits((idx >>> 3).toInt) = (bits((idx >>> 3).toInt) | (1 << (idx & 7).toInt)).toByte
        i += 1
    }
    Bloom(bits, k)

  def decode(r: ByteReader): Bloom =
    val k = r.u8()
    Bloom(r.lenBytes(), k)
