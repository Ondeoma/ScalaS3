package scalas3.core

import scodec.bits.ByteVector

/** バイト列の比較など。キーはすべて符号なし辞書順で比較する。 */
object Bytes:

  /** 符号なし辞書順の比較。 */
  def compare(a: ByteVector, b: ByteVector): Int =
    java.util.Arrays.compareUnsigned(a.toArrayUnsafe, b.toArrayUnsafe)

  /** 等しいか。ByteVector の == は 1 バイトずつ読むので遅い（大きな値の比較で目立つ）。 */
  def equal(a: ByteVector, b: ByteVector): Boolean =
    a.size == b.size && java.util.Arrays.equals(a.toArrayUnsafe, b.toArrayUnsafe)

  def compare(a: Array[Byte], b: Array[Byte]): Int =
    java.util.Arrays.compareUnsigned(a, b)

  val unsignedOrdering: Ordering[ByteVector] = (a, b) => compare(a, b)

  val arrayOrdering: Ordering[Array[Byte]] = (a, b) => compare(a, b)

  /** prefix で始まるキーをすべて含む最小の上限（排他的）。全バイト 0xFF なら None。 */
  def prefixEnd(prefix: ByteVector): Option[ByteVector] =
    val arr = prefix.toArray
    var i = arr.length - 1
    while i >= 0 && (arr(i) & 0xff) == 0xff do i -= 1
    if i < 0 then None
    else
      arr(i) = (arr(i) + 1).toByte
      Some(ByteVector.view(arr, 0, i + 1))

/** CRC32C チェックサム。 */
object Crc32c:
  def apply(bytes: Array[Byte], offset: Int, length: Int): Int =
    val c = new java.util.zip.CRC32C()
    c.update(bytes, offset, length)
    c.getValue.toInt

  def apply(bytes: Array[Byte]): Int = apply(bytes, 0, bytes.length)

  def apply(bytes: ByteVector): Int =
    val c = new java.util.zip.CRC32C()
    c.update(bytes.toByteBuffer)
    c.getValue.toInt
