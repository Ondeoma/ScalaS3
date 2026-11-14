package scalas3.core

import scodec.bits.ByteVector

/** 手書きのバイナリ形式用の可変長バッファ。単一スレッドで使うこと。 */
final class ByteWriter(initialCapacity: Int = 256):
  private var buf = new Array[Byte](math.max(16, initialCapacity))
  private var pos = 0

  def size: Int = pos

  private def ensure(n: Int): Unit =
    if pos + n > buf.length then
      var cap = buf.length * 2
      while cap < pos + n do cap *= 2
      buf = java.util.Arrays.copyOf(buf, cap)

  def u8(v: Int): this.type =
    ensure(1); buf(pos) = v.toByte; pos += 1; this

  def u32(v: Int): this.type =
    ensure(4)
    buf(pos) = (v >>> 24).toByte
    buf(pos + 1) = (v >>> 16).toByte
    buf(pos + 2) = (v >>> 8).toByte
    buf(pos + 3) = v.toByte
    pos += 4
    this

  def u64(v: Long): this.type =
    u32((v >>> 32).toInt); u32(v.toInt)

  /** 符号なし LEB128。 */
  def varLong(v: Long): this.type =
    // 最大 10 バイト。先に 1 回だけ確保して、1 バイトごとの確認を省く
    ensure(10)
    var x = v
    while (x & ~0x7fL) != 0L do
      buf(pos) = ((x & 0x7f) | 0x80).toByte
      pos += 1
      x = x >>> 7
    buf(pos) = x.toByte
    pos += 1
    this

  def varInt(v: Int): this.type = varLong(v.toLong & 0xffffffffL)

  def bytes(b: Array[Byte]): this.type =
    ensure(b.length); System.arraycopy(b, 0, buf, pos, b.length); pos += b.length; this

  def bytes(b: ByteVector): this.type =
    val n = b.size.toInt
    ensure(n); b.copyToArray(buf, pos); pos += n; this

  /** 長さ（varint）付きのバイト列。 */
  def lenBytes(b: ByteVector): this.type = varInt(b.size.toInt).bytes(b)

  def lenBytes(b: Array[Byte]): this.type = varInt(b.length).bytes(b)

  /** 長さ付きの UTF-8 文字列。 */
  def string(s: String): this.type = lenBytes(s.getBytes(java.nio.charset.StandardCharsets.UTF_8))

  def toArray: Array[Byte] = java.util.Arrays.copyOf(buf, pos)
  def toByteVector: ByteVector = ByteVector.view(toArray)

  /** 書いた内容をコピーせずに返す。以後このバッファに書いてはいけない（大きなバッファの最後のコピーを省く）。 */
  def result(): ByteVector = ByteVector.view(buf, 0, pos)

  def reset(): Unit = pos = 0

final class DecodeError(msg: String) extends RuntimeException(msg)

/** 手書きのバイナリ形式の読み取り。範囲外アクセスは DecodeError。 */
final class ByteReader(val buf: Array[Byte], start: Int, end: Int):
  def this(buf: Array[Byte]) = this(buf, 0, buf.length)

  private var p = start

  def position: Int = p
  def remaining: Int = end - p
  def hasRemaining: Boolean = p < end

  private def need(n: Int): Unit =
    // p + n は n が大きいと桁あふれするので、残りと比べる
    if n < 0 || n > end - p then throw DecodeError(s"unexpected end of data (need $n, have ${end - p})")

  def u8(): Int =
    need(1); val v = buf(p) & 0xff; p += 1; v

  def u32(): Int =
    need(4)
    val v = ((buf(p) & 0xff) << 24) | ((buf(p + 1) & 0xff) << 16) | ((buf(p + 2) & 0xff) << 8) | (buf(p + 3) & 0xff)
    p += 4
    v

  def u64(): Long =
    val hi = u32().toLong & 0xffffffffL
    val lo = u32().toLong & 0xffffffffL
    (hi << 32) | lo

  def varLong(): Long =
    var result = 0L
    var shift = 0
    var done = false
    while !done do
      if shift > 63 then throw DecodeError("varint too long")
      val b = u8()
      result |= (b & 0x7fL) << shift
      shift += 7
      done = (b & 0x80) == 0
    result

  def varInt(): Int =
    val v = varLong()
    if v < 0 || v > 0xffffffffL then throw DecodeError("varint out of range")
    v.toInt

  def bytes(n: Int): Array[Byte] =
    need(n)
    val out = java.util.Arrays.copyOfRange(buf, p, p + n)
    p += n
    out

  def lenBytes(): Array[Byte] = bytes(varInt())

  /** 長さ付きの UTF-8 文字列（ByteWriter.string で書いたもの）。 */
  def string(): String =
    val n = varInt()
    need(n)
    val s = new String(buf, p, n, java.nio.charset.StandardCharsets.UTF_8)
    p += n
    s

  def skip(n: Int): Unit =
    need(n); p += n
