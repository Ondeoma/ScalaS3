package scalas3.block

/** GF(2^8) の演算（原始多項式 0x11d）。 */
object GF256:
  val exp: Array[Int] = new Array[Int](512)
  val log: Array[Int] = new Array[Int](256)

  locally {
    var x = 1
    for i <- 0 until 255 do
      exp(i) = x
      log(x) = i
      x <<= 1
      if (x & 0x100) != 0 then x ^= 0x11d
    for i <- 255 until 512 do exp(i) = exp(i - 255)
  }

  /** 乗算表（mulTable(a)(b) = a*b）。 */
  val mulTable: Array[Array[Byte]] = Array.tabulate(256, 256)((a, b) => mul(a, b).toByte)

  def mul(a: Int, b: Int): Int = if a == 0 || b == 0 then 0 else exp(log(a) + log(b))
  def div(a: Int, b: Int): Int =
    require(b != 0, "division by zero")
    if a == 0 then 0 else exp(log(a) + 255 - log(b))
  def pow(a: Int, n: Int): Int = if n == 0 then 1 else if a == 0 then 0 else exp((log(a) * n) % 255)

  /** 正方行列の逆行列（ガウス・ジョルダン法）。正則でなければ例外。 */
  def invert(m: Array[Array[Int]]): Array[Array[Int]] =
    val n = m.length
    val a = Array.tabulate(n, 2 * n)((r, c) => if c < n then m(r)(c) else if c - n == r then 1 else 0)
    for col <- 0 until n do
      val pivot = (col until n).find(r => a(r)(col) != 0).getOrElse(throw IllegalArgumentException("singular matrix"))
      if pivot != col then
        val t = a(pivot); a(pivot) = a(col); a(col) = t
      val inv = div(1, a(col)(col))
      for c <- 0 until 2 * n do a(col)(c) = mul(a(col)(c), inv)
      for r <- 0 until n if r != col && a(r)(col) != 0 do
        val f = a(r)(col)
        for c <- 0 until 2 * n do a(r)(c) ^= mul(f, a(col)(c))
    Array.tabulate(n, n)((r, c) => a(r)(c + n))

  def multiply(a: Array[Array[Int]], b: Array[Array[Int]]): Array[Array[Int]] =
    Array.tabulate(a.length, b(0).length) { (r, c) =>
      var acc = 0
      for i <- b.indices do acc ^= mul(a(r)(i), b(i)(c))
      acc
    }

/** 組織符号の Reed-Solomon 符号（k 個のデータ断片 + m 個のパリティ断片。どの k 個からでも復元できる）。
  *
  * 符号化行列は、ヴァンデルモンド行列に上 k 行の逆行列を掛けて、上 k 行を単位行列にしたもの。
  */
final class ReedSolomon(val k: Int, val m: Int):
  require(k >= 1 && m >= 0 && k + m <= 255, s"invalid parameters k=$k m=$m")

  /** (k+m) × k の符号化行列。 */
  val matrix: Array[Array[Int]] =
    val vander = Array.tabulate(k + m, k)((r, c) => GF256.pow(r, c))
    val top = GF256.invert(Array.tabulate(k, k)((r, c) => vander(r)(c)))
    GF256.multiply(vander, top)

  /** 1 断片の大きさ（データ size を k 等分し、端数を切り上げる）。 */
  def shardSize(size: Int): Int = math.max(1, (size + k - 1) / k)

  /** データを k 個に分け（足りない分は 0 で埋める）、パリティを加えた k+m 個の断片を返す。 */
  def encode(data: Array[Byte]): Array[Array[Byte]] =
    val len = shardSize(data.length)
    val shards = Array.tabulate(k + m) { i =>
      val s = new Array[Byte](len)
      if i < k then
        val from = i * len
        if from < data.length then System.arraycopy(data, from, s, 0, math.min(len, data.length - from))
      s
    }
    for p <- 0 until m do
      val row = matrix(k + p)
      val out = shards(k + p)
      for i <- 0 until k do
        val table = GF256.mulTable(row(i))
        val in = shards(i)
        var j = 0
        while j < len do
          out(j) = (out(j) ^ table(in(j) & 0xff)).toByte
          j += 1
    shards

  /** 手元にある断片（番号 → 中身）から元のデータを復元する。k 個以上必要。 */
  def decode(present: Map[Int, Array[Byte]], size: Int): Array[Byte] =
    require(present.size >= k, s"need $k shards, got ${present.size}")
    val len = shardSize(size)
    val data: Array[Array[Byte]] =
      if (0 until k).forall(present.contains) then Array.tabulate(k)(present)
      else
        val rows = present.keys.toVector.sorted.take(k)
        val sub = Array.tabulate(k, k)((r, c) => matrix(rows(r))(c))
        val inv = GF256.invert(sub)
        Array.tabulate(k) { i =>
          val out = new Array[Byte](len)
          for (row, r) <- rows.zipWithIndex do
            val table = GF256.mulTable(inv(i)(r))
            val in = present(row)
            var j = 0
            while j < len do
              out(j) = (out(j) ^ table(in(j) & 0xff)).toByte
              j += 1
          out
        }
    val result = new Array[Byte](size)
    for i <- 0 until k do
      val from = i * len
      if from < size then System.arraycopy(data(i), 0, result, from, math.min(len, size - from))
    result
