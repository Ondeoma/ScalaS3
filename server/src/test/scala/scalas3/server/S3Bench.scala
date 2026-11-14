package scalas3.server

import scalas3.s3.SigV4

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.time.format.DateTimeFormatter
import java.time.{ZoneOffset, ZonedDateTime}
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{ConcurrentLinkedQueue, Executors}
import scala.jdk.CollectionConverters.*

/** S3 互換サーバーのベンチマーク（任意の S3 互換エンドポイントに使える）。
  * {{{
  * sbt "server/Test/runMain scalas3.server.S3Bench http://127.0.0.1:3900 AK SK put 4096 32 20"
  *   引数: エンドポイント アクセスキー 秘密鍵 操作(put|get) オブジェクトの大きさ 並行数 秒数
  * }}}
  * get は直前の put（同じ並行数）で書いたキーを読む（同じバケット bench を使う）。
  */
object S3Bench:
  private val fmt = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'")

  def main(args: Array[String]): Unit =
    val Array(endpoint, ak, sk, op, sizeS, concS, secsS) = args: @unchecked
    val size = sizeS.toInt
    val conc = concS.toInt
    val secs = secsS.toInt
    val uri = URI.create(endpoint)
    val host = s"${uri.getHost}:${uri.getPort}"
    val http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).executor(Executors.newFixedThreadPool(conc * 2)).build()

    def send(method: String, path: String, body: Array[Byte]): HttpResponse[Array[Byte]] =
      val amzDate = ZonedDateTime.now(ZoneOffset.UTC).format(fmt)
      val scope = SigV4.Scope(amzDate.take(8), "us-east-1", "s3")
      val hs = Map("host" -> host, "x-amz-date" -> amzDate, "x-amz-content-sha256" -> SigV4.UnsignedPayload)
      val names = hs.keys.toList.sorted
      val canonical = SigV4.canonicalRequest(method, path, Nil, n => hs.get(n).toList, names, SigV4.UnsignedPayload)
      val sig = SigV4.sign(SigV4.signingKey(sk, scope), SigV4.stringToSign(amzDate, scope, canonical))
      val req = HttpRequest.newBuilder(URI.create(endpoint + path))
        .method(method, if body.isEmpty then HttpRequest.BodyPublishers.noBody() else HttpRequest.BodyPublishers.ofByteArray(body))
        .header("x-amz-date", amzDate).header("x-amz-content-sha256", SigV4.UnsignedPayload)
        .header("Authorization", s"AWS4-HMAC-SHA256 Credential=$ak/${scope.render}, SignedHeaders=${names.mkString(";")}, Signature=$sig")
        .build()
      http.send(req, HttpResponse.BodyHandlers.ofByteArray())

    try send("PUT", "/bench", Array.emptyByteArray) // バケット（既にあってもよい）
    catch
      case e: Exception =>
        println(s"[bench] cannot reach $endpoint: $e")
        sys.exit(1)
    val payload = Array.tabulate[Byte](size)(i => (i * 31 + 7).toByte)
    // get は、直前の put で各ワーカーが実際に書けた件数（最大 200）の中から読む（書けていないキーを読むと 404 になり、
    // 大きなオブジェクトで結果が実際より速く見える）。キーは 1 から連番なので二分探索で求める
    def exists(key: String) = send("HEAD", key, Array.emptyByteArray).statusCode() == 200
    val written: Array[Int] =
      if op != "get" then Array.fill(conc)(200)
      else
        Array.tabulate(conc) { w =>
          var lo = 0
          var hi = 200
          while lo < hi do
            val mid = (lo + hi + 1) / 2
            if exists(s"/bench/s$size-w$w-$mid") then lo = mid else hi = mid - 1
          lo
        }
    if op == "get" && written.contains(0) then
      println(s"[bench] some workers have no objects to read (run put with the same concurrency first): ${written.mkString(",")}")
      sys.exit(1)
    val latencies = ConcurrentLinkedQueue[Long]()
    val ops = AtomicLong()
    val errors = AtomicLong()
    val statuses = java.util.concurrent.ConcurrentHashMap[String, AtomicLong]()
    val deadline = System.nanoTime() + secs * 1_000_000_000L
    val pool = Executors.newFixedThreadPool(conc)
    (0 until conc).foreach { w =>
      pool.submit(new Runnable:
        def run(): Unit =
          var i = 0
          while System.nanoTime() < deadline do
            val key = if op == "put" then s"/bench/s$size-w$w-${i + 1}" else s"/bench/s$size-w$w-${(i % written(w)) + 1}"
            val t0 = System.nanoTime()
            val r = try Some(send(if op == "put" then "PUT" else "GET", key, if op == "put" then payload else Array.emptyByteArray)) catch case _: Exception => None
            val t1 = System.nanoTime()
            if r.exists(_.statusCode() / 100 == 2) then
              ops.incrementAndGet(); latencies.add(t1 - t0)
            else
              errors.incrementAndGet()
              statuses.computeIfAbsent(r.fold("exception")(x => s"${x.statusCode()} ${new String(x.body()).take(120)}"), _ => AtomicLong()).incrementAndGet()
            i += 1
      )
    }
    pool.shutdown()
    pool.awaitTermination(secs + 60L, java.util.concurrent.TimeUnit.SECONDS)
    val ls = latencies.asScala.toVector.sorted
    def pct(p: Double) = if ls.isEmpty then 0.0 else ls(math.min(ls.size - 1, (ls.size * p).toInt)) / 1e6
    val n = ops.get
    println(f"[bench] $op size=$size conc=$conc: ${n / secs.toDouble}%.1f ops/s  ${n * size / secs.toDouble / 1048576}%.1f MiB/s  p50=${pct(0.5)}%.1fms p99=${pct(0.99)}%.1fms errors=${errors.get}")
    statuses.asScala.toList.sortBy(-_._2.get).take(3).foreach((k, v) => println(s"[bench]   error x${v.get}: $k"))
    sys.exit(0)
