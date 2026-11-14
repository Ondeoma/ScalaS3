package scalas3.server

import scalas3.s3.SigV4

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.time.format.DateTimeFormatter
import java.time.{ZoneOffset, ZonedDateTime}
import java.util.concurrent.atomic.{AtomicLong, LongAdder}
import java.util.concurrent.{ConcurrentLinkedQueue, Executors, ThreadLocalRandom}
import scala.jdk.CollectionConverters.*

/** 長時間の負荷（soak）。PUT・GET・DELETE を混ぜて流し続け、1 分ごとに結果を表示する。
  * {{{
  * sbt "server/Test/runMain scalas3.server.S3Soak http://127.0.0.1:3900 AK SK 60 16 20000 20 262144"
  *   引数: エンドポイント アクセスキー 秘密鍵 分数 並行数 キーの数 [操作ごとの待ち（ミリ秒）] [大きさの上限]
  * }}}
  *   - 大きさは 1 KiB（インライン）、16 KiB、256 KiB、2 MiB を混ぜる。キーの数を絞って上書きを起こし、GC が回収する
  *     ゴミを作る
  *   - 各ワーカーは自分のキーだけを扱い、キーごとに最後に書いた版を覚える。GET は中身がその版と一致するか、消したキーは
  *     404 になるかを確かめる（一致しなければ mismatch として数える）
  */
object S3Soak:
  private val fmt = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'")
  private val sizes = Vector(1024, 16 * 1024, 256 * 1024, 2 * 1024 * 1024)

  /** キーと版から決まる中身（検証に使う）。 */
  private def payload(key: Int, version: Int, size: Int): Array[Byte] =
    val seed = key * 1_000_003L + version
    val a = new Array[Byte](size)
    var x = seed
    var i = 0
    while i < size do
      x = x * 6364136223846793005L + 1442695040888963407L
      a(i) = (x >>> 56).toByte
      i += 1
    a

  def main(args: Array[String]): Unit =
    val Array(endpoint, ak, sk, minutesS, concS, keysS) = args.take(6): @unchecked
    val pauseMs = args.lift(6).fold(0L)(_.toLong)
    val maxSize = args.lift(7).fold(Int.MaxValue)(_.toInt)
    val usable = sizes.filter(_ <= maxSize)
    val minutes = minutesS.toInt
    val conc = concS.toInt
    val keys = keysS.toInt
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

    send("PUT", "/soak", Array.emptyByteArray)
    final class Window:
      val ops = Map("put" -> LongAdder(), "get" -> LongAdder(), "delete" -> LongAdder())
      val bytes = LongAdder()
      val errors = LongAdder()
      val mismatches = LongAdder()
      val latencies = ConcurrentLinkedQueue[Long]()
    val window = java.util.concurrent.atomic.AtomicReference(Window())
    val totalMismatch = AtomicLong()
    val totalErrors = AtomicLong()
    val reported = AtomicLong()
    val started = System.nanoTime()
    // エラーの中身を最初の 50 件だけ表示する（原因の切り分けに使う）
    def report(what: String): Unit =
      if reported.incrementAndGet() <= 50 then
        System.err.println(f"[soak] error at ${(System.nanoTime() - started) / 1e9}%.1fs: $what")
    val deadline = System.nanoTime() + minutes * 60_000_000_000L
    val pool = Executors.newFixedThreadPool(conc)
    (0 until conc).foreach { w =>
      pool.submit(new Runnable:
        def run(): Unit =
          // このワーカーのキー（key % conc == w）。値は (版, 大きさ)。版 -1 は消した
          val state = scala.collection.mutable.Map.empty[Int, (Int, Int)]
          val rnd = ThreadLocalRandom.current()
          while System.nanoTime() < deadline do
            val win = window.get
            val key = rnd.nextInt(keys / conc) * conc + w
            val path = s"/soak/k$key"
            val dice = rnd.nextInt(100)
            val t0 = System.nanoTime()
            var op = "?"
            try
              if dice < 55 || !state.contains(key) then
                op = "PUT"
                val size = usable(rnd.nextInt(usable.size))
                val v = state.get(key).fold(0)(_._1.max(0) + 1)
                val r = send("PUT", path, payload(key, v, size))
                if r.statusCode() == 200 then { state(key) = (v, size); win.ops("put").increment(); win.bytes.add(size.toLong) }
                else { win.errors.increment(); totalErrors.incrementAndGet(); report(s"PUT $path -> ${r.statusCode()} ${String(r.body()).take(300)}") }
              else if dice < 90 then
                op = "GET"
                val (v, size) = state(key)
                val r = send("GET", path, Array.emptyByteArray)
                val ok =
                  if v < 0 then r.statusCode() == 404
                  else r.statusCode() == 200 && java.util.Arrays.equals(r.body(), payload(key, v, size))
                if r.statusCode() >= 500 then { win.errors.increment(); totalErrors.incrementAndGet(); report(s"GET $path -> ${r.statusCode()} ${String(r.body()).take(300)}") }
                else if !ok then { win.mismatches.increment(); totalMismatch.incrementAndGet() }
                else { win.ops("get").increment(); win.bytes.add(r.body().length.toLong) }
              else
                op = "DELETE"
                val r = send("DELETE", path, Array.emptyByteArray)
                if r.statusCode() == 204 then { state(key) = (-1, 0); win.ops("delete").increment() }
                else { win.errors.increment(); totalErrors.incrementAndGet(); report(s"DELETE $path -> ${r.statusCode()} ${String(r.body()).take(300)}") }
              win.latencies.add(System.nanoTime() - t0)
            catch case e: Exception => { win.errors.increment(); totalErrors.incrementAndGet(); report(s"$op $path ${e.getClass.getName}: ${e.getMessage}") }
            if pauseMs > 0 then Thread.sleep(pauseMs)
      )
    }
    pool.shutdown()
    // 1 分ごとに区切って表示する
    var minute = 0
    while !pool.awaitTermination(60, java.util.concurrent.TimeUnit.SECONDS) do
      minute += 1
      val w = window.getAndSet(Window())
      val ls = w.latencies.asScala.toVector.sorted
      def pct(p: Double) = if ls.isEmpty then 0.0 else ls(math.min(ls.size - 1, (ls.size * p).toInt)) / 1e6
      println(f"[soak] min=$minute%3d put=${w.ops("put").sum / 60.0}%7.1f/s get=${w.ops("get").sum / 60.0}%7.1f/s del=${w.ops("delete").sum / 60.0}%6.1f/s ${w.bytes.sum / 60.0 / 1048576}%7.1f MiB/s p50=${pct(0.5)}%6.1fms p99=${pct(0.99)}%7.1fms errors=${w.errors.sum} mismatches=${w.mismatches.sum}")
      Console.flush()
    println(s"[soak] done: errors=${totalErrors.get} mismatches=${totalMismatch.get}")
    sys.exit(0)
