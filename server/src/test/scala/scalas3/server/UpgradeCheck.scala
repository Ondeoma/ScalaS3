package scalas3.server

import scalas3.s3.SigV4

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.security.MessageDigest
import java.time.format.DateTimeFormatter
import java.time.{Duration, ZoneOffset, ZonedDateTime}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong, AtomicReference}
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue, Executors, ThreadLocalRandom, TimeUnit}
import scala.jdk.CollectionConverters.*
import scala.sys.process.*

/** ローリングアップグレードの試験。Podman のコンテナで 3 ノードを古いイメージで動かし、データを書いてから、1 台ずつ
  * 新しいイメージに入れ替える（データのボリュームはそのまま）。it/upgrade/run.sh が 2 つのイメージを作ってから呼ぶ。
  * {{{
  * sbt "server/Test/runMain scalas3.server.UpgradeCheck localhost/scalas3:upgrade-old localhost/scalas3:upgrade-new"
  * }}}
  * 確かめること：
  *   - 入れ替える前に書いたオブジェクト（インライン、1 ブロック、複数ブロック）が、入れ替えの後も全ノードから同じ中身で読める
  *   - 入れ替えの間も書き込みと読み込みを続け、成功した書き込みが失われない（古い版と新しい版が混ざった状態を含む）
  *   - 古い版で始めたマルチパートアップロードを、新しい版で続けて完了できる
  *   - 全ノードを止めて新しい版だけで起動し直しても、すべて読める
  * 入れ替え中のノードには要求を送らない（ロードバランサーが外す想定）。ほかのノードへの要求の失敗は数えて表示する。
  */
object UpgradeCheck:
  private val network = "s3upgrade"
  private val nodes = Vector("un1", "un2", "un3")
  private val ports = Map("un1" -> 4711, "un2" -> 4721, "un3" -> 4731)
  private val ak = "upgradekey"
  private val sk = "upgradesecretupgradesecret"
  private val bucket = "upgrade"
  private val fmt = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'")
  private val writerSizes = Vector(1000, 64 * 1024, 300 * 1024)

  private def podman(args: String*): Int = Process("podman" +: args).!(ProcessLogger(_ => (), _ => ()))
  private def log(msg: String): Unit = { println(s"[upgrade] $msg"); Console.flush() }

  private def startNode(n: String, image: String): Unit =
    val peers = nodes.map(x => s"$x=$x:3901").mkString(",")
    val code = podman(
      "run", "-d", "--name", n, "--hostname", n, "--network", network, "-p", s"${ports(n)}:3900",
      "-v", s"$n-data:/var/lib/scalas3/data",
      "-e", s"SCALAS3_NODE_ID=$n", "-e", s"SCALAS3_PEERS=$peers", "-e", s"SCALAS3_RPC_ADVERTISE=$n:3901",
      "-e", s"SCALAS3_ACCESS_KEY=$ak", "-e", s"SCALAS3_SECRET_KEY=$sk", "-e", s"SCALAS3_ZONE=z-$n",
      "-e", "JAVA_TOOL_OPTIONS=-XX:+UseG1GC -Xmx256m -XX:MaxDirectMemorySize=96m",
      image
    )
    if code != 0 then throw IllegalStateException(s"podman run $n ($image) failed: $code")

  private def removeAll(): Unit =
    nodes.foreach(n => podman("rm", "-f", "-t", "5", n))
    nodes.foreach(n => podman("volume", "rm", "-f", s"$n-data"))
    podman("network", "rm", "-f", network): Unit

  /** キーと版から決まる中身。 */
  private def payload(key: String, version: Int, size: Int): Array[Byte] =
    var x = key.hashCode.toLong * 1_000_003L + version
    val a = new Array[Byte](size)
    var i = 0
    while i < size do
      x = x * 6364136223846793005L + 1442695040888963407L
      a(i) = (x >>> 56).toByte
      i += 1
    a

  private def sha(b: Array[Byte]) = MessageDigest.getInstance("SHA-256").digest(b).map(x => f"${x & 0xff}%02x").mkString

  def main(args: Array[String]): Unit =
    val Array(oldImage, newImage) = args.take(2): @unchecked
    val code =
      try run(oldImage, newImage)
      catch
        case e: Throwable =>
          e.printStackTrace()
          if !sys.env.contains("KEEP") then removeAll()
          2
    sys.exit(code)

  private def run(oldImage: String, newImage: String): Int =
    removeAll()
    podman("network", "create", network)
    nodes.foreach(startNode(_, oldImage))
    val http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).connectTimeout(Duration.ofSeconds(2))
      .executor(Executors.newFixedThreadPool(16, r => { val t = Thread(r); t.setDaemon(true); t })).build()

    def send(node: String, method: String, path: String, body: Array[Byte] = Array.emptyByteArray, query: List[(String, String)] = Nil): HttpResponse[Array[Byte]] =
      val amzDate = ZonedDateTime.now(ZoneOffset.UTC).format(fmt)
      val scope = SigV4.Scope(amzDate.take(8), "us-east-1", "s3")
      val hs = Map("host" -> s"127.0.0.1:${ports(node)}", "x-amz-date" -> amzDate, "x-amz-content-sha256" -> SigV4.UnsignedPayload)
      val names = hs.keys.toList.sorted
      val canonical = SigV4.canonicalRequest(method, path, query, n => hs.get(n).toList, names, SigV4.UnsignedPayload)
      val sig = SigV4.sign(SigV4.signingKey(sk, scope), SigV4.stringToSign(amzDate, scope, canonical))
      val qs = if query.isEmpty then "" else query.map((k, v) => s"${SigV4.uriEncode(k, true)}=${SigV4.uriEncode(v, true)}").mkString("?", "&", "")
      val req = HttpRequest.newBuilder(URI.create(s"http://127.0.0.1:${ports(node)}$path$qs")).timeout(Duration.ofSeconds(30))
        .method(method, if body.isEmpty then HttpRequest.BodyPublishers.noBody() else HttpRequest.BodyPublishers.ofByteArray(body))
        .header("x-amz-date", amzDate).header("x-amz-content-sha256", SigV4.UnsignedPayload)
        .header("Authorization", s"AWS4-HMAC-SHA256 Credential=$ak/${scope.render}, SignedHeaders=${names.mkString(";")}, Signature=$sig")
        .build()
      http.send(req, HttpResponse.BodyHandlers.ofByteArray())

    def awaitServing(n: String, seconds: Int = 180): Unit =
      val by = System.nanoTime() + seconds * 1_000_000_000L
      while System.nanoTime() < by && !scala.util.Try(send(n, "HEAD", s"/$bucket").statusCode() == 200).getOrElse(false) do Thread.sleep(500)
      if System.nanoTime() >= by then throw IllegalStateException(s"$n did not start serving")

    // ---- 古い版で起動し、バケットを作る ----
    val readyBy = System.nanoTime() + 180_000_000_000L
    var ready = false
    while !ready && System.nanoTime() < readyBy do
      ready = scala.util.Try(send("un1", "PUT", s"/$bucket").statusCode()).toOption.exists(c => c == 200 || c == 409)
      if !ready then Thread.sleep(1000)
    if !ready then throw IllegalStateException("cluster did not become ready")
    nodes.foreach(awaitServing(_))
    log(s"cluster ready on $oldImage")

    // ---- 入れ替える前のデータ ----
    val seedSizes = Vector(1000, 100 * 1024, 6 * 1024 * 1024) // インライン、1 ブロック、複数ブロック（4 MiB ごと）
    val seed = (0 until 45).map(i => s"seed/$i" -> payload(s"seed/$i", 0, seedSizes(i % 3))).toVector
    seed.foreach { (k, body) =>
      val r = send(nodes(k.drop(5).toInt % 3), "PUT", s"/$bucket/$k", body)
      if r.statusCode() != 200 then throw IllegalStateException(s"seed PUT $k failed: ${r.statusCode()}")
    }
    val seedSums = seed.map((k, b) => k -> sha(b)).toMap
    // 古い版でマルチパートアップロードを始め、1 つ目の部分を送っておく
    val mpKey = "multipart/object"
    val part1 = payload(mpKey, 1, 5 * 1024 * 1024)
    val part2 = payload(mpKey, 2, 1024 * 1024)
    val init = send("un1", "POST", s"/$bucket/$mpKey", query = List("uploads" -> ""))
    val uploadId = "<UploadId>([^<]+)</UploadId>".r.findFirstMatchIn(String(init.body())).map(_.group(1))
      .getOrElse(throw IllegalStateException(s"CreateMultipartUpload failed: ${init.statusCode()} ${String(init.body())}"))
    val p1 = send("un2", "PUT", s"/$bucket/$mpKey", part1, List("partNumber" -> "1", "uploadId" -> uploadId))
    val etag1 = p1.headers().firstValue("ETag").orElseThrow()
    log(s"wrote ${seed.size} objects (${seed.map(_._2.length.toLong).sum / 1048576} MiB) and part 1 of a multipart upload")

    // ---- 入れ替えの間も続ける書き込みと読み込み ----
    val down = AtomicReference[Option[String]](None)
    val phase = AtomicReference("mixed")
    val stop = AtomicBoolean(false)
    val acked = ConcurrentHashMap[String, Int]()     // 成功した最後の版
    val attempted = ConcurrentHashMap[String, Int]() // 送った最後の版（失敗したものも効いたかもしれない）
    val ops = AtomicLong()
    val errors = ConcurrentHashMap[String, AtomicLong]()
    val lost = ConcurrentLinkedQueue[String]()
    val errorSamples = ConcurrentLinkedQueue[String]()
    def error(what: String): Unit =
      errors.computeIfAbsent(phase.get, _ => AtomicLong()).incrementAndGet()
      if errorSamples.size < 30 then errorSamples.add(s"${phase.get}: $what"): Unit

    /** GET の結果が正しいか。読めてよいのは、GET を送る前に成功していた最後の版（lo）から、GET を受け取った時点で
      * 送っていた最後の版（hi）まで。lo は GET の前に、hi は GET の後に取る（書き込みと並行して読むため）。
      */
    def matches(key: String, lo: Int, body: Array[Byte]): Boolean =
      val hi = attempted.getOrDefault(key, -1)
      (lo max 0 to hi).exists(v => java.util.Arrays.equals(body, payload(key, v, writerSizes(v % writerSizes.size))))

    val writers = 4
    val pool = Executors.newFixedThreadPool(writers)
    (0 until writers).foreach { w =>
      pool.submit(new Runnable:
        def run(): Unit =
          val rnd = ThreadLocalRandom.current()
          while !stop.get do
            val live = nodes.filterNot(down.get.contains)
            val node = live(rnd.nextInt(live.size))
            val key = s"w$w/k${rnd.nextInt(40)}"
            try
              if rnd.nextInt(10) < 6 || !acked.containsKey(key) then
                val v = attempted.getOrDefault(key, -1) + 1
                attempted.put(key, v)
                val r = send(node, "PUT", s"/$bucket/$key", payload(key, v, writerSizes(v % writerSizes.size)))
                if r.statusCode() == 200 then acked.put(key, v) else error(s"PUT $key on $node -> ${r.statusCode()} ${String(r.body()).take(200)}")
              else
                val lo = acked.getOrDefault(key, -1)
                val r = send(node, "GET", s"/$bucket/$key")
                if r.statusCode() != 200 then error(s"GET $key on $node -> ${r.statusCode()} ${String(r.body()).take(200)}")
                else if !matches(key, lo, r.body()) then lost.add(s"${phase.get}: GET $key on $node returned an unexpected version"): Unit
              ops.incrementAndGet()
            catch case e: Exception => error(s"$key on $node: ${e.getClass.getSimpleName}: ${e.getMessage}")
            Thread.sleep(10)
      )
    }

    /** 入れ替える前のデータと、書き込みの結果を、全ノードから確かめる。食い違いの数を返す。 */
    def verifyAll(label: String): Int =
      var bad = 0
      for n <- nodes do
        seed.foreach { (k, _) =>
          val r = scala.util.Try(send(n, "GET", s"/$bucket/$k")).toOption
          if !r.exists(x => x.statusCode() == 200 && sha(x.body()) == seedSums(k)) then
            bad += 1
            if bad <= 10 then log(s"  $label: $k on $n -> ${r.map(_.statusCode())}")
        }
        acked.asScala.keys.foreach { k =>
          val lo = acked.getOrDefault(k, -1)
          val r = scala.util.Try(send(n, "GET", s"/$bucket/$k")).toOption
          if !r.exists(x => x.statusCode() == 200 && matches(k, lo, x.body())) then
            bad += 1
            if bad <= 10 then log(s"  $label: $k on $n -> ${r.map(_.statusCode())}")
        }
      log(s"verify ($label): ${seed.size + acked.size} objects on ${nodes.size} nodes, mismatches=$bad")
      bad

    Thread.sleep(10_000)
    var failures = verifyAll("before upgrade")

    // ---- 1 台ずつ入れ替える ----
    val started = System.nanoTime()
    for n <- nodes do
      phase.set(s"replacing $n")
      down.set(Some(n))
      val t0 = System.nanoTime()
      podman("stop", "-t", "30", n)
      podman("rm", "-f", n)
      startNode(n, newImage)
      awaitServing(n)
      log(f"$n is running $newImage (${(System.nanoTime() - t0) / 1e9}%.0fs to serve again)")
      // 追いつくのを待ってから戻す
      Thread.sleep(15_000)
      down.set(None)
      phase.set(s"after $n")
      Thread.sleep(15_000)
    phase.set("all new")
    Thread.sleep(20_000)
    stop.set(true)
    pool.shutdown()
    pool.awaitTermination(1, TimeUnit.MINUTES)
    log(f"rolling upgrade took ${(System.nanoTime() - started) / 1e9}%.0fs; client ops=${ops.get} keys=${acked.size}")

    failures += verifyAll("after rolling upgrade")

    // ---- 古い版で始めたマルチパートアップロードを、新しい版で完了する ----
    val p2 = send("un3", "PUT", s"/$bucket/$mpKey", part2, List("partNumber" -> "2", "uploadId" -> uploadId))
    val etag2 = p2.headers().firstValue("ETag").orElseThrow()
    val complete =
      s"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>$etag1</ETag></Part><Part><PartNumber>2</PartNumber><ETag>$etag2</ETag></Part></CompleteMultipartUpload>"
    val done = send("un1", "POST", s"/$bucket/$mpKey", complete.getBytes, List("uploadId" -> uploadId))
    val mpOk = done.statusCode() == 200 && nodes.forall { n =>
      val r = send(n, "GET", s"/$bucket/$mpKey")
      r.statusCode() == 200 && java.util.Arrays.equals(r.body(), part1 ++ part2)
    }
    log(s"multipart upload started on the old version and completed on the new one: ${if mpOk then "ok" else s"FAILED (${done.statusCode()} ${String(done.body()).take(300)})"}")
    if !mpOk then failures += 1

    // ---- 全部止めて、新しい版だけで起動し直す ----
    nodes.foreach(n => podman("stop", "-t", "30", n))
    nodes.foreach(n => podman("start", n))
    nodes.foreach(awaitServing(_))
    Thread.sleep(5_000)
    failures += verifyAll("after a full restart on the new version")

    val errs = errors.asScala.toVector.sortBy(_._1).map((p, c) => s"$p=${c.get}")
    log(s"client errors by phase: ${if errs.isEmpty then "none" else errs.mkString(", ")}")
    errorSamples.asScala.take(15).foreach(e => log(s"  $e"))
    lost.asScala.take(10).foreach(e => log(s"  LOST: $e"))
    val ok = failures == 0 && lost.isEmpty
    log(if ok then "PASS" else s"FAIL (mismatches=$failures, unexpected reads=${lost.size})")
    if !sys.env.contains("KEEP") then removeAll()
    if ok then 0 else 1
