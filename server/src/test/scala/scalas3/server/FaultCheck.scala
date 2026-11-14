package scalas3.server

import scalas3.s3.SigV4
import scalas3.sim.Linearizability
import scalas3.sim.Linearizability.{Event, Op}

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.security.MessageDigest
import java.time.format.DateTimeFormatter
import java.time.{Duration, ZoneOffset, ZonedDateTime}
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ConcurrentLinkedQueue, Executors, ThreadLocalRandom, TimeUnit}
import scala.jdk.CollectionConverters.*
import scala.sys.process.*

/** 実際のクラスタで障害を起こしながら、読み書きの履歴が線形化可能かを調べる（Jepsen と同じ考え方）。
  *
  * Podman のコンテナで 3 ノードを動かし、ネットワークからの切り離し・強制終了（SIGKILL して起動し直す）・一時停止
  * （プロセスを凍結する）を繰り返す。その間、ホストのクライアントがランダムなノードに S3 で読み・書き・CAS
  * （If-Match / If-None-Match の条件付き PUT）を送り、キーごとの履歴を Linearizability.check で確かめる。
  * 一度に障害を起こすのは 1 台だけ（過半数は残る）。
  * {{{
  * podman build -t localhost/scalas3:jepsen .
  * sbt "server/Test/runMain scalas3.server.FaultCheck 5"     # 引数：分数
  * }}}
  */
object FaultCheck:
  private val image = "localhost/scalas3:jepsen"
  private val network = "s3fault"
  private val nodes = Vector("fn1", "fn2", "fn3")
  private val ports = Map("fn1" -> 4911, "fn2" -> 4921, "fn3" -> 4931)
  private val ak = "faultkey"
  private val sk = "faultsecretfaultsecret"
  // 検査器は 1 キーあたり 62 操作までなので、キーを 3 秒ごとに入れ替える（世代 × 80 キー）
  private val keysPerEpoch = 80
  private val epochNanos = 3_000_000_000L
  private val fmt = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'")

  private def podman(args: String*): Int = Process("podman" +: args).!(ProcessLogger(_ => (), _ => ()))

  private def startCluster(): Unit =
    nodes.foreach(n => podman("rm", "-f", n))
    podman("network", "rm", "-f", network)
    podman("network", "create", network)
    val peers = nodes.map(n => s"$n=$n:3901").mkString(",")
    nodes.foreach { n =>
      podman(
        "run", "-d", "--name", n, "--hostname", n, "--network", network, "-p", s"${ports(n)}:3900",
        "-e", s"SCALAS3_NODE_ID=$n", "-e", s"SCALAS3_PEERS=$peers", "-e", s"SCALAS3_RPC_ADVERTISE=$n:3901",
        "-e", s"SCALAS3_ACCESS_KEY=$ak", "-e", s"SCALAS3_SECRET_KEY=$sk", "-e", s"SCALAS3_ZONE=z-$n",
        "-e", "JAVA_TOOL_OPTIONS=-XX:+UseG1GC -Xmx200m -XX:MaxDirectMemorySize=64m",
        image
      )
    }

  private def stopCluster(): Unit =
    nodes.foreach(n => podman("rm", "-f", n))
    podman("network", "rm", "-f", network): Unit

  private def daemonPool(n: Int) = Executors.newFixedThreadPool(n, r => { val t = Thread(r); t.setDaemon(true); t })

  def main(args: Array[String]): Unit =
    val code =
      try run(args.headOption.fold(3)(_.toInt))
      catch
        case e: Throwable =>
          e.printStackTrace()
          stopCluster()
          2
    sys.exit(code)

  private def run(minutes: Int): Int =
    startCluster()
    val http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).connectTimeout(Duration.ofSeconds(2)).executor(daemonPool(32)).build()

    def send(node: String, method: String, path: String, body: Array[Byte], headers: Map[String, String] = Map.empty): HttpResponse[Array[Byte]] =
      val endpoint = s"http://127.0.0.1:${ports(node)}"
      val amzDate = ZonedDateTime.now(ZoneOffset.UTC).format(fmt)
      val scope = SigV4.Scope(amzDate.take(8), "us-east-1", "s3")
      val hs = Map("host" -> s"127.0.0.1:${ports(node)}", "x-amz-date" -> amzDate, "x-amz-content-sha256" -> SigV4.UnsignedPayload)
      val names = hs.keys.toList.sorted
      val canonical = SigV4.canonicalRequest(method, path, Nil, n => hs.get(n).toList, names, SigV4.UnsignedPayload)
      val sig = SigV4.sign(SigV4.signingKey(sk, scope), SigV4.stringToSign(amzDate, scope, canonical))
      val b = HttpRequest.newBuilder(URI.create(endpoint + path)).timeout(Duration.ofSeconds(5))
        .method(method, if body.isEmpty then HttpRequest.BodyPublishers.noBody() else HttpRequest.BodyPublishers.ofByteArray(body))
        .header("x-amz-date", amzDate).header("x-amz-content-sha256", SigV4.UnsignedPayload)
        .header("Authorization", s"AWS4-HMAC-SHA256 Credential=$ak/${scope.render}, SignedHeaders=${names.mkString(";")}, Signature=$sig")
      headers.foreach((k, v) => b.header(k, v))
      http.send(b.build(), HttpResponse.BodyHandlers.ofByteArray())

    // クラスタが書けるようになるまで待つ（バケットを作る）
    val readyBy = System.nanoTime() + 180_000_000_000L
    var ready = false
    while !ready && System.nanoTime() < readyBy do
      ready = scala.util.Try(send("fn1", "PUT", "/fault", Array.emptyByteArray).statusCode()).toOption.exists(c => c == 200 || c == 409)
      if !ready then Thread.sleep(1000)
    if !ready then throw IllegalStateException("cluster did not become ready")
    println("[fault] cluster ready")

    def md5(s: String) = MessageDigest.getInstance("MD5").digest(s.getBytes).map(b => f"${b & 0xff}%02x").mkString
    def now = System.nanoTime() / 1000
    val history = ConcurrentLinkedQueue[(String, Event)]()
    val ids = AtomicInteger()
    val faults = ConcurrentLinkedQueue[String]()
    val started = System.nanoTime()
    val deadline = started + minutes * 60_000_000_000L

    /** ノードが S3 に答えるようになるまで待つ（次の障害を起こす前に。待たないと、起動中のノードと新たに止めたノードで
      * 2 台が欠け、過半数を失う）。
      */
    def awaitServing(n: String): Unit =
      val by = System.nanoTime() + 90_000_000_000L
      while System.nanoTime() < by && !scala.util.Try(send(n, "HEAD", "/fault", Array.emptyByteArray).statusCode() == 200).getOrElse(false) do
        Thread.sleep(500)

    // ---- 障害（一度に 1 台）----
    val nemesis = daemonPool(1)
    nemesis.submit(new Runnable:
      def run(): Unit =
        val rnd = ThreadLocalRandom.current()
        while System.nanoTime() < deadline - 20_000_000_000L do
          Thread.sleep(5000 + rnd.nextInt(5000))
          val n = nodes(rnd.nextInt(nodes.size))
          // FAULT_KINDS=isolate,kill,pause で種類を絞れる
          val kinds = sys.env.get("FAULT_KINDS").fold(Vector(0, 1, 2))(_.split(',').toVector.map { case "isolate" => 0; case "kill" => 1; case _ => 2 })
          kinds(rnd.nextInt(kinds.size)) match
            case 0 =>
              faults.add(f"${(System.nanoTime() - started) / 1e9}%5.0fs isolate $n"); podman("network", "disconnect", network, n)
              Thread.sleep(8000 + rnd.nextInt(6000)); podman("network", "connect", network, n); awaitServing(n)
            case 1 =>
              faults.add(f"${(System.nanoTime() - started) / 1e9}%5.0fs kill $n"); podman("kill", "-s", "KILL", n)
              Thread.sleep(3000 + rnd.nextInt(4000)); podman("start", n); awaitServing(n)
            case _ =>
              faults.add(f"${(System.nanoTime() - started) / 1e9}%5.0fs pause $n"); podman("pause", n)
              Thread.sleep(6000 + rnd.nextInt(6000)); podman("unpause", n); awaitServing(n)
    )

    // ---- クライアント ----
    val clients = 9
    val pool = daemonPool(clients)
    (0 until clients).foreach { c =>
      pool.submit(new Runnable:
        def run(): Unit =
          val rnd = ThreadLocalRandom.current()
          var seq = 0
          while System.nanoTime() < deadline do
            seq += 1
            val key = s"e${(System.nanoTime() - started) / epochNanos}-k${rnd.nextInt(keysPerEpoch)}"
            val node = nodes(rnd.nextInt(nodes.size))
            val value = c * 1_000_000L + seq
            val id = ids.incrementAndGet()
            val t0 = now
            val kind = rnd.nextInt(10)
            def done(op: Op) = history.add(key -> Event(id, t0, now, completed = true, op))
            // 結果が分からない（タイムアウトや 5xx）：効果があったかもしれない
            def unknown(op: Op) = history.add(key -> Event(id, t0, 0, completed = false, op))
            try
              if kind < 4 then
                val r = send(node, "GET", s"/fault/$key", Array.emptyByteArray)
                r.statusCode() match
                  case 200 => done(Op.Read(Some(String(r.body()).toLong)))
                  case 404 => done(Op.Read(None))
                  case _   => unknown(Op.Read(None))
              else if kind < 7 then
                val r = send(node, "PUT", s"/fault/$key", value.toString.getBytes)
                if r.statusCode() == 200 then done(Op.Write(value)) else unknown(Op.Write(value))
              else
                // 直前に読んだ値を期待値にした CAS
                val g = send(node, "GET", s"/fault/$key", Array.emptyByteArray)
                val cur = g.statusCode() match
                  case 200 => Some(Some(String(g.body()).toLong))
                  case 404 => Some(None)
                  case _   => None
                cur match
                  case None => () // 読めなかったので何もしない
                  case Some(expected) =>
                    val cond = expected.fold(Map("If-None-Match" -> "*"))(v => Map("If-Match" -> s"\"${md5(v.toString)}\""))
                    val t1 = now
                    val r = send(node, "PUT", s"/fault/$key", value.toString.getBytes, cond)
                    r.statusCode() match
                      case 200 => history.add(key -> Event(id, t1, now, completed = true, Op.Cas(expected, value, succeeded = true)))
                      case 412 => history.add(key -> Event(id, t1, now, completed = true, Op.Cas(expected, value, succeeded = false)))
                      case _   => history.add(key -> Event(id, t1, 0, completed = false, Op.Write(value)))
            catch case _: Exception => if kind >= 4 && kind < 7 then unknown(Op.Write(value)) else if kind < 4 then unknown(Op.Read(None)) else ()
            Thread.sleep(20)
      )
    }
    pool.shutdown()
    pool.awaitTermination(minutes + 2L, TimeUnit.MINUTES)
    nemesis.shutdown()
    nemesis.awaitTermination(1, TimeUnit.MINUTES)
    // すべて戻してから終える
    nodes.foreach { n => podman("unpause", n); podman("network", "connect", network, n); podman("start", n) }

    val events = history.asScala.toVector
    val perKey = events.groupMap(_._1)(_._2)
    val results = perKey.toVector.sortBy(_._1).map((k, h) => (k, h.size, h.count(!_.completed), Linearizability.check(h).ok))
    results.filterNot(_._4).foreach((k, n, unk, _) => println(f"[fault] NOT linearizable: key=$k ops=$n indeterminate=$unk"))
    println(s"[fault] keys=${results.size} ops=${results.map(_._2).sum} indeterminate=${results.map(_._3).sum} maxOpsPerKey=${results.map(_._2).maxOption.getOrElse(0)}")
    val done = events.map(_._2).filter(_.completed).map(_.op)
    println(s"[fault] completed: reads=${done.count(_.isInstanceOf[Op.Read])} writes=${done.count(_.isInstanceOf[Op.Write])} " +
      s"cas-ok=${done.count { case Op.Cas(_, _, true) => true; case _ => false }} cas-failed=${done.count { case Op.Cas(_, _, false) => true; case _ => false }}")
    val fs = faults.asScala.toVector
    println(s"[fault] faults: ${fs.size} (${fs.groupBy(_.trim.split(' ')(1)).view.mapValues(_.size).toMap})")
    // 30 秒ごとの成功と不確定（可用性の推移）
    val startMicros = started / 1000
    events.map(_._2).groupBy(e => (e.invoke - startMicros) / 30_000_000L).toVector.sortBy(_._1).foreach { (w, es) =>
      val f = fs.filter(x => x.trim.takeWhile(_ != 's').trim.toDouble.toLong / 30 == w).map(_.trim.dropWhile(_ != ' ').trim)
      println(f"[fault] t=${w * 30}%4ds ok=${es.count(_.completed)}%4d indeterminate=${es.count(!_.completed)}%4d  ${f.mkString(", ")}")
    }
    val ok = results.forall(_._4)
    println(if ok then "[fault] PASS" else "[fault] FAIL")
    // KEEP=1 なら、調べられるようにクラスタを残す
    if !sys.env.contains("KEEP") then stopCluster()
    if ok then 0 else 1
