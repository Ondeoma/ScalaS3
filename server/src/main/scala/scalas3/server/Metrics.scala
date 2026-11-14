package scalas3.server

import cats.data.Kleisli
import cats.effect.IO
import org.http4s.{HttpApp, Request}

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.{AtomicLong, DoubleAdder}
import scala.jdk.CollectionConverters.*

/** Prometheus のテキスト形式で出すための、最小限のメトリクス（カウンタとヒストグラム）。 */
final class Metrics:
  private val counters = ConcurrentHashMap[(String, Map[String, String]), AtomicLong]()
  private val buckets = Vector(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0)
  private final class Histogram:
    val counts: Vector[AtomicLong] = Vector.fill(buckets.size)(AtomicLong())
    val count = AtomicLong()
    val sum = DoubleAdder()
  private val histograms = ConcurrentHashMap[(String, Map[String, String]), Histogram]()
  private val gauges = ConcurrentHashMap[String, () => Seq[(Map[String, String], Double)]]()

  def inc(name: String, labels: Map[String, String] = Map.empty, by: Long = 1): Unit =
    counters.computeIfAbsent((name, labels), _ => AtomicLong()).addAndGet(by): Unit

  def observe(name: String, labels: Map[String, String], seconds: Double): Unit =
    val h = histograms.computeIfAbsent((name, labels), _ => Histogram())
    buckets.indices.foreach(i => if seconds <= buckets(i) then h.counts(i).incrementAndGet(): Unit)
    h.count.incrementAndGet()
    h.sum.add(seconds)

  /** 出力のたびに値を計算するゲージ。 */
  def gauge(name: String)(f: () => Seq[(Map[String, String], Double)]): Unit = gauges.put(name, f): Unit

  private def fmt(labels: Map[String, String]): String =
    if labels.isEmpty then "" else labels.toList.sorted.map((k, v) => s"""$k="${v.replace("\"", "\\\"")}"""").mkString("{", ",", "}")

  def render: String =
    val sb = new StringBuilder
    counters.asScala.toList.groupBy(_._1._1).toList.sortBy(_._1).foreach { (name, entries) =>
      sb.append(s"# TYPE $name counter\n")
      entries.sortBy(_._1._2.toString).foreach { case ((_, l), v) => sb.append(s"$name${fmt(l)} ${v.get}\n") }
    }
    histograms.asScala.toList.groupBy(_._1._1).toList.sortBy(_._1).foreach { (name, entries) =>
      sb.append(s"# TYPE $name histogram\n")
      entries.foreach { case ((_, l), h) =>
        buckets.indices.foreach(i => sb.append(s"${name}_bucket${fmt(l + ("le" -> buckets(i).toString))} ${h.counts(i).get}\n"))
        sb.append(s"${name}_bucket${fmt(l + ("le" -> "+Inf"))} ${h.count.get}\n")
        sb.append(s"${name}_sum${fmt(l)} ${h.sum.sum}\n")
        sb.append(s"${name}_count${fmt(l)} ${h.count.get}\n")
      }
    }
    gauges.asScala.toList.sortBy(_._1).foreach { (name, f) =>
      sb.append(s"# TYPE $name gauge\n")
      scala.util.Try(f()).getOrElse(Nil).foreach((l, v) => sb.append(s"$name${fmt(l)} $v\n"))
    }
    sb.toString

  /** S3 の HTTP 応答を数える。 */
  def instrument(app: HttpApp[IO]): HttpApp[IO] = Kleisli { (req: Request[IO]) =>
    IO.monotonic.flatMap { start =>
      app.run(req).flatTap { resp =>
        IO.monotonic.map { end =>
          val method = req.method.name
          inc("scalas3_s3_requests_total", Map("method" -> method, "status" -> resp.status.code.toString))
          observe("scalas3_s3_request_duration_seconds", Map("method" -> method), (end - start).toNanos / 1e9)
          resp.contentLength.foreach(n => inc("scalas3_s3_response_bytes_total", Map("method" -> method), n))
          req.contentLength.foreach(n => inc("scalas3_s3_request_bytes_total", Map("method" -> method), n))
        }
      }
    }
  }
