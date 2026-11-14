package scalas3.sim

import cats.effect.std.Queue
import cats.effect.{Deferred, IO, Ref, Resource}
import cats.syntax.all.*
import fs2.{Chunk, Stream}
import scalas3.runtime.{Connection, NodeAddress, Transport}

import scala.concurrent.duration.*

/** メモリ上のネットワーク。TestControl の下で動かすと、遅延も含めて決定的になる。
  *
  * TCP と同じく、1 本の接続の中ではバイト列の順序が保たれ、欠落しない。
  * 障害は接続単位で起きる（分断すると、既存の接続は切れ、新しい接続は確立できない）。
  */
final class SimNetwork private (ref: Ref[IO, SimNetwork.State], config: SimNetwork.Config, tracer: Option[Ref[IO, Vector[String]]]):
  import SimNetwork.*

  /** host として振る舞うノード用の Transport。 */
  def transport(host: String): Transport = new Transport:

    def connect(to: NodeAddress): Resource[IO, Connection] =
      val open =
        for
          lat <- latency
          _ <- IO.sleep(lat)
          s <- ref.get
          _ <- IO.raiseWhen(s.blocked(host, to.host))(java.net.ConnectException(s"partitioned: $host -> $to"))
          listener <- IO.fromOption(s.listeners.get(to))(java.net.ConnectException(s"connection refused: $to"))
          id <- ref.modify(s => (s.copy(nextId = s.nextId + 1), s.nextId))
          broken <- Deferred[IO, Throwable]
          toServer <- Pipe.create
          toClient <- Pipe.create
          localAddr = NodeAddress(host, 40000 + (id % 20000).toInt)
          client = SimConnection(to, toClient, toServer, broken, latency, trace(s"$host->$to#$id"))
          server = SimConnection(localAddr, toServer, toClient, broken, latency, trace(s"$to->$host#$id"))
          _ <- trace(s"connect $host->$to#$id")("")
          _ <- ref.update(s => s.copy(conns = s.conns.updated(id, LiveConn(host, to.host, broken))))
          _ <- listener.offer(server)
        yield (id, client)
      Resource.make(open)((id, c) => c.close >> ref.update(s => s.copy(conns = s.conns - id))).map(_._2)

    def listen(at: NodeAddress): Resource[IO, (NodeAddress, Stream[IO, Connection])] =
      val addr = NodeAddress(host, at.port)
      Resource
        .make(
          Queue.unbounded[IO, SimConnection].flatTap(q =>
            ref.modify { s =>
              if s.listeners.contains(addr) then (s, IO.raiseError(java.net.BindException(s"address in use: $addr")))
              else (s.copy(listeners = s.listeners.updated(addr, q)), IO.unit)
            }.flatten
          )
        )(_ => ref.update(s => s.copy(listeners = s.listeners - addr)))
        .map { q =>
          val conns = Stream
            .fromQueueUnterminated(q)
            .flatMap(c => Stream.resource(Resource.make(IO.pure(c))(_.close)))
          (addr, conns)
        }

  /** 決定性の検査用：送受信を時刻とともに記録する。記録しないときは、メッセージを組み立てない（書き込みのたびに
    * CRC と 16 進表記を計算しないように）。
    */
  private def trace(label: String): Tracer =
    tracer match
      case None => _ => IO.unit
      case Some(t) => msg => IO.monotonic.flatMap(now => t.update(_ :+ s"${now.toMicros} $label $msg"))

  def traceLog: IO[Vector[String]] = tracer.fold(IO.pure(Vector.empty[String]))(_.get)

  private def latency: IO[FiniteDuration] =
    ref.modify { s =>
      val (r, jitter) = s.rng.nextLong(config.jitter.toNanos + 1)
      (s.copy(rng = r), config.baseLatency + jitter.nanos)
    }

  // ---- 障害注入 ----

  /** a と b の間の通信を（双方向に）遮断し、既存の接続を切る。 */
  def partition(a: String, b: String): IO[Unit] =
    ref.modify { s =>
      val s1 = s.copy(partitions = s.partitions + (a -> b) + (b -> a))
      val victims = s.conns.values.filter(c => s1.blocked(c.from, c.to)).toList
      (s1, victims.traverse_(_.broken.complete(PartitionedException()).void))
    }.flatten

  /** host を他の全ノードから切り離す。 */
  def isolate(host: String, others: Iterable[String]): IO[Unit] =
    others.toList.filterNot(_ == host).traverse_(partition(host, _))

  def heal(a: String, b: String): IO[Unit] =
    ref.update(s => s.copy(partitions = s.partitions - (a -> b) - (b -> a)))

  def healAll: IO[Unit] = ref.update(_.copy(partitions = Set.empty))

object SimNetwork:

  final case class Config(baseLatency: FiniteDuration = 1.millis, jitter: FiniteDuration = 1.millis)

  /** 記録用の関数。メッセージは記録するときだけ評価する。 */
  private type Tracer = (=> String) => IO[Unit]

  final class PartitionedException extends java.io.IOException("connection broken by network partition")

  private final case class LiveConn(from: String, to: String, broken: Deferred[IO, Throwable])

  private final case class State(
      listeners: Map[NodeAddress, Queue[IO, SimConnection]],
      conns: Map[Long, LiveConn],
      partitions: Set[(String, String)],
      nextId: Long,
      rng: SimRandom
  ):
    def blocked(a: String, b: String): Boolean = partitions.contains(a -> b)

  /** 一方向のバイト列の通り道。到着時刻付きで FIFO に並ぶ。 */
  private final case class Pipe(q: Queue[IO, Option[(FiniteDuration, Chunk[Byte])]], last: Ref[IO, FiniteDuration])
  private object Pipe:
    def create: IO[Pipe] = (Queue.unbounded[IO, Option[(FiniteDuration, Chunk[Byte])]], Ref.of[IO, FiniteDuration](Duration.Zero)).mapN(Pipe(_, _))

  private final class SimConnection(
      val remote: NodeAddress,
      in: Pipe,
      out: Pipe,
      broken: Deferred[IO, Throwable],
      latency: IO[FiniteDuration],
      trace: Tracer
  ) extends Connection:

    private def deliveryTime: IO[FiniteDuration] =
      for
        now <- IO.monotonic
        lat <- latency
        t <- out.last.modify { last =>
          val at = (now + lat).max(last)
          (at, at)
        }
      yield t

    def reads: Stream[IO, Byte] =
      Stream
        .fromQueueNoneTerminated(in.q)
        .evalMap { (at, chunk) =>
          IO.monotonic.flatMap(now => IO.sleep(at - now).whenA(at > now)).as(chunk)
        }
        .unchunks
        .interruptWhen(broken.get.map(Left(_)))

    def write(bytes: Chunk[Byte]): IO[Unit] =
      broken.tryGet.flatMap {
        case Some(e) => IO.raiseError(e)
        case None =>
          trace(s"${bytes.size} ${scalas3.core.Crc32c(bytes.toArray)} ${bytes.toByteVector.take(48).toHex}") >> deliveryTime.flatMap(at => out.q.offer(Some((at, bytes))))
      }

    /** 送信側を閉じる（相手の reads は、送信済みのデータを読み終えると終了する）。 */
    def close: IO[Unit] = deliveryTime.flatMap(at => out.q.offer(Some((at, Chunk.empty)))) >> out.q.offer(None)

  def create(seed: Long, config: Config = Config(), trace: Boolean = false): IO[SimNetwork] =
    (Ref.of[IO, State](State(Map.empty, Map.empty, Set.empty, 1L, SimRandom(seed))), Ref.of[IO, Vector[String]](Vector.empty))
      .mapN((r, t) => SimNetwork(r, config, Option.when(trace)(t)))
