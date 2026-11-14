package scalas3.cluster

import cats.effect.{IO, Resource}
import cats.syntax.all.*
import scalas3.raft.NodeId
import scalas3.rpc.{Method, RemoteException, RpcClient, RpcHandler}
import scodec.codecs.{int32, provide}

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.*

final case class HealthConfig(
    /** 死活確認を送る間隔。 */
    probeInterval: FiniteDuration = 2.seconds,
    /** 1 回の死活確認の待ち時間。 */
    probeTimeout: FiniteDuration = 2.seconds,
    /** これだけ応答がなければ Down とみなす。 */
    downAfter: FiniteDuration = 30.seconds
)

/** 1 つのノードの、このノードから見た状態。lastSeen は最後に応答があった時刻（エポックミリ秒）。features は相手が
  * 対応している機能の水準（Health.Features）。-1 はまだ分からない。
  */
final case class NodeHealth(up: Boolean, lastSeen: Long, features: Int = -1)

/** ほかのノードの死活を見張る（設計 C10）。
  *
  * レイアウトの全ノードに定期的に死活確認を送り、downAfter のあいだ応答がなければ Down とする。Down は表示と警告
  * （ログ、Admin API、メトリクス）と、ブロックの読み書きで後回しにすることだけに使い、データは動かさない。一時的な停止
  * （再起動やネットワークの瞬断）で誤ってデータが大量に動くのを避けるため。取り外すときは管理者がレイアウトで Draining にする。
  *
  * 判断はノードごと（このノードから見て届くか）で、クラスタで合意はしない。
  *
  * 死活確認の応答には、相手が対応している機能の水準を載せる。ローリングアップグレードの途中で、古い版が読めない
  * Raft のコマンドを新しい版が出さないようにするため（`supports`）。古い版は health.ping2 を知らないので、
  * UnknownMethod が返れば水準 0 とみなす。
  */
final class Health private (self: NodeId, state: AtomicReference[Map[NodeId, NodeHealth]]):

  /** いまの状態（自分は含まない）。 */
  def snapshot: Map[NodeId, NodeHealth] = state.get

  /** Down と分かっているか（知らないノードは Down ではない）。読み書きの順番を決めるときに使う。 */
  def isDown(n: NodeId): Boolean = n != self && state.get.get(n).exists(!_.up)

  /** 自分以外のすべてのノードが、水準 level 以上の機能に対応していると分かっているか（分からないノードがあれば false）。 */
  def supports(nodes: Set[NodeId], level: Int): Boolean =
    val known = state.get
    nodes.forall(n => n == self || known.get(n).exists(_.features >= level))

object Health:
  /** この版が対応している機能の水準。古い版が読めないコマンドを足したら上げ、使う側で supports を確かめる。
    *   - 0：health.ping2 より前の版
    *   - 1：レンジの統合（Raft の Freeze と Merge、ReadRequest の MergeState と Export）
    */
  val Features: Int = 1
  val MergeFeature: Int = 1

  val Ping: Method[Unit, Unit] = Method(330, "health.ping", provide(()), provide(()))
  val PingV2: Method[Unit, Int] = Method(331, "health.ping2", provide(()), int32)

  def handler: RpcHandler = RpcHandler.of(Ping.handledBy(_ => IO.unit), PingV2.handledBy(_ => IO.pure(Features)))

  /** 死活確認。応答があれば相手の機能の水準を返す。 */
  private def probe(rpc: RpcClient, address: scalas3.runtime.NodeAddress, timeout: FiniteDuration): IO[Int] =
    rpc.callWithin(address, PingV2, (), timeout).recoverWith {
      case RemoteException(RemoteException.UnknownMethod, _) => rpc.callWithin(address, Ping, (), timeout).as(0)
    }

  /** 見張りを始める。layout は全ノードとそのアドレス。 */
  def start(self: NodeId, rpc: RpcClient, layout: IO[Layout], config: HealthConfig = HealthConfig()): Resource[IO, Health] =
    for
      state <- Resource.eval(IO(AtomicReference(Map.empty[NodeId, NodeHealth])))
      health = new Health(self, state)
      _ <- probeLoop(self, rpc, layout, config, state).background
    yield health

  private def probeLoop(self: NodeId, rpc: RpcClient, layout: IO[Layout], config: HealthConfig, state: AtomicReference[Map[NodeId, NodeHealth]]): IO[Nothing] =
    val once =
      for
        l <- layout
        now <- IO.realTime.map(_.toMillis)
        peers = l.nodes.toList.filter(_._1 != self).sortBy(_._1)
        results <- peers.parTraverse((n, info) => probe(rpc, info.address, config.probeTimeout).attempt.map(r => n -> r.toOption))
        _ <- IO {
          val before = state.get
          val next = results.map { (n, answer) =>
            // 初めて見たノードは、いま見えたことにする（起動直後に Down と言わない）
            val seen = if answer.isDefined then now else before.get(n).fold(now)(_.lastSeen)
            // 応答がなければ、前に分かった水準を使い続ける（入れ替えたノードは、次の応答で分かり直す）
            val features = answer.getOrElse(before.get(n).fold(-1)(_.features))
            n -> NodeHealth(up = now - seen < config.downAfter.toMillis, lastSeen = seen, features = features)
          }.toMap
          state.set(next)
          next.foreach { (n, h) =>
            val was = before.get(n).forall(_.up)
            if was && !h.up then System.err.println(s"[scalas3] WARNING: node $n is down (no response for ${(now - h.lastSeen) / 1000}s)")
            else if !was && h.up then System.err.println(s"[scalas3] node $n is up again")
          }
        }
      yield ()
    (once.attempt >> IO.sleep(config.probeInterval)).foreverM
