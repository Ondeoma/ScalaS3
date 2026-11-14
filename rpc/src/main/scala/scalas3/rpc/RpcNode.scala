package scalas3.rpc

import cats.effect.{Deferred, IO, Ref, Resource}
import cats.syntax.all.*
import fs2.Stream
import scalas3.runtime.{NodeAddress, Transport}

import scala.concurrent.duration.*

/** RPC の待ち受け。受けた接続ごとに RpcPeer を動かす。 */
object RpcServer:
  def serve(transport: Transport, at: NodeAddress, handler: RpcHandler): Resource[IO, NodeAddress] =
    transport.listen(at).flatMap { (bound, conns) =>
      val loop = conns
        .map { c =>
          Stream
            .resource(RpcPeer.run(c, handler, initiator = false))
            .evalMap(_.closed)
            .drain
            .handleErrorWith(_ => Stream.empty)
        }
        .parJoinUnbounded
        .compile
        .drain
      loop.background.as(bound)
    }

/** 宛先ごとに接続を 1 本ずつ保ち、切れたら次の呼び出しで張り直す。
  *
  * 呼び出しには待ち時間の上限を設け、超えたらその接続を捨てる（次の呼び出しで張り直す）。相手のマシンが落ちたり、
  * コンテナがネットワークごと消えたりすると、切断（RST）が届かず、古い接続に送った要求は返事を待ち続ける。
  * 捨てないと、相手が戻った後もその接続を使い続け、Raft の送信やブロックの書き込みが止まったままになった
  * （FaultCheck で見つけた）。接続の確立にも上限を設ける（死んだ相手への接続は OS の既定で 1〜2 分待つ）。
  */
final class RpcClient private (
    transport: Transport,
    handler: RpcHandler,
    peers: Ref[IO, Option[Map[NodeAddress, RpcClient.Slot]]],
    callTimeout: FiniteDuration,
    connectTimeout: FiniteDuration
):

  def call[Req, Resp](to: NodeAddress, m: Method[Req, Resp], req: Req): IO[Resp] = callWithin(to, m, req, callTimeout)

  /** 待ち時間を指定して呼ぶ（死活確認など、短く待ちたいとき）。超えたら接続を捨てる。 */
  def callWithin[Req, Resp](to: NodeAddress, m: Method[Req, Resp], req: Req, timeout: FiniteDuration): IO[Resp] =
    peer(to).flatMap { p =>
      p.call(m, req).timeout(timeout).onError { case _: java.util.concurrent.TimeoutException => evict(to, p) }
    }

  /** 宛先への RpcPeer を返す（必要なら接続する）。 */
  def peer(to: NodeAddress): IO[RpcPeer] =
    IO.uncancelable { poll =>
      Deferred[IO, RpcClient.Connected].flatMap { fresh =>
        peers.modify {
          case None => (None, IO.raiseError(ConnectionClosedException("rpc client is closed")))
          case Some(m) =>
            m.get(to) match
              case Some(d) => (Some(m), poll(d.get).flatMap(IO.fromEither).map(_._1))
              case None    => (Some(m.updated(to, fresh)), establish(to, fresh, poll))
        }.flatten
      }
    }.flatMap(p => p.isClosed.ifM(evict(to, p) >> peer(to), IO.pure(p)))

  private def establish(
      to: NodeAddress,
      d: RpcClient.Slot,
      poll: cats.effect.Poll[IO]
  ): IO[RpcPeer] =
    val res = transport.connect(to).flatMap(c => RpcPeer.run(c, handler, initiator = true))
    // 接続できなかった（要求はまだ送っていない）ことが分かるよう、接続の時間切れは ConnectException にする
    poll(res.allocated.timeout(connectTimeout)).adaptError { case _: java.util.concurrent.TimeoutException =>
      java.net.ConnectException(s"connect to $to timed out after $connectTimeout")
    }.attempt.flatMap {
      case Right(pr) =>
        d.complete(Right(pr)).flatMap {
          // 接続が切れたら、マップから外して資源を解放する
          case true => (pr._1.closed >> evict(to, pr._1)).start.as(pr._1)
          // 接続している間に closeAll された（スロットは閉じたことで完了済み）。誰も解放しないので、ここで解放する
          case false => pr._2.attempt >> IO.raiseError(ConnectionClosedException("rpc client is closed"))
        }
      case Left(e) =>
        d.complete(Left(e)) >> peers.update(_.map(_ - to)) >> IO.raiseError(e)
    }

  private def evict(to: NodeAddress, p: RpcPeer): IO[Unit] =
    peers.get.flatMap {
      case None => IO.unit
      case Some(m) =>
        m.get(to).traverse_(d =>
          d.tryGet.flatMap {
            // マップから外せたときだけ解放する（evict が同時に走っても、解放は 1 回だけになる）
            case Some(Right((cur, release))) if cur eq p =>
              peers
                .modify {
                  case Some(mm) if mm.get(to).contains(d) => (Some(mm - to), true)
                  case other                              => (other, false)
                }
                .ifM(release.attempt.void, IO.unit)
            case _ => IO.unit
          }
        )
    }

  private def closeAll: IO[Unit] =
    peers.getAndSet(None).flatMap(_.fold(IO.unit)(_.values.toList.traverse_ { d =>
      // 接続中のスロットは閉じたことで完了させる（接続し終えた側が、自分で解放する）
      d.complete(Left(ConnectionClosedException("rpc client is closed"))).flatMap {
        case true  => IO.unit
        case false =>
          d.get.flatMap {
            case Right((_, release)) => release.attempt.void
            case Left(_)             => IO.unit
          }
      }
    }))

object RpcClient:

  /** 宛先ごとの接続。接続し終えたら、RpcPeer と、その資源を解放する処理で完了する。 */
  private[rpc] type Connected = Either[Throwable, (RpcPeer, IO[Unit])]
  private[rpc] type Slot = Deferred[IO, Connected]

  def apply(
      transport: Transport,
      handler: RpcHandler = RpcHandler.empty,
      callTimeout: FiniteDuration = 15.seconds,
      connectTimeout: FiniteDuration = 5.seconds
  ): Resource[IO, RpcClient] =
    Resource.make(
      Ref.of[IO, Option[Map[NodeAddress, RpcClient.Slot]]](Some(Map.empty))
        .map(new RpcClient(transport, handler, _, callTimeout, connectTimeout))
    )(_.closeAll)
