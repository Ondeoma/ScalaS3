package scalas3.cluster

import cats.effect.IO
import cats.syntax.all.*
import scalas3.raft.NodeId
import scalas3.rpc.{Method, RpcClient}

import scala.concurrent.duration.FiniteDuration

/** ClusterBlocks と ClusterShards に共通の、ほかのノードへの呼び出し、過半数の書き込み、読み込みの順番。 */
private[cluster] final class Peers(self: NodeId, rpc: RpcClient, timeout: FiniteDuration, isDown: NodeId => Boolean):

  /** ノード n に RPC を送る。 */
  def call[A, B](l: Layout, n: NodeId, m: Method[A, B], a: A): IO[B] =
    IO.fromOption(l.nodes.get(n).map(_.address))(IllegalStateException(s"unknown node $n")).flatMap(ad => rpc.call(ad, m, a)).timeout(timeout)

  /** 自分なら localOp、ほかのノードなら RPC。 */
  def on[A, B](l: Layout, n: NodeId, m: Method[A, B], a: A)(localOp: => IO[B]): IO[B] =
    if n == self then localOp else call(l, n, m, a)

  /** ノード n への書き込み 1 つ。Down のノードへは書かない（応答のないノードを待つと、過半数に届いていても RPC の
    * タイムアウトまで止まる。書けなかった分は後で修復が補う）。
    */
  def write(n: NodeId)(io: IO[Unit]): IO[Either[Throwable, Unit]] =
    if isDown(n) then IO.pure(Left(NotEnoughReplicasException(s"node $n is down"))) else io.attempt

  /** 並行に書き、need 個に届かなければ NotEnoughReplicasException（failure は成功した数からメッセージを作る）。 */
  def quorum(writes: List[IO[Either[Throwable, Unit]]], need: Int)(failure: Int => String): IO[Unit] =
    Quorum.write(writes, need).flatMap(ok => IO.raiseWhen(ok < need)(NotEnoughReplicasException(failure(ok))))

  /** 読み込みの順番：preferred → 他の全ノード（ID 順）。Down と分かっているノードは最後に回す。 */
  def readOrder(l: Layout, preferred: Vector[NodeId]): Vector[NodeId] =
    val all = (preferred ++ l.nodes.keys.toVector.sorted).distinct
    all.filterNot(isDown) ++ all.filter(isDown)

  /** ノードごとに並行に「持っているか」を聞き、あると答えたものを返す。答えなかったノードは「持っていない」とみなす。 */
  def found[R](byNode: Map[NodeId, Vector[R]])(has: (NodeId, Vector[R]) => IO[Vector[Boolean]]): IO[Vector[R]] =
    byNode.toList
      .parTraverse((n, xs) => has(n, xs).attempt.map(_.fold(_ => Vector.empty, r => xs.zip(r).collect { case (x, true) => x })))
      .map(_.flatten.toVector)
