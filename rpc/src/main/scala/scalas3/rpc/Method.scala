package scalas3.rpc

import cats.effect.IO
import scodec.Codec
import scodec.bits.ByteVector

/** 送信キューの優先度。Raft のハートビートや投票は Control、ブロック転送は Bulk。 */
enum Priority:
  case Control, Bulk

/** RPC メソッドの定義。id はクラスタ内で一意であること。 */
final case class Method[Req, Resp](
    id: Int,
    name: String,
    req: Codec[Req],
    resp: Codec[Resp],
    priority: Priority = Priority.Control
):
  require(id >= 0 && id <= 0xffff, s"method id out of range: $id")

  def encodeReq(r: Req): IO[ByteVector] = IO.fromTry(req.encode(r).map(_.bytes).toTry)
  def decodeReq(b: ByteVector): IO[Req] =
    IO.fromEither(req.decodeValue(b.bits).toEither.left.map(e => RemoteException(RemoteException.BadRequest, e.messageWithContext)))
  def encodeResp(r: Resp): IO[ByteVector] = IO.fromTry(resp.encode(r).map(_.bytes).toTry)
  def decodeResp(b: ByteVector): IO[Resp] =
    IO.fromEither(resp.decodeValue(b.bits).toEither.left.map(e => ProtocolException(s"bad response for $name: ${e.messageWithContext}")))

  def handledBy(f: Req => IO[Resp]): RpcRoute =
    RpcRoute(id, priority, bytes => decodeReq(bytes).flatMap(f).flatMap(encodeResp))

final case class RpcRoute(methodId: Int, priority: Priority, run: ByteVector => IO[ByteVector])

/** 受信したリクエストの振り分け先。 */
final class RpcHandler private (private val routes: Map[Int, RpcRoute]):
  def get(id: Int): Option[RpcRoute] = routes.get(id)
  def ++(other: RpcHandler): RpcHandler = RpcHandler(routes ++ other.routes)

object RpcHandler:
  val empty: RpcHandler = RpcHandler(Map.empty)
  def of(routes: RpcRoute*): RpcHandler =
    val dup = routes.groupBy(_.methodId).collect { case (id, rs) if rs.size > 1 => id }
    require(dup.isEmpty, s"duplicate method ids: ${dup.mkString(",")}")
    RpcHandler(routes.map(r => r.methodId -> r).toMap)

/** 相手側で処理が失敗したことを表す。 */
final case class RemoteException(code: Int, message: String) extends RuntimeException(s"remote error $code: $message")

object RemoteException:
  val Internal = 1
  val UnknownMethod = 2
  val BadRequest = 3

/** 接続が切れたため、応答を受け取れなかった（相手で処理されたかどうかは不明）。 */
final class ConnectionClosedException(msg: String, cause: Throwable = null) extends java.io.IOException(msg, cause)
