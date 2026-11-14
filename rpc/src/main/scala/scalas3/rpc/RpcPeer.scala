package scalas3.rpc

import cats.effect.std.{Queue, Supervisor}
import cats.effect.{Deferred, Fiber, IO, Ref, Resource}
import cats.syntax.all.*
import scalas3.runtime.{Connection, NodeAddress}
import scodec.bits.ByteVector

import java.nio.charset.StandardCharsets.UTF_8

/** 1 本の接続の上で、双方向に RPC を多重化する。
  *
  *   - 接続を張った側（initiator）は奇数、受けた側は偶数のストリーム ID を使う。
  *   - 送信は優先度ごとのキューに入れ、Control を Bulk より先に送る。
  *   - 呼び出し側がキャンセルすると、相手に Cancel フレームを送って処理を止める。
  */
final class RpcPeer private (
    val remote: NodeAddress,
    pending: Ref[IO, Option[Map[Int, RpcPeer.Reply]]],
    nextId: Ref[IO, Int],
    send: (Frame, Priority) => IO[Unit],
    closedSignal: Deferred[IO, Throwable]
):

  /** 接続が切れたら（理由とともに）完了する。 */
  def closed: IO[Throwable] = closedSignal.get

  def isClosed: IO[Boolean] = closedSignal.tryGet.map(_.isDefined)

  def call[Req, Resp](m: Method[Req, Resp], req: Req): IO[Resp] =
    m.encodeReq(req).flatMap(body => callRaw(m.id, m.priority, body)).flatMap(m.decodeResp)

  def callRaw(methodId: Int, priority: Priority, body: ByteVector): IO[ByteVector] =
    IO.uncancelable { poll =>
      for
        id <- nextId.getAndUpdate(_ + 2)
        d <- Deferred[IO, Either[Throwable, ByteVector]]
        registered <- pending.modify {
          case Some(m) => (Some(m.updated(id, d)), true)
          case None    => (None, false)
        }
        _ <- IO.raiseUnless(registered)(ConnectionClosedException(s"connection to $remote is closed"))
        payload = ByteVector.fromInt(methodId, 2) ++ body
        _ <- send(Frame(id, FrameKind.Request, 0, payload), priority)
        r <- poll(d.get).onCancel(
          pending.update(_.map(_ - id)) >> send(Frame(id, FrameKind.Cancel, 0, ByteVector.empty), Priority.Control).attempt.void
        )
        v <- IO.fromEither(r)
      yield v
    }

object RpcPeer:

  /** 応答を待つ呼び出し（接続が切れたら Left で完了する）。 */
  private[rpc] type Reply = Deferred[IO, Either[Throwable, ByteVector]]

  /** 1 回の書き込みにまとめるフレームの量の目安。 */
  private val WriteBatchBytes = 256L * 1024

  /** 接続の上で RpcPeer を動かす。Resource を解放すると、送受信を止める（接続自体は呼び出し側が閉じる）。 */
  def run(conn: Connection, handler: RpcHandler, initiator: Boolean): Resource[IO, RpcPeer] =
    for
      control <- Resource.eval(Queue.unbounded[IO, fs2.Chunk[Byte]])
      bulk <- Resource.eval(Queue.unbounded[IO, fs2.Chunk[Byte]])
      ready <- Resource.eval(Queue.unbounded[IO, Unit])
      pending <- Resource.eval(Ref.of[IO, Option[Map[Int, Reply]]](Some(Map.empty)))
      nextId <- Resource.eval(Ref.of[IO, Int](if initiator then 1 else 2))
      closed <- Resource.eval(Deferred[IO, Throwable])
      inflight <- Resource.eval(Ref.of[IO, Map[Int, Fiber[IO, Throwable, Unit]]](Map.empty))
      supervisor <- Supervisor[IO](await = false)

      send = (f: Frame, p: Priority) =>
        closed.tryGet.flatMap {
          case Some(e) => IO.raiseError(ConnectionClosedException(s"connection to ${conn.remote} is closed", e))
          case None =>
            val q = if p == Priority.Control then control else bulk
            q.offer(Frame.encode(f)) >> ready.offer(())
        }

      // 接続が終わったら、待っている呼び出しを全部失敗させる。
      shutdown = (cause: Throwable) =>
        closed.complete(cause).flatMap { first =>
          (pending.getAndSet(None).flatMap(_.fold(IO.unit)(_.values.toList.traverse_(
            _.complete(Left(ConnectionClosedException(s"connection to ${conn.remote} closed", cause))).void
          ))) >> inflight.get.flatMap(_.values.toList.traverse_(_.cancel))).whenA(first)
        }

      // 溜まっているフレームをまとめて 1 回で書く（小さなフレームを 1 つずつ書くと、TLS ではブロッキング用の
      // スレッドとの受け渡しが毎回起きて遅い）。Control を先に、Bulk はまとめる量の上限まで
      drainAndWrite = {
        def takeBulk(acc: Vector[fs2.Chunk[Byte]], size: Long): IO[Vector[fs2.Chunk[Byte]]] =
          if size >= WriteBatchBytes then IO.pure(acc)
          else bulk.tryTake.flatMap {
            case Some(c) => takeBulk(acc :+ c, size + c.size)
            case None    => IO.pure(acc)
          }
        def loop: IO[Unit] =
          control.tryTakeN(None).flatMap { cs =>
            val csSize = cs.foldLeft(0L)(_ + _.size)
            takeBulk(cs.toVector, csSize).flatMap { frames =>
              if frames.isEmpty then IO.unit
              else
                val out = if frames.size == 1 then frames.head else fs2.Chunk.concat(frames)
                // 書いている間に届いた分や、上限で残した分があれば続けて書く（ready の通知は先に読み捨てている）
                conn.write(out) >> (control.size, bulk.size).flatMapN((c, b) => loop.whenA(c + b > 0))
            }
          }
        loop
      }

      writer = (ready.take >> ready.tryTakeN(None) >> drainAndWrite).foreverM: IO[Nothing]

      respond = (id: Int, r: Either[Throwable, ByteVector]) =>
        val frame = r match
          case Right(b) => Frame(id, FrameKind.Response, 0, b)
          case Left(RemoteException(code, msg)) => errorFrame(id, code, msg)
          case Left(e) => errorFrame(id, RemoteException.Internal, Option(e.getMessage).getOrElse(e.getClass.getName))
        send(frame, Priority.Control).attempt.void

      onRequest = (f: Frame) =>
        if f.payload.size < 2 then respond(f.streamId, Left(RemoteException(RemoteException.BadRequest, "short request")))
        else
          val methodId = f.payload.take(2).toInt(signed = false)
          handler.get(methodId) match
            case None => respond(f.streamId, Left(RemoteException(RemoteException.UnknownMethod, s"method $methodId")))
            case Some(route) =>
              // inflight に登録してから処理を始める（先に始めると、すぐ終わった処理が登録より先に自分を外し、
              // 終わった処理の登録が接続を閉じるまで残り続ける）
              IO.uncancelable { _ =>
                Deferred[IO, Unit].flatMap { registered =>
                  val work = registered.get >> route.run(f.payload.drop(2)).attempt.flatMap { r =>
                    inflight.update(_ - f.streamId) >> respond(f.streamId, r)
                  }
                  supervisor.supervise(work).flatMap(fib => inflight.update(_.updated(f.streamId, fib))) >> registered.complete(()).void
                }
              }

      complete = (id: Int, r: Either[Throwable, ByteVector]) =>
        pending.modify {
          case Some(m) => (Some(m - id), m.get(id))
          case None    => (None, None)
        }.flatMap(_.traverse_(_.complete(r)))

      reader = conn.reads
        .through(Frame.decode)
        .evalMap { f =>
          f.kind match
            case FrameKind.Request  => onRequest(f)
            case FrameKind.Response => complete(f.streamId, Right(f.payload))
            case FrameKind.Error    => complete(f.streamId, Left(decodeError(f.payload)))
            case FrameKind.Cancel =>
              inflight.modify(m => (m - f.streamId, m.get(f.streamId))).flatMap(_.traverse_(_.cancel))
        }
        .compile
        .drain

      _ <- (reader.attempt.flatMap(r => shutdown(r.left.getOrElse(ConnectionClosedException("connection closed by peer")))))
        .background
      _ <- writer.handleErrorWith(e => shutdown(e).void).background
      _ <- Resource.onFinalize(shutdown(ConnectionClosedException("rpc peer released")))
    yield RpcPeer(conn.remote, pending, nextId, send, closed)

  private def errorFrame(id: Int, code: Int, msg: String): Frame =
    Frame(id, FrameKind.Error, 0, ByteVector(code.toByte) ++ ByteVector(msg.getBytes(UTF_8)))

  private def decodeError(b: ByteVector): Throwable =
    if b.isEmpty then RemoteException(RemoteException.Internal, "")
    else RemoteException(b(0) & 0xff, new String(b.drop(1).toArray, UTF_8))
