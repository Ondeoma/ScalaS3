package scalas3.raft

import cats.effect.IO
import cats.syntax.all.*
import scalas3.rpc.{Method, Priority, RpcClient, RpcHandler}
import scalas3.runtime.NodeAddress
import scodec.bits.ByteVector

/** Raft のメッセージを RPC で運ぶ。ハートビートや投票と、エントリやスナップショットの転送で優先度を分ける。 */
object RaftRpc:
  private val unit = scodec.codecs.provide(())

  val DeliverControl: Method[(Long, NodeId, Message), Unit] = Method(100, "raft.control", RaftCodec.envelope, unit, Priority.Control)
  val DeliverBulk: Method[(Long, NodeId, Message), Unit] = Method(101, "raft.bulk", RaftCodec.envelope, unit, Priority.Bulk)
  val DeliverBatchControl: Method[(Long, NodeId, Vector[Message]), Unit] = Method(102, "raft.batch.control", RaftCodec.batchEnvelope, unit, Priority.Control)
  val DeliverBatchBulk: Method[(Long, NodeId, Vector[Message]), Unit] = Method(103, "raft.batch.bulk", RaftCodec.batchEnvelope, unit, Priority.Bulk)
  val DeliverSnapshotChunk: Method[RaftCodec.SnapshotChunk, Unit] = Method(104, "raft.snapshot.chunk", RaftCodec.snapshotChunk, unit, Priority.Bulk)

  private def isBulk(m: Message): Boolean = m match
    case Message.Append(_, _, _, ents, _, _) => ents.nonEmpty
    case _: Message.Snapshot                 => true
    case _                                   => false

  private def deliverMethod(m: Message): Method[(Long, NodeId, Message), Unit] = if isBulk(m) then DeliverBulk else DeliverControl

  /** グループ ID から配送先を引く。知らないグループ宛てのメッセージは捨てる。 */
  def handler(lookup: Long => IO[Option[RaftNode]]): RpcHandler =
    val deliver = (env: (Long, NodeId, Message)) => lookup(env._1).flatMap(_.fold(IO.unit)(_.receive(env._2, env._3)))
    val deliverAll = (env: (Long, NodeId, Vector[Message])) =>
      lookup(env._1).flatMap(_.fold(IO.unit)(n => env._3.traverse_(n.receive(env._2, _))))
    // 知らないグループ宛てのかたまりは失敗させる（送り手は転送をやめ、後でやり直す）
    val deliverChunk = (c: RaftCodec.SnapshotChunk) =>
      lookup(c.group).flatMap {
        case Some(n) => n.receiveSnapshotChunk(c.from, c.term, c.meta, c.id, c.seq, c.last, c.data)
        case None    => IO.raiseError(IllegalStateException(s"unknown group ${c.group}"))
      }
    RpcHandler.of(DeliverControl.handledBy(deliver), DeliverBulk.handledBy(deliver),
      DeliverBatchControl.handledBy(deliverAll), DeliverBatchBulk.handledBy(deliverAll), DeliverSnapshotChunk.handledBy(deliverChunk))

  def transport(client: RpcClient, addresses: NodeId => Option[NodeAddress]): RaftTransport = new RaftTransport:
    def send(group: Long, from: NodeId, to: NodeId, msg: Message): IO[Unit] =
      addresses(to) match
        case None       => IO.unit
        case Some(addr) => client.call(addr, deliverMethod(msg), (group, from, msg))

    // かたまりを 1 つずつ送り、届いたのを確かめてから次を送る（受け手のメモリに溜めない）
    override def sendSnapshot(group: Long, from: NodeId, to: NodeId, term: Long, meta: SnapshotMeta, id: Long, chunks: fs2.Stream[IO, ByteVector]): IO[Unit] =
      addresses(to) match
        case None => IO.unit
        case Some(addr) =>
          chunks.zipWithIndex.zipWithNext
            .evalMap { case ((data, seq), next) =>
              client.call(addr, DeliverSnapshotChunk, RaftCodec.SnapshotChunk(group, from, term, meta, id, seq.toInt, next.isEmpty, data))
            }
            .compile
            .drain

    // スナップショットは大きいので 1 件ずつ、それ以外は順序を保ったまままとめて送る
    override def sendAll(group: Long, from: NodeId, to: NodeId, msgs: Vector[Message]): IO[Unit] =
      addresses(to) match
        case None => IO.unit
        case Some(addr) =>
          runs(msgs).traverse_ {
            case Vector(m) => client.call(addr, deliverMethod(m), (group, from, m))
            case ms        => client.call(addr, if ms.exists(isBulk) then DeliverBatchBulk else DeliverBatchControl, (group, from, ms))
          }

  /** スナップショットの前後で区切る。 */
  private def runs(msgs: Vector[Message]): Vector[Vector[Message]] =
    msgs.foldLeft(Vector.empty[Vector[Message]]) { (acc, m) =>
      m match
        case _: Message.Snapshot => acc :+ Vector(m) :+ Vector.empty
        case _ =>
          if acc.isEmpty then Vector(Vector(m))
          else acc.init :+ (acc.last :+ m)
    }.filter(_.nonEmpty)
