package scalas3.meta

import cats.effect.{IO, Ref}
import cats.syntax.all.*
import scalas3.core.Bytes
import scalas3.raft.NodeId
import scalas3.rpc.RpcClient
import scalas3.runtime.NodeAddress
import scodec.bits.ByteVector

import scala.concurrent.duration.*

final class MetaUnavailableException(msg: String, cause: Throwable = null) extends java.io.IOException(msg, cause)

/** キーから担当レンジを探し、そのリーダーに読み書きを送る。
  *
  * 記述子とリーダーの位置はキャッシュし、NotLeader や区間外の応答で更新する。
  * 再試行するのは「確実に適用されていない」場合だけ（NotLeader、レンジなし、区間外、提案の破棄、接続失敗）。
  */
final class MetaClient private (
    self: NodeId,
    rpc: RpcClient,
    addressOf: NodeId => Option[NodeAddress],
    local: RangeManager,
    systemReplicas: IO[Set[NodeId]],
    descs: Ref[IO, Vector[RangeDescriptor]],
    hints: Ref[IO, Map[Long, NodeId]],
    timeout: FiniteDuration
):
  import MetaClient.MaxRelocates

  // ---------------- 公開 API ----------------

  def get(key: ByteVector): IO[Option[ByteVector]] =
    readAt(key, ReadRequest.Get(key)).flatMap {
      case ReadResult.Value(v) => IO.pure(v)
      case other               => IO.raiseError(MetaUnavailableException(s"unexpected read result $other"))
    }

  /** [from, until) を、レンジをまたいで最大 limit 件読む。2 番目の値は「続きがあるか」。 */
  def scan(from: ByteVector, until: Option[ByteVector], limit: Int): IO[(Vector[(ByteVector, ByteVector)], Boolean)] =
    def loop(cursor: ByteVector, acc: Vector[(ByteVector, ByteVector)]): IO[(Vector[(ByteVector, ByteVector)], Boolean)] =
      val remaining = limit - acc.size
      if remaining <= 0 then IO.pure((acc, true))
      else
        locate(cursor).flatMap { d =>
          readTo(d, cursor, ReadRequest.Scan(cursor, until, remaining)).flatMap {
            case ReadResult.Entries(es, more) =>
              val all = acc ++ es
              if more then IO.pure((all, true))
              else
                d.end match
                  case Some(e) if until.forall(u => Bytes.compare(e, u) < 0) => loop(e, all)
                  case _                                                      => IO.pure((all, false))
            case other => IO.raiseError(MetaUnavailableException(s"unexpected scan result $other"))
          }
        }
    loop(from, Vector.empty)

  /** 1 つのレンジに収まる書き込み（条件付き）。 */
  def write(conditions: Vector[Precondition], ops: Vector[KvOp]): IO[WriteResult] =
    val key = (conditions.map(_.key) ++ ops.map(_.key)).headOption.getOrElse(Keys.SystemStart)
    val cmd = MetaCodec.command(Command.Write(conditions, ops))
    def attempt(n: Int): IO[WriteResult] =
      locate(key).flatMap(d => sendTo(d, write = true, cmd)).map(MetaCodec.writeResult).flatMap {
        // 分割直後など、キャッシュが古かった
        case WriteResult.NotInRange(d) if n < MaxRelocates => relearn(d, n) >> attempt(n + 1)
        case r => IO.pure(r)
      }
    attempt(0)

  /** システムレンジに記録されたすべての記述子（キャッシュも更新する）。 */
  def descriptors: IO[Vector[RangeDescriptor]] = refresh >> descs.get

  /** システムレンジに記録された記述子を、重なりを解かずにそのまま読む（統合の後に残った古い記述子を片付けるため）。 */
  def systemDescriptors: IO[Vector[RangeDescriptor]] =
    systemDescriptor.flatMap { sys =>
      def page(from: ByteVector, acc: Vector[RangeDescriptor]): IO[Vector[RangeDescriptor]] =
        readTo(sys, from, ReadRequest.Scan(from, Bytes.prefixEnd(Keys.RangeDescriptorPrefix), 500)).flatMap {
          case ReadResult.Entries(es, more) =>
            val ds = acc ++ es.map((_, v) => RangeDescriptor.decode(v))
            if more && es.nonEmpty then page(es.last._1 :+ 0.toByte, ds) else IO.pure(ds)
          case other => IO.raiseError(MetaUnavailableException(s"unexpected descriptor scan $other"))
        }
      page(Keys.RangeDescriptorPrefix, Vector.empty)
    }

  /** 特定のレンジに読み込みを送る（統合の状態や中身を読む。区間外の応答でも別のレンジには送り直さない）。 */
  def readRange(d: RangeDescriptor, q: ReadRequest): IO[ReadResult] =
    sendTo(d, write = false, MetaCodec.readRequest(q)).map(MetaCodec.readResult)

  /** 特定のレンジにコマンドを提案する（統合の凍結など）。 */
  def proposeTo(d: RangeDescriptor, cmd: Command): IO[WriteResult] =
    sendTo(d, write = true, MetaCodec.command(cmd)).map(MetaCodec.writeResult)

  /** レンジのキャッシュを捨てる（分割した直後など）。 */
  def forget(rangeId: Long): IO[Unit] = descs.update(_.filterNot(_.id == rangeId))

  // ---------------- 内部 ----------------

  private def systemDescriptor: IO[RangeDescriptor] =
    systemReplicas.map(rs => RangeDescriptor(0, Keys.SystemStart, Some(Keys.SystemEnd), rs, 0))

  /** 区間外の応答やレンジの見つからないときに、やり直すまでの待ち（新しい記述子が公開されるまで少し待つ）。 */
  private def relocateBackoff(n: Int): IO[Unit] = IO.sleep((10 * math.min(n + 1, 20)).millis)

  /** 区間外の応答で教わった記述子を覚え、少し待ってからキャッシュを読み直す。 */
  private def relearn(d: RangeDescriptor, n: Int): IO[Unit] = learn(d) >> relocateBackoff(n) >> refresh

  private def learn(d: RangeDescriptor): IO[Unit] =
    descs.update { ds =>
      val others = ds.filterNot(o => o.id == d.id || o.overlaps(d.start, d.end))
      (others :+ d).sortWith((a, b) => Bytes.compare(a.start, b.start) < 0)
    }

  private def refresh: IO[Unit] =
    (systemDescriptors, descs.get).flatMapN { (all, cached) =>
      // 分割の途中では、親（古い世代）と子が重なることがある。統合の後は、取り込まれた右のレンジ（古い世代）と
      // 広げた左のレンジが重なる。手元で学んだ記述子のほうが新しいこともあるので混ぜる。同じレンジは世代の新しいほう、
      // 重なるレンジも世代の新しいほうを採る（子レンジは分割後の親と同じ世代から、統合後のレンジは両方より大きい世代になる）
      val sorted = (all ++ cached).filter(_.id != 0).groupBy(_.id).values.map(_.maxBy(_.generation)).toVector.sortBy(-_.generation)
      val chosen = sorted.foldLeft(Vector.empty[RangeDescriptor])((acc, d) =>
        if acc.exists(o => o.overlaps(d.start, d.end)) then acc else acc :+ d
      )
      descs.set(chosen.sortWith((a, b) => Bytes.compare(a.start, b.start) < 0))
    }

  private def locate(key: ByteVector): IO[RangeDescriptor] =
    if Bytes.compare(key, Keys.SystemEnd) < 0 then systemDescriptor
    else
      def find = descs.get.map(_.find(_.contains(key)))
      find.flatMap {
        case Some(d) => IO.pure(d)
        case None =>
          // 分割直後で、新しいレンジの記述子がまだ公開されていないことがある
          def retry(n: Int): IO[RangeDescriptor] =
            refresh >> find.flatMap {
              case Some(d)                  => IO.pure(d)
              case None if n < MaxRelocates => relocateBackoff(n) >> retry(n + 1)
              case None                     => IO.raiseError(MetaUnavailableException(s"no range for key ${key.toHex}"))
            }
          retry(0)
      }

  private def readAt(key: ByteVector, q: ReadRequest): IO[ReadResult] =
    locate(key).flatMap(d => readTo(d, key, q))

  private def readTo(d: RangeDescriptor, key: ByteVector, q: ReadRequest): IO[ReadResult] =
    def attempt(desc: RangeDescriptor, n: Int): IO[ReadResult] =
      sendTo(desc, write = false, MetaCodec.readRequest(q)).map(MetaCodec.readResult).flatMap {
        case ReadResult.NotInRange(nd) if n < MaxRelocates => relearn(nd, n) >> locate(key).flatMap(attempt(_, n + 1))
        case r => IO.pure(r)
      }
    attempt(d, 0)

  /** レンジのレプリカのどれかに送る。リーダーへの転送や再試行を行う。 */
  private def sendTo(d: RangeDescriptor, write: Boolean, payload: ByteVector): IO[ByteVector] =
    def call(node: NodeId): IO[MetaService.Reply] =
      if node == self then
        if write then MetaService.write(local, d.id, payload) else MetaService.read(local, d.id, payload)
      else
        IO.fromOption(addressOf(node))(MetaUnavailableException(s"unknown node $node")).flatMap { addr =>
          rpc.call(addr, if write then MetaService.Write else MetaService.Read, (d.id, payload))
        }

    /** followHint が true なら、直前に教わったリーダーへ送る。そうでなければレプリカを順に試す。 */
    def loop(attempt: Int, lastError: Option[Throwable], followHint: Boolean): IO[ByteVector] =
      if attempt > 30 then IO.raiseError(MetaUnavailableException(s"range ${d.id} unavailable", lastError.orNull))
      else
        hints.get.map(_.get(d.id)).flatMap { hint =>
          val order = (hint.toList ++ d.replicas.toList.sorted).distinct
          val target = hint.filter(_ => followHint || attempt == 0).getOrElse(order(attempt % math.max(1, order.size)))
          val backoff = if attempt == 0 || followHint then IO.unit else IO.sleep((20 * math.min(attempt, 10)).millis)
          backoff >> call(target).attempt.flatMap {
            case Right(MetaService.Reply.Ok(p)) => hints.update(_.updated(d.id, target)).as(p)
            // リーダーの情報は、キャッシュした記述子のレプリカ一覧より新しいことがある（レプリカの入れ替え直後など）
            case Right(MetaService.Reply.NotLeader(Some(h))) if h != target =>
              hints.update(_.updated(d.id, h)) >> loop(attempt + 1, None, followHint = true)
            case Right(MetaService.Reply.NotLeader(_)) => hints.update(_ - d.id) >> loop(attempt + 1, None, false)
            case Right(MetaService.Reply.RangeNotFound) => hints.update(_ - d.id) >> loop(attempt + 1, None, false)
            case Right(MetaService.Reply.Dropped)       => loop(attempt + 1, None, false)
            case Left(e: java.net.ConnectException)     => hints.update(_ - d.id) >> loop(attempt + 1, Some(e), false)
            // 読み込みは何度でもやり直せる
            case Left(e @ (_: scalas3.rpc.ConnectionClosedException | _: java.util.concurrent.TimeoutException)) if !write =>
              hints.update(_ - d.id) >> loop(attempt + 1, Some(e), false)
            // 書き込みは、送った後に切れたり時間切れになったりすると、効いたかどうか分からない。やり直さずに「一時的に
            // 使えない」として返す（S3 では 503。クライアントが判断してやり直す）
            case Left(e @ (_: scalas3.rpc.ConnectionClosedException | _: java.util.concurrent.TimeoutException)) =>
              hints.update(_ - d.id) >> IO.raiseError(MetaUnavailableException(s"range ${d.id}: the write may or may not have been applied", e))
            case Left(e) => IO.raiseError(e)
          }
        }
    loop(0, None, false).timeoutTo(timeout, IO.raiseError(MetaUnavailableException(s"range ${d.id}: timed out")))

object MetaClient:
  /** 区間外の応答やレンジが見つからないときに、記述子を読み直してやり直す回数の上限。 */
  private val MaxRelocates = 50

  def apply(
      self: NodeId,
      rpc: RpcClient,
      addressOf: NodeId => Option[NodeAddress],
      local: RangeManager,
      systemReplicas: IO[Set[NodeId]],
      timeout: FiniteDuration
  ): IO[MetaClient] =
    (Ref.of[IO, Vector[RangeDescriptor]](Vector.empty), Ref.of[IO, Map[Long, NodeId]](Map.empty)).mapN(
      new MetaClient(self, rpc, addressOf, local, systemReplicas, _, _, timeout)
    )
