package scalas3.raft

import cats.effect.std.{Queue, Semaphore}
import cats.effect.{Deferred, IO, Ref, Resource}
import cats.syntax.all.*
import scodec.bits.ByteVector

import scala.collection.mutable
import scala.concurrent.duration.*

/** Raft で複製する状態マシン。メソッドはすべて Raft のイベントループから順に呼ばれる。 */
trait StateMachine:
  /** 起動時点で永続化済みの適用位置。メモリ上の状態マシンなら 0。 */
  def appliedIndex: IO[Long]

  /** コミット済みのエントリを適用する。command が None なのは Noop と構成変更（位置の記録だけ行う）。 */
  def apply(index: Long, command: Option[ByteVector]): IO[ByteVector]

  /** 最後に適用した位置の時点の状態。 */
  def snapshot: IO[ByteVector]
  def restore(index: Long, data: ByteVector): IO[Unit]

  /** 最後に適用した位置までの状態を手元のディスクに永続化する。対応していれば true。
    * true なら、ログを切り詰めるときに snapshot を作って保存しない（状態全体をメモリに並べずに済む）。
    * 再起動時は appliedIndex がスナップショットの位置以上になるので、保存したデータは使われない。
    */
  def persist: IO[Boolean] = IO.pure(false)

  // ---- スナップショットの送受信（ストリーム） ----
  //
  // 送る側：ループの中で snapshotSource を確保し（その時点の一貫したビュー）、裏でかたまりを順に送る。
  // 受ける側：かたまりはループの外で stageSnapshot に預け、揃ったらループの中で install で取り込む。
  // 既定の実装は、全体を 1 つのバイト列にまとめて snapshot / restore を使う（メモリ上の状態マシン向け）。

  /** 最後に適用した位置の時点の状態を、かたまりの列で返す。かたまりをつないだものが restore に渡せるデータになること。 */
  def snapshotSource: Resource[IO, fs2.Stream[IO, ByteVector]] = Resource.eval(snapshot).map(fs2.Stream.emit(_))

  private val stagedChunks = scala.collection.concurrent.TrieMap.empty[Long, Vector[ByteVector]]

  /** 届いたかたまりを預ける（ループの外から呼ばれる。1 つの転送については seq の順に呼ばれ、seq 0 で始まる）。 */
  def stageSnapshot(id: Long, seq: Int, chunk: ByteVector): IO[Unit] =
    IO(stagedChunks.update(id, (if seq == 0 then Vector.empty else stagedChunks.getOrElse(id, Vector.empty)) :+ chunk))

  /** 使わなかった預かりを捨てる。 */
  def discardStaged(id: Long): IO[Unit] = IO(stagedChunks.remove(id)).void

  /** スナップショットを取り込む。staged があれば預けたかたまり、なければ inline を使う。
    * 戻り値は Raft のストレージに保存すべきデータ。状態マシンが自分で永続化したなら None（再起動時は persistedSnapshot を使う）。
    */
  def install(meta: SnapshotMeta, staged: Option[Long], inline: ByteVector): IO[Option[ByteVector]] =
    val data = staged.fold(IO.pure(inline))(id => IO(stagedChunks.remove(id).fold(ByteVector.empty)(_.foldLeft(ByteVector.empty)(_ ++ _))))
    data.flatMap(d => restore(meta.index, d).as(Some(d)))

  /** install で永続化したスナップショットの位置。Raft のストレージより新しければ、起動時にこちらを使う。 */
  def persistedSnapshot: IO[Option[SnapshotMeta]] = IO.pure(None)

/** ノード間でメッセージを届ける手段。届かなくてもよい（Raft が再送する）。 */
trait RaftTransport:
  def send(group: Long, from: NodeId, to: NodeId, msg: Message): IO[Unit]

  /** スナップショットを送る（届いたら相手で Message.Snapshot として扱われる）。既定は 1 つのメッセージにまとめる。 */
  def sendSnapshot(group: Long, from: NodeId, to: NodeId, term: Long, meta: SnapshotMeta, @scala.annotation.unused id: Long, chunks: fs2.Stream[IO, ByteVector]): IO[Unit] =
    chunks.compile.fold(ByteVector.empty)(_ ++ _).flatMap(d => send(group, from, to, Message.Snapshot(term, meta, d)))

  /** 同じ相手への複数のメッセージを、順序を保って送る（まとめて 1 回で送れる実装はそうする）。 */
  def sendAll(group: Long, from: NodeId, to: NodeId, msgs: Vector[Message]): IO[Unit] =
    msgs.traverse_(send(group, from, to, _))

/** ノード全体で同時に扱うスナップショットの数の上限（グループ間で共有する）。
  * スナップショットの送受信は LSM の一貫したビューを握り、その時点の memtable をメモリに留めるので、多数のレンジで同時に
  * 起きるとヒープを使い切る（ノードを止めて戻したとき、全レンジが一斉にスナップショットを必要とする）。
  */
final case class SnapshotLimits(sends: Option[Semaphore[IO]], installs: Option[Semaphore[IO]])

object SnapshotLimits:
  val unlimited: SnapshotLimits = SnapshotLimits(None, None)
  def apply(sends: Int, installs: Int): IO[SnapshotLimits] =
    (Semaphore[IO](sends.toLong), Semaphore[IO](installs.toLong)).mapN((s, i) => SnapshotLimits(Some(s), Some(i)))

final case class RaftNodeConfig(
    raft: RaftConfig = RaftConfig(),
    tickInterval: FiniteDuration = 100.millis,
    snapshotThreshold: Long = 10000,
    /** 最後のスナップショット以降に適用したコマンドの合計がこれを超えたら、エントリ数によらずログを切り詰める。
      * ログのエントリはメモリにも置くので、これがグループあたりのメモリの上限の目安になる。
      */
    snapshotBytes: Long = 8L * 1024 * 1024,
    /** リーダーは、遅れているフォロワーのためにこの量まではログを切り詰めずに残す（超えたらスナップショットで送る）。
      * 残さないと、少し遅れただけのフォロワーにレンジ全体のスナップショットを何度も送ることになり、両側でメモリを
      * 使い切った（ログを 8 MiB ごとに切り詰めると、小さな PUT が続くレンジでは 1 秒に何度も起きる）。
      */
    maxRetainedBytes: Long = 32L * 1024 * 1024,
    outboundQueue: Int = 4096
)

sealed abstract class RaftException(msg: String) extends RuntimeException(msg)
final class NotLeaderException(val leaderHint: Option[NodeId]) extends RaftException(s"not leader (hint: ${leaderHint.getOrElse("?")})")
final class RaftBusyException(reason: String) extends RaftException(reason)

/** 提案したエントリが別の term のエントリで上書きされた（コミットされなかった）。 */
final class ProposalDroppedException extends RaftException("proposal was overwritten by a new leader")

/** RaftCore を動かすドライバ。1 本のイベントループが、永続化・送信・適用を Ready の順序どおりに行う。 */
final class RaftNode private (
    val id: NodeId,
    events: Queue[IO, RaftNode.Event],
    statusRef: Ref[IO, RaftStatus],
    sm: StateMachine,
    chunkSeq: Ref[IO, Map[Long, Int]],
    failure: Ref[IO, Option[Throwable]]
):
  import RaftNode.Event

  /** コマンドを提案し、適用結果を返す。リーダー以外では NotLeaderException。 */
  def propose(command: ByteVector): IO[ByteVector] = ask(Event.Propose(command, _))

  /** 線形化可能な読み込みの障壁。完了後にローカルの状態マシンを読めば、呼び出し前に成功した書き込みが必ず見える。 */
  def readBarrier: IO[Unit] = ask(Event.Read(_))

  /** 構成を変える。Joint Consensus を経て新しい構成が適用されたら完了する。 */
  def changeMembership(voters: Set[NodeId], learners: Set[NodeId]): IO[Unit] = ask(Event.Members(voters, learners, _))

  /** 結果を受け取る Deferred をつけてイベントを送り、結果を待つ。 */
  private def ask[A](event: Deferred[IO, Either[Throwable, A]] => Event): IO[A] =
    Deferred[IO, Either[Throwable, A]].flatMap(d => events.offer(event(d)) >> d.get.rethrow)

  def transferLeadership(to: NodeId): IO[Unit] = events.offer(Event.Transfer(to))

  def receive(from: NodeId, msg: Message): IO[Unit] = events.offer(Event.Msg(from, msg))

  /** スナップショットのかたまりを受け取る（ループの外で預け、最後のかたまりでループに知らせる）。順番が飛んだら失敗させる
    * （送り手はやり直す）。
    */
  def receiveSnapshotChunk(from: NodeId, term: Long, meta: SnapshotMeta, sid: Long, seq: Int, last: Boolean, chunk: ByteVector): IO[Unit] =
    // 新しい転送が始まったら、途中でやめた転送（最後のかたまりが届いていないもの）の預かりを捨てる。受け取り終えて
    // 取り込みを待っているものは消さない（消すと取り込めなくなる）
    chunkSeq.modify { m =>
      val expected = if seq == 0 then 0 else m.getOrElse(sid, -1)
      if expected != seq then (m, None)
      else
        val abandoned = if seq == 0 then m.keySet - sid else Set.empty[Long]
        val next = (m -- abandoned)
        (if last then next - sid else next.updated(sid, seq + 1), Some(abandoned))
    }.flatMap {
      case None => IO.raiseError(IllegalStateException(s"snapshot chunk $seq of $sid is out of order"))
      case Some(abandoned) =>
        abandoned.toList.sorted.traverse_(sm.discardStaged) >>
          sm.stageSnapshot(sid, seq, chunk) >> events.offer(Event.SnapshotReady(from, term, meta, sid)).whenA(last)
    }

  def status: IO[RaftStatus] = statusRef.get

  /** イベントループが例外で止まったら、その例外。止まったグループは、呼び出し側がストレージと状態マシンから起動し直す。 */
  def failed: IO[Option[Throwable]] = failure.get

object RaftNode:

  /** 1 件を待って取り、続けて溜まっている分（最大 255 件）もまとめて取る。 */
  private def takeBatch[A](q: Queue[IO, A]): IO[List[A]] =
    q.take.flatMap(first => q.tryTakeN(Some(255)).map(first :: _))

  /** 止めるときに、ノード ID の順にファイバーを取り消す（実行ごとに順序が変わらないように）。 */
  private def cancelInOrder[F <: cats.effect.Fiber[IO, Throwable, ?]](fibers: Ref[IO, Map[NodeId, F]]): IO[Unit] =
    fibers.get.flatMap(_.toList.sortBy(_._1).traverse_(_._2.cancel))

  private def commandBytes(p: Payload): Long = p match
    case Payload.Command(d) => d.size
    case _                  => 0L

  private enum Event:
    case Tick
    case Msg(from: NodeId, msg: Message)
    case Propose(data: ByteVector, d: Deferred[IO, Either[Throwable, ByteVector]])
    case Read(d: Deferred[IO, Either[Throwable, Unit]])
    case Members(voters: Set[NodeId], learners: Set[NodeId], d: Deferred[IO, Either[Throwable, Unit]])
    case Transfer(to: NodeId)
    /** 裏で行った sm.persist が終わった。Right(Some(meta)) なら meta まで永続化済み、Right(None) なら persist に
      * 対応していない、Left なら失敗した（次の機会にやり直す）。
      */
    case Persisted(result: Either[Throwable, Option[SnapshotMeta]])
    /** スナップショットのかたまりが揃った（預かりの id）。 */
    case SnapshotReady(from: NodeId, term: Long, meta: SnapshotMeta, sid: Long)
    /** 裏で送っていたスナップショットが終わった（成功でも失敗でも）。 */
    case SnapshotSent(to: NodeId)

  /** グループを起動する。
    *
    * @param initial ストレージが空のときに使う初期状態（スナップショット相当）。
    *                クラスタ作成時は (0, 0, 初期構成)、分割で生まれたレンジは分割時点の構成を持つ (5, 5, 構成)。
    *                データを持たずに作られたレプリカは (0, 0, 空の構成) で、リーダーからスナップショットを受け取る。
    */
  def start(
      id: NodeId,
      group: Long,
      initial: SnapshotMeta,
      storage: RaftStorage,
      sm: StateMachine,
      transport: RaftTransport,
      config: RaftNodeConfig = RaftNodeConfig(),
      seed: Long = 0L,
      limits: SnapshotLimits = SnapshotLimits.unlimited
  ): Resource[IO, RaftNode] =
    for
      loaded <- Resource.eval(storage.load)
      smApplied <- Resource.eval(sm.appliedIndex)
      smSnap <- Resource.eval(sm.persistedSnapshot)
      // 状態マシンが取り込んで永続化したスナップショットが、Raft のストレージより新しいことがある（取り込みの後、
      // ストレージを書き換える前に落ちた）。そのときは状態マシンの方を正とする
      fromSm = smSnap.filter(m => m.index > loaded.snapshot.fold(0L)(_._1.index) && m.index >= initial.index)
      applied <- Resource.eval(fromSm match
        case Some(m) => IO.pure(math.max(smApplied, m.index))
        case None =>
          loaded.snapshot match
            case Some((meta, data)) if smApplied < meta.index =>
              if data.isEmpty then IO.raiseError(IllegalStateException(s"group $group: state machine is behind the raft snapshot ${meta.index} and no snapshot data was stored"))
              else sm.restore(meta.index, data).as(meta.index)
            case _ => IO.pure(smApplied)
      )
      base = fromSm.getOrElse(loaded.snapshot.map(_._1).filter(_.index >= initial.index).getOrElse(initial))
      // スナップショットの位置のエントリが一致すれば、その後のエントリを残す（Raft の InstallSnapshot と同じ規則）
      entries = fromSm.fold(loaded.entries) { m =>
        if loaded.entries.exists(e => e.index == m.index && e.term == m.term) then loaded.entries.filter(_.index > m.index) else Vector.empty
      }
      // ログの term より小さい term からは始められない
      hs = if loaded.hs.term < base.term then HardState(base.term, None) else loaded.hs
      // 種は XOR ではなくハッシュで混ぜる（XOR だと別のノードが同じ種になりうる。例: 1^'a' == 2^'b'）
      core = RaftCore(id, config.raft, hs, base, entries, applied, scala.util.hashing.MurmurHash3.stringHash(s"$id/$seed/$group").toLong)
      events <- Resource.eval(Queue.unbounded[IO, Event])
      statusRef <- Resource.eval(Ref.of[IO, RaftStatus](core.status))
      outbound <- Resource.eval(Ref.of[IO, Map[NodeId, Queue[IO, Message]]](Map.empty))
      // 送信用の処理は自分で持ち、止めるときはノード ID の順に取り消す（Supervisor は取り消しの順序が実行ごとに変わり、
      // 決定的シミュレーションの再現性を損なう）
      senders <- Resource.make(Ref.of[IO, Map[NodeId, cats.effect.Fiber[IO, Throwable, Nothing]]](Map.empty))(cancelInOrder)
      // ログを切り詰めるための永続化は裏で行う（終わるまでイベントループを止めない）。止めるときは取り消す
      persister <- Resource.make(Ref.of[IO, Option[cats.effect.FiberIO[Unit]]](None))(_.get.flatMap(_.traverse_(_.cancel)))
      // スナップショットの送信も裏で行う（相手ごとに 1 本）。止めるときはノード ID の順に取り消す
      snapshotters <- Resource.make(Ref.of[IO, Map[NodeId, cats.effect.FiberIO[Unit]]](Map.empty))(cancelInOrder)
      chunkSeq <- Resource.eval(Ref.of[IO, Map[Long, Int]](Map.empty))
      failure <- Resource.eval(Ref.of[IO, Option[Throwable]](None))
      loop = Loop(id, group, core, storage, sm, transport, config, limits, statusRef, outbound, senders, applied, base.index,
        task => task.attempt.flatMap(r => events.offer(Event.Persisted(r))).start.flatMap(f => persister.set(Some(f))),
        (to, task) => task.guarantee(events.offer(Event.SnapshotSent(to))).start.flatMap(f => snapshotters.update(_.updated(to, f))))
      // ループが例外で止まったら記録する（黙って止まると、そのグループだけが動かなくなる）
      _ <- takeBatch(events).flatMap(loop.handle).foreverM
        .handleErrorWith(e =>
          IO(System.err.println(s"[raft] group $group on $id stopped: $e")) >> IO(e.printStackTrace()) >> failure.set(Some(e))
        )
        .background
      _ <- (IO.sleep(config.tickInterval) >> events.offer(Event.Tick)).foreverM.background
    yield new RaftNode(id, events, statusRef, sm, chunkSeq, failure)

  /** イベントループの状態。1 つのファイバーからだけ触る。 */
  private final class Loop(
      id: NodeId,
      group: Long,
      core: RaftCore,
      storage: RaftStorage,
      sm: StateMachine,
      transport: RaftTransport,
      config: RaftNodeConfig,
      limits: SnapshotLimits,
      statusRef: Ref[IO, RaftStatus],
      outbound: Ref[IO, Map[NodeId, Queue[IO, Message]]],
      senders: Ref[IO, Map[NodeId, cats.effect.Fiber[IO, Throwable, Nothing]]],
      initialApplied: Long,
      initialSnapIndex: Long,
      startPersist: IO[Option[SnapshotMeta]] => IO[Unit],
      startSnapshotSend: (NodeId, IO[Unit]) => IO[Unit]
  ):
    private var nextReq = 1L
    private var applied = initialApplied
    private var snapIndex = initialSnapIndex
    private var bytesSinceSnapshot = 0L
    private var persisting = false
    /** この位置までのログは fsync 済み（前の周までに書いた分）。 */
    private var durableIndex = 0L
    /** スナップショットを送っている相手（相手ごとに 1 本だけ）。 */
    private val sendingSnapshots = mutable.Set.empty[NodeId]
    private var snapshotSeq = 0L
    private val awaitingAccept = mutable.Map.empty[Long, Either[Deferred[IO, Either[Throwable, ByteVector]], Deferred[IO, Either[Throwable, Unit]]]]
    private val byIndex = mutable.Map.empty[Long, (Long, Deferred[IO, Either[Throwable, ByteVector]])]
    private val memberWaiters = mutable.ArrayBuffer.empty[(Long, Long, Deferred[IO, Either[Throwable, Unit]])] // (index, term, d)
    private val readWaiters = mutable.Map.empty[Long, Deferred[IO, Either[Throwable, Unit]]]
    private val readsByIndex = mutable.ArrayBuffer.empty[(Long, Deferred[IO, Either[Throwable, Unit]])]

    private def req(): Long = { val r = nextReq; nextReq += 1; r }

    /** まだファイルに書いていない永続化の内容（1 周分をまとめて 1 回で書く）。 */
    private val pending = mutable.ArrayBuffer.empty[(Option[HardState], Option[Long], Vector[LogEntry])]

    private def flushPending: IO[Unit] =
      IO.defer {
        if pending.isEmpty then IO.unit
        else
          val items = pending.toVector
          pending.clear()
          storage.appendAll(items)
      }

    def handle(all: List[Event]): IO[Unit] =
      val (done, evs) = all.partitionMap {
        case Event.Persisted(m) => Left(m)
        case ev                 => Right(ev)
      }
      all.foreach { case Event.SnapshotSent(to) => sendingSnapshots -= to; case _ => () }
      for
        posts <- evs.traverse {
          case Event.SnapshotSent(_) => IO.pure(None)
          // 揃ったスナップショットは、中身を預かりに置いたまま Message.Snapshot として RaftCore に渡す
          case Event.SnapshotReady(from, term, meta, sid) =>
            IO.defer(stepAndPersist(Input.Receive(from, Message.Snapshot(term, meta, ByteVector.empty)), Some(sid))).map(Some(_))
          case ev => IO.defer(stepAndPersist(toInput(ev), None)).map(Some(_))
        }.map(_.flatten)
        _ <- flushPending
        // リーダーの Append は自分の fsync を待たずに送る（Raft 論文 §10.2.1）。安全なのは、フォロワーの応答が
        // このループの次の周以降でしか処理されず、そのときにはこの周の fsync が済んでいるから（自分の分を数えて
        // コミットするのは、永続化した後に限られる）。投票の応答などは従来どおり fsync の後に送る。
        _ <- posts.traverse_(_.ready.messages.traverse_ { case (to, m: Message.Append) => enqueue(to, m); case _ => IO.unit })
        // 前の周までに永続化したエントリのコミットは、この周の fsync を待たずに適用して応答する（コミットの判断に
        // 使った過半数の永続化は済んでいる）。この周に書いたエントリ（単一ノードのグループの提案など）は fsync の後
        _ <- posts.traverse_(_.ready.committed.traverse_(e => if e.index <= durableIndex then applyEntry(e) else IO.unit))
        _ <- storage.sync.whenA(posts.exists(_.persisted))
        _ <- IO { durableIndex = core.lastIndex }
        _ <- posts.traverse_(afterPersist)
        _ <- done.traverse_(compact)
        _ <- maybeCompact
        _ <- statusRef.set(core.status.copy(applied = applied))
      yield ()

    private def toInput(ev: Event): Input = ev match
      case Event.Tick           => Input.Tick
      case _: Event.Persisted | _: Event.SnapshotReady | _: Event.SnapshotSent => throw IllegalStateException(s"not an input: $ev")
      case Event.Msg(from, m)   => Input.Receive(from, m)
      case Event.Transfer(to)   => Input.TransferLeader(to)
      case Event.Propose(data, d) =>
        val r = req(); awaitingAccept(r) = Left(d); Input.Propose(r, data)
      case Event.Read(d) =>
        val r = req(); readWaiters(r) = d; Input.ReadIndex(r)
      case Event.Members(v, l, d) =>
        val r = req(); awaitingAccept(r) = Right(d); Input.ProposeMembership(r, v, l)

    private final case class Post(ready: Ready, persisted: Boolean)

    /** step して、永続化の書き込み（fsync 以外）まで行う。staged は、届いたスナップショットの預かりの id。 */
    private def stepAndPersist(input: Input, staged: Option[Long]): IO[Post] =
      val r = core.step(input)
      // スナップショットでログを作り直す前に、溜めていた書き込みを済ませる。状態マシンに取り込んでから（永続化まで）
      // ストレージを書き換える。間で落ちても、起動時に状態マシンの persistedSnapshot から再開できる
      val installSnapshot = r.snapshot match
        case Some((meta, data)) =>
          for
            _ <- flushPending
            stored <- limits.installs.fold(sm.install(meta, staged, data))(_.permit.surround(sm.install(meta, staged, data)))
            _ <- storage.rewrite(meta, stored.getOrElse(ByteVector.empty), core.hardState, core.logEntries)
            _ <- IO { applied = meta.index; snapIndex = meta.index; bytesSinceSnapshot = 0L }
          yield ()
        // RaftCore が受け取らなかった（古い、または既に持っている）
        case None => staged.traverse_(sm.discardStaged)
      val needsWrite = r.hardState.isDefined || r.truncateFrom.isDefined || r.append.nonEmpty
      // 切り詰めた位置からは、新しいエントリがまだ fsync されていない
      r.truncateFrom.foreach(t => durableIndex = math.min(durableIndex, t - 1))
      installSnapshot >> IO(if needsWrite then pending += ((r.hardState, r.truncateFrom, r.append))).as(Post(r, needsWrite))

    private def afterPersist(p: Post): IO[Unit] =
      val r = p.ready
      for
        _ <- r.messages.traverse_ { case (_, _: Message.Append) => IO.unit; case (to, m) => enqueue(to, m) }
        _ <- r.snapshotSends.traverse_(startSnapshot)
        _ <- r.accepted.traverse_ { (reqId, index, term) =>
          IO(awaitingAccept.remove(reqId)).flatMap {
            case Some(Left(d))  => IO(byIndex(index) = (term, d))
            case Some(Right(d)) => IO(memberWaiters += ((index, term, d)))
            case None           => IO.unit
          }
        }
        _ <- r.rejected.traverse_ { (reqId, err) =>
          val ex = err match
            case RaftError.NotLeader(h) => NotLeaderException(h)
            case RaftError.Busy(why)    => RaftBusyException(why)
          IO(awaitingAccept.remove(reqId)).flatMap {
            case Some(w) => w.fold(_.complete(Left(ex)), _.complete(Left(ex))).void
            case None    => IO(readWaiters.remove(reqId)).flatMap(_.traverse_(_.complete(Left(ex))))
          }
        }
        _ <- r.committed.traverse_(applyEntry)
        _ <- r.reads.traverse_ { (reqId, index) =>
          IO(readWaiters.remove(reqId)).flatMap(_.traverse_ { d =>
            if applied >= index then d.complete(Right(())).void else IO(readsByIndex += (index -> d))
          })
        }
        _ <- releaseReads
      yield ()

    private def applyEntry(e: LogEntry): IO[Unit] =
      if e.index <= applied then IO.unit
      else
        val cmd = e.payload match
          case Payload.Command(d) => Some(d)
          case _                  => None
        bytesSinceSnapshot += commandBytes(e.payload)
        sm.apply(e.index, cmd).attempt.flatMap { result =>
          IO { applied = e.index } >>
            IO(byIndex.remove(e.index)).flatMap(_.traverse_ { (term, d) =>
              d.complete(if term == e.term then result else Left(ProposalDroppedException())).void
            }) >>
            settleMembers(e)
        }

    private def settleMembers(e: LogEntry): IO[Unit] =
      val done = memberWaiters.filter { (idx, term, _) =>
        (idx == e.index && term != e.term) ||
        (e.index >= idx && (e.payload match
          case Payload.Config(m) => !m.isJoint
          case _                 => false))
      }
      memberWaiters --= done
      done.toList.traverse_ { (idx, term, d) =>
        d.complete(if idx == e.index && term != e.term then Left(ProposalDroppedException()) else Right(())).void
      }

    private def releaseReads: IO[Unit] =
      val ready = readsByIndex.filter(_._1 <= applied)
      readsByIndex --= ready
      ready.toList.traverse_(_._2.complete(Right(())).void)

    /** 閾値を超えたら、状態マシンの永続化を裏で始める。終わったら Persisted が届き、compact でログを切り詰める。 */
    private def maybeCompact: IO[Unit] =
      compactionTarget match
        case None => IO.unit
        case Some(target) =>
          val meta = core.metaAt(target)
          persisting = true
          startPersist(sm.persist.map(ok => Option.when(ok)(meta)))

    /** どこまで切り詰めるか。リーダーはフォロワーが受け取り済みの位置より先は切り詰めない（上限を超えたら切り詰める）。
      * 少ししか進まないなら待つ（切り詰めのたびに LSM を flush するので、毎周起こさない）。
      */
    private def compactionTarget: Option[Long] =
      val retained = applied - snapIndex
      if persisting || retained <= 0 || (retained < config.snapshotThreshold && bytesSinceSnapshot < config.snapshotBytes) then None
      else
        val overCap = bytesSinceSnapshot >= config.maxRetainedBytes || retained >= config.snapshotThreshold * 10
        val target = if overCap then applied else math.min(applied, core.followerMatchFloor.getOrElse(applied))
        Option.when(target > snapIndex && (overCap || (target - snapIndex) * 4 >= retained))(target)

    private def compact(result: Either[Throwable, Option[SnapshotMeta]]): IO[Unit] =
      persisting = false
      val target = result match
        case Left(_)          => None
        case Right(Some(meta)) => Option.when(meta.index > snapIndex)((meta, IO.pure(ByteVector.empty)))
        // persist に対応しない状態マシンは、いまの適用位置の snapshot を保存する
        case Right(None) => Option.when(applied > snapIndex)((core.metaAt(applied), sm.snapshot))
      target.traverse_ { (meta, makeData) =>
        for
          data <- makeData
          _ <- flushPending
          // ここから Compacted を渡すまで core は進まないので、残すエントリは一度だけ取り出す
          kept = core.logEntries.filter(_.index > meta.index)
          _ <- storage.rewrite(meta, data, core.hardState, kept)
          _ <- IO {
            snapIndex = meta.index
            // 残したエントリ（切り詰めた位置より後で、適用済みの分）の大きさから数え直す
            bytesSinceSnapshot = kept.iterator.filter(_.index <= applied).map(e => commandBytes(e.payload)).sum
          }
          r = core.step(Input.Compacted(meta))
          _ <- IO.raiseUnless(r.isEmpty)(IllegalStateException(s"unexpected output from compaction: $r"))
        yield ()
      }

    /** スナップショットを裏で送り始める。送るたびに、いまの適用位置の一貫したビューから作る（保存済みのものは古いことが
      * ある）。RaftCore は応答が来るまで選挙のタイムアウトごとに送り直しを求めるが、送っている間は重ねない。
      */
    private def startSnapshot(to: NodeId): IO[Unit] =
      if sendingSnapshots.contains(to) then IO.unit
      else
        // ビューはいま（適用位置と揃えて）取り、送信の枠は裏で待つ（待つ間に留める memtable は、取る前の flush で小さい）
        val meta = core.metaAt(applied)
        val term = core.currentTerm
        snapshotSeq += 1
        val sid = (scala.util.hashing.MurmurHash3.stringHash(s"$id/$group").toLong << 32) | (snapshotSeq & 0xffffffffL)
        sendingSnapshots += to
        sm.snapshotSource.allocated
          .flatMap { (chunks, release) =>
            val send = transport.sendSnapshot(group, id, to, term, meta, sid, chunks).attempt.void
            startSnapshotSend(to, limits.sends.fold(send)(_.permit.surround(send)).guarantee(release))
          }
          .handleErrorWith(_ => IO { sendingSnapshots -= to })

    private def enqueue(to: NodeId, m: Message): IO[Unit] =
      outbound.get.flatMap(_.get(to) match
        case Some(q) => q.tryOffer(m).void
        case None =>
          Queue.bounded[IO, Message](config.outboundQueue).flatMap { q =>
            outbound.update(_.updated(to, q)) >>
              // 送っている間に溜まった分は、次の 1 回にまとめる
              takeBatch(q).flatMap(ms => transport.sendAll(group, id, to, ms.toVector).attempt).foreverM.start.flatMap(f => senders.update(_.updated(to, f))) >>
              q.tryOffer(m).void
          })
