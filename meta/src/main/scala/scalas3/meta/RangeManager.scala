package scalas3.meta

import cats.effect.std.{Mutex, Supervisor}
import cats.effect.{IO, Ref, Resource}
import cats.syntax.all.*
import fs2.io.file.Path
import scalas3.core.{ByteReader, ByteWriter, Bytes}
import scalas3.lsm.{Lsm, WriteOp}
import scalas3.raft.*
import scalas3.rpc.{Method, RpcHandler}
import scalas3.runtime.FileSystem
import scodec.bits.ByteVector

import scala.concurrent.duration.*

final case class MetaConfig(
    raft: RaftNodeConfig = RaftNodeConfig(),
    /** これを超えたレンジは分割する。 */
    splitBytes: Long = 64L * 1024 * 1024,
    /** 隣り合う 2 つのレンジの中身の合計がこれ以下なら統合する。0 なら統合しない。右のレンジの中身を 1 つの Raft の
      * エントリで運ぶので、大きくしすぎないこと。分割と統合を繰り返さないよう、splitBytes より十分小さくする。
      */
    mergeBytes: Long = 2L * 1024 * 1024,
    /** 統合してよいか。ローリングアップグレードの途中（統合のコマンドを読めない古い版のノードがいる間）は false にする。 */
    mergeAllowed: IO[Boolean] = IO.pure(true),
    maintenanceInterval: FiniteDuration = 1.second,
    /** 統合できるかを調べる間隔（0 なら管理作業のたび）。調べるたびに、リーダーのレンジとその右隣の中身を mergeBytes まで
      * LSM から読むので、レンジが多いと毎秒の管理作業では重い（75 レンジで、ページキャッシュから毎秒約 200 MB を読み、
      * CPU を 1 コアの半分ほど使った）。統合は急がないので、間隔をあける。
      */
    mergeCheckInterval: FiniteDuration = 1.minute,
    requestTimeout: FiniteDuration = 10.seconds,
    /** 分割点を「オブジェクトの境目」に揃える関数。None なら、その付近では分割しない。 */
    splitBoundary: ByteVector => Option[ByteVector] = Some(_),
    /** レンジを置くべきノード（レイアウトから決める）。None なら今の配置のまま。 */
    placement: (MetaClient, RangeDescriptor) => IO[Option[Set[NodeId]]] = (_, _) => IO.pure(None)
)

final case class Replica(rangeId: Long, node: RaftNode, sm: RangeStateMachine, stop: IO[Unit])

/** ノードが受け持つレンジのレプリカを管理する。
  *   - 起動時に、LSM に記録されたレンジを全部立ち上げる
  *   - 分割を適用したら、新しいレンジのレプリカを立ち上げる
  *   - システムレンジの記述子を定期的に見て、自分が担当なのに持っていないレンジを（データなしで）作る
  *   - 自分がリーダーのレンジについて、記述子の公開と分割を行う
  */
final class RangeManager private (
    val nodeId: NodeId,
    fs: FileSystem,
    dir: Path,
    lsm: Lsm,
    raftLog: SharedRaftLog,
    transport: RaftTransport,
    config: MetaConfig,
    replicas: Ref[IO, Map[Long, Replica]],
    lock: Mutex[IO],
    supervisor: Supervisor[IO],
    seed: Long,
    clientRef: Ref[IO, Option[MetaClient]],
    adopting: Ref[IO, Set[Long]],
    snapshotLimits: SnapshotLimits,
    /** 統合で取り込まれ、このノードで片付けたレンジ。再び立ち上げない。 */
    retired: Ref[IO, Set[Long]],
    /** 最後に統合を調べた時刻（IO.monotonic）。 */
    lastMergeCheck: Ref[IO, Option[FiniteDuration]]
):

  /** 管理作業や未知のグループの処理で使うクライアントを渡す（作成の順序の都合で後から）。 */
  def attach(client: MetaClient): IO[Unit] = clientRef.set(Some(client))

  def replica(rangeId: Long): IO[Option[Replica]] = replicas.get.map(_.get(rangeId))
  def all: IO[Vector[Replica]] = replicas.get.map(_.values.toVector.sortBy(_.rangeId))

  /** レンジのレプリカを立ち上げる（既にあれば何もしない）。 */
  def ensure(rangeId: Long): IO[Unit] =
    lock.lock.surround {
      (replicas.get.map(_.contains(rangeId)), retired.get.map(_.contains(rangeId))).mapN(_ || _).ifM(
        IO.unit,
        for
          sm <- RangeStateMachine.load(rangeId, lsm, d => supervisor.supervise(ensure(d.id)).void, retire)
          init <- lsm.get(Keys.rangeLocal(rangeId, Keys.RaftInit)).map(_.fold(SnapshotMeta(0, 0, Membership.empty))(MetaCodec.snapshotMeta))
          _ <- migrateLegacyLog(rangeId)
          started <- (for
            storage <- Resource.eval(raftLog.storage(rangeId))
            node <- RaftNode.start(nodeId, rangeId, init, storage, sm, transport, config.raft, seed ^ rangeId, snapshotLimits)
          yield node).allocated
          (node, stop) = started
          _ <- replicas.update(_.updated(rangeId, Replica(rangeId, node, sm, stop)))
        yield ()
      )
    }

  private def legacyDir(rangeId: Long): Path = dir / "raft" / rangeId.toString

  /** レンジごとのログ（以前の形式）が残っていれば、共有ログに移して消す。 */
  private def migrateLegacyLog(rangeId: Long): IO[Unit] =
    (fs.exists(legacyDir(rangeId)), raftLog.knows(rangeId)).flatMapN {
      case (true, false) =>
        for
          loaded <- FileRaftStorage.open(fs, legacyDir(rangeId)).use(_.load)
          st <- raftLog.storage(rangeId)
          _ <- loaded.snapshot match
            case Some((meta, data)) => st.rewrite(meta, data, loaded.hs, loaded.entries)
            case None               => st.append(Some(loaded.hs), None, loaded.entries) >> st.sync
          _ <- removeLegacyDir(rangeId)
        yield ()
      case (true, true) => removeLegacyDir(rangeId)
      case _            => IO.unit
    }

  private def removeLegacyDir(rangeId: Long): IO[Unit] =
    val d = legacyDir(rangeId)
    fs.exists(d).ifM(fs.list(d).flatMap(_.traverse_(fs.deleteIfExists)) >> fs.deleteIfExists(d).void, IO.unit)

  def stopAll: IO[Unit] = replicas.getAndSet(Map.empty).flatMap(_.values.toList.traverse_(_.stop.attempt))

  /** LSM に記録されたレンジをすべて立ち上げる。 */
  def recover: IO[Unit] =
    lsm
      .scan(ByteVector(Keys.RangeLocalTag), Some(ByteVector(Keys.SystemTag)))
      // スナップショットの切り替えの途中で落ちたレンジは、記述子が消えていても印が残っている
      .collect { case (k, _) if k.size == 10 && (k(9) == Keys.Descriptor.toByte || k(9) == Keys.Installing.toByte) => k.slice(1, 9).toLong() }
      .changes
      .evalMap(ensure)
      .compile
      .drain >> sweepStaging

  /** 持っていないレンジのスナップショットの預かりを消す（持っているレンジの分は RangeStateMachine.load で消している）。 */
  private def sweepStaging: IO[Unit] =
    replicas.get.flatMap { rs =>
      lsm.snapshot.use(s =>
        RangeStateMachine.deleteAll(
          lsm,
          s.scan(Keys.StagingTag, Bytes.prefixEnd(Keys.StagingTag)).map(_._1).filter(k => k.size < 10 || !rs.contains(k.slice(2, 10).toLong()))
        )
      )
    }

  /** レンジのこのノードのキー：レンジ固有のキー、スナップショットの預かり、data があればその区間のデータ。 */
  private def replicaKeys(rangeId: Long, data: Option[RangeDescriptor]): IO[Vector[ByteVector]] =
    lsm.snapshot.use { s =>
      val local = Keys.rangeLocalPrefix(rangeId)
      val staging = Keys.stagingRange(rangeId)
      (s.scan(local, Bytes.prefixEnd(local)) ++ s.scan(staging, Bytes.prefixEnd(staging)) ++
        data.fold(fs2.Stream.empty.covary[IO])(d => s.scan(d.start, d.end))).map(_._1).compile.toVector
    }

  /** レンジの Raft のログ（共有ログの分と、以前の形式のディレクトリ）を消す。 */
  private def dropLog(rangeId: Long): IO[Unit] = raftLog.drop(rangeId) >> removeLegacyDir(rangeId)

  /** 統合で取り込まれる右のレンジの、このノードのレプリカを止めて片付ける（左のレンジが統合を適用するときに呼ぶ）。
    *
    * 区間のデータは消さない（左のレンジのものになる）。レンジ固有のキー、スナップショットの預かり、Raft のログを消す。
    * 左の Raft の適用の中から呼ばれるので、管理用の lock は取らない（destroy が左を止めようとして lock を持ったまま
    * 左の適用の終わりを待つと、互いに待ち合う）。
    */
  private def retire(rangeId: Long): IO[Unit] =
    for
      _ <- retired.update(_ + rangeId)
      r <- replicas.modify(m => (m - rangeId, m.get(rangeId)))
      _ <- r.traverse_(_.stop.attempt)
      keys <- replicaKeys(rangeId, None)
      // 記述子と切り替え中の印を先に消す（ここで落ちても、起動時に右のレプリカが立ち上がらない）
      local = Set(Keys.Descriptor, Keys.Installing).map(Keys.rangeLocal(rangeId, _))
      _ <- lsm.write(keys.filter(local.contains).map(WriteOp.Delete(_)))
      _ <- RangeStateMachine.deleteAll(lsm, fs2.Stream.emits(keys.filterNot(local.contains)))
      _ <- dropLog(rangeId)
    yield ()

  /** システムレンジに記録されたレンジの記述子。 */
  private def publishedDescriptor(client: MetaClient, rangeId: Long): IO[Option[RangeDescriptor]] =
    client.get(Keys.rangeDescriptor(rangeId)).map(_.map(RangeDescriptor.decode))

  /** このノードのレプリカの記述子（データ未受信のものを除く）。 */
  private def localDescriptors: IO[Vector[RangeDescriptor]] = all.flatMap(_.traverse(_.sm.current)).map(_.flatten)

  /** 管理作業を 1 回行う（テストからも呼べる）。 */
  def maintain(client: MetaClient): IO[Unit] =
    for
      _ <- restartFailed
      rs <- all
      // 統合の相手探しと、取り込まれたレンジの古い記述子の片付けに使う（重なりを解かない一覧）
      leads <- rs.existsM(_.node.status.map(_.role == Role.Leader))
      // 統合のコマンドを読めない古い版のノードがいる間（ローリングアップグレードの途中）は、統合しない
      mergeDue <- mergeCheckDue
      mergeOk <- if config.mergeBytes > 0 && leads && mergeDue then config.mergeAllowed else IO.pure(false)
      system <- if mergeOk then client.systemDescriptors.attempt.map(_.toOption) else IO.pure(None)
      _ <- rs.traverse_ { r =>
        (r.node.status, r.sm.frozen).flatMapN { (st, frozen) =>
          // 凍結したレンジは、取り込まれるのを待つだけ（記述子の公開、配置替え、分割はしない）
          if frozen then cleanupFrozen(client, r).attempt.void
          else if st.role == Role.Leader then
            (publishDescriptor(client, r) >> rebalance(client, r) >> maybeSplit(client, r) >>
              system.traverse_(maybeMerge(client, r, st, _))).attempt.void
          else cleanupRemoved(client, r).attempt.void
        }
      }
      _ <- adoptMissing(client).attempt.void
      _ <- balanceLeaders(client).attempt.void
    yield ()

  /** 前回から mergeCheckInterval 経っていれば、今回を統合を調べる回にする。 */
  private def mergeCheckDue: IO[Boolean] =
    IO.monotonic.flatMap { now =>
      lastMergeCheck.modify {
        case Some(last) if now - last < config.mergeCheckInterval => (Some(last), false)
        case _                                                    => (Some(now), true)
      }
    }

  /** リーダーが 1 台に偏らないよう、受け持ちの多いノードから少ないノードへ譲る（Raft のリーダー移譲）。
    *
    * 各ノードは、手元のレプリカの状態から「どのノードが何個のレンジのリーダーか」を数え、自分の受け持ちが最少のノードより
    * 2 以上多ければ、追いついている投票メンバーのうち受け持ちの最も少ないノードに 1 つ譲る。急に揺れないよう、1 回の
    * 管理作業で譲るのは 1 つだけ。全員が同じ規則で動くので、何周かで差が 1 以内に収まる。
    *
    * レプリカの入れ替え（Learner の追加 → 追いつき → 構成の変更）はリーダーが進めるので、その途中のレンジが手元に
    * 1 つでもあれば譲らない（譲るたびに入れ替えが最初からやり直しになり、いつまでも終わらなかった）。
    */
  private def balanceLeaders(client: MetaClient): IO[Unit] =
    all.flatMap(_.traverse(r => (r.node.status, r.sm.current).mapN((st, d) => (r, st, d)))).flatMap { all3 =>
      // 入れ替えの途中、または入れ替えるべきレンジ（望ましい置き場所と投票メンバーが違う）があれば譲らない
      all3.existsM { (_, st, d) =>
        if st.membership.isJoint || st.membership.learners.nonEmpty || d.exists(_.replicas != st.membership.voters) then IO.pure(true)
        else d.fold(IO.pure(false))(dd => config.placement(client, dd).map(_.exists(want => want.nonEmpty && want != st.membership.voters)))
      }.flatMap(changing => if changing then IO.unit else balanceAmong(all3.map((r, st, _) => (r, st))))
    }

  private def balanceAmong(statuses: Vector[(Replica, RaftStatus)]): IO[Unit] =
    IO.defer {
      val counts = statuses.flatMap(_._2.leader).groupMapReduce(identity)(_ => 1)(_ + _)
      def count(n: NodeId) = counts.getOrElse(n, 0)
      val mine = count(nodeId)
      val candidates = statuses.iterator.collect {
        case (r, st) if st.role == Role.Leader && !st.membership.isJoint =>
          // 追いついている投票メンバーだけに譲る（遅れていると、移譲の間に書き込みが止まる）
          val caughtUp = (st.membership.voters - nodeId).filter(v => st.matched.get(v).exists(_ >= st.commit))
          caughtUp.toList.sortBy(v => (count(v), v)).headOption.map(t => (r, t))
      }.flatten.toList
      candidates
        .filter((_, t) => mine - count(t) >= 2)
        .sortBy((r, t) => (count(t), r.rangeId))
        .headOption
        .traverse_((r, t) => r.node.transferLeadership(t))
    }

  /** イベントループが例外で止まったレプリカを、ストレージと状態マシンから起動し直す（状態マシンの起動時の回復も通る）。 */
  private def restartFailed: IO[Unit] =
    all.flatMap(_.traverse_ { r =>
      r.node.failed.flatMap(_.traverse_ { e =>
        IO(System.err.println(s"[meta] restarting range ${r.rangeId} after failure: $e")) >>
          lock.lock.surround(replicas.update(_ - r.rangeId) >> r.stop.attempt.void) >>
          ensure(r.rangeId)
      })
    })

  /** 知らないグループ宛ての Raft メッセージが来た。新しく自分が加えられたレンジなら、データなしで作る。 */
  def onUnknownGroup(rangeId: Long): IO[Unit] =
    adopting.modify(s => (s + rangeId, !s.contains(rangeId))).flatMap { first =>
      val work = clientRef.get.flatMap(_.traverse_ { client =>
        for
          desc <- publishedDescriptor(client, rangeId)
          local <- localDescriptors
          _ <- desc match
            // 区間が分かり、ローカルのどのレンジとも重ならないときだけ作る（親が分割を適用する前に子を作ると、
            // 親の古い書き込みが子のデータを上書きしてしまう）
            case Some(d) if !local.exists(_.overlaps(d.start, d.end)) => ensure(rangeId)
            case _                                                    => IO.unit
        yield ()
      })
      supervisor.supervise(work.attempt >> IO.sleep(config.maintenanceInterval) >> adopting.update(_ - rangeId)).void.whenA(first)
    }

  /** レイアウトに合わせてレプリカを入れ替える（リーダーだけが行う）。
    *   1. 追加するノードを Learner として加える（スナップショットで追いつく）
    *   2. 追いついたら、Joint Consensus で投票メンバーを新しい組に替える
    *   3. 記述子のレプリカを合わせる
    */
  private def rebalance(client: MetaClient, r: Replica): IO[Unit] =
    (r.sm.current, r.node.status).flatMapN {
      case (Some(d), st) if !st.membership.isJoint =>
        config.placement(client, d).flatMap {
          // 自分が外される側なら、先に残るメンバー（追いついているもの）にリーダーを譲る。リーダーが自分を外す構成の
          // 変更は進まず、レンジがいつまでも移らなかった
          case Some(desired) if desired.nonEmpty && !desired.contains(nodeId) && desired != st.membership.voters =>
            val heirs = (desired & st.membership.voters).filter(v => st.matched.get(v).exists(_ >= st.commit))
            heirs.toList.sorted.headOption.fold(IO.unit)(r.node.transferLeadership)
          case Some(desired) if desired.nonEmpty && desired != st.membership.voters =>
            val toAdd = desired -- st.membership.voters
            val missingLearners = toAdd -- st.membership.learners
            if missingLearners.nonEmpty then
              r.node.changeMembership(st.membership.voters, st.membership.learners ++ missingLearners).timeout(config.requestTimeout)
            else
              val caughtUp = toAdd.forall(n => st.matched.get(n).exists(_ >= st.commit))
              r.node.changeMembership(desired, Set.empty).timeout(config.requestTimeout).whenA(caughtUp)
          case _ =>
            val voters = st.membership.voters
            r.node.propose(MetaCodec.command(Command.SetReplicas(voters, d.generation))).void.whenA(d.replicas != voters && voters.nonEmpty)
        }
      case _ => IO.unit
    }

  /** 自分が外されたレンジのレプリカを片付ける。 */
  private def cleanupRemoved(client: MetaClient, r: Replica): IO[Unit] =
    (publishedDescriptor(client, r.rangeId), r.sm.current).flatMapN {
      case (Some(sys), local) if !sys.replicas.contains(nodeId) && local.forall(_.generation < sys.generation) =>
        destroy(r, local)
      case _ => IO.unit
    }

  private def destroy(r: Replica, local: Option[RangeDescriptor]): IO[Unit] =
    lock.lock.surround {
      for
        _ <- replicas.update(_ - r.rangeId)
        _ <- r.stop.attempt
        keys <- replicaKeys(r.rangeId, local)
        _ <- RangeStateMachine.deleteAll(lsm, fs2.Stream.emits(keys))
        _ <- dropLog(r.rangeId)
      yield ()
    }

  /** 自分のレンジの記述子を、システムレンジの記録より新しければ書き込む。 */
  private def publishDescriptor(client: MetaClient, r: Replica): IO[Unit] =
    r.sm.current.flatMap(_.traverse_ { d =>
      val key = Keys.rangeDescriptor(d.id)
      client.get(key).flatMap { cur =>
        val curGen = cur.map(RangeDescriptor.decode(_).generation)
        if curGen.exists(_ >= d.generation) then IO.unit
        else
          val cond = Precondition.unchanged(key, cur)
          client.write(Vector(cond), Vector(KvOp.Put(key, RangeDescriptor.encode(d)))).void
      }
    })

  private def maybeSplit(client: MetaClient, r: Replica): IO[Unit] =
    (r.sm.approxBytes, r.sm.current).flatMapN {
      case (bytes, Some(d)) if bytes > config.splitBytes && d.id != 0L =>
        findSplitKey(d, bytes).flatMap(_.traverse_ { key =>
          for
            newId <- allocateRangeId(client)
            res <- r.node.propose(MetaCodec.command(Command.Split(key, newId, d.generation))).map(MetaCodec.writeResult)
            _ <- res match
              // 分割した 2 つのレンジの記述子をすぐに公開する（次の管理作業まで待たせない）
              case WriteResult.Ok =>
                client.forget(d.id) >> publishDescriptor(client, r).attempt.void >>
                  r.sm.current.flatMap(_.traverse_ { left =>
                    val right = RangeDescriptor(newId, key, d.end, d.replicas, left.generation)
                    val rk = Keys.rangeDescriptor(newId)
                    client.write(Vector(Precondition.Absent(rk)), Vector(KvOp.Put(rk, RangeDescriptor.encode(right)))).attempt.void
                  })
              case _ => IO.unit
          yield ()
        })
      case _ => IO.unit
    }

  /** 右隣のレンジと統合する（設計 §17「レンジの統合」）。
    *   1. 右隣を探す（記述子の start が自分の end と一致するもの）
    *   2. 右が凍結されていなければ、両方の中身の合計が mergeBytes 以下で、投票メンバーが同じで、構成の変更の途中でない
    *      ときだけ、右に Freeze を提案する
    *   3. 右が凍結されていれば（自分が凍結させたのでも、前のリーダーが凍結させて落ちたのでも）、右の中身を読み、
    *      自分に Merge を提案する。凍結は取り消さないので、統合が終わるまで、左隣のリーダーが管理作業のたびに続きを進める
    *   4. 広げた記述子を公開し、右の記述子をシステムレンジから消す
    */
  private def maybeMerge(client: MetaClient, r: Replica, st: RaftStatus, system: Vector[RangeDescriptor]): IO[Unit] =
    r.sm.current.flatMap {
      case Some(d) if d.id != 0L && d.end.isDefined =>
        sweepAbsorbed(client, d, system) >> {
          val right = system.filter(x => x.id != d.id && x.id != 0L && d.end.contains(x.start)).maxByOption(_.generation)
          right.traverse_ { rd =>
            client.readRange(rd, ReadRequest.MergeState).flatMap {
              case ReadResult.Value(Some(v)) =>
                val (frozen, rdesc) = MetaCodec.mergeState(v)
                if !d.end.contains(rdesc.start) then IO.unit
                else if frozen then absorb(client, r, d, rdesc)
                else
                  val quiet = !st.membership.isJoint && st.membership.learners.isEmpty && d.replicas == st.membership.voters && rdesc.replicas == d.replicas
                  (if quiet then (localBytes(d), localBytes(rdesc)).mapN(_ + _) else IO.pure(Long.MaxValue)).flatMap { total =>
                    if total > mergeLimit then IO.unit
                    else
                      client.proposeTo(rdesc, Command.Freeze(rdesc.generation)).flatMap {
                        case WriteResult.Ok =>
                          IO(System.err.println(s"[meta] merging range ${rdesc.id} into ${d.id} (about $total bytes)")) >> absorb(client, r, d, rdesc)
                        case _ => IO.unit
                      }
                  }
              case _ => IO.unit
            }
          }
        }
      case _ => IO.unit
    }

  /** 凍結した右のレンジの中身を読み、自分に Merge を提案する。 */
  private def absorb(client: MetaClient, r: Replica, d: RangeDescriptor, right: RangeDescriptor): IO[Unit] =
    client.readRange(right, ReadRequest.Export(Int.MaxValue)).flatMap {
      case ReadResult.Entries(es, _) =>
        for
          leftBytes <- localBytes(d)
          bytes = leftBytes + es.foldLeft(0L)((n, kv) => n + kv._1.size + kv._2.size)
          res <- r.node.propose(MetaCodec.command(Command.Merge(right, es, d.generation, bytes))).map(MetaCodec.writeResult)
          _ <- res match
            case WriteResult.Ok =>
              client.forget(right.id) >> client.forget(d.id) >> publishDescriptor(client, r).attempt.void >>
                client.systemDescriptors.flatMap(sys => r.sm.current.flatMap(_.traverse_(sweepAbsorbed(client, _, sys)))).attempt.void
            case _ => IO.unit
        yield ()
      case _ => IO.unit
    }

  /** 自分の区間に含まれる、世代の古い別のレンジの記述子（統合で取り込んだもの）をシステムレンジから消す。 */
  private def sweepAbsorbed(client: MetaClient, d: RangeDescriptor, system: Vector[RangeDescriptor]): IO[Unit] =
    system
      .filter(x => x.id != d.id && x.id != 0L && x.generation < d.generation && d.contains(x.start) && d.end.forall(e => x.end.exists(xe => Bytes.compare(xe, e) <= 0)))
      .traverse_ { x =>
        val key = Keys.rangeDescriptor(x.id)
        client.write(Vector(Precondition.Equals(key, RangeDescriptor.encode(x))), Vector(KvOp.Delete(key))).attempt.void
      }

  /** 統合する上限。分割と統合を繰り返さないよう、分割の大きさの 1/4 を超えないようにする。 */
  private def mergeLimit: Long = math.min(config.mergeBytes, config.splitBytes / 4)

  /** 区間の中身のおおよその量（このノードの LSM から。mergeLimit を超えたら数えるのをやめる）。 */
  private def localBytes(d: RangeDescriptor): IO[Long] =
    lsm.scan(d.start, d.end)
      .scan(0L)((n, kv) => n + kv._1.size + kv._2.size)
      .takeThrough(_ <= mergeLimit)
      .compile
      .last
      .map(_.getOrElse(0L))

  /** 凍結したまま残ったレプリカ（統合した左のレンジがこのノードにない、または左が統合を適用する前に記述子が消えた）を
    * 片付ける。区間のデータは消さない（左のレンジが持っているかもしれない）。
    */
  private def cleanupFrozen(client: MetaClient, r: Replica): IO[Unit] =
    client.get(Keys.rangeDescriptor(r.rangeId)).flatMap {
      case None =>
        replicas.get.map(_.contains(r.rangeId)).ifM(IO(System.err.println(s"[meta] retiring merged range ${r.rangeId}")) >> retire(r.rangeId), IO.unit)
      case _    => IO.unit
    }

  /** 区間のおよそ真ん中で、境界関数が認めるキーを探す。 */
  private def findSplitKey(d: RangeDescriptor, approx: Long): IO[Option[ByteVector]] =
    lsm
      .scan(d.start, d.end)
      .scan((0L, Option.empty[ByteVector])) { case ((acc, _), (k, v)) => (acc + k.size + v.size, Some(k)) }
      .dropWhile(_._1 < approx / 2)
      .map(_._2.flatMap(config.splitBoundary))
      .collectFirst { case Some(k) if Bytes.compare(k, d.start) > 0 && d.end.forall(e => Bytes.compare(k, e) < 0) => k }
      .compile
      .last

  private def allocateRangeId(client: MetaClient): IO[Long] =
    client.get(Keys.NextRangeId).flatMap { cur =>
      val next = cur.fold(2L)(_.toLong())
      val cond = Precondition.unchanged(Keys.NextRangeId, cur)
      client.write(Vector(cond), Vector(KvOp.Put(Keys.NextRangeId, ByteVector.fromLong(next + 1)))).flatMap {
        case WriteResult.Ok => IO.pure(next)
        case _              => allocateRangeId(client)
      }
    }

  /** 自分が担当なのにローカルに無いレンジを、データなしで作る（リーダーからスナップショットが届く）。
    * ただし、ローカルの既存レンジと区間が重なる間は作らない（親レンジがまだ分割を適用していない）。
    */
  private def adoptMissing(client: MetaClient): IO[Unit] =
    for
      descs <- client.descriptors
      local <- localDescriptors
      _ <- descs
        .filter(d => d.replicas.contains(nodeId) && !local.exists(_.id == d.id))
        .filterNot(d => local.exists(l => l.overlaps(d.start, d.end)))
        .traverse_(d => ensure(d.id))
    yield ()

object RangeManager:
  def apply(
      nodeId: NodeId,
      fs: FileSystem,
      dir: Path,
      lsm: Lsm,
      transport: RaftTransport,
      config: MetaConfig,
      seed: Long
  ): Resource[IO, RangeManager] =
    for
      sup <- Supervisor[IO](await = false)
      replicas <- Resource.eval(Ref.of[IO, Map[Long, Replica]](Map.empty))
      lock <- Resource.eval(Mutex[IO])
      clientRef <- Resource.eval(Ref.of[IO, Option[MetaClient]](None))
      adopting <- Resource.eval(Ref.of[IO, Set[Long]](Set.empty))
      retired <- Resource.eval(Ref.of[IO, Set[Long]](Set.empty))
      lastMergeCheck <- Resource.eval(Ref.of[IO, Option[FiniteDuration]](None))
      // 共有ログはレプリカより長く生かす（Resource は逆順に解放される）
      raftLog <- SharedRaftLog.open(fs, dir / "raftlog")
      // スナップショットはノード全体で、送信 2 本・取り込み 1 本まで（RaftNode の SnapshotLimits を参照）
      limits <- Resource.eval(SnapshotLimits(sends = 2, installs = 1))
      rm <- Resource.make(IO.pure(new RangeManager(nodeId, fs, dir, lsm, raftLog, transport, config, replicas, lock, sup, seed, clientRef, adopting, limits, retired, lastMergeCheck)))(_.stopAll)
    yield rm

  /** クラスタを作る。全初期メンバーが同じ内容を書くので、レンジ 0 と 1 の「インデックス 0 時点の状態」が一致する。 */
  def bootstrap(lsm: Lsm, members: Set[NodeId], extra: Seq[(ByteVector, ByteVector)] = Nil): IO[Unit] =
    lsm.get(Keys.rangeLocal(0, Keys.Descriptor)).flatMap {
      case Some(_) => IO.unit
      case None =>
        // 初期状態（記述子など）はログを通さずに書くので、分割で生まれたレンジと同じく、初期位置を 0 より大きくする。
        // ログが空のまま加わったレプリカは、ログの再生ではなくスナップショットで追いつくことになり、初期状態も受け取れる
        // （0 だと、記述子のないレプリカができ、それがリーダーになるとレンジの入れ替えが止まった）
        val initial = MetaCodec.splitInitial(members)
        val init = MetaCodec.snapshotMeta(initial)
        val system = RangeDescriptor(0, Keys.SystemStart, Some(Keys.SystemEnd), members, 1)
        val data = RangeDescriptor(1, Keys.DataStart, None, members, 1)
        val ops = Seq(system, data).flatMap { d =>
          Seq(
            WriteOp.Put(Keys.rangeLocal(d.id, Keys.Descriptor), RangeDescriptor.encode(d)),
            WriteOp.Put(Keys.rangeLocal(d.id, Keys.RaftInit), init),
            WriteOp.Put(Keys.rangeLocal(d.id, Keys.Applied), ByteVector.fromLong(initial.index)),
            WriteOp.Put(Keys.rangeDescriptor(d.id), RangeDescriptor.encode(d))
          )
        } ++ extra.map((k, v) => WriteOp.Put(k, v)) :+ WriteOp.Put(Keys.NextRangeId, ByteVector.fromLong(2))
        lsm.write(ops) >> lsm.flush
    }

/** レンジへの読み書きを受け付ける RPC。 */
object MetaService:

  enum Reply:
    case Ok(payload: ByteVector)
    case NotLeader(hint: Option[NodeId])
    case RangeNotFound
    case Dropped

  private def encodeReply(r: Reply): ByteVector =
    val w = ByteWriter()
    r match
      case Reply.Ok(p)        => w.u8(0).lenBytes(p)
      case Reply.NotLeader(h) => w.u8(1); h.fold(w.u8(0))(x => w.u8(1).string(x))
      case Reply.RangeNotFound => w.u8(2)
      case Reply.Dropped      => w.u8(3)
    w.toByteVector

  private def decodeReply(b: ByteVector): Reply =
    val r = ByteReader(b.toArray)
    r.u8() match
      case 0 => Reply.Ok(ByteVector.view(r.lenBytes()))
      case 1 => Reply.NotLeader(if r.u8() == 1 then Some(r.string()) else None)
      case 2 => Reply.RangeNotFound
      case _ => Reply.Dropped

  private val reply: scodec.Codec[Reply] = scodec.codecs.bytes.xmap(decodeReply, encodeReply)
  private val req: scodec.Codec[(Long, ByteVector)] = scodec.codecs.int64 :: scodec.codecs.bytes

  val Write: Method[(Long, ByteVector), Reply] = Method(200, "meta.write", req, reply)
  val Read: Method[(Long, ByteVector), Reply] = Method(201, "meta.read", req, reply)

  private def guard(io: IO[ByteVector]): IO[Reply] =
    io.map(Reply.Ok(_)).recover {
      case e: NotLeaderException     => Reply.NotLeader(e.leaderHint)
      case _: ProposalDroppedException => Reply.Dropped
    }

  def write(rm: RangeManager, rangeId: Long, cmd: ByteVector): IO[Reply] =
    rm.replica(rangeId).flatMap {
      case None    => IO.pure(Reply.RangeNotFound)
      case Some(r) => guard(r.node.propose(cmd))
    }

  def read(rm: RangeManager, rangeId: Long, q: ByteVector): IO[Reply] =
    rm.replica(rangeId).flatMap {
      case None => IO.pure(Reply.RangeNotFound)
      case Some(r) =>
        r.sm.current.flatMap {
          case None => IO.pure(Reply.RangeNotFound) // データ未受信
          case Some(_) =>
            guard(r.node.readBarrier >> IO(MetaCodec.readRequest(q)).flatMap(r.sm.read).map(MetaCodec.readResult))
        }
    }

  def handler(rm: RangeManager): RpcHandler =
    RpcHandler.of(
      Write.handledBy((id, cmd) => write(rm, id, cmd)),
      Read.handledBy((id, q) => read(rm, id, q))
    )
