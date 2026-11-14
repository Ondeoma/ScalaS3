package scalas3.meta

import cats.effect.{IO, Ref, Resource}
import cats.syntax.all.*
import fs2.Stream
import scalas3.core.{ByteReader, ByteWriter, Bytes}
import scalas3.lsm.{Lsm, WriteOp}
import scalas3.lsm.LsmSnapshot
import scalas3.raft.{NodeId, RaftCodec, SnapshotMeta, StateMachine}
import scodec.bits.ByteVector

/** 1 つのレンジの状態マシン。ノード内で共有する LSM に、自分の区間のキーとレンジ固有のキーだけを書く。
  *
  * 適用のたびに、変更と適用位置（RangeLocal/Applied）を同じバッチで書くので、
  * LSM が電源断で巻き戻っても「どこまで適用したか」と中身は常に一致する。
  */
final class RangeStateMachine private (
    val rangeId: Long,
    lsm: Lsm,
    descriptor: Ref[IO, Option[RangeDescriptor]],
    onSplit: RangeDescriptor => IO[Unit],
    stats: Ref[IO, Option[Long]], // approxBytes のメモリ上の写し（None なら LSM から読む）
    frozenRef: Ref[IO, Boolean],
    /** 統合で取り込む右のレンジの、このノードのレプリカを止めて片付ける（RangeManager.retire）。 */
    onRetire: Long => IO[Unit]
) extends StateMachine:

  def current: IO[Option[RangeDescriptor]] = descriptor.get

  /** 統合のために凍結されているか。 */
  def frozen: IO[Boolean] = frozenRef.get

  private def frozenKey = Keys.rangeLocal(rangeId, Keys.Frozen)
  private def descriptorKey = Keys.rangeLocal(rangeId, Keys.Descriptor)

  private def requireDescriptor: IO[RangeDescriptor] =
    descriptor.get.flatMap(IO.fromOption(_)(IllegalStateException(s"range $rangeId not initialized")))
  private def reloadFrozen: IO[Unit] = lsm.get(frozenKey).flatMap(v => frozenRef.set(v.isDefined))

  def appliedIndex: IO[Long] = lsm.get(Keys.rangeLocal(rangeId, Keys.Applied)).map(_.fold(0L)(_.toLong()))

  private def appliedOp(index: Long) = WriteOp.Put(Keys.rangeLocal(rangeId, Keys.Applied), ByteVector.fromLong(index))
  private def statsKey = Keys.rangeLocal(rangeId, Keys.Stats)

  /** レンジに書かれたデータのおおよその量（分割の判断に使う）。 */
  def approxBytes: IO[Long] =
    stats.get.flatMap {
      case Some(v) => IO.pure(v)
      case None    => lsm.get(statsKey).map(_.fold(0L)(_.toLong())).flatTap(v => stats.set(Some(v)))
    }

  def apply(index: Long, command: Option[ByteVector]): IO[ByteVector] =
    command match
      case None => lsm.write(Seq(appliedOp(index))).as(ByteVector.empty)
      case Some(bytes) =>
        IO(MetaCodec.command(bytes)).attempt.flatMap {
          case Left(e)                              => finish(index, WriteResult.Rejected(s"bad command: $e"))
          case Right(Command.Write(conds, ops))     => write(index, conds, ops)
          case Right(Command.Split(key, newId, gen)) => split(index, key, newId, gen)
          case Right(Command.SetReplicas(rs, gen))   => setReplicas(index, rs, gen)
          case Right(Command.Freeze(gen))            => freeze(index, gen)
          case Right(Command.Merge(right, es, gen, bytes)) => merge(index, right, es, gen, bytes)
        }

  private def finish(index: Long, r: WriteResult): IO[ByteVector] =
    lsm.write(Seq(appliedOp(index))).as(MetaCodec.writeResult(r))

  private def write(index: Long, conds: Vector[Precondition], ops: Vector[KvOp]): IO[ByteVector] =
    (descriptor.get, frozenRef.get).tupled.flatMap {
      case (None, _) => finish(index, WriteResult.Rejected("range not initialized"))
      // 凍結したレンジは書き込みを受けない（クライアントは記述子を読み直し、統合後の左のレンジに送り直す）
      case (Some(d), true) => finish(index, WriteResult.NotInRange(d))
      case (Some(d), _) if !(conds.map(_.key) ++ ops.map(_.key)).forall(d.contains) => finish(index, WriteResult.NotInRange(d))
      case (Some(_), _) =>
        conds.zipWithIndex
          .findM { (c, _) =>
            lsm.get(c.key).map { v =>
              c match
                case Precondition.Absent(_)       => v.isDefined
                case Precondition.Exists(_)       => v.isEmpty
                case Precondition.Equals(_, want) => !v.exists(Bytes.equal(_, want))
            }
          }
          .flatMap {
            case Some((_, i)) => finish(index, WriteResult.ConditionFailed(i))
            case None =>
              for
                before <- approxBytes
                delta = ops.map {
                  case KvOp.Put(k, v) => k.size + v.size
                  case KvOp.Delete(_) => 0L
                }.sum
                lsmOps = ops.map {
                  case KvOp.Put(k, v) => WriteOp.Put(k, v)
                  case KvOp.Delete(k) => WriteOp.Delete(k)
                }
                _ <- lsm.write(lsmOps ++ Seq(WriteOp.Put(statsKey, ByteVector.fromLong(before + delta)), appliedOp(index)))
                _ <- stats.set(Some(before + delta))
              yield MetaCodec.writeResult(WriteResult.Ok)
          }
    }

  private def split(index: Long, key: ByteVector, newId: Long, gen: Long): IO[ByteVector] =
    (descriptor.get, frozenRef.get).tupled.flatMap {
      case (None, _) => finish(index, WriteResult.Rejected("range not initialized"))
      case (Some(_), true) => finish(index, WriteResult.Rejected("range is frozen for a merge"))
      case (Some(d), _) =>
        if d.generation != gen then finish(index, WriteResult.Rejected("generation mismatch"))
        else if !d.contains(key) || key == d.start then finish(index, WriteResult.Rejected("split key outside range"))
        else
          val left = d.copy(end = Some(key), generation = d.generation + 1)
          // 子は分割後の親と同じ世代から始める。こうすると、分割前の親の記述子（世代が小さい）と重なったとき、新しいほうが選ばれる
          val right = RangeDescriptor(newId, key, d.end, d.replicas, d.generation + 1)
          for
            existing <- lsm.get(Keys.rangeLocal(newId, Keys.Descriptor))
            r <-
              if existing.isDefined then
                // 子レンジが既にこのノードにある（スナップショットで先に作られた）。左側の縮小だけは全レプリカで同じように行う
                lsm.write(
                  Seq(WriteOp.Put(descriptorKey, RangeDescriptor.encode(left)), appliedOp(index))
                ) >> descriptor.set(Some(left)).as(MetaCodec.writeResult(WriteResult.Ok))
              else
                for
                  bytes <- approxBytes
                  _ <- lsm.write(
                    Seq(
                      WriteOp.Put(descriptorKey, RangeDescriptor.encode(left)),
                      WriteOp.Put(statsKey, ByteVector.fromLong(bytes / 2)),
                      WriteOp.Put(Keys.rangeLocal(newId, Keys.Descriptor), RangeDescriptor.encode(right)),
                      WriteOp.Put(Keys.rangeLocal(newId, Keys.RaftInit), MetaCodec.snapshotMeta(MetaCodec.splitInitial(d.replicas))),
                      WriteOp.Put(Keys.rangeLocal(newId, Keys.Applied), ByteVector.fromLong(MetaCodec.splitInitial(d.replicas).index)),
                      WriteOp.Put(Keys.rangeLocal(newId, Keys.Stats), ByteVector.fromLong(bytes / 2)),
                      appliedOp(index)
                    )
                  )
                  _ <- descriptor.set(Some(left))
                  _ <- stats.set(Some(bytes / 2))
                  _ <- onSplit(right)
                yield MetaCodec.writeResult(WriteResult.Ok)
          yield r
    }

  private def setReplicas(index: Long, rs: Set[NodeId], gen: Long): IO[ByteVector] =
    descriptor.get.flatMap {
      case Some(d) if d.generation == gen =>
        val nd = d.copy(replicas = rs, generation = d.generation + 1)
        lsm.write(Seq(WriteOp.Put(descriptorKey, RangeDescriptor.encode(nd)), appliedOp(index))) >>
          descriptor.set(Some(nd)).as(MetaCodec.writeResult(WriteResult.Ok))
      case _ => finish(index, WriteResult.Rejected("generation mismatch"))
    }

  private def freeze(index: Long, gen: Long): IO[ByteVector] =
    (descriptor.get, frozenRef.get).tupled.flatMap {
      case (Some(_), true) => finish(index, WriteResult.Ok) // 既に凍結済み
      case (Some(d), false) if d.generation == gen && d.id != 0L =>
        lsm.write(Seq(WriteOp.Put(frozenKey, ByteVector.empty), appliedOp(index))) >> frozenRef.set(true).as(MetaCodec.writeResult(WriteResult.Ok))
      case _ => finish(index, WriteResult.Rejected("generation mismatch"))
    }

  /** 右隣の凍結したレンジを取り込む。
    *   1. 手元の右のレプリカを止めて片付ける（遅れて動いている右が、後から古いデータを区間に書き込まないように）
    *   2. 右の区間の手元のデータを消す（右のレプリカの進み具合はノードごとに違うので、コマンドの中身で置き換える）
    *   3. 中身、広げた記述子、適用位置を 1 回で書く
    * 途中で落ちても適用位置が進んでいないので、起動後にこのコマンドをもう一度適用する（何度行っても同じ結果になる）。
    */
  private def merge(index: Long, right: RangeDescriptor, entries: Vector[(ByteVector, ByteVector)], gen: Long, bytes: Long): IO[ByteVector] =
    (descriptor.get, frozenRef.get).tupled.flatMap {
      case (Some(d), false) if d.generation == gen && d.end.contains(right.start) && right.id != rangeId && right.id != 0L && rangeId != 0L =>
        val merged = d.copy(end = right.end, generation = math.max(d.generation, right.generation) + 1)
        for
          _ <- onRetire(right.id)
          _ <- deleteKeys(right.start, right.end, _ => true)
          _ <- lsm.write(
            entries.filter((k, _) => right.contains(k)).map((k, v) => WriteOp.Put(k, v)) ++ Seq(
              WriteOp.Put(descriptorKey, RangeDescriptor.encode(merged)),
              WriteOp.Put(statsKey, ByteVector.fromLong(bytes)),
              appliedOp(index)
            )
          )
          _ <- descriptor.set(Some(merged))
          _ <- stats.set(Some(bytes))
        yield MetaCodec.writeResult(WriteResult.Ok)
      case (_, true) => finish(index, WriteResult.Rejected("range is frozen for a merge"))
      case _         => finish(index, WriteResult.Rejected("generation mismatch or not adjacent"))
    }

  /** 適用位置のキーはデータと同じ書き込みで LSM に入るので、flush すれば適用位置までの状態が永続化される。 */
  override def persist: IO[Boolean] = lsm.flush.as(true)

  /** 区間のキーと、レンジ固有のキー（適用位置を除く）を 1 つのかたまりにする。 */
  def snapshot: IO[ByteVector] =
    for
      _ <- lsm.flush
      d <- requireDescriptor
      snap <- lsm.snapshot.use(rangeEntries(_, d, Set(Keys.rangeLocal(rangeId, Keys.Applied))).compile.toVector)
    yield RangeStateMachine.encodeEntries(snap)

  /** レンジ固有のキー（excluded を除く）と、区間 d のデータ。 */
  private def rangeEntries(s: LsmSnapshot, d: RangeDescriptor, excluded: Set[ByteVector]): Stream[IO, (ByteVector, ByteVector)] =
    val local = Keys.rangeLocalPrefix(rangeId)
    s.scan(local, Bytes.prefixEnd(local)).filter((k, _) => !excluded.contains(k)) ++ s.scan(d.start, d.end)

  // ---- スナップショットの送受信（ストリーム） ----
  //
  // 受け取ったかたまりは、レンジの区間の外の預かり（Keys.staging）に置き、元のデータには触れない。揃ったら
  // 「切り替え中」の印を永続化してから、元のデータを消して預かりから写す。転送の途中で落ちても元のデータは残り、
  // 切り替えの途中で落ちても、起動時に印を見てやり直せる（何度行っても同じ結果になる）。

  private val installing = java.util.concurrent.atomic.AtomicReference[Option[Long]](None)
  private def markerKey = Keys.rangeLocal(rangeId, Keys.Installing)

  /** スナップショットに含めないレンジ固有のキー（受け取った側で書き直す）。 */
  private def excludedLocal: Set[ByteVector] = Set(Keys.Applied, Keys.SnapMeta, Keys.Installing).map(Keys.rangeLocal(rangeId, _))

  override def snapshotSource: Resource[IO, Stream[IO, ByteVector]] =
    for
      d <- Resource.eval(requireDescriptor)
      // 先に flush して、ビューが留める memtable を小さくする（送り終わるまで留め続けるため）
      _ <- Resource.eval(lsm.flush)
      s <- lsm.snapshot
    yield rangeEntries(s, d, excludedLocal).chunkN(RangeStateMachine.ChunkEntries).map(c => RangeStateMachine.encodeEntries(c.toVector))

  // 途中でやめた転送の預かりは RaftNode が discardStaged で消す（どの転送が途中かは RaftNode が知っている）
  override def stageSnapshot(id: Long, seq: Int, chunk: ByteVector): IO[Unit] =
    val prefix = Keys.staging(rangeId, id)
    lsm.write(RangeStateMachine.decodeEntries(chunk).map((k, v) => WriteOp.Put(prefix ++ k, v)))

  override def discardStaged(id: Long): IO[Unit] = deleteStaging(Keys.staging(rangeId, id))

  /** このレンジの預かりをすべて消す（起動時。転送は行われていない）。 */
  private[meta] def deleteAllStaging: IO[Unit] = deleteStaging(Keys.stagingRange(rangeId))

  /** prefix で始まる預かりを消す。取り込み中の id の分は残す。 */
  private def deleteStaging(prefix: ByteVector): IO[Unit] =
    val skip = installing.get.map(Keys.staging(rangeId, _))
    deleteKeys(prefix, Bytes.prefixEnd(prefix), k => !skip.exists(k.startsWith))

  private def deleteKeys(from: ByteVector, until: Option[ByteVector], pred: ByteVector => Boolean): IO[Unit] =
    lsm.snapshot.use(s => RangeStateMachine.deleteAll(lsm, s.scan(from, until).map(_._1).filter(pred)))

  override def install(meta: SnapshotMeta, staged: Option[Long], inline: ByteVector): IO[Option[ByteVector]] =
    (for
      sid <- staged match
        case Some(id) => IO.pure(id)
        // 1 つのメッセージで届いたスナップショットも、いったん預かりに置いてから同じ手順で取り込む
        case None => val id = -1L - meta.index; stageSnapshot(id, 0, inline).as(id)
      _ <- IO(installing.set(Some(sid)))
      d <- lsm
        .get(Keys.staging(rangeId, sid) ++ descriptorKey)
        .flatMap(IO.fromOption(_)(IllegalStateException(s"snapshot for range $rangeId has no descriptor")))
        .map(RangeDescriptor.decode)
      // 切り替え中の印を永続化してから、元のデータに手を付ける
      _ <- lsm.write(Seq(WriteOp.Put(markerKey, RangeStateMachine.encodeMarker(sid, meta, d)))) >> lsm.flush
      _ <- applyStaged(sid, meta, d)
    yield None).guarantee(IO(installing.set(None)))

  /** 預かりからレンジに写し、適用位置とスナップショットの位置を書いて印を消す。最後に flush して永続化する。 */
  private[meta] def applyStaged(sid: Long, meta: SnapshotMeta, d: RangeDescriptor): IO[Unit] =
    val prefix = Keys.staging(rangeId, sid)
    val local = Keys.rangeLocalPrefix(rangeId)
    for
      // 元のレンジ固有のキー（印を除く）と区間のデータを消す
      _ <- deleteKeys(local, Bytes.prefixEnd(local), _ != markerKey)
      _ <- deleteKeys(d.start, d.end, _ => true)
      _ <- lsm.snapshot.use(
        _.scan(prefix, Bytes.prefixEnd(prefix)).chunkN(1000)
          .evalMap(c => lsm.write(c.toVector.map((k, v) => WriteOp.Put(k.drop(prefix.size), v))))
          .compile.drain
      )
      _ <- lsm.write(Seq(appliedOp(meta.index), WriteOp.Put(Keys.rangeLocal(rangeId, Keys.SnapMeta), MetaCodec.snapshotMeta(meta)), WriteOp.Delete(markerKey)))
      _ <- deleteKeys(prefix, Bytes.prefixEnd(prefix), _ => true)
      _ <- descriptor.set(Some(d))
      _ <- stats.set(None)
      _ <- reloadFrozen
      _ <- lsm.flush
    yield ()

  override def persistedSnapshot: IO[Option[SnapshotMeta]] =
    lsm.get(Keys.rangeLocal(rangeId, Keys.SnapMeta)).map(_.map(MetaCodec.snapshotMeta))

  /** 起動時：切り替えの途中で落ちていたらやり直し、残った預かりを消す。 */
  private[meta] def recoverInstall: IO[Unit] =
    lsm.get(markerKey).flatMap(_.traverse_ { m =>
      val (sid, meta, d) = RangeStateMachine.decodeMarker(m)
      applyStaged(sid, meta, d)
    }) >> deleteAllStaging

  def restore(index: Long, data: ByteVector): IO[Unit] =
    for
      entries <- IO(RangeStateMachine.decodeEntries(data))
      d <- IO.fromOption(entries.collectFirst {
        case (k, v) if k == descriptorKey => RangeDescriptor.decode(v)
      })(IllegalStateException(s"snapshot for range $rangeId has no descriptor"))
      // 既存の区間とレンジ固有のキーを消してから入れ直す
      old <- lsm.snapshot.use(rangeEntries(_, d, Set.empty).map(_._1).compile.toVector)
      newKeys = entries.map(_._1).toSet
      _ <- lsm.write(
        old.filterNot(newKeys.contains).map(WriteOp.Delete(_)) ++
          entries.map((k, v) => WriteOp.Put(k, v)) :+ appliedOp(index)
      )
      _ <- descriptor.set(Some(d))
      _ <- stats.set(None)
      _ <- reloadFrozen
    yield ()

  // ---- 読み込み（readBarrier の後で呼ぶこと） ----

  def read(q: ReadRequest): IO[ReadResult] =
    (descriptor.get, frozenRef.get).tupled.flatMap {
      case (None, _) => IO.raiseError(IllegalStateException(s"range $rangeId not initialized"))
      case (Some(d), frozen) =>
        q match
          case ReadRequest.MergeState => IO.pure(ReadResult.Value(Some(MetaCodec.mergeState(frozen, d))))
          case ReadRequest.Export(limit) =>
            if !frozen then IO.pure(ReadResult.Value(None))
            else scanLimited(d.start, d.end, limit)
          // 凍結したレンジは読み込みも受けない（統合後の左のレンジが新しい書き込みを受けているかもしれない）
          case _ if frozen => IO.pure(ReadResult.NotInRange(d))
          case ReadRequest.Get(k) =>
            if !d.contains(k) then IO.pure(ReadResult.NotInRange(d)) else lsm.get(k).map(ReadResult.Value(_))
          case ReadRequest.Scan(from, until, limit) =>
            if !d.contains(from) then IO.pure(ReadResult.NotInRange(d))
            else
              // 区間の終わりで打ち切る
              val stop = (until, d.end) match
                case (Some(u), Some(e)) => Some(if Bytes.compare(u, e) < 0 then u else e)
                case (u, e)             => u.orElse(e)
              scanLimited(from, stop, limit)
    }

  /** 最大 limit 件と、続きがあるか。 */
  private def scanLimited(from: ByteVector, until: Option[ByteVector], limit: Int): IO[ReadResult] =
    lsm.scan(from, until).take(limit.toLong + 1).compile.toVector.map(es => ReadResult.Entries(es.take(limit), es.size > limit))

object RangeStateMachine:
  /** LSM に保存されている記述子から作る（なければ未初期化）。 */
  def load(rangeId: Long, lsm: Lsm, onSplit: RangeDescriptor => IO[Unit], onRetire: Long => IO[Unit] = _ => IO.unit): IO[RangeStateMachine] =
    (lsm.get(Keys.rangeLocal(rangeId, Keys.Descriptor)), lsm.get(Keys.rangeLocal(rangeId, Keys.Frozen))).flatMapN { (d, f) =>
      (Ref.of[IO, Option[RangeDescriptor]](d.map(RangeDescriptor.decode)), Ref.of[IO, Option[Long]](None), Ref.of[IO, Boolean](f.isDefined)).mapN(
        RangeStateMachine(rangeId, lsm, _, onSplit, _, _, onRetire)
      )
    }.flatTap(_.recoverInstall)

  /** keys を 1000 件ずつ消す。 */
  private[meta] def deleteAll(lsm: Lsm, keys: Stream[IO, ByteVector]): IO[Unit] =
    keys.chunkN(1000).evalMap(ks => lsm.write(ks.toVector.map(WriteOp.Delete(_)))).compile.drain

  /** スナップショットのかたまり 1 つに入れるキーの数。 */
  private val ChunkEntries = 512

  /** かたまり：[件数 varInt]([キー lenBytes][値 lenBytes])*。かたまりをつないだものも読める。 */
  private[meta] def encodeEntries(es: Vector[(ByteVector, ByteVector)]): ByteVector =
    val w = ByteWriter(es.foldLeft(16L)((n, kv) => n + kv._1.size + kv._2.size + 8).toInt)
    w.varInt(es.size)
    es.foreach((k, v) => w.lenBytes(k).lenBytes(v))
    w.result()

  private[meta] def decodeEntries(data: ByteVector): Vector[(ByteVector, ByteVector)] =
    val r = ByteReader(data.toArray)
    val out = Vector.newBuilder[(ByteVector, ByteVector)]
    while r.hasRemaining do
      val n = r.varInt()
      var i = 0
      while i < n do
        out += ((ByteVector.view(r.lenBytes()), ByteVector.view(r.lenBytes())))
        i += 1
    out.result()

  private[meta] def encodeMarker(sid: Long, meta: SnapshotMeta, d: RangeDescriptor): ByteVector =
    val w = ByteWriter()
    w.u64(sid)
    RaftCodec.meta(w, meta)
    w.lenBytes(RangeDescriptor.encode(d))
    w.toByteVector

  private def decodeMarker(b: ByteVector): (Long, SnapshotMeta, RangeDescriptor) =
    val r = ByteReader(b.toArray)
    (r.u64(), RaftCodec.meta(r), RangeDescriptor.decode(ByteVector.view(r.lenBytes())))
