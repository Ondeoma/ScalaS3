package scalas3.s3

import cats.effect.IO
import cats.syntax.all.*
import fs2.Stream
import scalas3.core.Bytes
import scalas3.meta.{KvOp, MetaClient, Precondition, WriteResult}
import scodec.bits.ByteVector

import java.nio.charset.StandardCharsets.UTF_8
import java.util.Base64
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.{Duration, FiniteDuration}

/** 一覧の続きの位置。Key ならそのキーの次から、Prefix ならその共通プレフィックスの次から。 */
enum Marker:
  case Key(key: String)
  case Prefix(prefix: String)

  def token: String =
    val (t, s) = this match
      case Key(k)    => ('k', k)
      case Prefix(p) => ('p', p)
    Base64.getUrlEncoder.withoutPadding.encodeToString((t.toString + s).getBytes(UTF_8))

object Marker:
  def fromToken(t: String): Option[Marker] =
    scala.util.Try(new String(Base64.getUrlDecoder.decode(t), UTF_8)).toOption.flatMap { s =>
      s.headOption match
        case Some('k') => Some(Key(s.drop(1)))
        case Some('p') => Some(Prefix(s.drop(1)))
        case _         => None
    }

object ObjectStore:
  /** 新しい順に並ぶバージョン ID（時刻を反転した 16 桁 ＋ 乱数 16 桁）。 */
  def newVersionId(millis: Long): String =
    f"${Long.MaxValue - millis}%016x" + ByteVector.view(Crypto.randomBytes(8)).toHex

  /** "null" 版の並び順のキー。 */
  def nullSortKey(millis: Long): String = f"${Long.MaxValue - millis}%016x" + "-null"

  /** 条件付きの書き込みが競合したときに、はじめからやり直す回数の上限。 */
  private val MaxRetries = 20

  /** キャッシュがこの件数を超えたら、書き足すときに期限切れのものを捨てる。 */
  private val CacheSweepSize = 1024

  /** 楽観的な書き込みの 1 回の結果。 */
  private enum Attempt[+A]:
    case Done(value: A)
    case Retry

final case class ListResult(
    contents: Vector[(String, ObjectRecord)],
    commonPrefixes: Vector[String],
    truncated: Boolean,
    next: Option[Marker]
)

/** S3 のバケット・オブジェクト・マルチパートのメタデータ操作。すべて MetaClient 経由。
  *
  * @param master   アクセスキーの秘密鍵を暗号化して保存するためのマスター鍵（無ければ平文）
  * @param cacheTtl バケットとアクセスキーをキャッシュする時間（0 ならキャッシュしない）
  */
final class ObjectStore(meta: MetaClient, master: Option[Array[Byte]] = None, cacheTtl: FiniteDuration = Duration.Zero):
  import ObjectStore.*
  import ObjectStore.Attempt.{Done, Retry}

  // バケットとアクセスキーは、すべての要求で読むので短い時間だけキャッシュする（自ノードでの変更はその場で捨てる）。
  // 他のノードでの変更は最大 cacheTtl だけ遅れて見える。時刻は IO.monotonic（シミュレーションでも決定的）
  private val cache = AtomicReference(Map.empty[ByteVector, (Option[ByteVector], Long)])

  private def cachedGet(key: ByteVector): IO[Option[ByteVector]] =
    if cacheTtl.length == 0 then meta.get(key)
    else
      IO.monotonic.flatMap { now =>
        val t = now.toNanos
        cache.get.get(key) match
          case Some((v, expires)) if expires > t => IO.pure(v)
          case _ =>
            meta.get(key).flatTap { v =>
              IO(cache.updateAndGet { m =>
                // 存在しないアクセスキーなどで際限なく増えないよう、大きくなったら期限切れを捨てる
                val live = if m.size >= CacheSweepSize then m.filter(_._2._2 > t) else m
                live.updated(key, (v, t + cacheTtl.toNanos))
              }).void
            }
      }

  private def invalidate(keys: ByteVector*): IO[Unit] = IO(cache.updateAndGet(_ -- keys)).void

  /** 条件付きの書き込みを、競合したらはじめからやり直す。attempt は「まだやり直してよいか」を受け取る。 */
  private def retrying[A](attempt: Boolean => IO[Attempt[A]]): IO[A] =
    def go(n: Int): IO[A] = attempt(n < MaxRetries).flatMap {
      case Done(a) => IO.pure(a)
      case Retry   => go(n + 1)
    }
    go(0)

  /** 書き込みの結果を Attempt にする。条件が崩れたら、まだやり直せるならやり直し、それ以外は otherwise に任せる。 */
  private def settle[A](r: WriteResult, canRetry: Boolean, value: => A)(otherwise: WriteResult => IO[Attempt[A]]): IO[Attempt[A]] =
    r match
      case WriteResult.Ok                             => IO.pure(Done(value))
      case WriteResult.ConditionFailed(_) if canRetry => IO.pure(Retry)
      case other                                      => otherwise(other)

  private def failed[A](what: String)(r: WriteResult): IO[A] = IO.raiseError(new Exception(s"$what failed: $r"))

  /** 1 件読んで f で作り直し、変わっていなければ書く（競合したらやり直す）。無ければ None。 */
  private def modify[A](key: ByteVector, what: String)(f: ByteVector => (ByteVector, A)): IO[Option[A]] =
    retrying { canRetry =>
      meta.get(key).flatMap {
        case None => IO.pure(Done(None))
        case Some(raw) =>
          val (updated, a) = f(raw)
          meta.write(Vector(Precondition.Equals(key, raw)), Vector(KvOp.Put(key, updated))).flatTap(_ => invalidate(key)).flatMap(
            settle(_, canRetry, Some(a))(failed(what))
          )
      }
    }

  // ---------------- バケット ----------------

  def getBucket(name: String): IO[Option[Bucket]] = cachedGet(S3Keys.bucket(name)).map(_.map(S3Codec.bucket))

  def listBuckets: IO[Vector[Bucket]] =
    scanPrefix(S3Keys.BucketPrefix).map((_, v) => S3Codec.bucket(v)).compile.toVector

  def createBucket(name: String, owner: AccessKey, now: Long): IO[Bucket] =
    val bkey = S3Keys.bucket(name)
    val kkey = S3Keys.accessKey(owner.id)
    retrying { canRetry =>
      (meta.get(S3Keys.BucketSeq), meta.get(kkey)).flatMapN { (seq, ownerRec) =>
        val id = seq.fold(1L)(_.toLong())
        val b = Bucket(name, id, now, owner.id)
        // 管理者でなければ、作ったバケットの Owner 権限をキーに足す
        val grant = ownerRec.filterNot(_ => owner.admin)
        val grantOps = grant.map { raw =>
          val k = S3Codec.accessKey(raw)
          KvOp.Put(kkey, S3Codec.accessKey(k.copy(grants = k.grants.updated(name, Perm.Owner.bit))))
        }
        val conds = Vector(Precondition.Absent(bkey), Precondition.unchanged(S3Keys.BucketSeq, seq)) ++ grant.map(Precondition.Equals(kkey, _))
        val ops = Vector(KvOp.Put(bkey, S3Codec.bucket(b)), KvOp.Put(S3Keys.BucketSeq, ByteVector.fromLong(id + 1))) ++ grantOps
        meta.write(conds, ops).flatTap(_ => invalidate(bkey, kkey)).flatMap {
          case WriteResult.ConditionFailed(0) =>
            getBucket(name).flatMap {
              case Some(existing) if existing.owner == owner.id => IO.raiseError(S3Error.bucketOwned)
              case Some(_)                                      => IO.raiseError(S3Error.bucketExists)
              case None                                         => IO.pure(Retry)
            }
          case r => settle(r, canRetry, b)(failed("create bucket"))
        }
      }
    }

  def deleteBucket(b: Bucket): IO[Unit] =
    val bk = S3Keys.bucket(b.name)
    val objects = S3Keys.objectsPrefix(b.id)
    retrying { canRetry =>
      meta.scan(objects, Bytes.prefixEnd(objects), 1).flatMap { (es, _) =>
        if es.nonEmpty then IO.raiseError(S3Error.bucketNotEmpty)
        else
          meta.get(bk).flatMap {
            case None => IO.raiseError(S3Error.noSuchBucket(b.name))
            case Some(raw) =>
              // その間に設定が変わったら、確かめ直してから消す
              meta.write(Vector(Precondition.Equals(bk, raw)), Vector(KvOp.Delete(bk))).flatTap(_ => invalidate(bk)).flatMap(
                settle(_, canRetry, ())(failed("delete bucket"))
              )
          }
      }
    }

  /** バケットの設定（versioning、cors など）を変える。None なら消す。 */
  def updateBucketConfig(name: String, key: String, value: Option[String]): IO[Bucket] =
    modify(S3Keys.bucket(name), "update bucket config") { raw =>
      val b = S3Codec.bucket(raw)
      val nb = b.copy(config = value.fold(b.config - key)(v => b.config.updated(key, v)))
      (S3Codec.bucket(nb), nb)
    }.flatMap(IO.fromOption(_)(S3Error.noSuchBucket(name)))

  // ---------------- 複製 ----------------

  def replicationPending(b: Bucket, key: String): IO[Boolean] = meta.get(S3Keys.replication(b.id, key)).map(_.isDefined)

  /** 複製待ちのオブジェクト（キー, 印の値）。 */
  def pendingReplication(b: Bucket): Stream[IO, (String, ByteVector)] =
    scanRecords(S3Keys.objectsPrefix(b.id), Bytes.prefixEnd(S3Keys.objectsPrefix(b.id)))
      .collect { case (p, v) if p.sub == S3Keys.SubReplication => (p.key, v) }

  /** 複製が済んだら印を消す（その間に変更があれば、印の値が変わっているので残す）。 */
  def clearReplication(b: Bucket, key: String, seen: ByteVector): IO[Unit] =
    meta.write(Vector(Precondition.Equals(S3Keys.replication(b.id, key), seen)), Vector(KvOp.Delete(S3Keys.replication(b.id, key)))).void

  def getRemote(name: String): IO[Option[Remote]] =
    meta.get(S3Keys.remote(name)).map(_.map(S3Codec.remote).map(r => r.copy(secret = Crypto.openSecret(master, r.secret))))
  def listRemotes: IO[Vector[Remote]] =
    scanPrefix(S3Keys.RemotePrefix).map((_, v) => S3Codec.remote(v).copy(secret = "")).compile.toVector
  def putRemote(r: Remote): IO[Unit] =
    meta.write(Vector.empty, Vector(KvOp.Put(S3Keys.remote(r.name), S3Codec.remote(r.copy(secret = Crypto.sealSecret(master, r.secret)))))).void
  def deleteRemote(name: String): IO[Unit] = meta.write(Vector.empty, Vector(KvOp.Delete(S3Keys.remote(name)))).void

  // ---------------- アクセスキー ----------------

  def getAccessKey(id: String): IO[Option[AccessKey]] =
    cachedGet(S3Keys.accessKey(id)).map(_.map(S3Codec.accessKey).map(k => k.copy(secret = Crypto.openSecret(master, k.secret))))

  def listAccessKeys: IO[Vector[AccessKey]] =
    scanPrefix(S3Keys.AccessKeyPrefix).map((_, v) => S3Codec.accessKey(v).copy(secret = "")).compile.toVector

  /** 保存する形（秘密鍵はマスター鍵があれば暗号化する）。 */
  private def sealedKey(k: AccessKey): ByteVector = S3Codec.accessKey(k.copy(secret = Crypto.sealSecret(master, k.secret)))

  /** 作るか置き換える。 */
  def putAccessKey(k: AccessKey): IO[Unit] =
    meta.write(Vector.empty, Vector(KvOp.Put(S3Keys.accessKey(k.id), sealedKey(k)))).void >> invalidate(S3Keys.accessKey(k.id))

  def deleteAccessKey(id: String): IO[Unit] =
    meta.write(Vector.empty, Vector(KvOp.Delete(S3Keys.accessKey(id)))).void >> invalidate(S3Keys.accessKey(id))

  /** 権限だけを変える（秘密鍵には触れない）。 */
  def updateGrants(id: String, f: Map[String, Int] => Map[String, Int]): IO[Option[AccessKey]] =
    modify(S3Keys.accessKey(id), "update grants") { raw =>
      val k = S3Codec.accessKey(raw)
      val nk = k.copy(grants = f(k.grants))
      (S3Codec.accessKey(nk), nk.copy(secret = ""))
    }

  /** 無ければ作る（起動時の管理者キー用）。 */
  def ensureAccessKey(k: AccessKey): IO[Unit] =
    meta.write(Vector(Precondition.Absent(S3Keys.accessKey(k.id))), Vector(KvOp.Put(S3Keys.accessKey(k.id), sealedKey(k)))).void >>
      invalidate(S3Keys.accessKey(k.id))

  // ---------------- オブジェクト ----------------

  /** 現在の版（削除マーカーの場合もある）。 */
  def getHead(b: Bucket, key: String): IO[Option[ObjectRecord]] = meta.get(S3Keys.head(b.id, key)).map(_.map(S3Codec.obj))

  /** 現在の版（削除マーカーなら None）。 */
  def getObject(b: Bucket, key: String): IO[Option[ObjectRecord]] = getHead(b, key).map(_.filterNot(_.deleteMarker))

  private def nullPointer(b: Bucket, key: String): IO[Option[String]] =
    meta.get(S3Keys.nullPointer(b.id, key)).map(_.map(_.decodeUtf8Lenient))

  private def getVersionBySortKey(b: Bucket, key: String, sortKey: String): IO[Option[ObjectRecord]] =
    meta.get(S3Keys.version(b.id, key, sortKey)).map(_.map(S3Codec.obj))

  /** 指定したバージョン。 */
  def getVersion(b: Bucket, key: String, versionId: String): IO[Option[ObjectRecord]] =
    if versionId == "null" then
      nullPointer(b, key).flatMap {
        case Some(sk) => getVersionBySortKey(b, key, sk)
        case None     => getHead(b, key).map(_.filter(_.versionId == "null"))
      }
    else getVersionBySortKey(b, key, versionId)

  /** 書き込む（バージョニングに応じて版を作る）。ifNoneMatch なら存在しないときだけ、ifMatch ならその ETag のときだけ書く。
    * 書いた版（versionId を埋めたもの）を返す。
    */
  def putObject(b: Bucket, key: String, rec: ObjectRecord, ifNoneMatch: Boolean, ifMatch: Option[String]): IO[ObjectRecord] =
    // よくある場合（バージョニングなし、条件なし）は、現在の版を読まずに置き換える（メタデータの往復が 1 回減る）
    if !b.versioned && !ifNoneMatch && ifMatch.isEmpty then
      val written = rec.copy(versionId = "null")
      meta.write(Vector.empty, Vector(KvOp.Put(S3Keys.head(b.id, key), S3Codec.obj(written))) ++ replOps(b, key, rec.lastModified)).flatMap {
        case WriteResult.Ok => IO.pure(written)
        case other          => failed("put")(other)
      }
    else putObjectChecked(b, key, rec, ifNoneMatch, ifMatch)

  private def putObjectChecked(b: Bucket, key: String, rec: ObjectRecord, ifNoneMatch: Boolean, ifMatch: Option[String]): IO[ObjectRecord] =
    val hk = S3Keys.head(b.id, key)
    retrying { canRetry =>
      meta.get(hk).flatMap { raw =>
        val cur = raw.map(S3Codec.obj)
        val live = cur.filterNot(_.deleteMarker)
        if ifNoneMatch && live.isDefined then IO.raiseError(S3Error.preconditionFailed)
        else if ifMatch.isDefined && live.isEmpty then IO.raiseError(S3Error.noSuchKey(key))
        else if ifMatch.exists(t => !live.exists(_.etag == t)) then IO.raiseError(S3Error.preconditionFailed)
        else
          versionOps(b, key, rec, cur).flatMap { (written, ops) =>
            meta.write(Vector(Precondition.unchanged(hk, raw)), ops).flatMap(settle(_, canRetry, written) {
              case WriteResult.ConditionFailed(_) => IO.raiseError(S3Error.preconditionFailed)
              case other                          => failed("put")(other)
            })
          }
      }
    }

  /** 複製の設定があれば、複製待ちの印を書く操作。 */
  private def replOps(b: Bucket, key: String, now: Long): Vector[KvOp] =
    if b.config.contains("replication") then Vector(KvOp.Put(S3Keys.replication(b.id, key), ByteVector.fromLong(now))) else Vector.empty

  /** "null" 版を版の一覧に入れ、ポインタをそこへ向ける操作。 */
  private def putNullVersion(b: Bucket, key: String, rec: ObjectRecord): Vector[KvOp] =
    val sk = ObjectStore.nullSortKey(rec.lastModified)
    Vector(KvOp.Put(S3Keys.version(b.id, key, sk), S3Codec.obj(rec)), KvOp.Put(S3Keys.nullPointer(b.id, key), ByteVector.view(sk.getBytes(UTF_8))))

  /** 新しい版を書くための操作（複製待ちの印を含む）。バージョニングが無効なら現在の版を置き換えるだけ。 */
  private def versionOps(b: Bucket, key: String, rec0: ObjectRecord, cur: Option[ObjectRecord]): IO[(ObjectRecord, Vector[KvOp])] =
    val putHead = (r: ObjectRecord) => KvOp.Put(S3Keys.head(b.id, key), S3Codec.obj(r))
    val written: IO[(ObjectRecord, Vector[KvOp])] = b.versioning match
      case None =>
        val rec = rec0.copy(versionId = "null")
        IO.pure((rec, Vector(putHead(rec))))
      case Some("Enabled") =>
        (nullPointer(b, key), IO(ObjectStore.newVersionId(rec0.lastModified))).mapN { (pointer, vid) =>
          // バージョニングを有効にする前からある "null" 版は、まだ版の一覧に入っていないので、ここで入れる
          val migrate = cur.filter(c => c.versionId == "null" && pointer.isEmpty).toVector.flatMap(putNullVersion(b, key, _))
          val rec = rec0.copy(versionId = vid)
          (rec, migrate ++ Vector(KvOp.Put(S3Keys.version(b.id, key, vid), S3Codec.obj(rec)), putHead(rec)))
        }
      case Some(_) =>
        // Suspended：新しい版は "null"。前の "null" 版は消える
        nullPointer(b, key).map { pointer =>
          val rec = rec0.copy(versionId = "null")
          val dropOld = pointer.toVector.map(p => KvOp.Delete(S3Keys.version(b.id, key, p)))
          (rec, dropOld ++ putNullVersion(b, key, rec) :+ putHead(rec))
        }
    written.map((r, ops) => (r, ops ++ replOps(b, key, rec0.lastModified)))

  /** 削除。バージョニングが有効なら削除マーカーを置き、その版を返す。無効なら消して None。 */
  def deleteObject(b: Bucket, key: String): IO[Option[ObjectRecord]] =
    IO.realTime.flatMap { now =>
      if !b.versioned then meta.write(Vector.empty, Vector(KvOp.Delete(S3Keys.head(b.id, key))) ++ replOps(b, key, now.toMillis)).as(None)
      else
        val marker = ObjectRecord("", 0, "", now.toMillis, Map.empty, Map.empty, "STANDARD", None, Body.Inline(ByteVector.empty), deleteMarker = true)
        putObject(b, key, marker, ifNoneMatch = false, None).map(Some(_))
    }

  /** 版を指定して消す。現在の版を消したら、残りのうち最新の版が現在の版になる。 */
  def deleteVersion(b: Bucket, key: String, versionId: String): IO[Option[ObjectRecord]] =
    val hk = S3Keys.head(b.id, key)
    val isNull = versionId == "null"
    retrying { canRetry =>
      for
        raw <- meta.get(hk)
        head = raw.map(S3Codec.obj)
        pointer <- nullPointer(b, key)
        sortKey = if isNull then pointer else Some(versionId)
        target <- sortKey.flatTraverse(getVersionBySortKey(b, key, _))
        // 版の一覧に無い "null" 版（バージョニング前のもの）が現在の版になっている場合
        headOnlyNull = isNull && pointer.isEmpty && head.exists(_.versionId == "null")
        result <- target.orElse(head.filter(_ => headOnlyNull)) match
          case None => IO.pure(Done(None))
          case Some(r) =>
            val drop = sortKey.toVector.map(sk => KvOp.Delete(S3Keys.version(b.id, key, sk))) ++
              Option.when(isNull)(KvOp.Delete(S3Keys.nullPointer(b.id, key)))
            val prefix = S3Keys.versionPrefix(b.id, key)
            // 現在の版を消すなら、残りのうち最新の版（消すものを除いた先頭）を現在の版にする
            val newHead: IO[Vector[KvOp]] =
              if !head.exists(_.versionId == versionId) then IO.pure(Vector.empty)
              else
                meta.scan(prefix, Bytes.prefixEnd(prefix), 2).map { (es, _) =>
                  es.filterNot((k, _) => sortKey.exists(sk => k == S3Keys.version(b.id, key, sk))).headOption match
                    case Some((_, v)) => Vector(KvOp.Put(hk, v))
                    case None         => Vector(KvOp.Delete(hk))
                }
            (newHead, IO.realTime).flatMapN { (nh, now) =>
              val repl = if nh.nonEmpty then replOps(b, key, now.toMillis) else Vector.empty
              meta.write(Vector(Precondition.unchanged(hk, raw)), drop ++ nh ++ repl).flatMap(settle(_, canRetry, Some(r))(failed("delete version")))
            }
      yield result
    }

  /** ListObjectVersions。キーの昇順、同じキーでは新しい順。 */
  def listVersions(
      b: Bucket,
      prefix: String,
      keyMarker: Option[String],
      versionMarker: Option[String],
      maxKeys: Int
  ): IO[(Vector[(String, ObjectRecord, Boolean)], Boolean)] =
    val start = keyMarker.fold(S3Keys.listStart(b.id, prefix))(k => S3Keys.objectBase(b.id, k))
    scanRecords(start, S3Keys.listEnd(b.id, prefix))
      .collect { case (p, v) if p.sub == S3Keys.SubHead || p.sub == S3Keys.SubVersion => (p, S3Codec.obj(v)) }
      .groupAdjacentBy(_._1.key)
      .flatMap { (key, chunk) =>
        val entries = chunk.toVector
        val head = entries.collectFirst { case (p, r) if p.sub == S3Keys.SubHead => r }
        val versions = entries.collect { case (p, r) if p.sub == S3Keys.SubVersion => r }
        val all = if versions.nonEmpty then versions else head.toVector
        Stream.emits(all.map(r => (key, r, head.exists(_.versionId == r.versionId))))
      }
      .filter { (key, r, _) =>
        keyMarker.forall(km => key > km || (key == km && versionMarker.exists(vm => r.versionId > vm && vm != "null")))
      }
      .take(maxKeys.toLong + 1)
      .compile
      .toVector
      .map(es => (es.take(maxKeys), es.size > maxKeys))

  /** ListObjects（V1/V2 共通）。 */
  def list(b: Bucket, prefix: String, delimiter: Option[String], after: Option[Marker], maxKeys: Int): IO[ListResult] =
    val prefixStart = S3Keys.listStart(b.id, prefix)
    val end = S3Keys.listEnd(b.id, prefix)
    val markerStart = after.map {
      case Marker.Key(k)    => S3Keys.objectBase(b.id, k) :+ 0x01.toByte
      case Marker.Prefix(p) => Bytes.prefixEnd(S3Keys.listStart(b.id, p)).getOrElse(prefixStart)
    }
    val start = markerStart.filter(m => Bytes.compare(m, prefixStart) > 0).getOrElse(prefixStart)
    val delim = delimiter.filter(_.nonEmpty)

    // (内容, 共通プレフィックス, 次の位置) を最大 maxKeys + 1 件集める
    def loop(from: ByteVector, contents: Vector[(String, ObjectRecord)], prefixes: Vector[String]): IO[ListResult] =
      def total = contents.size + prefixes.size
      if total > maxKeys then finish(contents, prefixes)
      else if end.exists(e => Bytes.compare(from, e) >= 0) then finish(contents, prefixes)
      else
        meta.scan(from, end, 500).flatMap { (es, more) =>
          var cs = contents
          var ps = prefixes
          var jump: Option[ByteVector] = None
          val it = es.iterator
          while jump.isEmpty && it.hasNext && cs.size + ps.size <= maxKeys do
            val (k, v) = it.next()
            S3Keys.parse(k).filter(_.sub == S3Keys.SubHead).foreach { p =>
              val rest = p.key.drop(prefix.length)
              delim.map(d => (d, rest.indexOf(d))).filter(_._2 >= 0) match
                case Some((d, idx)) =>
                  val cp = prefix + rest.take(idx + d.length)
                  if !ps.lastOption.contains(cp) then ps :+= cp
                  jump = Bytes.prefixEnd(S3Keys.listStart(b.id, cp))
                case None =>
                  val o = S3Codec.obj(v)
                  if !o.deleteMarker then cs :+= (p.key -> o)
            }
          jump match
            case Some(j)                     => loop(j, cs, ps)
            case None if more && es.nonEmpty => loop(es.last._1 :+ 0.toByte, cs, ps)
            case None                        => finish(cs, ps)
        }

    def finish(contents: Vector[(String, ObjectRecord)], prefixes: Vector[String]): IO[ListResult] =
      // キーと共通プレフィックスを辞書順に混ぜて maxKeys 件に切る
      val merged = (contents.map(c => Left(c): Either[(String, ObjectRecord), String]) ++ prefixes.map(Right(_)))
        .sortBy(_.fold(_._1, identity))
      val taken = merged.take(maxKeys)
      val truncated = merged.size > maxKeys
      val next = if truncated then taken.lastOption.map(_.fold(c => Marker.Key(c._1), p => Marker.Prefix(p))) else None
      IO.pure(ListResult(taken.collect { case Left(c) => c }, taken.collect { case Right(p) => p }, truncated, next))

    if maxKeys <= 0 then IO.pure(ListResult(Vector.empty, Vector.empty, false, None)) else loop(start, Vector.empty, Vector.empty)

  // ---------------- マルチパート ----------------

  def createUpload(b: Bucket, u: UploadRecord): IO[Unit] =
    meta.write(Vector.empty, Vector(KvOp.Put(S3Keys.upload(b.id, u.key, u.uploadId), S3Codec.upload(u)))).void

  def getUpload(b: Bucket, key: String, uploadId: String): IO[Option[UploadRecord]] =
    meta.get(S3Keys.upload(b.id, key, uploadId)).map(_.map(S3Codec.upload))

  def putPart(b: Bucket, key: String, uploadId: String, p: PartRecord): IO[Unit] =
    meta
      .write(
        Vector(Precondition.Exists(S3Keys.upload(b.id, key, uploadId))),
        Vector(KvOp.Put(S3Keys.part(b.id, key, uploadId, p.partNumber), S3Codec.part(p)))
      )
      .flatMap {
        case WriteResult.Ok => IO.unit
        case _              => IO.raiseError(S3Error.noSuchUpload)
      }

  def listParts(b: Bucket, key: String, uploadId: String, afterPart: Int, max: Int): IO[(Vector[PartRecord], Boolean)] =
    val prefix = S3Keys.partPrefix(b.id, key, uploadId)
    meta.scan(prefix ++ ByteVector.fromInt(afterPart + 1), Bytes.prefixEnd(prefix), max).map { (es, more) =>
      (es.map((_, v) => S3Codec.part(v)), more)
    }

  def allParts(b: Bucket, key: String, uploadId: String): IO[Vector[PartRecord]] =
    scanPrefix(S3Keys.partPrefix(b.id, key, uploadId)).map((_, v) => S3Codec.part(v)).compile.toVector

  /** アップロードとそのパートを消す操作。 */
  private def dropUpload(b: Bucket, key: String, uploadId: String, parts: Vector[PartRecord]): Vector[KvOp] =
    KvOp.Delete(S3Keys.upload(b.id, key, uploadId)) +: parts.map(p => KvOp.Delete(S3Keys.part(b.id, key, uploadId, p.partNumber)))

  /** アップロードを完了する：オブジェクトを書き、アップロードとすべてのパートを消す（同じレンジ内でアトミック）。 */
  def completeUpload(b: Bucket, key: String, uploadId: String, rec: ObjectRecord, parts: Vector[PartRecord]): IO[ObjectRecord] =
    val uk = S3Keys.upload(b.id, key, uploadId)
    val hk = S3Keys.head(b.id, key)
    retrying { canRetry =>
      meta.get(hk).flatMap { raw =>
        versionOps(b, key, rec, raw.map(S3Codec.obj)).flatMap { (written, ops) =>
          meta.write(Vector(Precondition.Exists(uk), Precondition.unchanged(hk, raw)), ops ++ dropUpload(b, key, uploadId, parts)).flatMap {
            case WriteResult.Ok                           => IO.pure(Done(written))
            case WriteResult.ConditionFailed(1) if canRetry => IO.pure(Retry) // 現在の版が変わった
            case _                                        => IO.raiseError(S3Error.noSuchUpload)
          }
        }
      }
    }

  def abortUpload(b: Bucket, key: String, uploadId: String): IO[Unit] =
    allParts(b, key, uploadId).flatMap { parts =>
      meta.write(Vector(Precondition.Exists(S3Keys.upload(b.id, key, uploadId))), dropUpload(b, key, uploadId, parts)).flatMap {
        case WriteResult.Ok => IO.unit
        case _              => IO.raiseError(S3Error.noSuchUpload)
      }
    }

  /** バケット内の未完了アップロード（キー、アップロード ID の順）。 */
  def listUploads(b: Bucket, prefix: String, keyMarker: Option[String], uploadIdMarker: Option[String], max: Int): IO[(Vector[UploadRecord], Boolean)] =
    scanRecords(S3Keys.listStart(b.id, prefix), S3Keys.listEnd(b.id, prefix))
      .collect { case (p, v) if p.sub == S3Keys.SubUpload => S3Codec.upload(v) }
      .filter { u =>
        keyMarker.forall(m => u.key > m || (u.key == m && uploadIdMarker.exists(u.uploadId > _)))
      }
      .take(max.toLong + 1)
      .compile
      .toVector
      .map(us => (us.take(max), us.size > max))

  // ---------------- GC 用 ----------------

  /** 参照されているブロック（オブジェクトとパート）をすべて列挙する。 */
  def referencedBlocks: Stream[IO, scalas3.block.BlockId] =
    scanRecords(ByteVector(S3Keys.ObjectsTag), Some(ByteVector(S3Keys.ObjectsTag + 1)))
      .flatMap { (p, v) =>
        if p.sub == S3Keys.SubHead || p.sub == S3Keys.SubVersion then Stream.emits(S3Codec.obj(v).body.allIds)
        else if p.sub == S3Keys.SubPart then Stream.emits(S3Codec.part(v).body.allIds)
        else Stream.empty
      }

  /** [from, until) を全部読む（ページごとに取りに行く）。 */
  def scanAll(from: ByteVector, until: Option[ByteVector]): Stream[IO, (ByteVector, ByteVector)] =
    Stream.unfoldLoopEval(from) { cursor =>
      meta.scan(cursor, until, 1000).map { (es, more) =>
        (es, if more && es.nonEmpty then Some(es.last._1 :+ 0.toByte) else None)
      }
    }.flatMap(Stream.emits)

  private def scanPrefix(prefix: ByteVector): Stream[IO, (ByteVector, ByteVector)] = scanAll(prefix, Bytes.prefixEnd(prefix))

  /** オブジェクトのレコードを、キーを分解して読む（分解できないキーは飛ばす）。 */
  private def scanRecords(from: ByteVector, until: Option[ByteVector]): Stream[IO, (S3Keys.Parsed, ByteVector)] =
    scanAll(from, until).map((k, v) => (S3Keys.parse(k), v)).collect { case (Some(p), v) => (p, v) }
