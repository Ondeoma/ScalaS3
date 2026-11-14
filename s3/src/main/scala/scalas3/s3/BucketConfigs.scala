package scalas3.s3

import cats.effect.IO
import cats.syntax.all.*

import java.time.{LocalDate, ZoneOffset}

/** ライフサイクルのルール（PutBucketLifecycleConfiguration の XML のうち、よく使う部分）。 */
final case class LifecycleRule(
    id: String,
    prefix: String,
    expirationDays: Option[Int],
    expirationDate: Option[Long],
    expiredDeleteMarker: Boolean,
    noncurrentDays: Option[Int],
    abortMultipartDays: Option[Int]
)

/** ライフサイクルとレプリケーションの設定に共通する、ルールの読み方。 */
private object RuleXml:
  /** 有効な <Rule> を 1 つずつ読む。<Rule> が 1 つも無ければ MalformedXML。 */
  def parse[A](xml: String)(rule: scala.xml.Node => Option[A]): Either[S3Error, List[A]] =
    Xml.parse(xml).flatMap { doc =>
      val rules = (doc \ "Rule").toList
      if rules.isEmpty then Left(S3Error.malformedXml) else Right(rules.filter(enabled).flatMap(rule))
    }

  private def enabled(r: scala.xml.Node): Boolean = (r \ "Status").text.trim == "Enabled"

  /** 対象のプレフィックス（Filter/Prefix、Filter/And/Prefix、古い形式の Prefix の順に探す。無ければ全部）。 */
  def prefix(r: scala.xml.Node): String =
    (r \ "Filter" \ "Prefix").headOption.orElse((r \ "Filter" \ "And" \ "Prefix").headOption).orElse((r \ "Prefix").headOption)
      .map(_.text.trim).getOrElse("")

  def id(r: scala.xml.Node): String = (r \ "ID").text.trim

  /** 設定されたルールをバケットごとに取り出す（読めない設定は飛ばす）。 */
  def configured[A](buckets: Vector[Bucket], kind: String)(parse: String => Either[S3Error, List[A]]): Vector[(Bucket, List[A])] =
    buckets.flatMap(b => b.config.get(kind).flatMap(parse(_).toOption).map(b -> _))

object Lifecycle:
  private val Day = 24L * 3600 * 1000

  def parse(xml: String): Either[S3Error, List[LifecycleRule]] =
    RuleXml.parse(xml) { r =>
      def int(path: scala.xml.NodeSeq) = path.headOption.flatMap(_.text.trim.toIntOption)
      val exp = r \ "Expiration"
      Some(
        LifecycleRule(
          RuleXml.id(r),
          RuleXml.prefix(r),
          int(exp \ "Days"),
          (exp \ "Date").headOption.flatMap(d => scala.util.Try(LocalDate.parse(d.text.trim.take(10)).atStartOfDay(ZoneOffset.UTC).toInstant.toEpochMilli).toOption),
          (exp \ "ExpiredObjectDeleteMarker").text.trim == "true",
          int(r \ "NoncurrentVersionExpiration" \ "NoncurrentDays"),
          int(r \ "AbortIncompleteMultipartUpload" \ "DaysAfterInitiation")
        )
      )
    }

  final case class Report(expired: Int, noncurrent: Int, markers: Int, aborted: Int):
    def +(o: Report): Report = Report(expired + o.expired, noncurrent + o.noncurrent, markers + o.markers, aborted + o.aborted)

  /** 全バケットのルールを now の時刻で 1 回適用する（オブジェクトを走査する）。 */
  def run(store: ObjectStore, now: Long): IO[Report] =
    store.listBuckets.flatMap { buckets =>
      RuleXml.configured(buckets, "lifecycle")(parse).foldLeftM(Report(0, 0, 0, 0)) { case (acc, (b, rules)) =>
        rules.foldLeftM(acc)((a, r) => applyRule(store, b, r, now).map(a + _))
      }
    }

  private def applyRule(store: ObjectStore, b: Bucket, r: LifecycleRule, now: Long): IO[Report] =
    def expired(o: ObjectRecord) =
      !o.deleteMarker && (r.expirationDays.exists(d => o.lastModified + d * Day <= now) || r.expirationDate.exists(_ <= now))
    for
      // 現在の版の期限切れと、noncurrent の版、残った削除マーカー
      versions <- store.listVersions(b, r.prefix, None, None, Int.MaxValue - 1).map(_._1)
      byKey = versions.groupBy(_._1).toVector
      results <- byKey.traverse { (key, vs) =>
        val ordered = vs.map((_, rec, latest) => (rec, latest))
        val current = ordered.find(_._2).map(_._1)
        val exp = current.filter(expired).fold(IO.pure(0))(_ => store.deleteObject(b, key).as(1))
        // noncurrent になった時刻 ＝ ひとつ新しい版が書かれた時刻
        val noncurrent = r.noncurrentDays.fold(IO.pure(0)) { days =>
          ordered.zip(ordered.drop(1)).collect {
            case ((newer, _), (older, false)) if newer.lastModified + days * Day <= now => older
          }.traverse(o => store.deleteVersion(b, key, o.versionId)).map(_.size)
        }
        val marker =
          if r.expiredDeleteMarker && ordered.size == 1 && ordered.head._1.deleteMarker then store.deleteVersion(b, key, ordered.head._1.versionId).as(1)
          else IO.pure(0)
        (exp, noncurrent, marker).tupled
      }
      aborted <- r.abortMultipartDays.fold(IO.pure(0)) { days =>
        store.listUploads(b, r.prefix, None, None, Int.MaxValue - 1).flatMap { (us, _) =>
          us.filter(_.initiated + days * Day <= now).traverse(u => store.abortUpload(b, u.key, u.uploadId).attempt).map(_.count(_.isRight))
        }
      }
    yield Report(results.map(_._1).sum, results.map(_._2).sum, results.map(_._3).sum, aborted)

/** レプリケーション設定。Destination/Bucket の ARN と、scalas3 独自の Remote（Admin API で登録した転送先の名前）。 */
final case class ReplicationRule(id: String, prefix: String, remote: String, bucket: String)

object ReplicationConfig:
  def parse(xml: String): Either[S3Error, List[ReplicationRule]] =
    RuleXml.parse(xml) { r =>
      val dest = r \ "Destination"
      val bucket = (dest \ "Bucket").text.trim.stripPrefix("arn:aws:s3:::")
      val remote = (dest \ "Remote").text.trim
      Option.when(bucket.nonEmpty && remote.nonEmpty)(ReplicationRule(RuleXml.id(r), RuleXml.prefix(r), remote, bucket))
    }

  /** 複製の設定があるバケットと、そのルール。 */
  def configured(buckets: Vector[Bucket]): Vector[(Bucket, List[ReplicationRule])] = RuleXml.configured(buckets, "replication")(parse)
