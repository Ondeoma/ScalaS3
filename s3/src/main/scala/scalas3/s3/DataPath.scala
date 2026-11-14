package scalas3.s3

import cats.effect.{IO, Outcome, Ref}
import cats.syntax.all.*
import fs2.{Chunk, Pull, Stream}
import scalas3.block.{BlockClient, BlockId, StripeClient}
import scodec.bits.ByteVector

import java.security.MessageDigest

/** 本文を取り込んだ結果。body のブロックは GC から保護中なので、メタデータを書いたあと release すること。 */
final case class Ingested(
    size: Long,
    md5: ByteVector,
    sha256: Option[String],
    checksum: Option[(String, String)],
    body: Body,
    /** 取り込みに keepAliveInterval 以上かかった（コミットの前に、ブロックが残っているかを確かめる） */
    longUpload: Boolean = false
)

/** 本文の書き込みと読み出し。複製（blocks）とイレイジャーコーディング（stripes）を使い分ける。
  *
  * EC のストレージクラスでも、ブロック 1 つに収まる小さなオブジェクトは複製で保存する（EC のオーバーヘッドを避ける）。
  */
final class DataPath(
    blocks: BlockClient,
    stripes: Option[StripeClient],
    blockSize: Int,
    inlineThreshold: Int,
    parallelism: Int = 4,
    /** アップロード中に、書いたブロックの更新時刻を進め直す間隔（GC の猶予期間の 1/3 程度にする）。 */
    keepAliveInterval: scala.concurrent.duration.FiniteDuration = scala.concurrent.duration.Duration(20, "minutes")
):

  /** 本文を固定長ブロックに分けて保存する。小さければメタデータに埋め込む。 */
  def ingest(
      in: Stream[IO, Byte],
      wantSha256: Boolean,
      checksumAlg: Option[String],
      redundancy: Redundancy = Redundancy.Replicated,
      key: Option[Array[Byte]] = None
  ): IO[Ingested] =
    IO.ref(Vector.empty[BlockRef]).flatMap { written =>
      // 長いアップロードの間、書き終えたブロックの更新時刻を定期的に進め直す。「使用中」の保護はノードのメモリにしかなく、
      // ブロックを置いたノードが再起動すると消える。そのまま猶予期間を過ぎると、GC に消されてしまう
      val keepAlive = (IO.sleep(keepAliveInterval) >> written.get.flatMap(rs => touch(Body.Blocks(rs)).attempt)).foreverM
      IO.monotonic.flatMap { started =>
        keepAlive.background
          .surround(ingestInto(in, wantSha256, checksumAlg, redundancy, key, written))
          .flatMap(ing => IO.monotonic.map(now => ing.copy(longUpload = now - started >= keepAliveInterval)))
      }.onError(_ => written.get.flatMap(rs => release(Body.Blocks(rs))))
    }

  private def ingestInto(
      in: Stream[IO, Byte],
      wantSha256: Boolean,
      checksumAlg: Option[String],
      redundancy: Redundancy,
      key: Option[Array[Byte]],
      written: Ref[IO, Vector[BlockRef]]
  ): IO[Ingested] =
    IO.defer {
      val md5 = MessageDigest.getInstance("MD5")
      val sha = Option.when(wantSha256)(MessageDigest.getInstance("SHA-256"))
      val ck = checksumAlg.flatMap(Checksummer(_))
      var size = 0L

      def observe(c: Chunk[Byte]): Unit =
        val s = c.toArraySlice
        md5.update(s.values, s.offset, s.size)
        sha.foreach(_.update(s.values, s.offset, s.size))
        ck.foreach(_.update(s.values, s.offset, s.size))
        size += s.size

      // 平文で数えて、暗号化するなら暗号文にしてから保存する（参照の大きさは平文のまま）
      val chunks = in.chunkN(blockSize, allowFewer = true).map { c => observe(c); c.toByteVector }

      // 最初のブロックを見て、小さく単独ならインライン、1 つだけなら複製、複数なら指定の方式
      val body: IO[Body] =
        chunks.pull.uncons1.flatMap {
          case None => Pull.output1(Body.Inline(ByteVector.empty))
          case Some((first, rest)) =>
            rest.pull.uncons1.flatMap {
              case None if first.size <= inlineThreshold => Pull.output1(Body.Inline(seal(key, first)))
              case None => Pull.eval(store(Stream.emit(first), Redundancy.Replicated, key, written)).flatMap(Pull.output1)
              case Some((second, rest2)) =>
                Pull.eval(store(Stream(first, second) ++ rest2, redundancy, key, written)).flatMap(Pull.output1)
            }
        }.stream.compile.lastOrError

      body.map { b =>
        Ingested(size, ByteVector.view(md5.digest()), sha.map(d => ByteVector.view(d.digest()).toHex), ck.map(c => checksumAlg.get.toLowerCase -> c.base64), b)
      }
    }

  private def seal(key: Option[Array[Byte]], plain: ByteVector): ByteVector = key.fold(plain)(Crypto.encrypt(_, plain))

  /** 複製したブロックとして書き、GC から守る。参照の大きさは size（平文の大きさ）。 */
  private def putReplicated(data: ByteVector, size: Long): IO[BlockRef] =
    blocks.put(data).flatTap(id => blocks.protect(List(id))).map(BlockRef(_, size.toInt))

  private def putOne(plain: ByteVector, redundancy: Redundancy, key: Option[Array[Byte]]): IO[BlockRef] =
    val data = seal(key, plain)
    (redundancy, stripes) match
      case (Redundancy.Erasure(k, m), Some(st)) =>
        st.putStripe(data, k, m).flatTap(id => st.protect(List(id))).map(BlockRef(_, plain.size.toInt, Some((k, m))))
      case _ => putReplicated(data, plain.size)

  private def store(chunks: Stream[IO, ByteVector], redundancy: Redundancy, key: Option[Array[Byte]], written: Ref[IO, Vector[BlockRef]]): IO[Body] =
    chunks
      .parEvalMap(parallelism)(data => putOne(data, redundancy, key).flatTap(r => written.update(_ :+ r)))
      .compile
      .toVector
      .map(Body.Blocks(_))

  /** GC からの保護を外す。 */
  def release(body: Body): IO[Unit] =
    blocks.release(body.blockIds) >> stripes.traverse_(_.release(body.stripeIds))

  /** GC から守り、更新時刻も進める（既存ブロックを新しいレコードで共有するときに使う）。 */
  def protect(body: Body): IO[Unit] =
    blocks.protect(body.blockIds) >> blocks.touch(body.blockIds) >>
      stripes.traverse_(st => st.protect(body.stripeIds) >> st.touch(body.stripeIds))

  /** どこにも残っていない（EC なら復元できない）ブロック。 */
  def missing(body: Body): IO[Set[BlockId]] =
    body match
      case Body.Inline(_) => IO.pure(Set.empty)
      case Body.Blocks(refs) =>
        val plain = refs.collect { case r if r.ec.isEmpty => r.id }
        val ec = refs.collect { case BlockRef(id, _, Some((k, m))) => (id, k, m) }
        (blocks.missing(plain), stripes.fold(IO.pure(Set.empty[BlockId]))(_.lost(ec))).mapN(_ ++ _)

  /** 更新時刻を進める。メタデータをコミットする直前に呼ぶこと（spec/BlockGc.tla の TouchOnCommit）。
    * これが無いと、猶予期間より長くかかったアップロードが GC のマークの途中でコミットしたとき、ブロックが消される。
    */
  def touch(body: Body): IO[Unit] =
    blocks.touch(body.blockIds) >> stripes.traverse_(_.touch(body.stripeIds))

  /** ブロックを 1 つ書く（保護はしない。呼び出し側が protect すること）。 */
  def putBlock(d: ByteVector): IO[BlockId] = blocks.put(d)

  private def fetch(r: BlockRef, key: Option[Array[Byte]] = None): IO[ByteVector] =
    val stored = r.size + (if key.isDefined then Crypto.Overhead else 0)
    val raw = r.ec match
      case None => blocks.get(r.id)
      case Some((k, m)) =>
        IO.fromOption(stripes)(IllegalStateException("erasure-coded data but no stripe client")).flatMap(_.getStripe(r.id, stored, k, m))
    key.fold(raw)(k => raw.flatMap(d => IO(Crypto.decrypt(k, d))))

  /** [from, until) に重なる参照と、その参照の本文の中での開始位置。 */
  private def overlapping(refs: Vector[BlockRef], from: Long, until: Long): Vector[(BlockRef, Long)] =
    refs.zip(refs.scanLeft(0L)(_ + _.size)).filter((r, off) => off < until && off + r.size > from)

  /** 開始位置 off の参照の中身 data から、[from, until) に入る部分を切り出す。 */
  private def clip(data: ByteVector, r: BlockRef, off: Long, from: Long, until: Long): ByteVector =
    data.slice(math.max(0, from - off), math.min(r.size.toLong, until - off))

  /** [from, until) のバイトを返す（暗号化されていれば key で復号する）。 */
  def read(body: Body, from: Long, until: Long, key: Option[Array[Byte]] = None): Stream[IO, Byte] =
    body match
      case Body.Inline(d) =>
        val plain = key.fold(d)(k => Crypto.decrypt(k, d))
        Stream.chunk(Chunk.byteVector(plain.slice(from, until)))
      case Body.Blocks(refs) =>
        Stream
          .emits(overlapping(refs, from, until))
          .covary[IO]
          .parEvalMap(2)((r, off) => fetch(r, key).map(clip(_, r, off, from, until)))
          .flatMap(d => Stream.chunk(Chunk.byteVector(d)))

  /** ボディの一部を切り出した新しいボディ（UploadPartCopy の範囲指定用）。
    * まるごと含まれる参照は共有し、境界にかかる部分は読み直して複製ブロックとして書く。
    */
  def sliceBody(body: Body, from: Long, until: Long): IO[Body] =
    body match
      case Body.Inline(d) => IO.pure(Body.Inline(d.slice(from, until)))
      case Body.Blocks(refs) =>
        // 途中で失敗したら、それまでに守ったブロックの保護を外す（でないと再起動まで GC で消せない）
        Ref.of[IO, Vector[BlockRef]](Vector.empty).flatMap { done =>
          overlapping(refs, from, until)
            .traverse { (r, off) =>
              val piece =
                if off >= from && off + r.size <= until then protect(Body.Blocks(Vector(r))).as(r)
                else
                  fetch(r).flatMap { data =>
                    val piece = clip(data, r, off, from, until)
                    putReplicated(piece, piece.size)
                  }
              piece.flatTap(p => done.update(_ :+ p))
            }
            .map(Body.Blocks(_))
            .guaranteeCase {
              case Outcome.Succeeded(_) => IO.unit
              case _                    => done.get.flatMap(rs => release(Body.Blocks(rs)))
            }
        }
