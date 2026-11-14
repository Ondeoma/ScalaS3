package scalas3.s3

import scalas3.block.BlockId
import scalas3.core.{ByteReader, ByteWriter, Bytes, DecodeError}
import scodec.bits.ByteVector

import java.nio.charset.StandardCharsets.UTF_8

/** config のキー：versioning（Enabled / Suspended）、cors（XML）、policy（JSON）、encryption（AES256）、
  * lifecycle（XML）、replication（XML）。
  */
final case class Bucket(name: String, id: Long, createdMillis: Long, owner: String, config: Map[String, String] = Map.empty):
  def versioning: Option[String] = config.get("versioning")
  def versioned: Boolean = versioning.isDefined

/** アクセスキーごとのバケット権限。 */
enum Perm(val bit: Int):
  case Read extends Perm(1)
  case Write extends Perm(2)
  case Owner extends Perm(4)

final case class AccessKey(id: String, secret: String, name: String, admin: Boolean, grants: Map[String, Int]):
  def can(bucket: String, p: Perm): Boolean = admin || grants.get(bucket).exists(g => (g & p.bit) != 0 || (g & Perm.Owner.bit) != 0)

/** 本文の一部。ec が None なら複製したブロック、Some((k, m)) ならイレイジャーコーディングのストライプ。 */
final case class BlockRef(id: BlockId, size: Int, ec: Option[(Int, Int)] = None)

/** 冗長化の方式（ストレージクラスごとに決める）。 */
enum Redundancy:
  case Replicated
  case Erasure(k: Int, m: Int)

enum Body:
  case Inline(data: ByteVector)
  case Blocks(refs: Vector[BlockRef])

  def allRefs: Vector[BlockRef] = this match
    case Inline(_)    => Vector.empty
    case Blocks(rs)   => rs

  /** 複製したブロックの ID。 */
  def blockIds: Vector[BlockId] = allRefs.filter(_.ec.isEmpty).map(_.id)

  /** ストライプの ID。 */
  def stripeIds: Vector[BlockId] = allRefs.filter(_.ec.isDefined).map(_.id)

  /** GC が参照を数えるための全 ID。 */
  def allIds: Vector[BlockId] = allRefs.map(_.id)

final case class ObjectRecord(
    versionId: String,
    size: Long,
    etag: String, // 引用符なし
    lastModified: Long,
    headers: Map[String, String], // content-type などの標準ヘッダー（小文字）
    userMeta: Map[String, String], // x-amz-meta-* の * 部分（小文字）
    storageClass: String,
    checksum: Option[(String, String)], // (アルゴリズム, base64)
    body: Body,
    deleteMarker: Boolean = false,
    encryption: Option[Encryption] = None
)

/** サーバー側暗号化の情報。SSE-S3 はデータ鍵をマスター鍵で包んで持つ。SSE-C は鍵の MD5 だけを持つ。 */
enum Encryption:
  case SseS3(wrappedKey: ByteVector)
  case SseC(keyMd5: String)

final case class UploadRecord(
    uploadId: String,
    key: String,
    initiated: Long,
    headers: Map[String, String],
    userMeta: Map[String, String],
    storageClass: String,
    encryption: Option[Encryption] = None
)

/** クラスタ間複製の転送先（S3 互換のエンドポイント）。secret はマスター鍵があれば暗号化して保存する。 */
final case class Remote(name: String, endpoint: String, region: String, accessKey: String, secret: String)

final case class PartRecord(partNumber: Int, md5: ByteVector, size: Long, lastModified: Long, body: Body):
  def etag: String = md5.toHex

/** S3 のレコードをメタデータのキー空間に置く方法（設計書 §5.2）。
  * {{{
  * [0x02]['b'][bucket 名]                    → Bucket
  * [0x02]['s']                               → 次のバケット ID
  * [0x02]['k'][アクセスキー ID]              → AccessKey
  * [0x10][bucketId u64][esc(key)][00 01][00]  → ObjectRecord（現在の版）
  * [0x10][bucketId u64][esc(key)][00 01][02][uploadId]          → UploadRecord
  * [0x10][bucketId u64][esc(key)][00 01][03][uploadId][part u32] → PartRecord
  * }}}
  * esc はキーの 0x00 を 0x00 0xFF に置き換える。こうすると辞書順が元のキーの順と一致し、
  * 1 つのオブジェクトに関するレコードが必ず隣り合う。
  */
object S3Keys:
  val ObjectsTag: Byte = 0x10
  val SubHead: Byte = 0x00
  val SubUpload: Byte = 0x02
  val SubPart: Byte = 0x03

  private def utf8(s: String) = ByteVector(s.getBytes(UTF_8))

  val BucketPrefix: ByteVector = ByteVector(0x02, 'b'.toByte)
  def bucket(name: String): ByteVector = BucketPrefix ++ utf8(name)
  val BucketSeq: ByteVector = ByteVector(0x02, 's'.toByte)
  val AccessKeyPrefix: ByteVector = ByteVector(0x02, 'k'.toByte)
  def accessKey(id: String): ByteVector = AccessKeyPrefix ++ utf8(id)

  def escape(key: String): ByteVector =
    val raw = key.getBytes(UTF_8)
    val w = ByteWriter(raw.length + 8)
    raw.foreach(b => if b == 0 then w.u8(0).u8(0xff) else w.u8(b))
    w.toByteVector

  def objectsPrefix(bucketId: Long): ByteVector = ByteVector(ObjectsTag) ++ ByteVector.fromLong(bucketId)
  def objectBase(bucketId: Long, key: String): ByteVector = objectsPrefix(bucketId) ++ escape(key) ++ ByteVector(0, 1)
  def head(bucketId: Long, key: String): ByteVector = objectBase(bucketId, key) :+ SubHead
  def upload(bucketId: Long, key: String, uploadId: String): ByteVector = (objectBase(bucketId, key) :+ SubUpload) ++ utf8(uploadId)
  def partPrefix(bucketId: Long, key: String, uploadId: String): ByteVector = (objectBase(bucketId, key) :+ SubPart) ++ utf8(uploadId)
  def part(bucketId: Long, key: String, uploadId: String, n: Int): ByteVector = partPrefix(bucketId, key, uploadId) ++ ByteVector.fromInt(n)

  val SubVersion: Byte = 0x01
  val SubReplication: Byte = 0x05
  val SubNullPointer: Byte = 0x06

  /** 複製待ちの印（値は変更した時刻）。オブジェクトのレコードと同じバッチで書く。 */
  def replication(bucketId: Long, key: String): ByteVector = objectBase(bucketId, key) :+ SubReplication

  val RemotePrefix: ByteVector = ByteVector(0x02, 'R'.toByte)
  def remote(name: String): ByteVector = RemotePrefix ++ utf8(name)

  /** バージョンのレコード。sortKey は新しい順に並ぶ文字列。 */
  def version(bucketId: Long, key: String, sortKey: String): ByteVector = (objectBase(bucketId, key) :+ SubVersion) ++ utf8(sortKey)
  def versionPrefix(bucketId: Long, key: String): ByteVector = objectBase(bucketId, key) :+ SubVersion

  /** "null" 版のバージョンキーを指すポインタ（"null" 版は 1 つだけ）。 */
  def nullPointer(bucketId: Long, key: String): ByteVector = objectBase(bucketId, key) :+ SubNullPointer

  /** 「key が prefix で始まるオブジェクト」のレコードがすべて入る区間の始まり。 */
  def listStart(bucketId: Long, prefix: String): ByteVector = objectsPrefix(bucketId) ++ escape(prefix)
  def listEnd(bucketId: Long, prefix: String): Option[ByteVector] = Bytes.prefixEnd(listStart(bucketId, prefix))

  final case class Parsed(bucketId: Long, key: String, sub: Byte, rest: ByteVector)

  /** オブジェクトのレコードのキーを分解する。 */
  def parse(k: ByteVector): Option[Parsed] =
    if k.size < 12 || k(0) != ObjectsTag then None
    else
      val bid = k.slice(1, 9).toLong()
      val arr = k.toArray
      val out = new java.io.ByteArrayOutputStream()
      var i = 9
      var end = -1
      while end < 0 && i + 1 < arr.length do
        if arr(i) == 0 then
          if arr(i + 1) == 1 then end = i
          else
            out.write(0); i += 2
        else
          out.write(arr(i)); i += 1
      if end < 0 || end + 2 >= arr.length then None
      else Some(Parsed(bid, new String(out.toByteArray, UTF_8), arr(end + 2), k.drop(end.toLong + 3)))

  /** レンジの分割点をオブジェクトの境目に揃える（[0x10][bid][esc(key)][00 01] まで）。 */
  def splitBoundary(k: ByteVector): Option[ByteVector] =
    if k.isEmpty || k(0) != ObjectsTag then Some(k)
    else
      val arr = k.toArray
      var i = 9
      var res: Option[ByteVector] = None
      while res.isEmpty && i + 1 < arr.length do
        if arr(i) == 0 && arr(i + 1) == 1 then res = Some(k.take(i.toLong + 2))
        else i += (if arr(i) == 0 then 2 else 1)
      res

/** レコードのバイナリ形式。 */
object S3Codec:
  private def map(w: ByteWriter, m: Map[String, String]): Unit =
    w.varInt(m.size)
    m.toVector.sortBy(_._1).foreach((k, v) => { w.string(k); w.string(v) })
  private def map(r: ByteReader): Map[String, String] = Vector.fill(r.varInt())((r.string(), r.string())).toMap

  private def body(w: ByteWriter, b: Body): Unit = b match
    case Body.Inline(d) => w.u8(0).lenBytes(d): Unit
    case Body.Blocks(refs) =>
      // 2: 参照ごとに EC の情報を持つ形式
      w.u8(2).varInt(refs.size)
      refs.foreach { r =>
        w.bytes(r.id.hash).varInt(r.size)
        r.ec match
          case None         => w.u8(0)
          case Some((k, m)) => w.u8(1).u8(k).u8(m)
      }
  private def body(r: ByteReader): Body = r.u8() match
    case 0 => Body.Inline(ByteVector.view(r.lenBytes()))
    case 1 => Body.Blocks(Vector.fill(r.varInt())(BlockRef(BlockId(ByteVector.view(r.bytes(32))), r.varInt())))
    case 2 =>
      Body.Blocks(Vector.fill(r.varInt()) {
        val id = BlockId(ByteVector.view(r.bytes(32)))
        val size = r.varInt()
        BlockRef(id, size, if r.u8() == 1 then Some((r.u8(), r.u8())) else None)
      })
    case k => throw DecodeError(s"body kind $k")

  private def enc(f: ByteWriter => Unit): ByteVector =
    val w = ByteWriter()
    w.u8(1) // 形式のバージョン
    f(w)
    w.toByteVector

  private def dec[A](b: ByteVector)(f: ByteReader => A): A =
    val r = ByteReader(b.toArray)
    if r.u8() != 1 then throw DecodeError("unknown record format")
    f(r)

  def bucket(b: Bucket): ByteVector = enc { w => w.string(b.name); w.u64(b.id).u64(b.createdMillis); w.string(b.owner); map(w, b.config) }
  def bucket(b: ByteVector): Bucket = dec(b)(r => Bucket(r.string(), r.u64(), r.u64(), r.string(), if r.hasRemaining then map(r) else Map.empty))

  private def encryption(w: ByteWriter, e: Option[Encryption]): Unit = e match
    case None                        => w.u8(0): Unit
    case Some(Encryption.SseS3(key)) => w.u8(1).lenBytes(key): Unit
    case Some(Encryption.SseC(md5))  => w.u8(2); w.string(md5)
  private def encryption(r: ByteReader): Option[Encryption] = r.u8() match
    case 0 => None
    case 1 => Some(Encryption.SseS3(ByteVector.view(r.lenBytes())))
    case 2 => Some(Encryption.SseC(r.string()))
    case k => throw DecodeError(s"encryption kind $k")

  def accessKey(k: AccessKey): ByteVector = enc { w =>
    w.string(k.id); w.string(k.secret); w.string(k.name); w.u8(if k.admin then 1 else 0)
    w.varInt(k.grants.size)
    k.grants.toVector.sortBy(_._1).foreach((b, g) => { w.string(b); w.varInt(g) })
  }
  def accessKey(b: ByteVector): AccessKey = dec(b) { r =>
    AccessKey(r.string(), r.string(), r.string(), r.u8() == 1, Vector.fill(r.varInt())((r.string(), r.varInt())).toMap)
  }

  def obj(o: ObjectRecord): ByteVector = enc { w =>
    w.string(o.versionId); w.u64(o.size); w.string(o.etag); w.u64(o.lastModified)
    map(w, o.headers); map(w, o.userMeta); w.string(o.storageClass)
    o.checksum match
      case Some((a, v)) => w.u8(1); w.string(a); w.string(v)
      case None         => w.u8(0)
    body(w, o.body)
    w.u8(if o.deleteMarker then 1 else 0)
    encryption(w, o.encryption)
  }
  def obj(b: ByteVector): ObjectRecord = dec(b) { r =>
    val o = ObjectRecord(r.string(), r.u64(), r.string(), r.u64(), map(r), map(r), r.string(), if r.u8() == 1 then Some((r.string(), r.string())) else None, body(r))
    if r.hasRemaining then o.copy(deleteMarker = r.u8() == 1, encryption = encryption(r)) else o
  }

  def upload(u: UploadRecord): ByteVector = enc { w =>
    w.string(u.uploadId); w.string(u.key); w.u64(u.initiated); map(w, u.headers); map(w, u.userMeta); w.string(u.storageClass)
    encryption(w, u.encryption)
  }
  def upload(b: ByteVector): UploadRecord = dec(b) { r =>
    val u = UploadRecord(r.string(), r.string(), r.u64(), map(r), map(r), r.string())
    if r.hasRemaining then u.copy(encryption = encryption(r)) else u
  }

  def remote(r: Remote): ByteVector = enc { w => w.string(r.name); w.string(r.endpoint); w.string(r.region); w.string(r.accessKey); w.string(r.secret) }
  def remote(b: ByteVector): Remote = dec(b)(r => Remote(r.string(), r.string(), r.string(), r.string(), r.string()))

  def part(p: PartRecord): ByteVector = enc { w =>
    w.u32(p.partNumber).bytes(p.md5).u64(p.size).u64(p.lastModified); body(w, p.body)
  }
  def part(b: ByteVector): PartRecord = dec(b)(r => PartRecord(r.u32(), ByteVector.view(r.bytes(16)), r.u64(), r.u64(), body(r)))
