package scalas3.lsm

import scalas3.core.{ByteWriter, Crc32c, DecodeError}
import scodec.bits.ByteVector

/** ある時点でのファイル構成。levels(0) は新しい順、levels(n>=1) は最小キーの昇順で重なりがない。 */
final case class Version(levels: Vector[Vector[SstMeta]]):
  def files: Iterator[SstMeta] = levels.iterator.flatten
  def fileIds: Set[Long] = files.map(_.id).toSet
  def levelBytes(n: Int): Long = levels(n).iterator.map(_.size).sum

  /** level より下（番号が大きい）にファイルがないか。 */
  def isBottom(level: Int): Boolean = levels.drop(level + 1).forall(_.isEmpty)

  /** key を含みうるファイル（新しい順）。L0 は重なりがあるので該当するものすべて、L1 以降は各レベル 1 つまで。 */
  def filesFor(key: ByteVector): Vector[SstMeta] =
    levels(0).filter(_.contains(key)) ++ levels.iterator.drop(1).flatMap(SstMeta.find(_, key)(identity))

object Version:
  def empty(maxLevels: Int): Version = Version(Vector.fill(maxLevels)(Vector.empty))

/** マニフェスト：版と次のファイル番号を丸ごと記録する。小規模なので差分ログにはしない。
  * {{{
  * [magic u32][format u8][nextFileId u64][levels u8][count varint]
  *   ([level u8][id u64][smallest lenBytes][largest lenBytes][size u64][entries u64])*
  * [crc32c u32]
  * }}}
  */
final case class Manifest(nextFileId: Long, version: Version)

object Manifest:
  private val Magic = 0x4c534d4d // "LSMM"
  private val Format = 1

  def encode(m: Manifest): ByteVector =
    val w = ByteWriter()
    val files = m.version.files.toVector
    w.u32(Magic).u8(Format).u64(m.nextFileId).u8(m.version.levels.size).varInt(files.size)
    files.foreach(f => w.u8(f.level).u64(f.id).lenBytes(f.smallest).lenBytes(f.largest).u64(f.size).u64(f.entries))
    val body = w.toArray
    ByteVector.view(body) ++ ByteVector.fromInt(Crc32c(body))

  def decode(bytes: ByteVector): Either[String, Manifest] =
    verifyCrc(bytes).flatMap { r =>
      try
        if r.u32() != Magic then Left("bad magic")
        else if r.u8() != Format then Left("unknown format")
        else
          val next = r.u64()
          val nlevels = r.u8()
          val n = r.varInt()
          val files = Vector.fill(n)(
            SstMeta(
              level = r.u8(),
              id = r.u64(),
              smallest = ByteVector.view(r.lenBytes()),
              largest = ByteVector.view(r.lenBytes()),
              size = r.u64(),
              entries = r.u64()
            )
          )
          val levels = Vector.tabulate(nlevels) { lv =>
            val fs = files.filter(_.level == lv)
            if lv == 0 then fs.sortBy(-_.id) else fs.sorted(using SstMeta.bySmallest)
          }
          Right(Manifest(next, Version(levels)))
      catch case e: DecodeError => Left(e.getMessage)
    }
