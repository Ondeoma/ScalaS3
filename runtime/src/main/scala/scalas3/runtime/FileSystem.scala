package scalas3.runtime

import cats.effect.{IO, Resource}
import fs2.io.file.Path
import scodec.bits.ByteVector

enum OpenMode:
  /** 読み取り専用。存在しなければエラー。 */
  case Read

  /** 読み書き。存在しなければ作成する。 */
  case ReadWrite

  /** 新規作成。既に存在すればエラー。 */
  case CreateNew

/** ディスク I/O の抽象。
  *
  * 永続性のモデル（本番の POSIX ファイルシステムに合わせる）:
  *   - ファイルへの書き込みは `FileHandle.sync` が完了するまで永続化されない。
  *   - ファイルの作成・削除・リネームは、親ディレクトリに対する `syncDir` が完了するまで永続化されない。
  *   - `rename` は同一ファイルシステム内でアトミックに置き換える。
  *
  * ドメインのコードは java.nio を直接使わず、必ずこの trait を通すこと（決定的シミュレーションのため）。
  */
trait FileSystem:
  def open(path: Path, mode: OpenMode): Resource[IO, FileHandle]
  def exists(path: Path): IO[Boolean]

  /** ディレクトリ直下のエントリ（ファイル名の昇順）。 */
  def list(dir: Path): IO[List[Path]]
  def createDirectories(dir: Path): IO[Unit]
  def delete(path: Path): IO[Unit]
  def deleteIfExists(path: Path): IO[Boolean]
  def rename(from: Path, to: Path): IO[Unit]
  def syncDir(dir: Path): IO[Unit]

  /** 最終更新時刻（エポックミリ秒）。 */
  def lastModified(path: Path): IO[Long]

  /** 最終更新時刻を現在にする（内容は変えない）。 */
  def touch(path: Path): IO[Unit]

  /** 一時ファイルに書いて sync し、リネームしてディレクトリを sync する。 */
  def writeAtomically(path: Path, data: ByteVector): IO[Unit] =
    val tmp = Path(path.toString + ".tmp")
    open(tmp, OpenMode.ReadWrite).use(h => h.truncate(0) >> h.write(0, data) >> h.sync) >>
      rename(tmp, path) >>
      path.parent.fold(IO.unit)(syncDir)

  def readAll(path: Path): IO[ByteVector] =
    open(path, OpenMode.Read).use(h => h.size.flatMap(s => h.read(0, s.toInt)))

trait FileHandle:
  def size: IO[Long]

  /** offset から最大 length バイト読む。EOF では短くなる。 */
  def read(offset: Long, length: Int): IO[ByteVector]
  def write(offset: Long, data: ByteVector): IO[Unit]
  def truncate(size: Long): IO[Unit]
  def sync: IO[Unit]

class FileSystemException(msg: String) extends java.io.IOException(msg)
