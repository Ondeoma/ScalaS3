package scalas3.runtime

import cats.effect.{IO, Resource}
import fs2.io.file.Path
import scodec.bits.ByteVector

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.{Files, NoSuchFileException, StandardCopyOption, StandardOpenOption as O}
import scala.jdk.CollectionConverters.*

/** java.nio による本番用の実装。 */
object NioFileSystem extends FileSystem:

  def open(path: Path, mode: OpenMode): Resource[IO, FileHandle] =
    val opts = mode match
      case OpenMode.Read      => Seq(O.READ)
      case OpenMode.ReadWrite => Seq(O.READ, O.WRITE, O.CREATE)
      case OpenMode.CreateNew => Seq(O.READ, O.WRITE, O.CREATE_NEW)
    Resource
      .make(IO.blocking(FileChannel.open(path.toNioPath, opts*)))(ch => IO.blocking(ch.close()))
      .map(NioHandle(_))

  def exists(path: Path): IO[Boolean] = IO.blocking(Files.exists(path.toNioPath))

  def list(dir: Path): IO[List[Path]] = IO.blocking {
    val s = Files.list(dir.toNioPath)
    try s.iterator().asScala.map(Path.fromNioPath).toList.sortBy(_.fileName.toString)
    finally s.close()
  }

  def createDirectories(dir: Path): IO[Unit] = IO.blocking(Files.createDirectories(dir.toNioPath)).void

  def delete(path: Path): IO[Unit] = IO.blocking(Files.delete(path.toNioPath))

  def deleteIfExists(path: Path): IO[Boolean] = IO.blocking(Files.deleteIfExists(path.toNioPath))

  def rename(from: Path, to: Path): IO[Unit] =
    IO.blocking(Files.move(from.toNioPath, to.toNioPath, StandardCopyOption.ATOMIC_MOVE)).void

  def syncDir(dir: Path): IO[Unit] = IO.blocking {
    // macOS などディレクトリの fsync に対応しない環境では失敗を無視する。
    try
      val ch = FileChannel.open(dir.toNioPath, O.READ)
      try ch.force(true)
      finally ch.close()
    catch
      case e: NoSuchFileException => throw e
      case _: java.io.IOException => ()
  }

  def lastModified(path: Path): IO[Long] = IO.blocking(Files.getLastModifiedTime(path.toNioPath).toMillis)

  def touch(path: Path): IO[Unit] =
    IO.blocking(Files.setLastModifiedTime(path.toNioPath, java.nio.file.attribute.FileTime.fromMillis(System.currentTimeMillis()))).void

  /** 1 回の read/write に渡す大きさの上限。JDK はヒープ上のバッファを読み書きするとき、その大きさの一時的な
    * ダイレクトバッファをスレッドごとに作って使い回す。大きなまま渡すと、ブロッキング用の多数のスレッドが
    * それぞれ大きなバッファを抱え、ヒープの外のメモリが数百 MB 膨らむ（Linux で測定：421 MB → 7 MB）。
    */
  private val IoChunk = 256 * 1024

  /** buf の残りを IoChunk ずつに区切って op に渡す。op が false を返したら（EOF など）やめる。 */
  private def inChunks(buf: ByteBuffer)(op: ByteBuffer => Boolean): Unit =
    var continue = true
    while continue && buf.hasRemaining do
      val limit = buf.limit()
      buf.limit(math.min(limit, buf.position() + IoChunk))
      continue = op(buf)
      buf.limit(limit): Unit

  private final class NioHandle(ch: FileChannel) extends FileHandle:
    def size: IO[Long] = IO.blocking(ch.size())

    def read(offset: Long, length: Int): IO[ByteVector] = IO.blocking {
      val buf = ByteBuffer.allocate(length)
      var pos = offset
      inChunks(buf) { b =>
        val n = ch.read(b, pos)
        if n >= 0 then pos += n
        n >= 0
      }
      buf.flip()
      ByteVector.view(buf)
    }

    def write(offset: Long, data: ByteVector): IO[Unit] = IO.blocking {
      val buf = data.toByteBuffer
      var pos = offset
      inChunks(buf) { b =>
        while b.hasRemaining do pos += ch.write(b, pos)
        true
      }
    }

    def truncate(size: Long): IO[Unit] = IO.blocking(ch.truncate(size)).void
    def sync: IO[Unit] = IO.blocking(ch.force(true))
