package scalas3.sim

import cats.effect.{IO, Ref, Resource}
import fs2.io.file.Path
import scalas3.runtime.{FileHandle, FileSystem, FileSystemException, OpenMode}
import scodec.bits.ByteVector

/** 電源断を模擬できるメモリ上のファイルシステム。
  *
  * 永続性のモデル:
  *   - ファイルの内容は `sync` で永続化される。電源断のとき、未 sync の書き込みは消える。
  *     ただし `tornWrites` が有効なら、未 sync の書き込みのうち先頭からランダムな個数（最後の 1 つは途中まで）が残る。
  *   - ディレクトリエントリ（作成・削除・リネーム）は、親ディレクトリの `syncDir` で永続化される。
  *   - ディレクトリそのものの作成は、単純化のため即座に永続化されたものとする。
  *
  * `failAfter(n)` を設定すると、n 回の操作のあと全操作が [[SimCrash]] で失敗する。
  * その後 `powerFail` を呼ぶと電源断後の状態になり、以後は新しいハンドルで操作できる。
  */
final class SimFileSystem private (ref: Ref[IO, SimFileSystem.State]) extends FileSystem:
  import SimFileSystem.*

  private def key(p: Path): String = p.normalize.toString
  private def parentKey(p: Path): Option[String] = p.normalize.parent.map(_.toString)

  /** キー k がディレクトリ dk の直下にあるか。 */
  private def isChildOf(k: String, dk: String): Boolean = Path(k).parent.exists(_.toString == dk)

  /** 操作の予算を 1 消費する。クラッシュ済みなら失敗。 */
  private def step[A](f: State => Either[Throwable, (State, A)]): IO[A] =
    ref
      .modify { s =>
        if s.crashed then (s, Left(SimCrash()))
        else
          s.budget match
            case Some(0L) => (s.copy(crashed = true), Left(SimCrash()))
            case b =>
              val s1 = s.copy(budget = b.map(_ - 1))
              f(s1) match
                case Left(e)        => (s1, Left(e))
                case Right((s2, a)) => (s2, Right(a))
      }
      .flatMap(IO.fromEither)

  private def requireParent(s: State, p: Path): Either[Throwable, Unit] =
    parentKey(p) match
      case Some(pk) if !s.dirs.contains(pk) => Left(FileSystemException(s"no such directory: $pk"))
      case _                                => Right(())

  def open(path: Path, mode: OpenMode): Resource[IO, FileHandle] =
    val k = key(path)
    val acquire = step { s =>
      for
        _ <- requireParent(s, path)
        (s1, ino) <- (mode, s.current.get(k)) match
          case (OpenMode.Read, None)         => Left(FileSystemException(s"no such file: $k"))
          case (OpenMode.CreateNew, Some(_)) => Left(FileSystemException(s"file exists: $k"))
          case (_, Some(ino))                => Right((s, ino))
          case (_, None) =>
            val ino = s.nextInode
            Right((s.copy(nextInode = ino + 1, inodes = s.inodes.updated(ino, Inode.empty), current = s.current.updated(k, ino)), ino))
      yield (s1, Handle(ino, s1.generation, mode != OpenMode.Read))
    }
    Resource.eval(acquire)

  def exists(path: Path): IO[Boolean] =
    val k = key(path)
    step(s => Right((s, s.current.contains(k) || s.dirs.contains(k))))

  def list(dir: Path): IO[List[Path]] =
    val dk = key(dir)
    step { s =>
      if !s.dirs.contains(dk) then Left(FileSystemException(s"no such directory: $dk"))
      else
        val files = s.current.keys.filter(isChildOf(_, dk))
        val subdirs = s.dirs.filter(isChildOf(_, dk))
        Right((s, (files ++ subdirs).toList.map(Path(_)).sortBy(_.fileName.toString)))
    }

  def createDirectories(dir: Path): IO[Unit] =
    step { s =>
      val all = Iterator.iterate(Option(dir.normalize))(_.flatMap(_.parent)).takeWhile(_.isDefined).flatten
      Right((s.copy(dirs = s.dirs ++ all.map(_.toString)), ()))
    }

  def delete(path: Path): IO[Unit] =
    deleteIfExists(path).flatMap(ok => IO.raiseUnless(ok)(FileSystemException(s"no such file: $path")))

  def deleteIfExists(path: Path): IO[Boolean] =
    val k = key(path)
    step(s => Right((s.copy(current = s.current - k), s.current.contains(k))))

  def rename(from: Path, to: Path): IO[Unit] =
    val fk = key(from)
    val tk = key(to)
    step { s =>
      for
        _ <- requireParent(s, to)
        ino <- s.current.get(fk).toRight(FileSystemException(s"no such file: $fk"))
      yield (s.copy(current = s.current - fk + (tk -> ino)), ())
    }

  def syncDir(dir: Path): IO[Unit] =
    val dk = key(dir)
    step { s =>
      val kept = s.durable.filterNot((k, _) => isChildOf(k, dk))
      val synced = s.current.filter((k, _) => isChildOf(k, dk))
      Right((s.copy(durable = kept ++ synced), ()))
    }

  /** path のファイルの inode を f に渡す。無ければ失敗。 */
  private def withFile[A](path: Path)(f: (State, Long) => (State, A)): IO[A] =
    val k = key(path)
    step(s => s.current.get(k).map(f(s, _)).toRight(FileSystemException(s"no such file: $k")))

  def lastModified(path: Path): IO[Long] =
    withFile(path)((s, ino) => (s, s.mtimes.getOrElse(ino, 0L)))

  def touch(path: Path): IO[Unit] =
    IO.realTime.flatMap(now => withFile(path)((s, ino) => (s.copy(mtimes = s.mtimes.updated(ino, now.toMillis)), ())))

  // ---- 障害注入 ----

  /** あと n 回の操作が成功したあと、全操作を SimCrash で失敗させる。 */
  def failAfter(n: Long): IO[Unit] = ref.update(_.copy(budget = Some(n)))

  /** 電源断を起こす。未永続化の変更を（設定に従って）捨て、既存のハンドルを無効にする。 */
  def powerFail: IO[Unit] = ref.update(crashState)

  /** 現在までに実行された操作が予算を使い切ったかどうか。 */
  def isCrashed: IO[Boolean] = ref.get.map(_.crashed)

  private final class Handle(ino: Long, gen: Long, writable: Boolean) extends FileHandle:
    private def withInode[A](f: (State, Inode) => Either[Throwable, (State, A)]): IO[A] =
      step { s =>
        if s.generation != gen then Left(FileSystemException("stale file handle (after power failure)"))
        else s.inodes.get(ino).toRight(FileSystemException("inode vanished")).flatMap(f(s, _))
      }

    private def update(s: State, i: Inode): State = s.copy(inodes = s.inodes.updated(ino, i))

    def size: IO[Long] = withInode((s, i) => Right((s, i.current.size)))

    def read(offset: Long, length: Int): IO[ByteVector] =
      withInode((s, i) => Right((s, i.current.drop(offset).take(length.toLong))))

    def write(offset: Long, data: ByteVector): IO[Unit] =
      if !writable then IO.raiseError(FileSystemException("read-only handle"))
      else
        IO.realTime.flatMap { now =>
          withInode { (s, i) =>
            val op = WriteOp.Data(offset, data)
            val s1 = update(s, i.copy(current = op.applyTo(i.current), pending = i.pending :+ op))
            Right((s1.copy(mtimes = s1.mtimes.updated(ino, now.toMillis)), ()))
          }
        }

    def truncate(size: Long): IO[Unit] =
      withInode { (s, i) =>
        val op = WriteOp.Trunc(size)
        Right((update(s, i.copy(current = op.applyTo(i.current), pending = i.pending :+ op)), ()))
      }

    def sync: IO[Unit] =
      withInode((s, i) => Right((update(s, Inode(i.current, i.current, Vector.empty)), ())))

object SimFileSystem:

  final class SimCrash extends FileSystemException("simulated crash")

  enum WriteOp:
    case Data(offset: Long, bytes: ByteVector)
    case Trunc(size: Long)

    def applyTo(content: ByteVector): ByteVector = this match
      case Data(offset, bytes) =>
        val padded = if content.size < offset then content ++ ByteVector.fill(offset - content.size)(0) else content
        padded.take(offset) ++ bytes ++ padded.drop(offset + bytes.size)
      case Trunc(size) =>
        if content.size >= size then content.take(size) else content ++ ByteVector.fill(size - content.size)(0)

  final case class Inode(durable: ByteVector, current: ByteVector, pending: Vector[WriteOp])
  object Inode:
    val empty: Inode = Inode(ByteVector.empty, ByteVector.empty, Vector.empty)

  final case class State(
      nextInode: Long,
      inodes: Map[Long, Inode],
      dirs: Set[String],
      current: Map[String, Long],
      durable: Map[String, Long],
      generation: Long,
      crashed: Boolean,
      budget: Option[Long],
      rng: SimRandom,
      tornWrites: Boolean,
      mtimes: Map[Long, Long] = Map.empty
  )

  private def crashState(s: State): State =
    var rng = s.rng
    val live = s.durable.values.toSet
    val inodes = s.inodes.collect {
      case (ino, i) if live.contains(ino) =>
        val content =
          if !s.tornWrites || i.pending.isEmpty then i.durable
          else
            val (r1, n) = rng.nextInt(i.pending.size + 1)
            rng = r1
            val full = i.pending.take(n)
            val base = full.foldLeft(i.durable)((c, op) => op.applyTo(c))
            // 次の書き込みが途中まで反映されることもある
            i.pending.lift(n) match
              case Some(WriteOp.Data(off, bytes)) if bytes.nonEmpty =>
                val (r2, cut) = rng.nextLong(bytes.size)
                rng = r2
                if cut == 0 then base else WriteOp.Data(off, bytes.take(cut)).applyTo(base)
              case _ => base
        ino -> Inode(content, content, Vector.empty)
    }
    s.copy(
      inodes = inodes,
      current = s.durable,
      generation = s.generation + 1,
      crashed = false,
      budget = None,
      rng = rng
    )

  def create(seed: Long, tornWrites: Boolean = true): IO[SimFileSystem] =
    Ref
      .of[IO, State](
        State(1L, Map.empty, Set.empty, Map.empty, Map.empty, 0L, false, None, SimRandom(seed), tornWrites)
      )
      .map(SimFileSystem(_))
