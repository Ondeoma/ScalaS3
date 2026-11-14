package scalas3.block

import cats.effect.std.Mutex
import cats.effect.{IO, Ref}
import cats.syntax.all.*
import fs2.Stream
import fs2.io.file.Path
import scalas3.runtime.FileSystem

/** BlockStore と ShardStore に共通の、GC からの保護。書き込み中のアップロードが使う ID を「使用中」として数え、
  * 消すときは ID ごとのロックを取ってから使用中でないことを確かめる（確かめてから消すまでの間に使用中にならないように）。
  */
private[block] final class GcGuard private (inUse: Ref[IO, Map[BlockId, Int]], locks: Vector[Mutex[IO]]):

  def protect(ids: Iterable[BlockId]): IO[Unit] =
    inUse.update(m => ids.foldLeft(m)((acc, id) => acc.updated(id, acc.getOrElse(id, 0) + 1)))

  def release(ids: Iterable[BlockId]): IO[Unit] =
    inUse.update(m => ids.foldLeft(m)((acc, id) => acc.get(id).fold(acc)(n => if n <= 1 then acc - id else acc.updated(id, n - 1))))

  /** id のロックを取って io を行う。 */
  def locked[A](id: BlockId)(io: IO[A]): IO[A] =
    locks((id.hash.take(4).toInt() & Int.MaxValue) % locks.size).lock.surround(io)

  /** id のロックを取り、使用中でなければ delete を行う。使用中なら false。 */
  def deleteUnlessInUse(id: BlockId)(delete: IO[Boolean]): IO[Boolean] =
    locked(id)(inUse.get.map(_.contains(id)).ifM(IO.pure(false), delete))

private[block] object GcGuard:
  def create: IO[GcGuard] =
    (Ref.of[IO, Map[BlockId, Int]](Map.empty), Vector.fill(64)(Mutex[IO]).sequence).mapN(new GcGuard(_, _))

private[block] object Legacy:
  /** 以前の形式（`<root>/xx/yy/<ファイル>`）のファイルをすべて流す。root が無ければ空。 */
  def files(fs: FileSystem, root: Path): Stream[IO, Path] =
    Stream.eval(fs.exists(root)).flatMap { exists =>
      if !exists then Stream.empty
      else Stream.evalSeq(fs.list(root)).flatMap(a => Stream.evalSeq(fs.list(a))).flatMap(b => Stream.evalSeq(fs.list(b)))
    }
