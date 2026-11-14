package scalas3.sim

import cats.syntax.all.*
import fs2.io.file.Path
import munit.CatsEffectSuite
import scalas3.runtime.OpenMode
import scodec.bits.ByteVector

class SimFileSystemSuite extends CatsEffectSuite:

  private val dir = Path("data")
  private val f = Path("data/a")
  private def bv(s: String) = ByteVector(s.getBytes)

  test("sync 済みの内容と syncDir 済みのエントリは電源断後も残る") {
    for
      fs <- SimFileSystem.create(1L, tornWrites = false)
      _ <- fs.createDirectories(dir)
      _ <- fs.open(f, OpenMode.CreateNew).use(h => h.write(0, bv("hello")) >> h.sync)
      _ <- fs.syncDir(dir)
      _ <- fs.open(f, OpenMode.ReadWrite).use(h => h.write(5, bv(" world")))
      _ <- fs.powerFail
      data <- fs.readAll(f)
    yield assertEquals(data, bv("hello"))
  }

  test("syncDir していないファイルは電源断で消える") {
    for
      fs <- SimFileSystem.create(1L, tornWrites = false)
      _ <- fs.createDirectories(dir)
      _ <- fs.open(f, OpenMode.CreateNew).use(h => h.write(0, bv("x")) >> h.sync)
      _ <- fs.powerFail
      ex <- fs.exists(f)
    yield assert(!ex)
  }

  test("rename は syncDir までは永続化されない") {
    val g = Path("data/b")
    for
      fs <- SimFileSystem.create(1L, tornWrites = false)
      _ <- fs.createDirectories(dir)
      _ <- fs.open(f, OpenMode.CreateNew).use(h => h.write(0, bv("x")) >> h.sync)
      _ <- fs.syncDir(dir)
      _ <- fs.rename(f, g)
      before <- fs.exists(g)
      _ <- fs.powerFail
      a <- fs.exists(f)
      b <- fs.exists(g)
    yield
      assert(before)
      assert(a && !b)
  }

  test("torn write では未 sync の書き込みの一部だけが残りうる（ただし sync 済みは必ず残る）") {
    val prefixes = (1L to 50L).toList.traverse { seed =>
      for
        fs <- SimFileSystem.create(seed)
        _ <- fs.createDirectories(dir)
        _ <- fs.open(f, OpenMode.CreateNew).use(h => h.write(0, bv("AAAA")) >> h.sync)
        _ <- fs.syncDir(dir)
        _ <- fs.open(f, OpenMode.ReadWrite).use(h => h.write(4, bv("BBBB")) >> h.write(8, bv("CCCC")))
        _ <- fs.powerFail
        data <- fs.readAll(f)
      yield data
    }
    prefixes.map { results =>
      results.foreach(d => assert(bv("AAAABBBBCCCC").startsWith(d) && d.size >= 4, d.decodeUtf8Lenient))
      assert(results.distinct.size > 1, "乱数で結果がばらつくはず")
    }
  }

  test("failAfter のあとは全操作が失敗し、powerFail で復帰する") {
    for
      fs <- SimFileSystem.create(1L)
      _ <- fs.createDirectories(dir)
      _ <- fs.failAfter(1)
      _ <- fs.exists(f)
      r <- fs.exists(f).attempt
      _ <- fs.powerFail
      ok <- fs.exists(dir)
    yield
      assert(r.left.exists(_.isInstanceOf[SimFileSystem.SimCrash]))
      assert(ok)
  }

  test("電源断の前に開いたハンドルは使えない") {
    for
      fs <- SimFileSystem.create(1L)
      _ <- fs.createDirectories(dir)
      r <- fs.open(f, OpenMode.CreateNew).use(h => fs.powerFail >> h.size.attempt)
    yield assert(r.isLeft)
  }

