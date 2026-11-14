package scalas3.block

import cats.effect.IO
import cats.syntax.all.*
import fs2.io.file.Path
import munit.CatsEffectSuite
import scalas3.runtime.OpenMode
import scalas3.sim.SimFileSystem
import scodec.bits.ByteVector

import scala.concurrent.duration.*

class BlockStoreSuite extends CatsEffectSuite:

  private val disks = Vector(Path("d0"), Path("d1"), Path("d2"))
  private def data(i: Int) = ByteVector(s"block-$i-".getBytes) ++ ByteVector.fill(100)(i.toByte)

  test("書いたブロックを読め、copies 台のディスクに置かれ、電源断後も残る") {
    for
      fs <- SimFileSystem.create(1L)
      ids <- BlockStore(fs, disks, BlockStoreConfig(copies = 2)).use(bs => (1 to 20).toList.traverse(i => bs.put(data(i))))
      _ <- fs.powerFail
      got <- BlockStore(fs, disks, BlockStoreConfig(copies = 2)).use { bs =>
        ids.traverse(bs.get).product(bs.list.compile.toList)
      }
    yield
      assertEquals(got._1, (1 to 20).map(data).toList)
      assertEquals(got._2.size, 40, "各ブロックが 2 台に置かれる")
  }

  test("1 台のディスク上のブロックが壊れていても、もう 1 台から読める") {
    for
      fs <- SimFileSystem.create(1L)
      r <- BlockStore(fs, disks, BlockStoreConfig(copies = 2)).use { bs =>
        for
          id <- bs.put(data(1))
          first = bs.placement(id).head
          // 1 台目のボリュームの、値の部分を壊す（レコードの頭は 45 バイト）
          p = disks(first) / "volumes" / "blocks" / f"${1L}%016d.vol"
          _ <- fs.open(p, OpenMode.ReadWrite).use(h => h.write(50, ByteVector(0, 0, 0)))
          got <- bs.get(id)
        yield got
      }
    yield assertEquals(r, data(1))
  }

  test("GC は古くて参照も使用中でもないブロックだけを消す") {
    val prog =
      for
        fs <- SimFileSystem.create(1L)
        r <- BlockStore(fs, disks, BlockStoreConfig(copies = 1)).use { bs =>
          for
            a <- bs.put(data(1))
            b <- bs.put(data(2))
            c <- bs.put(data(3))
            _ <- bs.protect(List(c))
            _ <- IO.sleep(2.hours)
            _ <- bs.put(data(2)) // 再利用：更新時刻が進む
            cutoff <- IO.realTime.map(_.toMillis - 1.hour.toMillis)
            listed <- bs.list.compile.toList
            deleted <- listed.traverse((id, d) => bs.deleteIfUnused(id, d, cutoff, _ => false).map(id -> _))
            remaining <- bs.list.map(_._1).compile.toList
          yield (a, b, c, deleted.toMap, remaining.toSet)
        }
      yield r
    scalas3.sim.Sim.run(prog).map { (a, b, c, deleted, remaining) =>
      assertEquals(deleted(a), true)
      assertEquals(deleted(b), false)
      assertEquals(deleted(c), false)
      assertEquals(remaining, Set(b, c))
    }
  }

  test("以前の形式（ブロックごとのファイル）は、起動時にボリュームへ移して消す") {
    for
      fs <- SimFileSystem.create(9L, tornWrites = false)
      ids <- (1 to 5).toList.traverse { i =>
        val d = data(i)
        val id = BlockId.of(d)
        val dir = disks(0) / "blocks" / id.hex.take(2) / id.hex.slice(2, 4)
        fs.createDirectories(dir) >> fs.writeAtomically(dir / id.hex, d).as(id)
      }
      r <- BlockStore(fs, disks, BlockStoreConfig(copies = 1)).use(bs => ids.traverse(bs.get))
      leftovers <- fs.list(disks(0) / "blocks").flatMap(_.flatTraverse(a => fs.list(a).flatMap(_.flatTraverse(fs.list))))
    yield
      assertEquals(r, (1 to 5).toList.map(data))
      assert(leftovers.isEmpty, s"leftover files: $leftovers")
  }
