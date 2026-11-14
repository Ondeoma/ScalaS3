package scalas3.block

import cats.effect.IO
import cats.syntax.all.*
import fs2.io.file.Path
import munit.CatsEffectSuite
import scalas3.runtime.OpenMode
import scalas3.sim.SimFileSystem
import scodec.bits.ByteVector

import scala.concurrent.duration.*

class VolumeStoreSuite extends CatsEffectSuite:

  private val dir = Path("vol")
  private def key(i: Int) = ByteVector.fromInt(i).padLeft(32)
  private def value(i: Int, size: Int = 500) = ByteVector.view(Array.tabulate[Byte](size)(j => (i * 7 + j).toByte))
  // 索引は自動では flush しない（落ちたときに索引の更新が失われる場合を作る）
  private val noFlush = VolumeConfig(indexFlushInterval = 1.hour)

  private def volumes(fs: SimFileSystem) = fs.list(dir).map(_.count(_.fileName.toString.endsWith(".vol")))

  test("書く・読む・重複・消す") {
    for
      fs <- SimFileSystem.create(1L, tornWrites = false)
      r <- VolumeStore.open(fs, dir).use { v =>
        for
          a <- v.put(key(1), value(1))
          b <- v.put(key(1), value(1))
          got <- v.get(key(1))
          missing <- v.get(key(2))
          deleted <- v.delete(key(1))
          after <- v.get(key(1))
        yield (a, b, got, missing, deleted, after)
      }
    yield assertEquals(r, (true, false, Some(value(1)), None, true, None))
  }

  test("索引を flush しないまま落ちても、ボリュームを読み直して値を取り戻す") {
    for
      fs <- SimFileSystem.create(2L, tornWrites = false)
      _ <- VolumeStore.open(fs, dir, noFlush).use(v => (1 to 50).toList.parTraverse_(i => v.put(key(i), value(i))))
      _ <- fs.powerFail
      got <- VolumeStore.open(fs, dir, noFlush).use(v => (1 to 50).toList.traverse(i => v.get(key(i))))
    yield assertEquals(got, (1 to 50).toList.map(i => Some(value(i))))
  }

  test("書きかけの末尾は切り捨て、その前の値は読め、続けて書ける") {
    for
      fs <- SimFileSystem.create(3L, tornWrites = false)
      _ <- VolumeStore.open(fs, dir, noFlush).use(v => (1 to 5).toList.traverse_(i => v.put(key(i), value(i))))
      // 最後のボリュームの末尾に、書きかけのレコードを足す
      p = dir / f"${1L}%016d.vol"
      _ <- fs.open(p, OpenMode.ReadWrite).use(h => h.size.flatMap(s => h.write(s, ByteVector(0x56, 0x4f, 0x4c, 0x31, 32, 1, 2, 3)) >> h.sync))
      _ <- fs.powerFail
      r <- VolumeStore.open(fs, dir, noFlush).use { v =>
        v.put(key(6), value(6)) >> (1 to 6).toList.traverse(i => v.get(key(i)))
      }
    yield assertEquals(r, (1 to 6).toList.map(i => Some(value(i))))
  }

  test("ボリュームを切り替え、消した分は詰め直して回収する。落ちた後も残した値は読め、消した値は消えたまま") {
    val small = VolumeConfig(volumeBytes = 4096, indexFlushInterval = 1.hour)
    for
      fs <- SimFileSystem.create(4L, tornWrites = false)
      r <- VolumeStore.open(fs, dir, small).use { v =>
        for
          _ <- (1 to 60).toList.traverse_(i => v.put(key(i), value(i)))
          before <- volumes(fs)
          _ <- (1 to 60).filter(_ % 5 != 0).toList.traverse_(i => v.delete(key(i)))
          compacted <- v.compact
          after <- volumes(fs)
        yield (before, compacted, after)
      }
      _ <- fs.powerFail
      got <- VolumeStore.open(fs, dir, small).use(v => (1 to 60).toList.traverse(i => v.get(key(i)).map(i -> _)))
    yield
      val (before, compacted, after) = r
      assert(before > 5, s"volumes before: $before")
      assert(compacted > 0 && after < before, s"compacted=$compacted before=$before after=$after")
      got.foreach { (i, v) =>
        assertEquals(v, Option.when(i % 5 == 0)(value(i)), s"key $i")
      }
  }

  test("詰め直しの最中に同じ値が書かれて（重複で時刻だけ進んで）も、値を失わない") {
    val small = VolumeConfig(volumeBytes = 4096, indexFlushInterval = 1.hour)
    (1L to 20L).toList.traverse { seed =>
      scalas3.sim.Sim.run(
        for
          fs <- SimFileSystem.create(seed, tornWrites = false)
          lost <- VolumeStore.open(fs, dir, small).use { v =>
            for
              _ <- (1 to 60).toList.traverse_(i => v.put(key(i), value(i)))
              _ <- (1 to 60).filter(_ % 5 != 0).toList.traverse_(i => v.delete(key(i)))
              live = (1 to 60).filter(_ % 5 == 0).toList
              // 詰め直しと並行して、残した値を同じ中身で書き直す（重複排除：場所は変わらず時刻だけ進む）
              _ <- IO.sleep(10.millis) // 書き直しで時刻が変わるように
              _ <- (v.compact, live.traverse_(i => v.put(key(i), value(i)) >> IO.cede)).parTupled
              got <- live.traverse(i => v.get(key(i)).map(i -> _))
            yield got.collect { case (i, g) if !g.contains(value(i)) => i }
          }
        yield lost,
        seed
      ).map(seed -> _)
    }.map { results =>
      val bad = results.filter(_._2.nonEmpty)
      assert(bad.isEmpty, s"values lost after compaction (seed -> keys): $bad")
    }
  }
