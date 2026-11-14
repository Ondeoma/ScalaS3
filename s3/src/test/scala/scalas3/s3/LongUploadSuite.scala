package scalas3.s3

import cats.effect.IO
import fs2.{Chunk, Stream}
import fs2.io.file.Path
import munit.CatsEffectSuite
import scalas3.block.BlockStore
import scalas3.sim.SimFileSystem

import scala.concurrent.duration.*

/** GC の猶予期間より長くかかるアップロード。 */
class LongUploadSuite extends CatsEffectSuite:

  private val blockSize = 1024
  private val grace = 600.millis

  /** 1 ブロックずつ間をあけて流す（全体で猶予期間より長くかかる）。 */
  private def slowBody(blocks: Int, gap: FiniteDuration): Stream[IO, Byte] =
    Stream.range(0, blocks).evalMap(i => IO.sleep(gap).as(Chunk.array(Array.fill(blockSize)(i.toByte)))).unchunks

  /** GC を繰り返す。参照は無く、「使用中」の保護も毎回外す（ブロックを置いたノードの再起動を模す）。 */
  private def hostileGc(store: BlockStore): IO[Nothing] =
    (IO.sleep(100.millis) >> store.list.map(_._1).compile.toVector.flatMap { ids =>
      store.release(ids ++ ids ++ ids) >> IO.realTime.flatMap { now =>
        store.list.evalMap((id, d) => store.deleteIfUnused(id, d, now.toMillis - grace.toMillis, _ => false)).compile.drain
      }
    }).foreverM

  test("アップロード中は書いたブロックの更新時刻を進め直すので、保護が消えても GC に消されない") {
    for
      fs <- SimFileSystem.create(1L, tornWrites = false)
      r <- BlockStore(fs, Vector(Path("d0"))).use { store =>
        val data = DataPath(store, None, blockSize, inlineThreshold = 16, keepAliveInterval = grace / 3)
        hostileGc(store).background.surround(data.ingest(slowBody(8, 200.millis), wantSha256 = false, None)).flatMap { ing =>
          data.missing(ing.body).map(m => (ing.longUpload, m))
        }
      }
    yield
      val (long, missing) = r
      assert(long, "longUpload should be set")
      assertEquals(missing, Set.empty)
  }

  test("ブロックが失われていれば、コミット前の確認で分かる") {
    for
      fs <- SimFileSystem.create(2L, tornWrites = false)
      m <- BlockStore(fs, Vector(Path("d0"))).use { store =>
        val data = DataPath(store, None, blockSize, inlineThreshold = 16)
        data.ingest(slowBody(3, 1.millis), wantSha256 = false, None).flatMap { ing =>
          val ids = ing.body.blockIds.toVector
          // 1 つ目を強制的に消す
          store.release(ids) >> store.list.filter(_._1 == ids.head).evalMap((id, d) => store.deleteIfUnused(id, d, Long.MaxValue, _ => false)).compile.drain >>
            data.missing(ing.body).map(_ -> ids.head)
        }
      }
    yield assertEquals(m._1, Set(m._2))
  }

  test("（比較）進め直さなければ、保護が消えたブロックは GC に消される") {
    for
      fs <- SimFileSystem.create(3L, tornWrites = false)
      missing <- BlockStore(fs, Vector(Path("d0"))).use { store =>
        val data = DataPath(store, None, blockSize, inlineThreshold = 16, keepAliveInterval = 1.hour)
        hostileGc(store).background.surround(data.ingest(slowBody(8, 200.millis), wantSha256 = false, None)).flatMap(ing => data.missing(ing.body))
      }
    yield assert(missing.nonEmpty, "without keep-alive some blocks should have been collected")
  }
