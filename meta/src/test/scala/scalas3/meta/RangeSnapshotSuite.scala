package scalas3.meta

import cats.effect.IO
import cats.syntax.all.*
import fs2.io.file.Path
import munit.CatsEffectSuite
import scalas3.lsm.{Lsm, LsmConfig, WriteOp}
import scalas3.raft.{Membership, SnapshotMeta}
import scalas3.sim.SimFileSystem
import scodec.bits.ByteVector

/** スナップショットのストリームの送受信と、取り込みの途中で落ちたときの扱い。 */
class RangeSnapshotSuite extends CatsEffectSuite:

  private def k(s: String) = Keys.DataStart ++ ByteVector(s.getBytes)
  private def put(key: ByteVector, v: String) =
    Some(MetaCodec.command(Command.Write(Vector.empty, Vector(KvOp.Put(key, ByteVector(v.getBytes))))))
  private val lsmConfig = LsmConfig(memtableBytes = 64 * 1024, blockBytes = 1024, targetFileBytes = 32 * 1024)
  private val meta = SnapshotMeta(5000, 3, Membership.of("a"))
  private val dir = Path("lsm")

  private def dataOf(lsm: Lsm) = lsm.scan(Keys.DataStart, None).compile.toVector
  private def stagingOf(lsm: Lsm) = lsm.scan(Keys.StagingTag, scalas3.core.Bytes.prefixEnd(Keys.StagingTag)).compile.toVector

  /** 送る側：1200 件（かたまりは複数になる）。受ける側：古いデータ（重なるキーと、送る側にないキー）。 */
  private def setup: IO[(Vector[ByteVector], Vector[(ByteVector, ByteVector)], SimFileSystem)] =
    for
      srcFs <- SimFileSystem.create(1L, tornWrites = false)
      src <- Lsm.open(srcFs, dir, lsmConfig).use { lsm =>
        for
          _ <- RangeManager.bootstrap(lsm, Set("a"))
          sm <- RangeStateMachine.load(1, lsm, _ => IO.unit)
          _ <- (1 to 1200).toList.traverse_(i => sm.apply(i.toLong, put(k(f"key$i%05d"), s"new$i")))
          chunks <- sm.snapshotSource.use(_.compile.toVector)
          data <- dataOf(lsm)
        yield (chunks, data)
      }
      dstFs <- SimFileSystem.create(2L, tornWrites = false)
      _ <- Lsm.open(dstFs, dir, lsmConfig).use { lsm =>
        for
          _ <- RangeManager.bootstrap(lsm, Set("a"))
          sm <- RangeStateMachine.load(1, lsm, _ => IO.unit)
          _ <- (1 to 300).toList.traverse_(i => sm.apply(i.toLong, put(k(if i % 2 == 0 then f"key$i%05d" else s"old$i"), s"old$i")))
          _ <- lsm.flush
        yield ()
      }
    yield (src._1, src._2, dstFs)

  test("かたまりに分けて送ったスナップショットを取り込むと、送る側と同じになる") {
    for
      (chunks, srcData, fs) <- setup
      _ = assert(chunks.size > 1, s"chunks: ${chunks.size}")
      r <- Lsm.open(fs, dir, lsmConfig).use { lsm =>
        for
          sm <- RangeStateMachine.load(1, lsm, _ => IO.unit)
          _ <- chunks.zipWithIndex.traverse_((c, i) => sm.stageSnapshot(7L, i, c))
          stored <- sm.install(meta, Some(7L), ByteVector.empty)
          data <- dataOf(lsm)
          applied <- sm.appliedIndex
          snap <- sm.persistedSnapshot
          staging <- stagingOf(lsm)
        yield (stored, data, applied, snap, staging)
      }
    yield
      val (stored, data, applied, snap, staging) = r
      assertEquals(stored, None)
      assertEquals(data, srcData)
      assertEquals((applied, snap), (meta.index, Some(meta)))
      assert(staging.isEmpty)
  }

  test("転送の途中で落ちたら、元のデータはそのままで、預かりは起動時に消える") {
    for
      (chunks, _, fs) <- setup
      before <- Lsm.open(fs, dir, lsmConfig).use(dataOf)
      _ <- Lsm.open(fs, dir, lsmConfig).use { lsm =>
        RangeStateMachine.load(1, lsm, _ => IO.unit).flatMap(sm => sm.stageSnapshot(9L, 0, chunks.head)) >> lsm.flush
      }
      _ <- fs.powerFail
      r <- Lsm.open(fs, dir, lsmConfig).use { lsm =>
        RangeStateMachine.load(1, lsm, _ => IO.unit).flatMap(sm => (dataOf(lsm), sm.appliedIndex, stagingOf(lsm)).tupled)
      }
    yield
      val (data, applied, staging) = r
      assertEquals(data, before)
      assertEquals(applied, 300L)
      assert(staging.isEmpty)
  }

  test("切り替えの途中で落ちたら、起動時にやり直して送る側と同じになる（印だけ書いた時点でも、写しかけでも）") {
    List(false, true).traverse_ { partial =>
      for
        (chunks, srcData, fs) <- setup
        _ <- Lsm.open(fs, dir, lsmConfig).use { lsm =>
          for
            sm <- RangeStateMachine.load(1, lsm, _ => IO.unit)
            _ <- chunks.zipWithIndex.traverse_((c, i) => sm.stageSnapshot(7L, i, c))
            d <- lsm.get(Keys.staging(1, 7L) ++ Keys.rangeLocal(1, Keys.Descriptor)).map(v => RangeDescriptor.decode(v.get))
            _ <- lsm.write(Seq(WriteOp.Put(Keys.rangeLocal(1, Keys.Installing), RangeStateMachine.encodeMarker(7L, meta, d)))) >> lsm.flush
            // 写しかけ：元のデータを半分消し、預かりの一部を写したところで落ちる
            _ <- (for
              old <- dataOf(lsm)
              _ <- lsm.write(old.take(old.size / 2).map((key, _) => WriteOp.Delete(key)))
              staged <- lsm.scan(Keys.staging(1, 7L), scalas3.core.Bytes.prefixEnd(Keys.staging(1, 7L))).take(100).compile.toVector
              _ <- lsm.write(staged.map((key, v) => WriteOp.Put(key.drop(Keys.staging(1, 7L).size), v)))
              _ <- lsm.flush
            yield ()).whenA(partial)
          yield ()
        }
        _ <- fs.powerFail
        r <- Lsm.open(fs, dir, lsmConfig).use { lsm =>
          RangeStateMachine.load(1, lsm, _ => IO.unit).flatMap { sm =>
            (dataOf(lsm), sm.appliedIndex, sm.persistedSnapshot, stagingOf(lsm), lsm.get(Keys.rangeLocal(1, Keys.Installing))).tupled
          }
        }
      yield
        val (data, applied, snap, staging, marker) = r
        assertEquals(data, srcData, s"partial=$partial")
        assertEquals((applied, snap), (meta.index, Some(meta)))
        assert(staging.isEmpty && marker.isEmpty)
    }
  }
