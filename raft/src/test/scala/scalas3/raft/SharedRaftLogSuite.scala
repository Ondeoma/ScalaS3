package scalas3.raft

import cats.effect.IO
import cats.syntax.all.*
import fs2.io.file.Path
import munit.CatsEffectSuite
import scalas3.sim.{Sim, SimFileSystem}
import scodec.bits.ByteVector

class SharedRaftLogSuite extends CatsEffectSuite:

  private val dir = Path("raftlog")
  private def e(i: Long, t: Long, size: Int = 8) = LogEntry(i, t, Payload.Command(ByteVector.fill(size.toLong)(i.toByte)))
  private val m = Membership.of("a")

  test("複数のグループの追記・切り詰め・ハードステートを、電源断の後に復元できる") {
    for
      fs <- SimFileSystem.create(1L, tornWrites = false)
      _ <- SharedRaftLog.open(fs, dir).use { log =>
        for
          a <- log.storage(1L)
          b <- log.storage(2L)
          _ <- a.append(Some(HardState(2, Some("a"))), None, Vector(e(1, 1), e(2, 1), e(3, 2)))
          _ <- b.append(Some(HardState(5, None)), None, Vector(e(1, 5)))
          _ <- a.append(None, Some(3), Vector(e(3, 3), e(4, 3)))
          _ <- a.append(Some(HardState(3, None)), Some(4), Vector.empty)
          _ <- (a.sync, b.sync).parTupled
        yield ()
      }
      _ <- fs.powerFail
      loaded <- SharedRaftLog.open(fs, dir).use(log => (log.storage(1L).flatMap(_.load), log.storage(2L).flatMap(_.load)).tupled)
    yield
      val (a, b) = loaded
      assertEquals(a.hs, HardState(3, None))
      assertEquals(a.entries, Vector(e(1, 1), e(2, 1), e(3, 3)))
      assertEquals(b.hs, HardState(5, None))
      assertEquals(b.entries, Vector(e(1, 5)))
  }

  test("sync していない末尾は失われ、書きかけのレコードは切り捨てられ、その後も書ける") {
    (1L to 30L).toList.traverse { seed =>
      for
        fs <- SimFileSystem.create(seed)
        _ <- SharedRaftLog.open(fs, dir).use { log =>
          log.storage(1L).flatMap { st =>
            st.append(None, None, Vector(e(1, 1), e(2, 1))) >> st.sync >>
              st.append(None, None, Vector(e(3, 1))) >> st.append(None, None, Vector(e(4, 1)))
          }
        }
        _ <- fs.powerFail
        first <- SharedRaftLog.open(fs, dir).use(log =>
          log.storage(1L).flatMap(st => st.load <* (st.append(None, None, Vector(e(5, 1))) >> st.sync))
        )
        _ <- fs.powerFail
        second <- SharedRaftLog.open(fs, dir).use(_.storage(1L).flatMap(_.load))
      yield
        val idx = first.entries.map(_.index)
        assert(idx.take(2) == Vector(1L, 2L) && idx.forall(_ <= 4), s"seed=$seed: $idx")
        assertEquals(second.entries.map(_.index), idx :+ 5L, s"seed=$seed")
    }
  }

  test("スナップショット後は古いエントリを捨て、スナップショットのデータとともに復元する") {
    for
      fs <- SimFileSystem.create(2L, tornWrites = false)
      _ <- SharedRaftLog.open(fs, dir).use { log =>
        log.storage(7L).flatMap { st =>
          st.append(Some(HardState(1, None)), None, (1L to 10L).map(e(_, 1)).toVector) >> st.sync >>
            st.rewrite(SnapshotMeta(8, 1, m), ByteVector(9, 9), HardState(1, None), Vector(e(9, 1), e(10, 1))) >>
            st.append(None, None, Vector(e(11, 1))) >> st.sync
        }
      }
      _ <- fs.powerFail
      loaded <- SharedRaftLog.open(fs, dir).use(_.storage(7L).flatMap(_.load))
    yield
      assertEquals(loaded.snapshot, Some((SnapshotMeta(8, 1, m), ByteVector(9, 9))))
      assertEquals(loaded.entries.map(_.index), Vector(9L, 10L, 11L))
  }

  test("セグメントが増えると、古いセグメントのグループを書き直してから消す（書かないグループの状態も残る）") {
    val cfg = SharedRaftLog.Config(segmentBytes = 4096, maxSegments = 3)
    Sim.run(
      for
        fs <- SimFileSystem.create(3L, tornWrites = false)
        _ <- SharedRaftLog.open(fs, dir, cfg).use { log =>
          for
            idle <- log.storage(1L)
            busy <- log.storage(2L)
            _ <- idle.append(Some(HardState(4, Some("x"))), None, Vector(e(1, 4), e(2, 4))) >> idle.sync
            // busy だけが書き続ける。セグメントが何度も切り替わる
            _ <- (1L to 400L).toList.traverse_(i => busy.append(None, None, Vector(e(i, 1, 64))) >> busy.sync)
            // 回収は書き込みの後に裏で進むので、少し待つ
            _ <- IO.sleep(scala.concurrent.duration.DurationInt(1).second)
            _ <- busy.append(None, None, Vector(e(401, 1, 64))) >> busy.sync
          yield ()
        }
        segs <- fs.list(dir / "log")
        _ <- fs.powerFail
        loaded <- SharedRaftLog.open(fs, dir, cfg).use(log => (log.storage(1L).flatMap(_.load), log.storage(2L).flatMap(_.load)).tupled)
      yield (segs.size, loaded)
    ).map { case (segCount, (idle, busy)) =>
      assert(segCount <= cfg.maxSegments + 1, s"segments: $segCount")
      assertEquals(idle.hs, HardState(4, Some("x")))
      assertEquals(idle.entries.map(_.index), Vector(1L, 2L))
      assertEquals(busy.entries.map(_.index), (1L to 401L).toVector)
    }
  }

  test("drop したグループは再起動後に残らない") {
    for
      fs <- SimFileSystem.create(4L, tornWrites = false)
      _ <- SharedRaftLog.open(fs, dir).use { log =>
        log.storage(1L).flatMap(st => st.append(Some(HardState(1, None)), None, Vector(e(1, 1))) >> st.sync) >>
          log.storage(2L).flatMap(st => st.append(Some(HardState(2, None)), None, Vector(e(1, 2))) >> st.sync) >>
          log.drop(1L)
      }
      _ <- fs.powerFail
      r <- SharedRaftLog.open(fs, dir).use(log => (log.knows(1L), log.knows(2L)).tupled)
    yield assertEquals(r, (false, true))
  }
