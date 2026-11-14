package scalas3.raft

import cats.effect.IO
import fs2.io.file.Path
import munit.CatsEffectSuite
import scalas3.core.{ByteReader, ByteWriter}
import scalas3.sim.SimFileSystem
import scodec.bits.ByteVector

class RaftStorageSuite extends CatsEffectSuite:

  private val dir = Path("raft")
  private def e(i: Long, t: Long) = LogEntry(i, t, Payload.Command(ByteVector.fromLong(i)))

  test("メッセージのコーデックは往復で元に戻る") {
    val m = Membership(Set("a", "b"), Set("c"), Set("x"))
    val msgs = List(
      Message.Vote(3, true, 10, 2, false),
      Message.VoteResp(3, false, true),
      Message.Append(4, 9, 3, Vector(e(10, 4), LogEntry(11, 4, Payload.Config(m)), LogEntry(12, 4, Payload.Noop)), 8, 77),
      Message.AppendResp(4, false, 5, 1),
      Message.Snapshot(5, SnapshotMeta(100, 4, m), ByteVector(1, 2, 3)),
      Message.SnapshotResp(5, 100),
      Message.TimeoutNow(6)
    )
    msgs.foreach { msg =>
      val w = ByteWriter()
      RaftCodec.message(w, msg)
      assertEquals(RaftCodec.message(ByteReader(w.toArray)), msg)
    }
  }

  test("追記・切り詰め・ハードステートを再起動後に復元できる") {
    for
      fs <- SimFileSystem.create(1L, tornWrites = false)
      _ <- FileRaftStorage.open(fs, dir).use { st =>
        st.append(Some(HardState(2, Some("a"))), None, Vector(e(1, 1), e(2, 1), e(3, 2))) >>
          st.append(None, Some(3), Vector(e(3, 3), e(4, 3))) >>
          st.append(Some(HardState(3, None)), Some(4), Vector.empty) >>
          st.sync
      }
      _ <- fs.powerFail
      loaded <- FileRaftStorage.open(fs, dir).use(_.load)
    yield
      assertEquals(loaded.hs, HardState(3, None))
      assertEquals(loaded.entries, Vector(e(1, 1), e(2, 1), e(3, 3)))
  }

  test("sync していない末尾は失われ、書きかけのレコードは切り捨てられる") {
    val results = (1L to 30L).toList.map { seed =>
      for
        fs <- SimFileSystem.create(seed)
        _ <- FileRaftStorage.open(fs, dir).use { st =>
          st.append(None, None, Vector(e(1, 1), e(2, 1))) >> st.sync >>
            st.append(None, None, Vector(e(3, 1))) >> st.append(None, None, Vector(e(4, 1)))
        }
        _ <- fs.powerFail
        loaded <- FileRaftStorage.open(fs, dir).use(st => st.load <* st.append(None, None, Vector(e(5, 1))))
      yield loaded.entries.map(_.index)
    }
    IO.parSequenceN(1)(results).map { rs =>
      rs.foreach(r => assert(List(Vector(1L, 2L), Vector(1L, 2L, 3L), Vector(1L, 2L, 3L, 4L)).contains(r), r.toString))
      assert(rs.distinct.size > 1)
    }
  }

  test("スナップショットでログを作り直す") {
    val m = Membership.of("a")
    for
      fs <- SimFileSystem.create(1L, tornWrites = false)
      _ <- FileRaftStorage.open(fs, dir).use { st =>
        st.append(Some(HardState(1, Some("a"))), None, (1L to 10L).map(e(_, 1)).toVector) >> st.sync >>
          st.rewrite(SnapshotMeta(8, 1, m), ByteVector(9, 9), HardState(1, Some("a")), Vector(e(9, 1), e(10, 1))) >>
          st.append(None, None, Vector(e(11, 1))) >> st.sync
      }
      _ <- fs.powerFail
      loaded <- FileRaftStorage.open(fs, dir).use(_.load)
    yield
      assertEquals(loaded.snapshot, Some((SnapshotMeta(8, 1, m), ByteVector(9, 9))))
      assertEquals(loaded.entries.map(_.index), Vector(9L, 10L, 11L))
  }
