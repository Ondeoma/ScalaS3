package scalas3.meta

import cats.effect.IO
import fs2.io.file.Path
import munit.CatsEffectSuite
import scalas3.lsm.Lsm
import scalas3.sim.SimFileSystem
import scodec.bits.ByteVector

class RangeStateMachineSuite extends CatsEffectSuite:

  private def k(s: String) = Keys.DataStart ++ ByteVector(s.getBytes)
  private def put(key: ByteVector, v: Int) = Some(MetaCodec.command(Command.Write(Vector.empty, Vector(KvOp.Put(key, ByteVector(v))))))
  private def res(b: ByteVector) = MetaCodec.writeResult(b)

  test("分割後は区間外のキーへの書き込みと読み込みを拒否し、新しいレンジの記述子と初期位置を書く") {
    for
      fs <- SimFileSystem.create(1L)
      created <- IO.ref(List.empty[RangeDescriptor])
      r <- Lsm.open(fs, Path("lsm")).use { lsm =>
        for
          _ <- RangeManager.bootstrap(lsm, Set("a"))
          sm <- RangeStateMachine.load(1, lsm, d => created.update(d :: _))
          _ <- sm.apply(1, put(k("a"), 1))
          _ <- sm.apply(2, put(k("m"), 2))
          split <- sm.apply(3, Some(MetaCodec.command(Command.Split(k("g"), 2, 1))))
          stale <- sm.apply(4, Some(MetaCodec.command(Command.Split(k("c"), 3, 1)))) // 古い世代
          outside <- sm.apply(5, put(k("z"), 3))
          inside <- sm.apply(6, put(k("b"), 4))
          readOutside <- sm.read(ReadRequest.Get(k("m")))
          right <- RangeStateMachine.load(2, lsm, _ => IO.unit)
          rightRead <- right.read(ReadRequest.Get(k("m")))
          rightApplied <- right.appliedIndex
          applied <- sm.appliedIndex
        yield (res(split), res(stale), res(outside), res(inside), readOutside, rightRead, rightApplied, applied)
      }
      c <- created.get
    yield
      val (split, stale, outside, inside, readOutside, rightRead, rightApplied, applied) = r
      assertEquals(split, WriteResult.Ok)
      assert(stale.isInstanceOf[WriteResult.Rejected])
      assert(outside.isInstanceOf[WriteResult.NotInRange])
      assertEquals(inside, WriteResult.Ok)
      assert(readOutside.isInstanceOf[ReadResult.NotInRange])
      assertEquals(rightRead, ReadResult.Value(Some(ByteVector(2))))
      assertEquals(rightApplied, MetaCodec.splitInitial(Set("a")).index)
      assertEquals(applied, 6L)
      assertEquals(c.map(_.id), List(2L))
  }

  test("統合：凍結した右は読み書きと分割を拒否し、左は右の中身で区間を置き換えて広げる（手元の古いデータは消える）") {
    def cmd(c: Command) = Some(MetaCodec.command(c))
    for
      fs <- SimFileSystem.create(2L)
      retired <- IO.ref(List.empty[Long])
      r <- Lsm.open(fs, Path("lsm")).use { lsm =>
        for
          _ <- RangeManager.bootstrap(lsm, Set("a"))
          left <- RangeStateMachine.load(1, lsm, _ => IO.unit, id => retired.update(id :: _))
          _ <- left.apply(1, put(k("a"), 1))
          _ <- left.apply(2, put(k("m"), 2))
          _ <- left.apply(3, cmd(Command.Split(k("g"), 2, 1)))
          right <- RangeStateMachine.load(2, lsm, _ => IO.unit)
          rd <- right.current.map(_.get)
          _ <- right.apply(6, put(k("n"), 3))
          badFreeze <- right.apply(7, cmd(Command.Freeze(rd.generation + 5)))
          freeze <- right.apply(8, cmd(Command.Freeze(rd.generation)))
          frozen <- right.frozen
          writeFrozen <- right.apply(9, put(k("p"), 4))
          readFrozen <- right.read(ReadRequest.Get(k("m")))
          splitFrozen <- right.apply(10, cmd(Command.Split(k("q"), 3, rd.generation)))
          exported <- right.read(ReadRequest.Export(100))
          state <- right.read(ReadRequest.MergeState)
          // 中身を変えて取り込む（このノードの右のデータは古く、統合のコマンドの中身が正しい、という状況を作る）
          entries = Vector(k("m") -> ByteVector(9))
          ld <- left.current.map(_.get)
          notAdjacent <- left.apply(4, cmd(Command.Merge(rd.copy(start = k("h")), entries, ld.generation, 10)))
          staleGen <- left.apply(5, cmd(Command.Merge(rd, entries, ld.generation - 1, 10)))
          merged <- left.apply(6, cmd(Command.Merge(rd, entries, ld.generation, 10)))
          after <- left.current.map(_.get)
          m <- left.read(ReadRequest.Get(k("m")))
          n <- left.read(ReadRequest.Get(k("n")))
          a <- left.read(ReadRequest.Get(k("a")))
          writeMerged <- left.apply(7, put(k("z"), 5))
        yield (res(badFreeze), res(freeze), frozen, res(writeFrozen), readFrozen, res(splitFrozen), exported, state, rd, ld,
          res(notAdjacent), res(staleGen), res(merged), after, m, n, a, res(writeMerged))
      }
      ret <- retired.get
    yield
      val (badFreeze, freeze, frozen, writeFrozen, readFrozen, splitFrozen, exported, state, rd, ld, notAdjacent, staleGen, merged, after, m, n, a, writeMerged) = r
      assert(badFreeze.isInstanceOf[WriteResult.Rejected])
      assertEquals(freeze, WriteResult.Ok)
      assert(frozen)
      assert(writeFrozen.isInstanceOf[WriteResult.NotInRange])
      assert(readFrozen.isInstanceOf[ReadResult.NotInRange])
      assert(splitFrozen.isInstanceOf[WriteResult.Rejected])
      assertEquals(exported, ReadResult.Entries(Vector(k("m") -> ByteVector(2), k("n") -> ByteVector(3)), false))
      state match
        case ReadResult.Value(Some(v)) => assertEquals(MetaCodec.mergeState(v), (true, rd))
        case other                     => fail(s"unexpected $other")
      assert(notAdjacent.isInstanceOf[WriteResult.Rejected])
      assert(staleGen.isInstanceOf[WriteResult.Rejected])
      assertEquals(merged, WriteResult.Ok)
      assertEquals(ret, List(2L))
      assertEquals(after.end, None)
      assertEquals(after.start, ld.start)
      assert(after.generation > math.max(ld.generation, rd.generation))
      assertEquals(m, ReadResult.Value(Some(ByteVector(9))))
      assertEquals(n, ReadResult.Value(None))
      assertEquals(a, ReadResult.Value(Some(ByteVector(1))))
      assertEquals(writeMerged, WriteResult.Ok)
  }

  test("スナップショットを別ノードに復元すると、区間内のデータとレンジ固有の状態が移り、古いデータは消える") {
    for
      fs1 <- SimFileSystem.create(1L)
      fs2 <- SimFileSystem.create(2L)
      data <- Lsm.open(fs1, Path("lsm")).use { lsm =>
        for
          _ <- RangeManager.bootstrap(lsm, Set("a"))
          sm <- RangeStateMachine.load(1, lsm, _ => IO.unit)
          _ <- sm.apply(1, put(k("a"), 1))
          _ <- sm.apply(2, put(k("b"), 2))
          snap <- sm.snapshot
        yield snap
      }
      got <- Lsm.open(fs2, Path("lsm")).use { lsm =>
        for
          _ <- lsm.put(k("stale"), ByteVector(9))
          sm <- RangeStateMachine.load(1, lsm, _ => IO.unit)
          _ <- sm.restore(10, data)
          a <- sm.read(ReadRequest.Get(k("a")))
          stale <- sm.read(ReadRequest.Get(k("stale")))
          applied <- sm.appliedIndex
        yield (a, stale, applied)
      }
    yield
      assertEquals(got._1, ReadResult.Value(Some(ByteVector(1))))
      assertEquals(got._2, ReadResult.Value(None))
      assertEquals(got._3, 10L)
  }
