package scalas3.lsm

import cats.effect.IO
import cats.syntax.all.*
import fs2.io.file.{Files, Path}
import munit.CatsEffectSuite
import scalas3.core.Bytes
import scalas3.runtime.{FileSystem, NioFileSystem}
import scalas3.sim.{SimFileSystem, SimRandom}
import scodec.bits.ByteVector

import scala.collection.immutable.TreeMap

class LsmSuite extends CatsEffectSuite:

  given Ordering[ByteVector] = Bytes.unsignedOrdering

  /** フラッシュとコンパクションが頻繁に起きる小さな設定。 */
  private val tiny = LsmConfig(
    memtableBytes = 2048,
    maxImmutables = 2,
    blockBytes = 128,
    targetFileBytes = 1024,
    l0CompactionTrigger = 3,
    baseLevelBytes = 4096,
    levelMultiplier = 4,
    maxLevels = 5
  )

  private val dir = Path("db")
  private def k(i: Int) = ByteVector(f"key-$i%06d".getBytes)
  private def v(s: String) = ByteVector(s.getBytes)

  private def all(lsm: Lsm): IO[List[(ByteVector, ByteVector)]] = lsm.scan(ByteVector.empty, None).compile.toList

  private def sim(seed: Long = 1L, torn: Boolean = true): IO[SimFileSystem] = SimFileSystem.create(seed, torn)

  test("put / get / delete / scan") {
    sim().flatMap { fs =>
      Lsm.open(fs, dir, tiny).use { lsm =>
        for
          _ <- lsm.put(k(1), v("a"))
          _ <- lsm.put(k(2), v("b"))
          _ <- lsm.put(k(3), v("c"))
          _ <- lsm.delete(k(2))
          g1 <- lsm.get(k(1))
          g2 <- lsm.get(k(2))
          sc <- lsm.scan(k(1), Some(k(3))).compile.toList
        yield
          assertEquals(g1, Some(v("a")))
          assertEquals(g2, None)
          assertEquals(sc, List(k(1) -> v("a")))
      }
    }
  }

  test("フラッシュとコンパクションを経ても、参照モデルと一致する（再オープンも含む）") {
    val prog = sim(torn = false).flatMap { fs =>
      def run(rng: SimRandom, model: TreeMap[ByteVector, ByteVector], steps: Int): IO[(SimRandom, TreeMap[ByteVector, ByteVector])] =
        Lsm.open(fs, dir, tiny).use { lsm =>
          def loop(r: SimRandom, m: TreeMap[ByteVector, ByteVector], n: Int): IO[(SimRandom, TreeMap[ByteVector, ByteVector])] =
            if n == 0 then lsm.flush.as((r, m))
            else
              val (r1, op) = r.nextInt(100)
              val (r2, key) = r1.nextInt(500)
              if op < 60 then
                val value = v(s"v$n-" + "x" * (n % 40))
                lsm.put(k(key), value) >> loop(r2, m.updated(k(key), value), n - 1)
              else if op < 85 then lsm.delete(k(key)) >> loop(r2, m - k(key), n - 1)
              else if op < 95 then
                lsm.get(k(key)).flatMap(g => IO(assertEquals(g, m.get(k(key)), s"get ${k(key).decodeUtf8Lenient}"))) >>
                  loop(r2, m, n - 1)
              else
                val (r3, span) = r2.nextInt(100)
                val lo = k(key)
                val hi = k(key + span)
                lsm.scan(lo, Some(hi)).compile.toList.flatMap { got =>
                  IO(assertEquals(got, m.range(lo, hi).toList))
                } >> loop(r3, m, n - 1)
          loop(rng, model, steps).flatTap((_, m) => all(lsm).map(got => assertEquals(got, m.toList)))
        }
      (1 to 5).toList.foldLeftM((SimRandom(99L), TreeMap.empty[ByteVector, ByteVector])) { case ((r, m), _) =>
        run(r, m, 1500)
      }.flatMap { (_, m) =>
        Lsm.open(fs, dir, tiny).use(lsm => all(lsm).product(lsm.version)).map { (got, ver) =>
          assertEquals(got, m.toList)
          assert(ver.levels.drop(1).exists(_.nonEmpty), s"コンパクションで L1 以降にファイルがあるはず: $ver")
        }
      }
    }
    scalas3.sim.Sim.run(prog)
  }

  test("スナップショットは、その後の書き込み・フラッシュ・コンパクションの影響を受けない") {
    val prog = sim().flatMap { fs =>
      Lsm.open(fs, dir, tiny).use { lsm =>
        for
          _ <- (0 until 200).toList.traverse_(i => lsm.put(k(i), v("old")))
          _ <- lsm.flush
          res <- lsm.snapshot.use { snap =>
            for
              _ <- (0 until 200).toList.traverse_(i => lsm.put(k(i), v("new-" + "y" * 30)))
              _ <- (0 until 100).toList.traverse_(i => lsm.delete(k(i)))
              _ <- lsm.flush
              got <- snap.scan(ByteVector.empty, None).compile.toList
              one <- snap.get(k(5))
            yield (got, one)
          }
          after <- all(lsm)
        yield
          assertEquals(res._1.map(_._2).distinct, List(v("old")))
          assertEquals(res._1.size, 200)
          assertEquals(res._2, Some(v("old")))
          assertEquals(after.size, 100)
      }
    }
    scalas3.sim.Sim.run(prog)
  }

  test("電源断のあとは、永続化済みのバッチ境界のどこかの状態に戻る（途中のバッチは見えない）") {
    def once(seed: Long): IO[(Boolean, Boolean, Boolean)] =
      val (rng1, budget) = SimRandom(seed).nextLong(3000)
      for
        fs <- sim(seed)
        _ <- fs.failAfter(200 + budget)
        // models(i) = i 個目のバッチまで適用した状態
        result <- Lsm
          .open(fs, dir, tiny)
          .use { lsm =>
            def loop(
                r: SimRandom,
                models: Vector[TreeMap[ByteVector, ByteVector]],
                durable: Int
            ): IO[(Vector[TreeMap[ByteVector, ByteVector]], Int)] =
              val (r1, op) = r.nextInt(100)
              val (r2, size) = r1.nextInt(5)
              val m = models.last
              val batch = (0 to size).toList.map { j =>
                val key = k((op * 7 + j * 13 + models.size) % 300)
                if (op + j) % 4 == 0 then WriteOp.Delete(key) else WriteOp.Put(key, v(s"b${models.size}-$j"))
              }
              val next = batch.foldLeft(m) {
                case (acc, WriteOp.Put(key, value)) => acc.updated(key, value)
                case (acc, WriteOp.Delete(key))     => acc - key
              }
              lsm.write(batch).attempt.flatMap {
                case Left(_) => IO.pure((models, durable))
                case Right(_) =>
                  val ms = models :+ next
                  if op < 5 then
                    lsm.flush.attempt.flatMap {
                      case Right(_) => loop(r2, ms, ms.size - 1)
                      case Left(_)  => IO.pure((ms, durable))
                    }
                  else loop(r2, ms, durable)
              }
            loop(rng1, Vector(TreeMap.empty), 0)
          }
          .attempt
        (models, durable) = result match
          case Right(x) => x
          case Left(e)  => fail(s"open/close should not fail: $e")
        _ <- fs.powerFail
        got <- Lsm.open(fs, dir, tiny).use(all)
      yield
        val matches = models.indices.filter(i => models(i).toList == got)
        assert(matches.nonEmpty, s"seed=$seed: 復旧後の状態がどのバッチ境界とも一致しない (${got.size} 件)")
        assert(matches.max >= durable, s"seed=$seed: flush 済みのバッチ $durable が失われた (一致: ${matches.max})")
        // (未 flush の書き込みが失われたか, flush が成功していたか, 途中まで flush されていたか)
        (matches.max < models.size - 1, durable > 0, matches.max > durable)

    scalas3.sim.Sim.run((1L to 30L).toList.traverse(once)).map { rs =>
      // テストが意味のある場面を通っていることの確認
      assert(rs.exists(_._1), "電源断で未永続化のデータが失われる場面がない")
      assert(rs.exists(_._2), "flush 済みのデータが残る場面がない")
      assert(rs.exists(_._3), "バックグラウンドのフラッシュで flush 以降も残る場面がない")
    }
  }

  test("本物のファイルシステムでも動く") {
    Files[IO].tempDirectory.use { tmp =>
      val fs: FileSystem = NioFileSystem
      val d = tmp / "db"
      for
        _ <- Lsm.open(fs, d, tiny).use(lsm => (0 until 500).toList.traverse_(i => lsm.put(k(i), v(s"v$i"))) >> lsm.flush)
        got <- Lsm.open(fs, d, tiny).use(all)
      yield assertEquals(got.map(_._1), (0 until 500).map(k).toList)
    }
  }

