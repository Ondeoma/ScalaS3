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


  // ---- 墓石とゴミの回収・大きさの見積もり ----

  /** 大きさによるコンパクションが L1 から先へ進まない設定（L1 の上限がとても大きい）。 */
  private val wide = tiny.copy(baseLevelBytes = 1L << 40, tombstoneMinDeletes = 16)

  /** cond が成り立つまで（最大 10 秒）バックグラウンドの処理を待つ。 */
  private def eventually(what: String)(cond: IO[Boolean]): IO[Unit] =
    def loop(n: Int): IO[Unit] =
      cond.flatMap(ok => if ok then IO.unit else if n == 0 then IO(fail(s"$what が起きない")) else IO.sleep(scala.concurrent.duration.DurationInt(20).millis) >> loop(n - 1))
    loop(500)

  /** tiny の設定で 400 件を書き、L2 以降まで押し出しておく。 */
  private def deepData(fs: SimFileSystem): IO[Unit] =
    Lsm.open(fs, dir, tiny).use { lsm =>
      (0 until 400).toList.traverse_(i => lsm.put(k(i), v("x" * 100))) >> lsm.flush >>
        eventually("L2 以降へのコンパクション")(lsm.version.map(ver => ver.levels.drop(2).exists(_.nonEmpty) && ver.levels(0).isEmpty))
    }

  test("大量に消したあと書き込みが少なくても、墓石の多いファイルを下へ押し出し、墓石と古い値を捨てる") {
    for
      fs <- sim(torn = false)
      _ <- deepData(fs)
      r <- Lsm.open(fs, dir, wide).use { lsm =>
        for
          _ <- (0 until 400).toList.traverse_(i => lsm.delete(k(i))) >> lsm.flush
          _ <- eventually("ゴミの回収")(lsm.version.map(ver => ver.files.forall(f => f.deletes == 0) && ver.files.map(_.entries).sum == 0L))
          got <- all(lsm)
          est <- lsm.approximateBytes(ByteVector.empty, None)
        yield (got, est)
      }
      reopened <- Lsm.open(fs, dir, wide).use(lsm => all(lsm).product(lsm.version))
    yield
      val (got, est) = r
      assertEquals(got, Nil)
      assertEquals(est, 0L)
      assertEquals(reopened._1, Nil)
      assertEquals(reopened._2.files.size, 0)
  }

  test("前の版が書いた（墓石の件数のない）マニフェストでも開け、件数をバックグラウンドで数えて書き戻す") {
    def manifestPath(fs: SimFileSystem) = fs.list(dir).map(_.filter(_.fileName.toString.startsWith("MANIFEST-")).head)
    for
      fs <- sim(torn = false)
      _ <- deepData(fs)
      // 一部を消して、墓石を含むファイルを作る（大きさの上限は大きく、墓石による押し出しもしない設定で）
      noGc = wide.copy(tombstoneMinDeletes = Long.MaxValue, l0CompactionTrigger = 1000)
      _ <- Lsm.open(fs, dir, noGc).use(lsm => (0 until 400 by 3).toList.traverse_(i => lsm.delete(k(i))) >> lsm.flush)
      p <- manifestPath(fs)
      bytes <- fs.readAll(p)
      orig = Manifest.decode(bytes).toOption.get
      _ = assert(orig.version.files.exists(_.deletes > 0), "墓石を含むファイルがない")
      // 末尾の墓石の件数を外し、CRC を付け直して、前の版の形にする
      body = bytes.dropRight(4).dropRight(8L * orig.version.files.size)
      legacy = body ++ ByteVector.fromInt(scalas3.core.Crc32c(body.toArray))
      _ <- fs.writeAtomically(p, legacy)
      _ = assert(Manifest.decode(legacy).toOption.get.version.files.forall(_.deletes == -1L))
      counted <- Lsm.open(fs, dir, noGc).use { lsm =>
        eventually("墓石の件数の数え直し")(lsm.version.map(_.files.forall(_.deletes >= 0))) >>
          (lsm.version, all(lsm)).tupled
      }
      written <- manifestPath(fs).flatMap(fs.readAll).map(b => Manifest.decode(b).toOption.get)
    yield
      val (ver, got) = counted
      // 開いたあとのコンパクションでファイルが入れ替わることがあるので、残っているファイルで比べる
      val want = orig.version.files.map(f => f.id -> f.deletes).toMap
      val kept = ver.files.filter(f => want.contains(f.id)).toVector
      assert(kept.exists(_.deletes > 0), s"墓石を含む元のファイルが残っていない: ${ver.files.map(_.id).toList}")
      kept.foreach(f => assertEquals(f.deletes, want(f.id), s"file ${f.id}"))
      assertEquals(written.version.files.map(f => f.id -> f.deletes).toMap, ver.files.map(f => f.id -> f.deletes).toMap)
      assertEquals(got.map(_._1), (0 until 400).filterNot(_ % 3 == 0).map(k).toList)
  }

  test("範囲の大きさの見積もりはインデックスから求め、生きている量は墓石も含めて上限まで読んで打ち切る") {
    for
      fs <- sim(torn = false)
      r <- Lsm.open(fs, dir, wide.copy(tombstoneMinDeletes = Long.MaxValue, memtableBytes = 1L << 20)).use { lsm =>
        for
          _ <- (0 until 200).toList.traverse_(i => lsm.put(k(i), v("y" * 50))) >> lsm.flush
          whole <- lsm.approximateBytes(ByteVector.empty, None)
          part <- lsm.approximateBytes(k(50), Some(k(100)))
          none <- lsm.approximateBytes(k(500), None)
          live <- lsm.liveBytes(k(50), Some(k(100)), 1L << 20)
          over <- lsm.liveBytes(ByteVector.empty, None, 1000L)
          // flush しない（墓石は memtable にあり、下の値を隠している）
          _ <- (0 until 200).toList.traverse_(i => lsm.delete(k(i)))
          dead <- lsm.liveBytes(ByteVector.empty, None, 500L)
        yield (whole, part, none, live, over, dead)
      }
    yield
      val (whole, part, none, live, over, dead) = r
      val entry = k(0).size + 50L
      assert(whole >= 200 * entry, s"全体の見積もりが小さすぎる: $whole")
      assert(part >= 50 * entry && part < whole / 2, s"一部の見積もり: $part / $whole")
      assertEquals(none, 0L)
      assertEquals(live, 50 * entry)
      assert(over > 1000L, s"上限を超えたら上限より大きい値: $over")
      assertEquals(dead, 501L) // 墓石だけを上限まで読んで打ち切る
  }
