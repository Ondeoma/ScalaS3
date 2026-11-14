package scalas3.sim

import cats.effect.IO
import fs2.Chunk
import munit.CatsEffectSuite
import scalas3.runtime.NodeAddress

import scala.concurrent.duration.*

class SimNetworkSuite extends CatsEffectSuite:

  private val serverAddr = NodeAddress("b", 9000)

  /** 受け取ったバイト列をそのまま返すサーバーを動かしながら body を実行する。 */
  private def withEcho[A](net: SimNetwork)(body: IO[A]): IO[A] =
    net.transport("b").listen(serverAddr).use { (_, conns) =>
      conns
        .map(c => c.reads.chunks.evalMap(c.write).drain.handleErrorWith(_ => fs2.Stream.empty))
        .parJoinUnbounded
        .compile
        .drain
        .background
        .surround(body)
    }

  test("エコー：順序を保ち、遅延は仮想時間で進む") {
    val prog =
      for
        net <- SimNetwork.create(42L, SimNetwork.Config(baseLatency = 10.millis, jitter = 5.millis))
        res <- withEcho(net) {
          net.transport("a").connect(serverAddr).use { c =>
            for
              start <- IO.monotonic
              _ <- (1 to 100).toList.foldLeft(IO.unit)((acc, i) => acc >> c.write(Chunk.array(Array(i.toByte))))
              got <- c.reads.take(100).compile.toList
              end <- IO.monotonic
            yield (got, end - start)
          }
        }
      yield res
    scalas3.sim.Sim.run(prog).map { (got, elapsed) =>
      assertEquals(got, (1 to 100).map(_.toByte).toList)
      assert(elapsed >= 20.millis, s"往復の遅延が反映されるはず: $elapsed")
    }
  }

  test("分断すると既存の接続は切れ、新しい接続も失敗する。修復すると繋がる") {
    val prog =
      for
        net <- SimNetwork.create(1L)
        res <- withEcho(net) {
          net.transport("a").connect(serverAddr).use { c =>
            for
              _ <- c.write(Chunk.array(Array[Byte](1)))
              first <- c.reads.take(1).compile.toList
              _ <- net.partition("a", "b")
              broken <- c.reads.compile.drain.attempt
              refused <- net.transport("a").connect(serverAddr).use_.attempt
              _ <- net.healAll
              again <- net.transport("a").connect(serverAddr).use { c2 =>
                c2.write(Chunk.array(Array[Byte](7))) >> c2.reads.take(1).compile.toList
              }
            yield (first, broken, refused, again)
          }
        }
      yield res
    scalas3.sim.Sim.run(prog).map { (first, broken, refused, again) =>
      assertEquals(first, List[Byte](1))
      assert(broken.isLeft)
      assert(refused.isLeft)
      assertEquals(again, List[Byte](7))
    }
  }

  test("接続を閉じると相手の reads は終了する") {
    val prog =
      for
        net <- SimNetwork.create(1L)
        got <- net.transport("b").listen(serverAddr).use { (_, conns) =>
          val server = conns.take(1).flatMap(_.reads).compile.toList
          server.both(net.transport("a").connect(serverAddr).use(_.write(Chunk.array(Array[Byte](1, 2, 3))))).map(_._1)
        }
      yield got
    scalas3.sim.Sim.run(prog).map(got => assertEquals(got, List[Byte](1, 2, 3)))
  }

  test("同じアドレスに二重に listen できない") {
    val prog = SimNetwork.create(1L).flatMap { net =>
      val t = net.transport("b")
      t.listen(serverAddr).use_ >> t.listen(serverAddr).use(_ => t.listen(serverAddr).use_.attempt)
    }
    scalas3.sim.Sim.run(prog).map(r => assert(r.isLeft))
  }

