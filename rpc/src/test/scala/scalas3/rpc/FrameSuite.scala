package scalas3.rpc

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import fs2.{Chunk, Stream}
import munit.ScalaCheckSuite
import org.scalacheck.Prop.*
import org.scalacheck.{Arbitrary, Gen}
import scodec.bits.ByteVector

class FrameSuite extends ScalaCheckSuite:

  private val genFrame: Gen[Frame] =
    for
      sid <- Arbitrary.arbitrary[Int]
      kind <- Gen.oneOf(FrameKind.values.toSeq)
      flags <- Gen.choose(0, 255)
      payload <- Gen.listOf(Arbitrary.arbitrary[Byte]).map(ByteVector(_))
    yield Frame(sid, kind, flags, payload)

  property("任意の区切り方で届いてもフレームを復元できる") {
    forAll(Gen.listOf(genFrame), Gen.choose(1, 17)) { (frames, chunkSize) =>
      val bytes = frames.foldLeft(ByteVector.empty)((acc, f) => acc ++ Frame.encode(f).toByteVector)
      val decoded = Stream
        .chunk(Chunk.byteVector(bytes))
        .rechunkRandomlyWithSeed(0.1, 2.0)(chunkSize.toLong)
        .covary[IO]
        .through(Frame.decode)
        .compile
        .toList
        .unsafeRunSync()
      assertEquals(decoded, frames)
    }
  }

  test("フレームの途中で切れたらエラー") {
    val bytes = Frame.encode(Frame(1, FrameKind.Request, 0, ByteVector(1, 2, 3))).toByteVector.dropRight(1)
    val r = Stream.chunk(Chunk.byteVector(bytes)).covary[IO].through(Frame.decode).compile.drain.attempt.unsafeRunSync()
    assert(r.left.exists(_.isInstanceOf[ProtocolException]))
  }

  test("巨大なフレーム長はエラー") {
    val bytes = ByteVector.fromInt(Frame.MaxPayload + 1) ++ ByteVector.fill(6)(0)
    val r = Stream.chunk(Chunk.byteVector(bytes)).covary[IO].through(Frame.decode).compile.drain.attempt.unsafeRunSync()
    assert(r.left.exists(_.isInstanceOf[ProtocolException]))
  }
