package scalas3.core

import munit.ScalaCheckSuite
import org.scalacheck.Prop.*
import org.scalacheck.{Arbitrary, Gen}
import scodec.bits.ByteVector

class BytesSuite extends ScalaCheckSuite:

  given Arbitrary[ByteVector] = Arbitrary(Gen.listOf(Arbitrary.arbitrary[Byte]).map(ByteVector(_)))

  property("compare は符号なし辞書順") {
    forAll { (a: ByteVector, b: ByteVector) =>
      val expected =
        a.toSeq.map(_ & 0xff).zip(b.toSeq.map(_ & 0xff)).find(_ != _) match
          case Some((x, y)) => Integer.compare(x, y)
          case None         => java.lang.Long.compare(a.size, b.size)
      assertEquals(Integer.signum(Bytes.compare(a, b)), Integer.signum(expected))
    }
  }

  property("prefixEnd は prefix で始まる全キーより大きく、それ以外の大きいキーの下限") {
    forAll { (prefix: ByteVector, suffix: ByteVector) =>
      Bytes.prefixEnd(prefix) match
        case None      => assert(prefix.toSeq.forall(_ == 0xff.toByte))
        case Some(end) =>
          assert(Bytes.compare(prefix ++ suffix, end) < 0)
          assert(!end.startsWith(prefix) || prefix.isEmpty)
    }
  }

  property("varint と固定長の往復") {
    forAll { (l: Long, i: Int, bs: ByteVector) =>
      val w = ByteWriter().varLong(l).u32(i).u64(l).lenBytes(bs).varInt(i)
      val r = ByteReader(w.toArray)
      assertEquals(r.varLong(), l)
      assertEquals(r.u32(), i)
      assertEquals(r.u64(), l)
      assertEquals(ByteVector(r.lenBytes()), bs)
      assertEquals(r.varInt(), i)
      assert(!r.hasRemaining)
    }
  }

  test("読み過ぎは DecodeError") {
    intercept[DecodeError](ByteReader(Array[Byte](1, 2)).u32())
  }
