package scalas3.s3

import cats.effect.{Deferred, IO}
import fs2.{Chunk, Stream}
import munit.CatsEffectSuite
import scodec.bits.ByteVector

import java.nio.charset.StandardCharsets.US_ASCII

class PayloadSuite extends CatsEffectSuite:

  test("CRC-64/NVME の検査値") {
    val c = Crc64Nvme()
    val b = "123456789".getBytes(US_ASCII)
    c.update(b, 0, b.length)
    assertEquals(c.value, 0xae8b14860a799888L)
  }

  // AWS のドキュメントにある aws-chunked の例（64KiB + 1KiB の 'a'）
  private val secret = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
  private val scope = SigV4.Scope("20130524", "us-east-1", "s3")
  private val amzDate = "20130524T000000Z"
  private val seed = "4f232c4386841ef735655705268965c44a0e4690baa4adea153f7db9fa80a0a9"
  private val sigs = List(
    "ad80c730a21e5b8d04586a2213dd63b9a0e99e0e2307b0ade35a65485a288648",
    "0055627c9e194cb4542bae2aa5492e3c1575bbb81b612b7d234b86a503ef5497",
    "b6c6ea8a5354eaf15b3cb7646744f4275b71ea724fed81ceb9323e279d449df9"
  )

  private def chunked(sizes: List[Int], signatures: List[String]): ByteVector =
    val parts = sizes.zip(signatures).map { (n, s) =>
      ByteVector(s"${n.toHexString};chunk-signature=$s\r\n".getBytes(US_ASCII)) ++ ByteVector.fill(n.toLong)('a') ++ ByteVector('\r', '\n')
    }
    parts.foldLeft(ByteVector.empty)(_ ++ _)

  private def decode(body: ByteVector, signing: Option[AwsChunked.Signing], chunkSize: Int): IO[Either[Throwable, ByteVector]] =
    Deferred[IO, Map[String, String]].flatMap { tr =>
      Stream.chunk(Chunk.byteVector(body)).rechunkRandomlyWithSeed(0.5, 1.5)(chunkSize.toLong).covary[IO]
        .through(AwsChunked.decode(signing, tr)).compile.to(ByteVector).attempt
    }

  test("署名付き aws-chunked をどんな区切り方でも検証して復元する（AWS ドキュメントの例）") {
    val signing = AwsChunked.Signing(SigV4.signingKey(secret, scope), amzDate, scope, seed)
    val body = chunked(List(65536, 1024, 0), sigs)
    List(7, 1000, 100000).foldLeft(IO.unit) { (acc, cs) =>
      acc >> decode(body, Some(signing), cs).map(r => assertEquals(r.map(_.size), Right(66560L)))
    }
  }

  test("チャンクの署名が違えば SignatureDoesNotMatch") {
    val signing = AwsChunked.Signing(SigV4.signingKey(secret, scope), amzDate, scope, seed)
    val bad = chunked(List(65536, 1024, 0), sigs.updated(1, "0" * 64))
    decode(bad, Some(signing), 4096).map(r => assertEquals(r.left.map(_.asInstanceOf[S3Error].code), Left("SignatureDoesNotMatch")))
  }

  test("署名なし aws-chunked とトレーラー") {
    val body = ByteVector("5\r\nhello\r\n6\r\n world\r\n0\r\nx-amz-checksum-crc32:DUoRhQ==\r\n\r\n".getBytes(US_ASCII))
    for
      tr <- Deferred[IO, Map[String, String]]
      out <- Stream.chunk(Chunk.byteVector(body)).covary[IO].through(AwsChunked.decode(None, tr)).compile.to(ByteVector)
      t <- tr.get
    yield
      assertEquals(new String(out.toArray, US_ASCII), "hello world")
      assertEquals(t, Map("x-amz-checksum-crc32" -> "DUoRhQ=="))
  }
