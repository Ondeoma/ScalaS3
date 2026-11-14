package scalas3.rpc

import cats.effect.IO
import fs2.{Chunk, Pipe, Pull, Stream}
import scodec.bits.ByteVector

enum FrameKind(val code: Int):
  case Request extends FrameKind(1)
  case Response extends FrameKind(2)
  case Error extends FrameKind(3)
  case Cancel extends FrameKind(4)

object FrameKind:
  private val byCode = values.map(k => k.code -> k).toMap
  def fromCode(c: Int): Option[FrameKind] = byCode.get(c)

/** ワイヤ上のフレーム。
  * {{{
  * [payloadLen u32][streamId u32][kind u8][flags u8][payload]
  * }}}
  */
final case class Frame(streamId: Int, kind: FrameKind, flags: Int, payload: ByteVector)

final class ProtocolException(msg: String) extends java.io.IOException(msg)

object Frame:
  val HeaderSize = 10
  val MaxPayload: Int = 16 * 1024 * 1024

  def encode(f: Frame): Chunk[Byte] =
    val n = f.payload.size.toInt
    val arr = new Array[Byte](HeaderSize + n)
    arr(0) = (n >>> 24).toByte; arr(1) = (n >>> 16).toByte; arr(2) = (n >>> 8).toByte; arr(3) = n.toByte
    val s = f.streamId
    arr(4) = (s >>> 24).toByte; arr(5) = (s >>> 16).toByte; arr(6) = (s >>> 8).toByte; arr(7) = s.toByte
    arr(8) = f.kind.code.toByte
    arr(9) = f.flags.toByte
    f.payload.copyToArray(arr, HeaderSize)
    Chunk.array(arr)

  /** バイト列をフレーム列に変換する。不正なフレームは ProtocolException。 */
  val decode: Pipe[IO, Byte, Frame] = in =>

    def parse(buf: ByteVector): (Vector[Frame], ByteVector) =
      var rest = buf
      val out = Vector.newBuilder[Frame]
      var continue = true
      while continue && rest.size >= HeaderSize do
        val n = rest.take(4).toInt(signed = false)
        if n < 0 || n > MaxPayload then throw ProtocolException(s"frame too large: $n")
        if rest.size >= HeaderSize + n then
          val sid = rest.slice(4, 8).toInt()
          val code = rest(8) & 0xff
          val kind = FrameKind.fromCode(code).getOrElse(throw ProtocolException(s"unknown frame kind: $code"))
          out += Frame(sid, kind, rest(9) & 0xff, rest.slice(HeaderSize.toLong, HeaderSize.toLong + n).compact)
          rest = rest.drop(HeaderSize.toLong + n)
        else continue = false
      (out.result(), rest)

    def go(s: Stream[IO, Byte], buf: ByteVector): Pull[IO, Frame, Unit] =
      s.pull.uncons.flatMap {
        case None =>
          if buf.isEmpty then Pull.done
          else Pull.raiseError[IO](ProtocolException(s"connection closed mid-frame (${buf.size} bytes buffered)"))
        case Some((chunk, tail)) =>
          Pull.eval(IO(parse(buf ++ chunk.toByteVector))).flatMap { (frames, rest) =>
            Pull.output(Chunk.from(frames)) >> go(tail, rest)
          }
      }

    go(in, ByteVector.empty).stream
