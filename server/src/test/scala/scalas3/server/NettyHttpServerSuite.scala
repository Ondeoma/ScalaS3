package scalas3.server

import cats.effect.{IO, Resource}
import cats.syntax.all.*
import fs2.{Chunk, Stream}
import munit.CatsEffectSuite
import org.http4s.{Header, Headers, HttpApp, Method, Response, Status}
import org.typelevel.ci.CIString

import java.io.{BufferedInputStream, InputStream, OutputStream}
import java.net.{InetSocketAddress, Socket}
import java.nio.charset.StandardCharsets.US_ASCII
import scala.concurrent.duration.*

/** NettyHttpServer を生のソケットで叩き、HTTP/1.1 の振る舞いを確かめる。 */
class NettyHttpServerSuite extends CatsEffectSuite:

  override def munitIOTimeout: Duration = 20.seconds

  private val app: HttpApp[IO] = HttpApp[IO] { req =>
    (req.method, req.uri.path.renderString) match
      case (Method.GET, "/hello") => IO.pure(Response[IO](Status.Ok).withEntity("hello"))
      case (Method.HEAD, "/hello") =>
        IO.pure(Response[IO](Status.Ok).withHeaders(Headers(Header.Raw(CIString("Content-Length"), "5"))))
      case (Method.PUT, "/echo") =>
        req.body.compile.count.map(n => Response[IO](Status.Ok).withEntity(s"len=$n"))
      case (Method.PUT, "/reject") => IO.pure(Response[IO](Status.Forbidden).withEntity("no"))
      case (Method.GET, "/big") =>
        val n = req.uri.query.params("n").toLong
        // 長さを伝えない（chunked で送られる）
        IO.pure(Response[IO](Status.Ok, body = Stream.range(0L, n, 65536L).map(off => Chunk.array(Array.fill(math.min(65536L, n - off).toInt)(7.toByte))).unchunks))
      case (Method.GET, "/fixed") =>
        val n = req.uri.query.params("n").toLong
        IO.pure(
          Response[IO](Status.Ok, body = Stream.chunk(Chunk.array(Array.fill(n.toInt)(3.toByte))))
            .withHeaders(Headers(Header.Raw(CIString("Content-Length"), n.toString)))
        )
      case (Method.GET, "/boom") => IO.raiseError(RuntimeException("boom"))
      // 応答は返したが、本体を作る途中で失敗する（ブロックが読めないなど）
      case (Method.GET, "/broken-small") =>
        IO.pure(Response[IO](Status.Ok, body = Stream.raiseError[IO](RuntimeException("block not found")))
          .withHeaders(Headers(Header.Raw(CIString("Content-Length"), "10"))))
      case (Method.GET, "/remote") =>
        IO.pure(Response[IO](Status.Ok).withEntity(req.remote.map(_.host.toString).getOrElse("?")))
      case _ => IO.pure(Response[IO](Status.NotFound))
  }

  private val server: Resource[IO, InetSocketAddress] =
    NettyHttpServer.resource(NettyHttpServer.Config("127.0.0.1", 0, strictBodyLimit = 1024), app)

  private final case class Resp(status: Int, headers: Map[String, String], body: Array[Byte]):
    def text = new String(body, US_ASCII)
    def header(n: String) = headers.get(n.toLowerCase)

  private def line(in: InputStream): String =
    val sb = StringBuilder()
    var c = in.read()
    while c != -1 && c != '\n' do
      if c != '\r' then sb += c.toChar
      c = in.read()
    if c == -1 && sb.isEmpty then null else sb.toString

  private def readN(in: InputStream, n: Int): Array[Byte] =
    val a = in.readNBytes(n)
    assertEquals(a.length, n, "short body")
    a

  /** 応答を 1 つ読む（HEAD なら本体を読まない）。 */
  private def readResponse(in: InputStream, head: Boolean = false): Resp =
    val statusLine = line(in)
    assert(statusLine != null, "connection closed before response")
    val status = statusLine.split(' ')(1).toInt
    val hs = Iterator.continually(line(in)).takeWhile(l => l != null && l.nonEmpty).map { l =>
      val i = l.indexOf(':'); l.take(i).trim.toLowerCase -> l.drop(i + 1).trim
    }.toMap
    val body =
      if head || status / 100 == 1 || status == 204 then Array.emptyByteArray
      else if hs.get("transfer-encoding").exists(_.contains("chunked")) then
        val out = java.io.ByteArrayOutputStream()
        var size = Integer.parseInt(line(in).trim, 16)
        while size > 0 do
          out.write(readN(in, size)); line(in)
          size = Integer.parseInt(line(in).trim, 16)
        line(in) // 終わりの空行
        out.toByteArray
      else hs.get("content-length").map(n => readN(in, n.toInt)).getOrElse(in.readAllBytes())
    Resp(status, hs, body)

  private def connect(addr: InetSocketAddress): Resource[IO, (InputStream, OutputStream, Socket)] =
    Resource.make(IO.blocking { val s = Socket(); s.connect(addr); s.setSoTimeout(10000); s })(s => IO.blocking(s.close()))
      .map(s => (BufferedInputStream(s.getInputStream), s.getOutputStream, s))

  private def send(out: OutputStream, s: String): Unit =
    out.write(s.getBytes(US_ASCII)); out.flush()

  test("1 本の接続で続けて要求でき、先に送った複数の要求にも順に応える") {
    server.flatMap(connect).use { (in, out, _) =>
      IO.blocking {
        send(out, "GET /hello HTTP/1.1\r\nHost: x\r\n\r\n")
        val a = readResponse(in)
        send(out, "GET /hello HTTP/1.1\r\nHost: x\r\n\r\nPUT /echo HTTP/1.1\r\nHost: x\r\nContent-Length: 3\r\n\r\nabcGET /hello HTTP/1.1\r\nHost: x\r\n\r\n")
        val b = readResponse(in); val c = readResponse(in); val d = readResponse(in)
        assertEquals((a.status, a.text), (200, "hello"))
        assertEquals((b.text, c.text, d.text), ("hello", "len=3", "hello"))
        assert(a.header("date").nonEmpty)
      }
    }
  }

  test("Expect: 100-continue には本体を読む前に 100 Continue を返す") {
    server.flatMap(connect).use { (in, out, _) =>
      IO.blocking {
        send(out, "PUT /echo HTTP/1.1\r\nHost: x\r\nContent-Length: 5\r\nExpect: 100-continue\r\n\r\n")
        val cont = readResponse(in)
        assertEquals(cont.status, 100)
        send(out, "hello")
        val r = readResponse(in)
        assertEquals((r.status, r.text), (200, "len=5"))
        // 接続は使い回せる
        send(out, "GET /hello HTTP/1.1\r\nHost: x\r\n\r\n")
        assertEquals(readResponse(in).text, "hello")
      }
    }
  }

  test("本体を読まずに応答したら 100 Continue を送らず、接続を閉じる") {
    server.flatMap(connect).use { (in, out, _) =>
      IO.blocking {
        send(out, "PUT /reject HTTP/1.1\r\nHost: x\r\nContent-Length: 1000000\r\nExpect: 100-continue\r\n\r\n")
        val r = readResponse(in)
        assertEquals(r.status, 403)
        assertEquals(r.header("connection"), Some("close"))
        assertEquals(in.read(), -1)
      }
    }
  }

  test("chunked の要求の本体を読める") {
    server.flatMap(connect).use { (in, out, _) =>
      IO.blocking {
        send(out, "PUT /echo HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabc\r\n4\r\ndefg\r\n0\r\n\r\n")
        assertEquals(readResponse(in).text, "len=7")
      }
    }
  }

  test("大きな応答は流して送る（長さが分からなければ chunked、分かれば Content-Length）。HEAD は本体を送らない") {
    server.flatMap(connect).use { (in, out, _) =>
      IO.blocking {
        send(out, "GET /big?n=3000000 HTTP/1.1\r\nHost: x\r\n\r\n")
        val big = readResponse(in)
        assertEquals(big.header("transfer-encoding"), Some("chunked"))
        assertEquals(big.body.length, 3000000)
        send(out, "GET /fixed?n=200000 HTTP/1.1\r\nHost: x\r\n\r\n")
        val fixed = readResponse(in)
        assertEquals((fixed.header("content-length"), fixed.body.length), (Some("200000"), 200000))
        send(out, "HEAD /hello HTTP/1.1\r\nHost: x\r\n\r\n")
        val head = readResponse(in, head = true)
        assertEquals((head.status, head.header("content-length")), (200, Some("5")))
        send(out, "GET /hello HTTP/1.1\r\nHost: x\r\n\r\n")
        assertEquals(readResponse(in).text, "hello")
      }
    }
  }

  test("壊れた要求には 400、アプリの失敗には 500 を返す。送信元のアドレスが分かる") {
    server.use { addr =>
      (connect(addr).use { (in, out, _) =>
        IO.blocking { send(out, "THIS IS NOT HTTP\r\n\r\n"); readResponse(in).status }
      }, connect(addr).use { (in, out, _) =>
        IO.blocking { send(out, "GET /boom HTTP/1.1\r\nHost: x\r\n\r\n"); readResponse(in).status }
      }, connect(addr).use { (in, out, _) =>
        IO.blocking { send(out, "GET /remote HTTP/1.1\r\nHost: x\r\n\r\n"); readResponse(in).text }
      }).tupled.map { (bad, boom, remote) =>
        assertEquals(bad, 400)
        assertEquals(boom, 500)
        assertEquals(remote, "127.0.0.1")
      }
    }
  }

  test("本体の途中で切断されても、サーバーは次の接続に応える") {
    server.use { addr =>
      connect(addr).use { (_, out, _) =>
        IO.blocking { send(out, "PUT /echo HTTP/1.1\r\nHost: x\r\nContent-Length: 100000\r\n\r\nabc") } >> IO.sleep(200.millis)
      } >> connect(addr).use { (in, out, _) =>
        IO.blocking { send(out, "GET /hello HTTP/1.1\r\nHost: x\r\n\r\n"); readResponse(in).text }
      }.map(assertEquals(_, "hello"))
    }
  }

  test("HTTP/1.0 は応答のあと閉じる（keep-alive を求めたときは使い回す）") {
    server.use { addr =>
      connect(addr).use { (in, out, _) =>
        IO.blocking {
          send(out, "GET /hello HTTP/1.0\r\n\r\n")
          val r = readResponse(in)
          assertEquals((r.status, r.text), (200, "hello"))
          assertEquals(in.read(), -1)
        }
      } >> connect(addr).use { (in, out, _) =>
        IO.blocking {
          send(out, "GET /hello HTTP/1.0\r\nConnection: keep-alive\r\n\r\n")
          assertEquals(readResponse(in).header("connection").map(_.toLowerCase), Some("keep-alive"))
          send(out, "GET /hello HTTP/1.0\r\nConnection: keep-alive\r\n\r\n")
          assertEquals(readResponse(in).text, "hello")
        }
      }
    }
  }

  test("本体を作る途中で失敗し、まだ何も送っていなければ、黙って閉じずに 500 を返す") {
    server.use { addr =>
      connect(addr).use { (in, out, _) =>
        IO.blocking { send(out, "GET /broken-small HTTP/1.1\r\nHost: x\r\n\r\n"); readResponse(in) }
      }.map { r =>
        assertEquals(r.status, 500)
        assertEquals(r.header("connection"), Some("close"))
      }
    }
  }

