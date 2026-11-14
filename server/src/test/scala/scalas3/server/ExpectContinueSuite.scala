package scalas3.server

import munit.FunSuite
import scodec.bits.ByteVector

import java.nio.charset.StandardCharsets.US_ASCII

class ExpectContinueSuite extends FunSuite:
  import ExpectContinue.{Mode, step}

  private def b(s: String) = ByteVector(s.getBytes(US_ASCII))

  test("Expect ヘッダーで 100 Continue を求め、本文の長さを数えて次のリクエストに備える") {
    val req1 = "PUT /b/k HTTP/1.1\r\nHost: x\r\nExpect: 100-continue\r\nContent-Length: 5\r\n\r\n"
    val (m1, c1) = step(Mode.Header(ByteVector.empty), b(req1), false)
    assert(c1)
    assertEquals(m1, Mode.Body(5))
    // 本文の途中に "Expect: 100-continue" があっても反応しない
    val (m2, c2) = step(m1, b("Expec"), false)
    assert(!c2)
    val req2 = "GET /b/k HTTP/1.1\r\nHost: x\r\n\r\n"
    val (m3, c3) = step(m2, b(req2), false)
    assert(!c3)
    assertEquals(m3, Mode.Header(ByteVector.empty))
  }

  test("ヘッダーが分割されて届いても検出する") {
    val req = "PUT /b/k HTTP/1.1\r\nexpect: 100-Continue\r\ncontent-length: 3\r\n\r\nabc"
    val (m, c) = req.grouped(4).foldLeft((Mode.Header(ByteVector.empty): Mode, false)) { case ((m, c), piece) =>
      val (m2, c2) = step(m, b(piece), false)
      (m2, c || c2)
    }
    assert(c)
    assertEquals(m, Mode.Header(ByteVector.empty))
  }

  test("chunked の本文が来たら以後は何もしない") {
    val (m, _) = step(Mode.Header(ByteVector.empty), b("PUT / HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n5\r\n"), false)
    assertEquals(m, Mode.Off)
  }

  test("ヘッダー名の大文字小文字や前後の空白を問わず、似た名前のヘッダーには反応しない") {
    val req = "PUT /b/k HTTP/1.1\r\nHost: x\r\nX-Expect: 100-continue\r\nCONTENT-LENGTH :  3\r\n  eXpEcT:100-Continue \r\n\r\nabc"
    val (m, c) = step(Mode.Header(ByteVector.empty), b(req), false)
    assert(c)
    assertEquals(m, Mode.Header(ByteVector.empty))
    val (m2, c2) = step(Mode.Header(ByteVector.empty), b("GET / HTTP/1.1\r\nX-Expect: 100-continue\r\nContent-Length-X: 9\r\n\r\n"), false)
    assert(!c2)
    assertEquals(m2, Mode.Header(ByteVector.empty))
  }
