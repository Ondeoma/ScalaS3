package scalas3.s3

import munit.FunSuite

class HelpersSuite extends FunSuite:

  test("percentDecode：% を含むパスでもサロゲートペアの文字を壊さない") {
    assertEquals(SigV4.percentDecode("a%20b/😀%2Fc"), "a b/😀/c")
    assertEquals(SigV4.percentDecode("%E3%81%82%"), "あ%")
    assertEquals(SigV4.percentDecode("100%zz"), "100%zz")
  }

  test("uriEncode：非予約文字以外を %XX にする") {
    assertEquals(SigV4.uriEncode("a b/あ~", encodeSlash = false), "a%20b/%E3%81%82~")
    assertEquals(SigV4.uriEncode("a/b", encodeSlash = true), "a%2Fb")
    assertEquals(SigV4.percentDecode(SigV4.uriEncode("x😀 y", encodeSlash = true)), "x😀 y")
  }

  test("glob：* と ? のワイルドカード（大文字小文字を区別しない）") {
    assert(Policy.glob("arn:aws:s3:::b/*", "arn:aws:s3:::b/x/y"))
    assert(Policy.glob("s3:Get*", "s3:getobject"))
    assert(Policy.glob("a?c", "abc"))
    assert(!Policy.glob("a?c", "ac"))
    assert(Policy.glob("*", "line1\nline2"))
    assert(Policy.glob("a*b*c", "aXbYbZc"))
    assert(!Policy.glob("a*a", "a"))
    assert(!Policy.glob("abc", "abcd"))
    // 照合する側に * があっても、パターンの * はワイルドカード
    assert(Policy.glob("arn:aws:s3:::b/private/*", "arn:aws:s3:::b/private/*secret"))
    assert(Policy.glob("a*b", "a*xb"))
    assert(Policy.glob("*", "*abc"))
    // CORS では ? はただの文字
    assert(Policy.glob("a?c", "a?c", questionMark = false))
    assert(!Policy.glob("a?c", "abc", questionMark = false))
  }

  test("IpAddress の条件：CIDR とアドレスだけの指定。ホスト名は引かない") {
    assert(Policy.condition("IpAddress", Some("10.1.2.3"), List("10.0.0.0/8")))
    assert(!Policy.condition("IpAddress", Some("11.1.2.3"), List("10.0.0.0/8")))
    assert(Policy.condition("IpAddress", Some("192.168.1.10"), List("192.168.1.10")))
    assert(Policy.condition("IpAddress", Some("2001:db8::1"), List("2001:db8::/32")))
    assert(!Policy.condition("IpAddress", Some("10.1.2.3"), List("2001:db8::/32")))
    assert(!Policy.condition("IpAddress", Some("10.1.2.3"), List("localhost")))
    assert(!Policy.condition("IpAddress", Some("10.1.2.3"), List("10.0.0.0/40")))
    assert(Policy.condition("NotIpAddress", Some("11.1.2.3"), List("10.0.0.0/8")))
  }

  test("チェックサムの base64") {
    def b64(alg: String, s: String) =
      val c = Checksummer(alg).get
      val b = s.getBytes("UTF-8")
      c.update(b, 0, b.length)
      c.base64
    assertEquals(b64("crc32", "hello world"), "DUoRhQ==")
    assertEquals(b64("sha256", ""), "47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=")
    assert(Checksummer.supported("CRC64NVME"))
    assert(!Checksummer.supported("md5"))
  }
