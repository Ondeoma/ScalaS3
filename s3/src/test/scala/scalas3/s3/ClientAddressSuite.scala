package scalas3.s3

import cats.effect.IO
import com.comcast.ip4s.{IpAddress, Port, SocketAddress}
import munit.FunSuite
import org.http4s.{Header, Headers, Request}
import org.typelevel.ci.CIString
import org.typelevel.vault.Vault

class ClientAddressSuite extends FunSuite:

  private def req(peer: String, headers: (String, String)*): Request[IO] =
    val ip = IpAddress.fromString(peer).get
    val conn = Request.Connection(SocketAddress(IpAddress.fromString("127.0.0.1").get, Port.fromInt(3900).get), SocketAddress(ip, Port.fromInt(50000).get), secure = false)
    Request[IO](
      headers = Headers(headers.toList.map((k, v) => Header.Raw(CIString(k), v))),
      attributes = Vault.empty.insert(Request.Keys.ConnectionInfo, conn)
    )

  private def tp(cidrs: String, hops: Option[Int] = None, header: ClientIpHeader = ClientIpHeader.XForwardedFor) =
    Some(TrustedProxies(TrustedProxies.parseCidrs(cidrs).toOption.get, hops, header))

  private def ip(s: String) = IpAddress.fromString(s)

  // 構成：client(203.0.113.7) → CDN(198.51.100.10) → リバースプロキシ(10.0.0.5) → scalas3
  private val proxyOnly = tp("10.0.0.0/8")
  private val withCdn = tp("10.0.0.0/8,198.51.100.0/24")

  test("設定がなければ、接続の相手を送信元とし、ヘッダーは見ない") {
    assertEquals(ClientAddress.resolve(req("203.0.113.7", "X-Forwarded-For" -> "1.1.1.1"), None).ip, ip("203.0.113.7"))
  }

  test("信頼しない相手から直接来た要求のヘッダーは見ない（偽装できない）") {
    assertEquals(ClientAddress.resolve(req("203.0.113.7", "X-Forwarded-For" -> "10.0.0.1"), proxyOnly).ip, ip("203.0.113.7"))
  }

  test("リバースプロキシが足した X-Forwarded-For から送信元を読み、クライアントが先頭に書いた偽の値は無視する") {
    assertEquals(ClientAddress.resolve(req("10.0.0.5", "X-Forwarded-For" -> "203.0.113.7"), proxyOnly).ip, ip("203.0.113.7"))
    assertEquals(ClientAddress.resolve(req("10.0.0.5", "X-Forwarded-For" -> "1.1.1.1, 203.0.113.7"), proxyOnly).ip, ip("203.0.113.7"))
  }

  test("CDN のアドレスの範囲を信頼すれば、CDN の手前のクライアントが送信元になる") {
    val xff = "9.9.9.9, 203.0.113.7, 198.51.100.10" // 9.9.9.9 はクライアントが書いた偽の値
    assertEquals(ClientAddress.resolve(req("10.0.0.5", "X-Forwarded-For" -> xff), withCdn).ip, ip("203.0.113.7"))
    // CDN を信頼しないと、CDN のアドレスが送信元になる
    assertEquals(ClientAddress.resolve(req("10.0.0.5", "X-Forwarded-For" -> xff), proxyOnly).ip, ip("198.51.100.10"))
  }

  test("段数で信頼する（CDN のアドレスが分からないとき）。直前の相手は範囲で確かめる") {
    val twoHops = tp("10.0.0.0/8", hops = Some(2))
    val xff = "9.9.9.9, 203.0.113.7, 198.51.100.10"
    assertEquals(ClientAddress.resolve(req("10.0.0.5", "X-Forwarded-For" -> xff), twoHops).ip, ip("203.0.113.7"))
    // 直前の相手が範囲外なら、段数の指定があってもヘッダーは見ない
    assertEquals(ClientAddress.resolve(req("203.0.113.50", "X-Forwarded-For" -> xff), twoHops).ip, ip("203.0.113.50"))
  }

  test("全部が信頼するプロキシなら、いちばん遠い側。X-Forwarded-For が複数行でも順につなぐ") {
    assertEquals(ClientAddress.resolve(req("10.0.0.5", "X-Forwarded-For" -> "10.1.2.3"), proxyOnly).ip, ip("10.1.2.3"))
    val r = req("10.0.0.5", "X-Forwarded-For" -> "1.1.1.1", "X-Forwarded-For" -> "203.0.113.7, 198.51.100.10")
    assertEquals(ClientAddress.resolve(r, withCdn).ip, ip("203.0.113.7"))
  }

  test("読めない値や unknown に行き当たったら、送信元は分からない（プロキシのアドレスで代用しない）") {
    assertEquals(ClientAddress.resolve(req("10.0.0.5", "X-Forwarded-For" -> "203.0.113.7, garbage"), proxyOnly).ip, None)
    assertEquals(ClientAddress.resolve(req("10.0.0.5", "Forwarded" -> "for=unknown, for=198.51.100.10"), tp("10.0.0.0/8,198.51.100.0/24", header = ClientIpHeader.Forwarded)).ip, None)
  }

  test("Forwarded ヘッダー（IPv6、ポート、引用符）と、同じ要素の proto で HTTPS かを決める") {
    val fwd = tp("10.0.0.0/8,198.51.100.0/24", header = ClientIpHeader.Forwarded)
    val r = req("10.0.0.5", "Forwarded" -> "for=\"[2001:db8::7]:4711\";proto=https, for=198.51.100.10;proto=http")
    assertEquals(ClientAddress.resolve(r, fwd), ClientAddress(ip("2001:db8::7"), secure = true))
  }

  test("X-Forwarded-Proto：数が揃えば同じ位置、1 つならその値。分からなければ HTTPS ではない") {
    val xff = "203.0.113.7, 198.51.100.10"
    assert(ClientAddress.resolve(req("10.0.0.5", "X-Forwarded-For" -> xff, "X-Forwarded-Proto" -> "https, http"), withCdn).secure)
    assert(ClientAddress.resolve(req("10.0.0.5", "X-Forwarded-For" -> xff, "X-Forwarded-Proto" -> "https"), withCdn).secure)
    assert(!ClientAddress.resolve(req("10.0.0.5", "X-Forwarded-For" -> xff, "X-Forwarded-Proto" -> "https, http, http"), withCdn).secure)
    assert(!ClientAddress.resolve(req("10.0.0.5", "X-Forwarded-For" -> xff), withCdn).secure)
    // 信頼しない相手の X-Forwarded-Proto は見ない
    assert(!ClientAddress.resolve(req("203.0.113.7", "X-Forwarded-Proto" -> "https"), withCdn).secure)
  }

  test("CF-Connecting-IP のような単一の値のヘッダー（直前の相手を信頼するときだけ）") {
    val cf = tp("10.0.0.0/8", header = TrustedProxies.parseHeader("CF-Connecting-IP"))
    assertEquals(ClientAddress.resolve(req("10.0.0.5", "CF-Connecting-IP" -> "203.0.113.7"), cf).ip, ip("203.0.113.7"))
    assertEquals(ClientAddress.resolve(req("203.0.113.9", "CF-Connecting-IP" -> "10.0.0.1"), cf).ip, ip("203.0.113.9"))
  }

  test("IPv4 を表す IPv6 アドレス（::ffff:10.0.0.5）も IPv4 の範囲で判定する。設定の書式の誤りは分かる") {
    assertEquals(ClientAddress.resolve(req("::ffff:10.0.0.5", "X-Forwarded-For" -> "203.0.113.7"), proxyOnly).ip, ip("203.0.113.7"))
    assert(TrustedProxies.parseCidrs("10.0.0.0/8, nonsense").isLeft)
    assertEquals(TrustedProxies.parseCidrs("192.168.1.10, 2001:db8::1").map(_.map(_.prefixBits)), Right(List(32, 128)))
  }
