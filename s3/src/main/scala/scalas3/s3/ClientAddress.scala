package scalas3.s3

import com.comcast.ip4s.{Cidr, IpAddress}
import org.http4s.Request
import org.typelevel.ci.CIString

/** 送信元の IP をどのヘッダーから読むか。 */
enum ClientIpHeader:
  /** `X-Forwarded-For: client, proxy1, proxy2`（各プロキシが右に足していく）。HTTPS かは `X-Forwarded-Proto`。 */
  case XForwardedFor

  /** RFC 7239 の `Forwarded: for=client;proto=https, for=proxy1`。HTTPS かは同じ要素の `proto`。 */
  case Forwarded

  /** CDN などが 1 つの値で渡すヘッダー（`CF-Connecting-IP`、`True-Client-IP`、`X-Real-IP` など）。
    * 直前のプロキシを信頼するときだけ使う。そのプロキシが、クライアントから届いた同じ名前のヘッダーを消すか
    * 上書きしていなければ、偽の値を送られる。
    */
  case Single(name: CIString)

/** 前段のリバースプロキシや CDN の設定。
  *
  * @param cidrs 信頼するプロキシのアドレスの範囲（リバースプロキシ、CDN の公開している範囲など）。直接つないできた
  *              相手がここに入っていなければ、ヘッダーは一切見ない（直接つないで偽のヘッダーを送られても騙されない）
  * @param hops  直接つないできた相手を 1 段目として、手前から何段目までを、アドレスによらず信頼するか。CDN のように
  *              アドレスの範囲が広く変わりやすいときに使う（例：CDN → リバースプロキシ → scalas3 なら 2）。この場合、
  *              リバースプロキシが CDN 以外からの接続を受けないようにしておくこと
  */
final case class TrustedProxies(cidrs: List[Cidr[IpAddress]], hops: Option[Int] = None, header: ClientIpHeader = ClientIpHeader.XForwardedFor):
  require(hops.forall(_ >= 1), "hops must be >= 1")

  private[s3] def trusts(ip: IpAddress, position: Int): Boolean =
    cidrs.exists(_.contains(ip)) || hops.exists(position <= _)

object TrustedProxies:
  /** `10.0.0.0/8,192.168.1.10,2001:db8::/32` のような並び。IP だけなら /32（IPv6 は /128）とみなす。 */
  def parseCidrs(s: String): Either[String, List[Cidr[IpAddress]]] =
    s.split(',').toList.map(_.trim).filter(_.nonEmpty).foldLeft[Either[String, List[Cidr[IpAddress]]]](Right(Nil)) { (acc, part) =>
      acc.flatMap { xs =>
        val parsed = Cidr.fromString(part).orElse(IpAddress.fromString(part).map(ip => Cidr(ip, if ip.asIpv4.isDefined then 32 else 128)))
        parsed.toRight(s"invalid address or CIDR: $part").map(xs :+ _)
      }
    }

  def parseHeader(s: String): ClientIpHeader = s.trim.toLowerCase match
    case "x-forwarded-for" => ClientIpHeader.XForwardedFor
    case "forwarded"       => ClientIpHeader.Forwarded
    case other             => ClientIpHeader.Single(CIString(other))

/** 送信元。ip が None なのは、プロキシが送信元を伝えなかった（`unknown` や読めない値）とき。 */
final case class ClientAddress(ip: Option[IpAddress], secure: Boolean)

object ClientAddress:

  /** 要求の送信元を決める。
    *
    * プロキシを信頼しない設定（None）や、直接つないできた相手が信頼する範囲にないときは、接続の相手そのもの。
    * そうでなければ、プロキシが足していった記録を右（自分に近い側）からたどり、信頼しない最初のアドレスを送信元とする。
    * 全部信頼できるなら、いちばん左（いちばん遠い側）。途中に読めない値があれば、送信元は分からない（None）。
    * 分からないときにプロキシのアドレスで代用すると、内部のアドレスを許すポリシーに誤って当てはまるため。
    */
  def resolve[F[_]](req: Request[F], proxies: Option[TrustedProxies]): ClientAddress =
    val peer = req.remote.map(_.host.collapseMappedV4)
    val direct = ClientAddress(peer, req.isSecure.contains(true))
    (proxies, peer) match
      // 直接つないできた相手は、段数の指定によらずアドレスの範囲で確かめる
      case (Some(tp), Some(p)) if tp.cidrs.exists(_.contains(p)) =>
        tp.header match
          case ClientIpHeader.Single(name) =>
            headerValues(req, name).lastOption match
              case None    => direct
              case Some(v) => ClientAddress(parseIp(v), forwardedProto(req, 1, 0).getOrElse(false))
          case ClientIpHeader.XForwardedFor =>
            val hops = headerValues(req, CIString("X-Forwarded-For")).flatMap(splitList).toVector
            if hops.isEmpty then direct
            else
              walk(tp, hops.map(parseIp)) match
                case Picked(ip, index) => ClientAddress(ip, forwardedProto(req, hops.size, index).getOrElse(false))
          case ClientIpHeader.Forwarded =>
            val elems = headerValues(req, CIString("Forwarded")).flatMap(splitList).toVector.map(parseForwardedElement)
            if elems.isEmpty then direct
            else
              walk(tp, elems.map(_._1.flatMap(parseIp))) match
                case Picked(ip, index) => ClientAddress(ip, elems(index)._2.exists(_.equalsIgnoreCase("https")))
      case _ => direct

  private final case class Picked(ip: Option[IpAddress], index: Int)

  /** 右からたどる。直接つないできた相手を 1 段目として数える。 */
  private def walk(tp: TrustedProxies, hops: Vector[Option[IpAddress]]): Picked =
    var i = hops.size - 1
    var position = 2
    var result: Picked = null
    while result == null && i >= 0 do
      hops(i) match
        case None                                  => result = Picked(None, i)
        case Some(ip) if tp.trusts(ip, position) => i -= 1; position += 1
        case Some(ip)                              => result = Picked(Some(ip), i)
    if result == null then Picked(hops(0), 0) else result

  /** HTTPS か。X-Forwarded-Proto が X-Forwarded-For と同じ数あれば同じ位置の値、1 つだけならその値。 */
  private def forwardedProto[F[_]](req: Request[F], count: Int, index: Int): Option[Boolean] =
    val protos = headerValues(req, CIString("X-Forwarded-Proto")).flatMap(splitList).toVector
    val chosen = if protos.size == count && count > 1 then protos.lift(index) else if protos.size == 1 then protos.headOption else None
    chosen.map(_.equalsIgnoreCase("https"))

  private def headerValues[F[_]](req: Request[F], name: CIString): List[String] =
    req.headers.headers.collect { case h if h.name == name => h.value }

  private def splitList(v: String): List[String] = v.split(',').toList.map(_.trim).filter(_.nonEmpty)

  /** IP（IPv6 は角かっこやポートつきでもよい。IPv4 にもポートがつくことがある）。 */
  private[s3] def parseIp(raw: String): Option[IpAddress] =
    val s = raw.trim.stripPrefix("\"").stripSuffix("\"")
    val noPort =
      if s.startsWith("[") then s.drop(1).takeWhile(_ != ']')
      else if s.count(_ == ':') == 1 then s.takeWhile(_ != ':') // IPv4:port
      else s
    IpAddress.fromString(noPort).map(_.collapseMappedV4)

  /** `for=...;proto=...;by=...` から for と proto。 */
  private def parseForwardedElement(e: String): (Option[String], Option[String]) =
    val pairs = e.split(';').toList.flatMap { kv =>
      kv.split("=", 2) match
        case Array(k, v) => Some(k.trim.toLowerCase -> v.trim.stripPrefix("\"").stripSuffix("\""))
        case _           => None
    }
    (pairs.collectFirst { case ("for", v) => v }, pairs.collectFirst { case ("proto", v) => v })
