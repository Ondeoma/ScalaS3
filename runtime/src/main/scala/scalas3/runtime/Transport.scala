package scalas3.runtime

import cats.effect.{IO, Resource}
import com.comcast.ip4s.{Host, Port, SocketAddress}
import fs2.io.net.{Network, Socket}
import fs2.{Chunk, Stream}

final case class NodeAddress(host: String, port: Int):
  override def toString: String = s"$host:$port"

/** 双方向のバイトストリーム（TCP 接続に相当）。 */
trait Connection:
  def remote: NodeAddress

  /** 受信したバイト列。1 つのコンシューマーからだけ読むこと。相手が閉じると終了し、切断されるとエラーになる。 */
  def reads: Stream[IO, Byte]
  def write(bytes: Chunk[Byte]): IO[Unit]

/** ノード間ネットワークの抽象。シミュレーションでは SimNetwork に差し替える。 */
trait Transport:
  def connect(to: NodeAddress): Resource[IO, Connection]

  /** 待ち受けを開始する。返すストリームの各要素は、下流がその要素を処理し終えるまで有効。
    * 典型的には `conns.map(handle).parJoinUnbounded` の形で使う。
    */
  def listen(at: NodeAddress): Resource[IO, (NodeAddress, Stream[IO, Connection])]

/** fs2-io による平文の TCP 実装（TLS は TlsTransport）。 */
object TcpTransport extends Transport:

  private def toAddr(a: NodeAddress): IO[SocketAddress[Host]] =
    IO.fromOption(
      for
        h <- Host.fromString(a.host)
        p <- Port.fromInt(a.port)
      yield SocketAddress(h, p)
    )(IllegalArgumentException(s"invalid address: $a"))

  private def wrap(s: Socket[IO], remoteAddr: NodeAddress): Connection = new Connection:
    val remote = remoteAddr
    def reads: Stream[IO, Byte] = s.reads
    def write(bytes: Chunk[Byte]): IO[Unit] = s.write(bytes)

  def connect(to: NodeAddress): Resource[IO, Connection] =
    Resource.eval(toAddr(to)).flatMap(Network[IO].connect(_, SocketOptions.default)).map(wrap(_, to))

  def listen(at: NodeAddress): Resource[IO, (NodeAddress, Stream[IO, Connection])] =
    for
      addr <- Resource.eval(toAddr(at))
      server <- Network[IO].bind(addr, SocketOptions.default)
    yield
      val bound = server.address.asIpUnsafe
      val conns = server.accept.map { s =>
        val peer = s.peerAddress.asIpUnsafe
        wrap(s, NodeAddress(peer.host.toString, peer.port.value))
      }
      (NodeAddress(bound.host.toString, bound.port.value), conns)
