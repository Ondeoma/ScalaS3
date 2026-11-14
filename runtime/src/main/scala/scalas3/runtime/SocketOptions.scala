package scalas3.runtime

import fs2.io.net.SocketOption

/** すべての TCP ソケットに付けるオプション。
  *
  * TCP_NODELAY を付けないと、ヘッダーと本体を別々に書く HTTP の応答や小さな RPC が Nagle のアルゴリズムで
  * 相手の遅延 ACK（Linux で最大 40 ms）を待ち、1 回の要求が 40 ms 以上かかる（Linux で測定：1 MiB の GET が
  * p50 45 ms に張り付いた）。
  */
object SocketOptions:
  val default: List[SocketOption] = List(SocketOption.noDelay(true))
