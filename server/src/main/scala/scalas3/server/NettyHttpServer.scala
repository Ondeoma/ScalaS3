package scalas3.server

import cats.effect.std.Dispatcher
import cats.effect.{IO, Resource}
import cats.syntax.traverse.*
import com.comcast.ip4s.SocketAddress
import fs2.{Chunk, Stream}
import io.netty.bootstrap.ServerBootstrap
import io.netty.buffer.Unpooled
import io.netty.channel.epoll.{Epoll, EpollIoHandler, EpollServerSocketChannel}
import io.netty.channel.group.DefaultChannelGroup
import io.netty.channel.kqueue.{KQueue, KQueueIoHandler, KQueueServerSocketChannel}
import io.netty.channel.nio.NioIoHandler
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.channel.{
  Channel,
  ChannelFuture,
  ChannelFutureListener,
  ChannelHandlerContext,
  ChannelInboundHandlerAdapter,
  ChannelInitializer,
  ChannelOption,
  EventLoopGroup,
  MultiThreadIoEventLoopGroup,
  ServerChannel
}
import io.netty.handler.codec.http.*
import io.netty.handler.timeout.{IdleStateEvent, IdleStateHandler}
import io.netty.util.ReferenceCountUtil
import io.netty.util.concurrent.GlobalEventExecutor
import org.http4s.{Header, Headers, HttpApp, Method, Request, Response, Status, Uri}
import org.typelevel.ci.CIString
import org.typelevel.vault.Vault

import java.net.InetSocketAddress
import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneOffset}
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.*

/** Netty の上で http4s のアプリを動かす HTTP/1.1 サーバー（S3 の公開ポート用）。
  *
  * Ember は `Expect: 100-continue` に応えず、http4s-netty は応答をヘッダー・本体・終端の 3 回に分けて書いて
  * そのたびに完了を待つため小さな応答が遅かった（docs/benchmark.md）。ここでは次のようにする。
  *   - HTTP の解析と組み立ては Netty（HttpServerCodec）に任せる
  *   - 要求の本体はアプリが読んだ分だけ Netty に読み込みを頼む（背圧）。最初に読まれたときに 100 Continue を返す
  *   - 本体が小さい応答（strictBodyLimit 以下）は、ヘッダーと本体を 1 回で書く
  *   - 大きな応答は、送信が詰まったときだけ書き込みの完了を待ちながら流す
  *   - 1 本の接続では要求を 1 件ずつ処理する（先に届いた次の要求は順番を待つ）。本体を読み切らずに応答した
  *     要求のあとは接続を閉じる
  *
  * 接続ごとの状態はイベントループのスレッドだけが触る。アプリ（cats-effect）からの操作は、イベントループに
  * 仕事を渡して行う。
  */
object NettyHttpServer:

  final case class Config(
      host: String,
      port: Int,
      maxInitialLineLength: Int = 64 * 1024,
      maxHeaderSize: Int = 64 * 1024,
      /** 要求の本体を受け渡す 1 回の大きさの上限。 */
      maxChunkSize: Int = 256 * 1024,
      idleTimeout: FiniteDuration = 5.minutes,
      /** これ以下の大きさの応答は、本体をまとめて 1 回で書く。 */
      strictBodyLimit: Int = 64 * 1024,
      /** 0 なら Netty の既定（CPU 数 × 2）。 */
      eventLoopThreads: Int = 0,
      /** HTTPS にするときの証明書と鍵。なければ平文。 */
      tls: Option[PublicTls] = None
  )

  private val logger = org.slf4j.LoggerFactory.getLogger("scalas3.server.NettyHttpServer")

  /** 待ち受けを始め、実際に待ち受けているアドレスを返す。解放すると、待ち受けと全接続を閉じる。 */
  def resource(cfg: Config, app: HttpApp[IO]): Resource[IO, InetSocketAddress] =
    for
      ssl <- cfg.tls.traverse(PublicTls.reloading)
      dispatcher <- Dispatcher.parallel[IO](await = false)
      transport <- Resource.make(IO(newTransport(cfg.eventLoopThreads)))((group, _) => await(group.shutdownGracefully(0, 5, TimeUnit.SECONDS)))
      (group, channelClass) = transport
      children <- Resource.eval(IO(DefaultChannelGroup(GlobalEventExecutor.INSTANCE)))
      server <- Resource.make(IO.async_[Channel] { cb =>
        val b = ServerBootstrap()
          .group(group)
          .channel(channelClass)
          .option(ChannelOption.SO_BACKLOG, Integer.valueOf(1024))
          .option(ChannelOption.SO_REUSEADDR, java.lang.Boolean.TRUE)
          .childOption(ChannelOption.TCP_NODELAY, java.lang.Boolean.TRUE)
          // 読み込みはこちらから頼む（要求を 1 件ずつ処理し、本体はアプリが読んだ分だけ読む）
          .childOption(ChannelOption.AUTO_READ, java.lang.Boolean.FALSE)
          .childHandler(new ChannelInitializer[Channel]:
            def initChannel(ch: Channel): Unit =
              children.add(ch)
              val p = ch.pipeline
              if cfg.idleTimeout.isFinite && cfg.idleTimeout > Duration.Zero then
                p.addLast(IdleStateHandler(0, 0, cfg.idleTimeout.toMillis, TimeUnit.MILLISECONDS)): Unit
              ssl.foreach(ref => p.addLast(ref.get.newHandler(ch.alloc)))
              p.addLast(HttpServerCodec(cfg.maxInitialLineLength, cfg.maxHeaderSize, cfg.maxChunkSize))
                .addLast(Http1Connection(app, dispatcher, cfg)): Unit
          )
        b.bind(InetSocketAddress(cfg.host, cfg.port)).addListener { (f: ChannelFuture) =>
          if f.isSuccess then cb(Right(f.channel)) else cb(Left(f.cause))
        }: Unit
      })(ch => await(ch.close()) >> await(children.close()))
      _ <- Resource.eval(IO(logger.info(s"${if ssl.isDefined then "HTTPS" else "HTTP"} server on ${server.localAddress} (${channelClass.getSimpleName})")))
    yield server.localAddress.asInstanceOf[InetSocketAddress]

  private def newTransport(threads: Int): (EventLoopGroup, Class[? <: ServerChannel]) =
    if Epoll.isAvailable then (MultiThreadIoEventLoopGroup(threads, EpollIoHandler.newFactory()), classOf[EpollServerSocketChannel])
    else if KQueue.isAvailable then (MultiThreadIoEventLoopGroup(threads, KQueueIoHandler.newFactory()), classOf[KQueueServerSocketChannel])
    else (MultiThreadIoEventLoopGroup(threads, NioIoHandler.newFactory()), classOf[NioServerSocketChannel])

  /** Netty の Future の完了を待つ。f は名前渡し：待つ IO を作るだけで操作が走らないように（Resource.make は解放の IO を
    * 確保の直後に作っておくので、値渡しだと close などがその時点で実行されてしまう）。
    */
  private def await(f: => io.netty.util.concurrent.Future[?]): IO[Unit] =
    IO.async_[Unit] { cb =>
      f.addListener((x: io.netty.util.concurrent.Future[?]) => cb(if x.isSuccess || x.isCancelled then Right(()) else Left(x.cause))): Unit
    }

  /** Date ヘッダーの値（1 秒ごとに作り直す）。 */
  private object HttpDate:
    @volatile private var cached: (Long, String) = (0L, "")
    def now: String =
      val sec = System.currentTimeMillis() / 1000
      val c = cached
      if c._1 == sec then c._2
      else
        val s = DateTimeFormatter.RFC_1123_DATE_TIME.format(Instant.ofEpochSecond(sec).atOffset(ZoneOffset.UTC))
        cached = (sec, s)
        s

  /** 1 件の要求と応答。 */
  private final class Exchange(val req: HttpRequest, val hasBody: Boolean):
    val keepAliveRequested: Boolean = HttpUtil.isKeepAlive(req)
    val isHead: Boolean = req.method == HttpMethod.HEAD
    /** 本体の終わり（LastHttpContent）まで受け取った。 */
    @volatile var bodyDone: Boolean = !hasBody
    /** 本体を最初に読まれたら 100 Continue を返す。 */
    var expectContinue: Boolean = hasBody && req.protocolVersion != HttpVersion.HTTP_1_0 && HttpUtil.is100ContinueExpected(req)
    /** 本体を待っている読み手。 */
    var pull: Either[Throwable, Option[Chunk[Byte]]] => Unit = null
    /** 本体のない要求でも、終わり（空の LastHttpContent）は届くので読み捨てる。 */
    var trailerPending: Boolean = !hasBody
    /** 応答を書き終えた後、接続を閉じる。 */
    @volatile var closeAfter: Boolean = false
    /** 応答を書き終え、次の要求に進むために本体の残りを読み捨てている。 */
    var draining: Boolean = false
    /** 応答のヘッダーを送り始めた（これより後の失敗は、接続を閉じて途中で切れたことを示すしかない）。 */
    @volatile var headersSent: Boolean = false

  private final class Http1Connection(app: HttpApp[IO], dispatcher: Dispatcher[IO], cfg: Config) extends ChannelInboundHandlerAdapter:
    private var ctx: ChannelHandlerContext = null
    private val inbox = java.util.ArrayDeque[HttpObject]()
    private var ex: Exchange = null
    @volatile private var closed = false

    override def handlerAdded(c: ChannelHandlerContext): Unit = ctx = c

    /** イベントループに仕事を渡す。止まった後（サーバーの終了中）なら false。 */
    private def onLoop(f: => Unit): Boolean =
      try
        ctx.executor.execute(() => f)
        true
      catch case _: java.util.concurrent.RejectedExecutionException => false

    override def channelActive(c: ChannelHandlerContext): Unit =
      c.read()
      super.channelActive(c)

    override def channelRead(c: ChannelHandlerContext, msg: AnyRef): Unit =
      msg match
        case o: HttpObject =>
          inbox.add(o)
          process()
        case other => ReferenceCountUtil.release(other): Unit

    override def channelInactive(c: ChannelHandlerContext): Unit =
      closed = true
      if ex != null && ex.pull != null then
        val cb = ex.pull
        ex.pull = null
        cb(Left(java.io.IOException("connection closed before the request body was complete")))
      while !inbox.isEmpty do ReferenceCountUtil.release(inbox.poll()): Unit
      super.channelInactive(c)

    override def exceptionCaught(c: ChannelHandlerContext, cause: Throwable): Unit =
      logger.debug("connection error", cause)
      c.close(): Unit

    override def userEventTriggered(c: ChannelHandlerContext, evt: AnyRef): Unit =
      evt match
        // 要求の合間か、本体を待っている間に無通信が続いたら閉じる（アプリの処理中は閉じない）
        case _: IdleStateEvent if ex == null || ex.pull != null => c.close(): Unit
        case _                                               => super.userEventTriggered(c, evt)

    /** 溜まった受信を、いまの状態に合わせて進める。足りなければ読み込みを頼む。イベントループで呼ぶ。 */
    private def process(): Unit =
      var more = true
      while more && !closed do
        if ex == null then
          inbox.poll() match
            case null              => ctx.read(); more = false
            case req: HttpRequest  => start(req)
            case other: HttpObject => ReferenceCountUtil.release(other): Unit // 前の要求の残り（通常は来ない）
        else if ex.pull != null || ex.draining || (ex.trailerPending && ex.bodyDone) then
          inbox.peek() match
            case null => ctx.read(); more = false
            case c: HttpContent =>
              inbox.poll()
              if ex.pull != null then deliver(c)
              else
                // 読み捨て（本体のない要求の終わり、または応答後の残り）
                val last = c.isInstanceOf[LastHttpContent]
                c.release()
                if last then
                  ex.trailerPending = false
                  ex.bodyDone = true
                  if ex.draining then ex = null
            case _: HttpRequest =>
              // 本体の終わりより前に次の要求は来ない（来たら壊れている）
              ctx.close(); more = false
            case _ => ReferenceCountUtil.release(inbox.poll()): Unit
        else more = false

    private def deliver(c: HttpContent): Unit =
      val cb = ex.pull
      if c.decoderResult.isFailure then
        c.release()
        ex.pull = null
        cb(Left(java.io.IOException("malformed request body", c.decoderResult.cause)))
        ctx.close(): Unit
      else
        val buf = c.content
        val n = buf.readableBytes
        val last = c.isInstanceOf[LastHttpContent]
        val bytes = if n == 0 then null else { val a = new Array[Byte](n); buf.readBytes(a); a }
        c.release()
        if last then ex.bodyDone = true
        if bytes != null then
          ex.pull = null
          cb(Right(Some(Chunk.array(bytes))))
        else if last then
          ex.pull = null
          cb(Right(None))
        // 空で終わりでもないなら、次を待つ

    private def start(req: HttpRequest): Unit =
      val hasBody = HttpUtil.isTransferEncodingChunked(req) || HttpUtil.getContentLength(req, 0L) > 0L
      val e = Exchange(req, hasBody)
      ex = e
      toRequest(e) match
        case None =>
          e.closeAfter = true
          writeSimple(e, HttpResponseStatus.BAD_REQUEST)
        case Some(request) =>
          dispatcher.unsafeRunAndForget(
            app
              .run(request)
              .handleErrorWith { err =>
                IO(logger.error(s"error handling ${req.method} ${req.uri}", err)).as(Response[IO](Status.InternalServerError))
              }
              .flatMap(resp => writeResponse(e, resp))
              // 相手が切ったのでなければ、本体の途中で失敗したということ（応答は途中で切れる）。原因を追えるように残す
              .handleErrorWith(err =>
                IO(if closed then logger.debug("error writing response", err) else logger.warn(s"error writing the response to ${req.method} ${req.uri}", err)) >>
                  IO {
                    e.closeAfter = true
                    // まだ何も送っていなければ、黙って閉じずに 500 を返す（クライアントが「応答なし」と区別できるように）
                    if !closed && !e.headersSent then
                      e.headersSent = true
                      writeSimple(e, HttpResponseStatus.INTERNAL_SERVER_ERROR)
                  }
              )
              .guarantee(IO(onLoop(finish(e))).void)
          )

    /** 応答を書き終えた。次の要求に進むか、接続を閉じる。イベントループで呼ぶ。 */
    private def finish(e: Exchange): Unit =
      if !closed && (ex eq e) then
        if e.closeAfter || (e.hasBody && !e.bodyDone) then ctx.close(): Unit
        else if e.trailerPending then
          e.draining = true
          process()
        else
          ex = null
          process()

    private def toRequest(e: Exchange): Option[Request[IO]] =
      val req = e.req
      if req.decoderResult.isFailure then None
      else
        for
          method <- Method.fromString(req.method.name).toOption
          uri <- Uri.fromString(req.uri).toOption
        yield
          val hs = List.newBuilder[Header.Raw]
          val it = req.headers.iteratorAsString()
          while it.hasNext do
            val h = it.next()
            hs += Header.Raw(CIString(h.getKey), h.getValue)
          val version = if req.protocolVersion == HttpVersion.HTTP_1_0 then org.http4s.HttpVersion.`HTTP/1.0` else org.http4s.HttpVersion.`HTTP/1.1`
          val attrs = (ctx.channel.localAddress, ctx.channel.remoteAddress) match
            case (l: InetSocketAddress, r: InetSocketAddress) =>
              Vault.empty.insert(
                Request.Keys.ConnectionInfo,
                Request.Connection(SocketAddress.fromInetSocketAddress(l).asIpUnsafe, SocketAddress.fromInetSocketAddress(r).asIpUnsafe, secure = cfg.tls.isDefined)
              )
            case _ => Vault.empty
          val body = if e.hasBody then Stream.repeatEval(pull(e)).unNoneTerminate.unchunks else Stream.empty
          Request[IO](method, uri, version, Headers(hs.result()), body, attrs)

    /** 本体を 1 つ受け取る（終わりなら None）。 */
    private def pull(e: Exchange): IO[Option[Chunk[Byte]]] =
      IO.async[Option[Chunk[Byte]]] { cb =>
        IO {
          val queued = onLoop {
            if e.bodyDone then cb(Right(None))
            else if closed then cb(Left(java.io.IOException("connection closed before the request body was complete")))
            else
              e.pull = cb
              if e.expectContinue then
                e.expectContinue = false
                ctx.writeAndFlush(DefaultFullHttpResponse(e.req.protocolVersion, HttpResponseStatus.CONTINUE)): Unit
              process()
          }
          if !queued then cb(Left(java.io.IOException("server is shutting down")))
          // 取り消されたら待ちを外す
          Some(IO(onLoop(if e.pull eq cb then e.pull = null)).void)
        }
      }

    private def writeSimple(e: Exchange, status: HttpResponseStatus): Unit =
      val r = DefaultFullHttpResponse(e.req.protocolVersion, status)
      r.headers.set(HttpHeaderNames.CONTENT_LENGTH, 0).set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE).set(HttpHeaderNames.DATE, HttpDate.now)
      ctx.writeAndFlush(r).addListener(ChannelFutureListener.CLOSE): Unit

    private def channelClosed = java.io.IOException("connection closed")

    private def writeResponse(e: Exchange, resp: Response[IO]): IO[Unit] =
      IO.defer {
        val version = e.req.protocolVersion
        val code = resp.status.code
        val status = HttpResponseStatus.valueOf(code)
        val headers = DefaultHttpHeaders()
        resp.headers.foreach(h => headers.add(h.name.toString, h.value): Unit)
        if !headers.contains(HttpHeaderNames.DATE) then headers.set(HttpHeaderNames.DATE, HttpDate.now): Unit
        // 本体を読み切らずに応答するなら、接続は使い回さない（残りの本体と次の要求の区切りが分からない）
        val keepAlive =
          e.keepAliveRequested && (e.bodyDone || !e.hasBody) && !headers.containsValue(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE, true)
        if !keepAlive then
          e.closeAfter = true
          headers.set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE): Unit
        else if version == HttpVersion.HTTP_1_0 then headers.set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE): Unit
        val noBody = e.isHead || code / 100 == 1 || code == 204 || code == 304
        val declared = resp.contentLength
        if noBody then
          if code / 100 == 1 || code == 204 then headers.remove(HttpHeaderNames.CONTENT_LENGTH).remove(HttpHeaderNames.TRANSFER_ENCODING): Unit
          writeFull(e, version, status, headers, Unpooled.EMPTY_BUFFER)
        else if declared.exists(_ <= cfg.strictBodyLimit) then
          // 小さな本体はまとめて、ヘッダーと 1 回で書く（1 つのチャンクだけなら、その配列をコピーせずに渡す）
          resp.body.compile.to(Chunk).flatMap { c =>
            val s = c.toArraySlice
            headers.set(HttpHeaderNames.CONTENT_LENGTH, s.length)
            writeFull(e, version, status, headers, Unpooled.wrappedBuffer(s.values, s.offset, s.length))
          }
        else
          if declared.isEmpty then
            headers.remove(HttpHeaderNames.CONTENT_LENGTH)
            if version == HttpVersion.HTTP_1_0 then
              // 長さを伝えられないので、接続を閉じて終わりを示す
              e.closeAfter = true
              headers.set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE): Unit
            else headers.set(HttpHeaderNames.TRANSFER_ENCODING, HttpHeaderValues.CHUNKED): Unit
          IO { e.headersSent = true; ctx.write(DefaultHttpResponse(version, status, headers)) } >>
            resp.body.chunks
              .evalMap { c =>
                IO.defer {
                  if closed then IO.raiseError(channelClosed)
                  else
                    val s = c.toArraySlice
                    val f = ctx.writeAndFlush(DefaultHttpContent(Unpooled.wrappedBuffer(s.values, s.offset, s.length)))
                    // 送信が詰まっているときだけ完了を待つ（待たないと、遅い相手に対してメモリに溜まり続ける）
                    if ctx.channel.isWritable then IO.unit else await(f)
                }
              }
              .compile
              .drain >>
            IO.defer(await(ctx.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT)))
      }

    private def writeFull(e: Exchange, version: HttpVersion, status: HttpResponseStatus, headers: HttpHeaders, content: io.netty.buffer.ByteBuf): IO[Unit] =
      IO.defer {
        if closed then IO.raiseError(channelClosed)
        else
          e.headersSent = true
          await(ctx.writeAndFlush(DefaultFullHttpResponse(version, status, content, headers, EmptyHttpHeaders.INSTANCE)))
      }
