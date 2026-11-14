package scalas3.server

import cats.effect.{IO, Resource}
import cats.syntax.all.*
import com.sun.net.httpserver.{HttpExchange, HttpsConfigurator, HttpsExchange, HttpsParameters, HttpsServer}
import io.circe.Json
import io.circe.parser.parse
import io.circe.syntax.*
import scalas3.pki.{JoinToken, Pki}
import scalas3.runtime.NodeAddress

import java.io.FileInputStream
import java.net.InetSocketAddress
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path}
import java.security.KeyStore
import java.security.cert.{CertificateException, X509Certificate}
import java.util.Base64
import java.util.concurrent.Executors
import javax.net.ssl.{KeyManagerFactory, SSLContext, SSLEngine, TrustManagerFactory, X509ExtendedTrustManager}
import scala.jdk.CollectionConverters.*

/** ノード間 TLS の証明書の自動発行（設計 §7.1、C9、C14）。docs/tls.md を参照。
  *
  *   - 内部 CA は運用者が `scalas3 cert init-ca` で 1 度だけ作る（CA の証明書と鍵、参加の秘密、参加トークン）
  *   - CA のディレクトリを渡されたノード（初期メンバー）は、起動時に自分の証明書を自分で発行する。さらに参加の受け付け
  *     （HTTPS、サーバー認証のみ）を開き、参加トークンの秘密を確かめてから、署名要求に署名する
  *   - 後から加わるノードは参加トークンを渡され、受け付けに署名要求を送って証明書を受け取る。トークンには CA の証明書の
  *     SHA-256 があるので、相手が本物のクラスタの受け付けかを確かめられる（なりすましに秘密を渡さない）
  *   - 発行した証明書はデータディレクトリの tls/ に置き、次の起動からはそれを使う
  *   - 有効期間の 2/3 を過ぎたら自動で更新する（`renewing`）。CA を持つノードは自分で発行し直し、後から加わったノードは
  *     いまの証明書で相互認証して受け付けの /v1/renew に頼む（参加トークンは要らない）。書き換えた鍵ストアは
  *     TlsTransport.reloading が読み直す
  */
object AutoTls:

  final case class Settings(
      caDir: Option[Path],
      joinToken: Option[JoinToken],
      /** 参加の受け付けのアドレス（後から加わるノードが使う）。 */
      joinAddress: Option[NodeAddress],
      /** 参加の受け付けを開くアドレス（CA を持つノード）。 */
      joinListen: NodeAddress,
      days: Int = 3 * 365,
      /** 更新が要るかを確かめる間隔。 */
      renewCheck: scala.concurrent.duration.FiniteDuration = scala.concurrent.duration.DurationInt(6).hours
  )

  /** 鍵ストア（ノードの鍵と証明書の鎖）、トラストストア（CA）、その合言葉。 */
  final case class Material(keystore: Path, truststore: Path, password: String)

  private def tlsDir(dataDir: Path) = dataDir.resolve("tls")
  private def joinAddressFile(dataDir: Path) = tlsDir(dataDir).resolve("join-address")

  /** 使う証明書を用意する。自動発行の設定がなく、発行済みのものもなければ None（平文の TCP）。 */
  def ensure(dataDir: Path, nodeId: String, s: Settings): IO[Option[Material]] =
    IO.blocking {
      val dir = tlsDir(dataDir)
      val m = Material(dir.resolve("node.p12"), dir.resolve("ca.p12"), "")
      if Files.exists(m.keystore) && Files.exists(m.truststore) then Some(m.copy(password = Files.readString(dir.resolve("password")).trim))
      else None
    }.flatMap {
      case some @ Some(_) => IO.pure(some)
      case None =>
        (s.caDir, s.joinToken) match
          case (Some(ca), _) => IO.blocking(issueLocally(dataDir, nodeId, ca, s.days)).map(Some(_))
          case (None, Some(token)) =>
            IO.fromOption(s.joinAddress)(IllegalArgumentException("SCALAS3_JOIN_ADDRESS is required with SCALAS3_JOIN_TOKEN"))
              .flatMap(addr => IO.blocking {
                val m = join(dataDir, nodeId, token, addr)
                // 更新のときに使う（環境変数を外して起動し直しても更新できるように）
                Files.writeString(joinAddressFile(dataDir), s"${addr.host}:${addr.port}")
                m
              })
              .map(Some(_))
          case (None, None) => IO.pure(None)
    }

  private def readCa(caDir: Path): (java.security.PrivateKey, X509Certificate, String) =
    val cert = Pki.readCerts(Files.readString(caDir.resolve("ca.crt"))).head
    val key = Pki.readKey(Files.readString(caDir.resolve("ca.key")))
    val secret = Files.readString(caDir.resolve("join.secret")).trim
    (key, cert, secret)

  private def save(dataDir: Path, key: java.security.PrivateKey, chain: Vector[X509Certificate], ca: X509Certificate, keepPassword: Option[String] = None): Material =
    val dir = tlsDir(dataDir)
    val password = keepPassword.getOrElse(Pki.newSecret())
    val m = Material(dir.resolve("node.p12"), dir.resolve("ca.p12"), password)
    if keepPassword.isEmpty then
      Pki.writePrivate(dir.resolve("password"), password)
      Pki.writeTrustStore(m.truststore, password, ca)
    // 鍵ストアは最後に書く（揃っていない状態で次の起動が使わないように。ensure は両方あるときだけ使う）
    Pki.writeKeyStore(m.keystore, password, key, chain)
    m

  private def issueLocally(dataDir: Path, nodeId: String, caDir: Path, days: Int, keepPassword: Option[String] = None): Material =
    val (caKey, caCert, _) = readCa(caDir)
    val kp = Pki.newKeyPair()
    val cert = Pki.signCsr(Pki.csr(nodeId, kp), nodeId, caKey, caCert, days)
    save(dataDir, kp.getPrivate, Vector(cert, caCert), caCert, keepPassword)

  // ---- 更新 ----

  private def loadStore(p: Path, password: String): KeyStore =
    val ks = KeyStore.getInstance("PKCS12")
    val in = FileInputStream(p.toFile)
    try ks.load(in, password.toCharArray)
    finally in.close()
    ks

  /** 鍵ストアのノードの証明書。 */
  def nodeCertificate(m: Material): X509Certificate =
    loadStore(m.keystore, m.password).getCertificate("node").asInstanceOf[X509Certificate]

  /** 有効期間の 2/3 を過ぎたか。 */
  def renewalDue(cert: X509Certificate, now: java.time.Instant): Boolean =
    val from = cert.getNotBefore.getTime
    val until = cert.getNotAfter.getTime
    now.toEpochMilli >= from + (until - from) * 2 / 3

  /** 更新が要れば更新する。更新したら true。CA を持つノードは自分で、そうでなければ受け付けの /v1/renew に頼む。 */
  def renewIfDue(dataDir: Path, nodeId: String, s: Settings, m: Material, now: java.time.Instant): IO[Boolean] =
    IO.blocking(renewalDue(nodeCertificate(m), now)).flatMap {
      case false => IO.pure(false)
      case true =>
        s.caDir match
          case Some(ca) => IO.blocking(issueLocally(dataDir, nodeId, ca, s.days, Some(m.password))).as(true)
          case None =>
            IO.blocking {
              val saved = joinAddressFile(dataDir)
              s.joinAddress.orElse(Option.when(Files.exists(saved))(parseHostPort(Files.readString(saved).trim)))
            }.flatMap {
              case None       => IO.raiseError(IllegalStateException("no join address is known for renewing the node certificate; set SCALAS3_JOIN_ADDRESS"))
              case Some(addr) => IO.blocking(renewRemotely(dataDir, nodeId, m, addr)).as(true)
            }
    }

  /** 定期的に更新を確かめる。失敗したら警告して、次の確認でもう一度試す。 */
  def renewing(dataDir: Path, nodeId: String, s: Settings, m: Material): Resource[IO, Unit] =
    val once = IO.realTimeInstant.flatMap(renewIfDue(dataDir, nodeId, s, m, _)).attempt.flatMap {
      case Right(true) =>
        IO.blocking(nodeCertificate(m)).flatMap(c => IO.println(s"[scalas3] renewed the node certificate (valid until ${c.getNotAfter.toInstant})"))
      case Right(false) => IO.unit
      case Left(e)      => IO.println(s"[scalas3] WARNING: could not renew the node certificate: ${Option(e.getMessage).getOrElse(e.toString)}")
    }
    (once >> IO.sleep(s.renewCheck)).foreverM.background.map(_ => ())

  private def keyManagers(m: Material): Array[javax.net.ssl.KeyManager] =
    val kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm)
    kmf.init(loadStore(m.keystore, m.password), m.password.toCharArray)
    kmf.getKeyManagers

  /** 受け付けの path に JSON を POST する（相手は CA の SHA-256 で確かめる）。200 でなければ失敗。
    * 応答の項目（PEM の証明書）を読む関数を返す。
    */
  private def post(addr: NodeAddress, path: String, body: Json, caSha256: String, keys: Array[javax.net.ssl.KeyManager], what: String)
      : String => Vector[X509Certificate] =
    val ssl = SSLContext.getInstance("TLSv1.3")
    ssl.init(keys, Array(pinnedTrustManager(caSha256)), null)
    val client = HttpClient.newBuilder().sslContext(ssl).build()
    val resp = client.send(
      HttpRequest.newBuilder(java.net.URI.create(s"https://${addr.host}:${addr.port}$path")).POST(HttpRequest.BodyPublishers.ofString(body.noSpaces)).build(),
      HttpResponse.BodyHandlers.ofString()
    )
    if resp.statusCode() != 200 then throw IllegalStateException(s"$what was refused (${resp.statusCode()}): ${resp.body()}")
    val c = parse(resp.body()).fold(e => throw e, identity).hcursor
    field => Pki.readCerts(c.get[String](field).fold(e => throw e, identity)).toVector

  private def csrJson(nodeId: String, kp: java.security.KeyPair): (String, Json) =
    "csr" -> Base64.getEncoder.encodeToString(Pki.csr(nodeId, kp)).asJson

  /** いまの証明書で相互認証して、受け付けに新しい証明書を発行してもらう。相手は鍵ストアにある CA で確かめる。 */
  private def renewRemotely(dataDir: Path, nodeId: String, m: Material, addr: NodeAddress): Material =
    val ca = loadStore(m.truststore, m.password).getCertificate("ca").asInstanceOf[X509Certificate]
    val kp = Pki.newKeyPair()
    val chain = post(addr, "/v1/renew", Json.obj(csrJson(nodeId, kp)), Pki.sha256Hex(ca), keyManagers(m), "renewal")("chain")
    if chain.isEmpty then throw IllegalStateException("the renewal endpoint returned no node certificate")
    chain.head.verify(ca.getPublicKey)
    if chain.head.getPublicKey != kp.getPublic then throw IllegalStateException("the renewed certificate does not match the new key")
    save(dataDir, kp.getPrivate, chain :+ ca, ca, Some(m.password))

  /** 参加の受け付けに署名要求を送って証明書を受け取る。相手の証明書の鎖は、トークンの CA の SHA-256 で確かめる。 */
  private def join(dataDir: Path, nodeId: String, token: JoinToken, addr: NodeAddress): Material =
    val kp = Pki.newKeyPair()
    val body = Json.obj("secret" -> token.secret.asJson, "nodeId" -> nodeId.asJson, csrJson(nodeId, kp))
    val certs = post(addr, "/v1/join", body, token.caSha256, null, "join")
    val chain = certs("chain")
    val ca = certs("ca").head
    if Pki.sha256Hex(ca) != token.caSha256 then throw IllegalStateException("the CA returned by the join endpoint does not match the token")
    save(dataDir, kp.getPrivate, chain :+ ca, ca)

  /** 鎖の最後（CA）の SHA-256 がトークンと一致し、先頭がその CA で署名されていれば信頼する。ホスト名は確かめない
    * （ノードは IP やノード ID で呼び合う。相手の確認は CA の固定で足りる）。拡張版の TrustManager にするのは、
    * JDK の HttpClient が通常版だとホスト名の確認を足してしまうため。
    */
  private def pinnedTrustManager(caSha256: String): X509ExtendedTrustManager = new X509ExtendedTrustManager:
    def getAcceptedIssuers: Array[X509Certificate] = Array.empty
    def checkClientTrusted(chain: Array[X509Certificate], authType: String): Unit = throw CertificateException("not a server")
    def checkClientTrusted(chain: Array[X509Certificate], authType: String, socket: java.net.Socket): Unit = checkClientTrusted(chain, authType)
    def checkClientTrusted(chain: Array[X509Certificate], authType: String, engine: SSLEngine): Unit = checkClientTrusted(chain, authType)
    def checkServerTrusted(chain: Array[X509Certificate], authType: String, socket: java.net.Socket): Unit = checkServerTrusted(chain, authType)
    def checkServerTrusted(chain: Array[X509Certificate], authType: String, engine: SSLEngine): Unit = checkServerTrusted(chain, authType)
    def checkServerTrusted(chain: Array[X509Certificate], authType: String): Unit =
      if chain.length < 2 then throw CertificateException("the join endpoint must present its CA certificate")
      val ca = chain.last
      if Pki.sha256Hex(ca) != caSha256 then throw CertificateException("the join endpoint's CA does not match the join token")
      try
        chain.head.checkValidity()
        chain.head.verify(ca.getPublicKey)
      catch case e: Exception => throw CertificateException("the join endpoint's certificate is not signed by the cluster CA", e)

  /** 参加の受け付け（HTTPS、サーバー認証のみ）。CA を持つノードが開く。 */
  def joinServer(listen: NodeAddress, m: Material, caDir: Path, days: Int = 3 * 365): Resource[IO, InetSocketAddress] =
    Resource.make(IO.blocking {
      val (caKey, caCert, secret) = readCa(caDir)
      // クライアント証明書は任意（参加はトークンの秘密で、更新はクラスタの CA の証明書で認める）
      val trust = KeyStore.getInstance("PKCS12")
      trust.load(null, null)
      trust.setCertificateEntry("ca", caCert)
      val tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
      tmf.init(trust)
      val ssl = SSLContext.getInstance("TLSv1.3")
      ssl.init(keyManagers(m), tmf.getTrustManagers, null)
      val server = HttpsServer.create(InetSocketAddress(listen.host, listen.port), 16)
      server.setHttpsConfigurator(new HttpsConfigurator(ssl):
        override def configure(p: HttpsParameters): Unit =
          val params = ssl.getDefaultSSLParameters
          params.setWantClientAuth(true)
          p.setSSLParameters(params)
      )
      server.createContext("/v1/join", (ex: HttpExchange) => handleJoin(ex, secret, caKey, caCert, days))
      server.createContext("/v1/renew", (ex: HttpExchange) => handleRenew(ex, caKey, caCert, days))
      val pool = Executors.newFixedThreadPool(2)
      server.setExecutor(pool)
      server.start()
      (server, pool)
    })((server, pool) => IO.blocking { server.stop(0); pool.shutdown() }).map(_._1.getAddress)

  private def reply(ex: HttpExchange, code: Int, body: Json): Unit =
    val bytes = body.noSpaces.getBytes(UTF_8)
    ex.getResponseHeaders.set("Content-Type", "application/json")
    ex.sendResponseHeaders(code, bytes.length.toLong)
    val out = ex.getResponseBody
    try out.write(bytes)
    finally out.close()

  private def errorJson(msg: String): Json = Json.obj("error" -> msg.asJson)

  private val NodeIdPattern = "[A-Za-z0-9._-]{1,64}"

  /** 受け付けの要求を処理する共通部分：POST だけを受け、本文（JSON）を handle に渡す。例外は 400 にする。 */
  private def serve(ex: HttpExchange)(handle: Option[io.circe.HCursor] => Unit): Unit =
    try
      if ex.getRequestMethod != "POST" then reply(ex, 405, errorJson("POST only"))
      else handle(parse(String(ex.getRequestBody.readNBytes(64 * 1024), UTF_8)).toOption.map(_.hcursor))
    catch case e: Exception => reply(ex, 400, errorJson(Option(e.getMessage).getOrElse(e.toString)))

  /** 署名要求に nodeId の名前で署名し、証明書と CA を返す。 */
  private def issue(ex: HttpExchange, csr: String, nodeId: String, caKey: java.security.PrivateKey, caCert: X509Certificate, days: Int, log: String): Unit =
    val cert = Pki.signCsr(Base64.getDecoder.decode(csr), nodeId, caKey, caCert, days)
    System.err.println(s"[scalas3] $log")
    reply(ex, 200, Json.obj("chain" -> Pki.certPem(cert).asJson, "ca" -> Pki.certPem(caCert).asJson))

  /** 証明書の更新。クライアント証明書（クラスタの CA で署名され、期限内のもの。TLS の層で確かめ済み）の名前で発行する。 */
  private def handleRenew(ex: HttpExchange, caKey: java.security.PrivateKey, caCert: X509Certificate, days: Int): Unit =
    serve(ex) { body =>
      val peer = ex match
        case h: HttpsExchange => scala.util.Try(h.getSSLSession.getPeerCertificates.toVector).toOption.flatMap(_.headOption).collect { case c: X509Certificate => c }
        case _                => None
      val nodeId = peer.flatMap(c =>
        javax.naming.ldap.LdapName(c.getSubjectX500Principal.getName).getRdns.asScala.find(_.getType.equalsIgnoreCase("CN")).map(_.getValue.toString)
      )
      nodeId match
        case None                                  => reply(ex, 403, errorJson("a node certificate issued by the cluster CA is required"))
        case Some(id) if !id.matches(NodeIdPattern) => reply(ex, 400, errorJson("invalid node id"))
        case Some(id) =>
          body.flatMap(_.get[String]("csr").toOption) match
            case None      => reply(ex, 400, errorJson("expected csr"))
            case Some(csr) => issue(ex, csr, id, caKey, caCert, days, s"renewed the node certificate for $id")
    }

  private def handleJoin(ex: HttpExchange, secret: String, caKey: java.security.PrivateKey, caCert: X509Certificate, days: Int): Unit =
    serve(ex) { body =>
      val fields = body.flatMap(c => (c.get[String]("secret").toOption, c.get[String]("nodeId").toOption, c.get[String]("csr").toOption).tupled)
      fields match
        case None                                                  => reply(ex, 400, errorJson("expected secret, nodeId and csr"))
        case Some((s, _, _)) if !Pki.secretEquals(s, secret)       => reply(ex, 403, errorJson("invalid join token"))
        case Some((_, nodeId, _)) if !nodeId.matches(NodeIdPattern) => reply(ex, 400, errorJson("invalid node id"))
        case Some((_, nodeId, csr))                                => issue(ex, csr, nodeId, caKey, caCert, days, s"issued a node certificate for $nodeId")
    }
