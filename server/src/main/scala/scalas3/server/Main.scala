package scalas3.server

import cats.effect.{ExitCode, IO, IOApp}
import cats.syntax.all.*
import fs2.io.file.Path
import scalas3.s3.{ClientIpHeader, Redundancy, S3Config, TrustedProxies}

/** 起動。設定は環境変数か、設定ファイル（HOCON、`--config <パス>` か SCALAS3_CONFIG。docs/scalas3.conf.example）で渡す。
  * 両方にあれば環境変数が優先する。設定ファイルの項目名は ConfigFile.keys を参照。
  *
  * | 変数 | 既定値 | 内容 |
  * |---|---|---|
  * | SCALAS3_NODE_ID | node1 | ノード ID |
  * | SCALAS3_DATA | ./data | メタデータなどの置き場所 |
  * | SCALAS3_DISKS | $SCALAS3_DATA/disk0 | ブロックを置くディスク（カンマ区切り） |
  * | SCALAS3_PEERS | (なし) | 他のノード `id=host:port,...`（RPC のアドレス） |
  * | SCALAS3_BOOTSTRAP | true | クラスタを作る初期メンバーなら true、後から参加するなら false |
  * | SCALAS3_ZONE | default | ゾーン（ラックや拠点）。コピーはなるべく別のゾーンに置く |
  * | SCALAS3_COPIES | 3 | ブロックを何ノードに置くか |
  * | SCALAS3_REPLICAS | 3 | メタデータを何ノードで複製するか |
  * | SCALAS3_DISK_COPIES | 単一ノードは 2、複数ノードは 1 | ノード内で何台のディスクに複製するか |
  * | SCALAS3_STORAGE_CLASSES | STANDARD=replica | ストレージクラス（例: `STANDARD=replica,EC42=ec:4:2`）。x-amz-storage-class で選ぶ |
  * | SCALAS3_MASTER_KEY | (なし) | マスター鍵（32 バイトを base64 で）。SSE-S3 と秘密鍵の暗号化に使う |
  * | SCALAS3_TLS_KEYSTORE / SCALAS3_TLS_PASSWORD | (なし) | ノード間通信を相互認証つき TLS にする（PKCS12 の鍵ストア） |
  * | SCALAS3_TLS_TRUSTSTORE | 鍵ストア自身 | 信頼する CA の証明書（PKCS12） |
  * | SCALAS3_TLS_CA_DIR | (なし) | 内部 CA のディレクトリ（`scalas3 cert init-ca` で作る）。証明書を自動で発行し、参加の受け付けを開く（docs/tls.md） |
  * | SCALAS3_JOIN_TOKEN / SCALAS3_JOIN_ADDRESS | (なし) | 後から加わるノード：参加トークンと、参加の受け付けのアドレス。証明書を受け取る |
  * | SCALAS3_JOIN_LISTEN | 0.0.0.0:3902 | 参加の受け付けを開くアドレス（CA を持つノード） |
  * | SCALAS3_BLOCK_SIZE / SCALAS3_INLINE_THRESHOLD | 4194304 / 4096 | ブロックの大きさと、メタデータに埋め込む大きさの上限（バイト。新しく書くデータにだけ効く） |
  * | SCALAS3_GC_INTERVAL / SCALAS3_GC_GRACE | 1h / 1h | ブロックの GC の間隔と猶予期間（猶予期間より新しいブロックは消さない） |
  * | SCALAS3_REPAIR_INTERVAL / SCALAS3_SCRUB_INTERVAL | 10m / 7d | 修復とスクラブ（全ブロックの検証）の間隔 |
  * | SCALAS3_ADMIN_LISTEN / SCALAS3_ADMIN_TOKEN | (なし) | Admin API の待ち受けと、ベアラートークン（両方あるときだけ起動） |
  * | SCALAS3_RPC_ADVERTISE | $SCALAS3_RPC_LISTEN | 他ノードから見た自分の RPC アドレス |
  * | SCALAS3_S3_LISTEN | 0.0.0.0:3900 | S3 API の待ち受け |
  * | SCALAS3_HTTP_SERVER | netty | 公開ポートの HTTP サーバー（`netty` か `ember`） |
  * | SCALAS3_S3_TLS_CERT / SCALAS3_S3_TLS_KEY | (なし) | 公開ポートを HTTPS にする（PEM の証明書の鎖と秘密鍵。更新されたら読み直す。netty のときだけ） |
  * | SCALAS3_TRUSTED_PROXIES | (なし) | 前段のリバースプロキシや CDN のアドレス（CIDR、カンマ区切り）。ここから来た要求だけ、ヘッダーから送信元を読む（docs/reverse-proxy.md） |
  * | SCALAS3_TRUSTED_PROXY_HOPS | (なし) | 直前の相手を 1 段目として、何段目までのプロキシをアドレスによらず信頼するか（CDN の範囲が分からないとき） |
  * | SCALAS3_CLIENT_IP_HEADER | x-forwarded-for | 送信元を読むヘッダー（`x-forwarded-for`、`forwarded`、または `cf-connecting-ip` などの単一の値のヘッダー） |
  * | SCALAS3_RPC_LISTEN | 127.0.0.1:3901 | ノード間 RPC の待ち受け |
  * | SCALAS3_REGION | us-east-1 | リージョン名 |
  * | SCALAS3_DOMAIN | (なし) | 仮想ホスト形式のドメイン |
  * | SCALAS3_ACCESS_KEY / SCALAS3_SECRET_KEY | (なし) | 管理者キー（初回に作成） |
  */
object Main extends IOApp:

  /** 設定ファイルの値（環境変数名 → 値）。run の始めに読む。 */
  @volatile private var fileSettings: Map[String, String] = Map.empty

  /** 設定の値。環境変数が優先で、無ければ設定ファイル。 */
  private def env(name: String): Option[String] =
    sys.env.get(name).orElse(fileSettings.get(name)).map(_.trim).filter(_.nonEmpty)

  /** 時間の指定（例：90s、10m、1h、7d）。 */
  private def duration(name: String, s: String): scala.concurrent.duration.FiniteDuration =
    scala.util.Try(scala.concurrent.duration.Duration(s.replaceAll("([0-9])([a-z])", "$1 $2"))).toOption.collect {
      case d: scala.concurrent.duration.FiniteDuration if d > scala.concurrent.duration.Duration.Zero => d
    }.getOrElse(throw IllegalArgumentException(s"$name must be a positive duration such as 90s, 10m or 1h: $s"))

  /** `STANDARD=replica,EC42=ec:4:2` の形式。 */
  private def parseClasses(s: String): Map[String, Redundancy] =
    s.split(',').toList.map(_.trim).filter(_.nonEmpty).map { entry =>
      entry.split("=", 2) match
        case Array(name, "replica") => name.trim -> Redundancy.Replicated
        case Array(name, spec) if spec.startsWith("ec:") =>
          spec.split(':') match
            case Array(_, k, m) => name.trim -> Redundancy.Erasure(k.toInt, m.toInt)
            case _              => throw IllegalArgumentException(s"bad storage class: $entry")
        case _ => throw IllegalArgumentException(s"bad storage class: $entry")
    }.toMap

  /** 前段のリバースプロキシや CDN の設定（docs/reverse-proxy.md）。 */
  private def publicTls: Option[PublicTls] =
    (env("SCALAS3_S3_TLS_CERT"), env("SCALAS3_S3_TLS_KEY")) match
      case (Some(c), Some(k)) => Some(PublicTls(java.nio.file.Path.of(c), java.nio.file.Path.of(k)))
      case (None, None)       => None
      case _                  => throw IllegalArgumentException("SCALAS3_S3_TLS_CERT and SCALAS3_S3_TLS_KEY must be set together")

  private def trustedProxies: Option[TrustedProxies] =
    val hops = env("SCALAS3_TRUSTED_PROXY_HOPS").map(h => h.toIntOption.filter(_ >= 1).getOrElse(throw IllegalArgumentException(s"SCALAS3_TRUSTED_PROXY_HOPS must be >= 1: $h")))
    val header = env("SCALAS3_CLIENT_IP_HEADER").map(TrustedProxies.parseHeader)
    env("SCALAS3_TRUSTED_PROXIES") match
      case None =>
        if hops.isDefined || header.isDefined then
          throw IllegalArgumentException("SCALAS3_TRUSTED_PROXY_HOPS and SCALAS3_CLIENT_IP_HEADER need SCALAS3_TRUSTED_PROXIES")
        None
      case Some(s) =>
        val cidrs = TrustedProxies.parseCidrs(s).fold(e => throw IllegalArgumentException(s"SCALAS3_TRUSTED_PROXIES: $e"), identity)
        require(cidrs.nonEmpty, "SCALAS3_TRUSTED_PROXIES is empty")
        Some(TrustedProxies(cidrs, hops, header.getOrElse(ClientIpHeader.XForwardedFor)))

  def run(args: List[String]): IO[ExitCode] =
    // 設定ファイル：--config <パス> か SCALAS3_CONFIG
    val configPath = args.sliding(2).collectFirst { case List("--config", p) => p }.orElse(sys.env.get("SCALAS3_CONFIG").filter(_.nonEmpty))
    configPath.foreach { p =>
      fileSettings = ConfigFile.loadFile(java.nio.file.Paths.get(p)).fold(e => throw IllegalArgumentException(e), identity)
    }
    val data = Path(env("SCALAS3_DATA").getOrElse("data"))
    val s3Listen = parseHostPort(env("SCALAS3_S3_LISTEN").getOrElse("0.0.0.0:3900"))
    val cfg = NodeConfig(
      nodeId = env("SCALAS3_NODE_ID").getOrElse("node1"),
      dataDir = data,
      disks = env("SCALAS3_DISKS").map(_.split(',').toVector.map(s => Path(s.trim))).getOrElse(Vector.empty),
      s3Host = s3Listen.host,
      s3Port = s3Listen.port,
      httpServer = env("SCALAS3_HTTP_SERVER").fold(HttpServerKind.Netty) {
        case "netty" => HttpServerKind.Netty
        case "ember" => HttpServerKind.Ember
        case other   => throw IllegalArgumentException(s"SCALAS3_HTTP_SERVER must be netty or ember: $other")
      },
      publicTls = publicTls,
      rpcListen = parseHostPort(env("SCALAS3_RPC_LISTEN").getOrElse("127.0.0.1:3901")),
      s3 = S3Config(
        region = env("SCALAS3_REGION").getOrElse("us-east-1"),
        // 新しく書くデータにだけ効く（ブロックは内容のハッシュで識別し、参照が大きさを持つので、混ざってもよい）
        blockSize = env("SCALAS3_BLOCK_SIZE").flatMap(_.toIntOption).getOrElse(S3Config().blockSize),
        inlineThreshold = env("SCALAS3_INLINE_THRESHOLD").flatMap(_.toIntOption).getOrElse(S3Config().inlineThreshold),
        domain = env("SCALAS3_DOMAIN"),
        storageClasses = env("SCALAS3_STORAGE_CLASSES").fold(Map("STANDARD" -> Redundancy.Replicated))(parseClasses),
        masterKey = env("SCALAS3_MASTER_KEY").map { k =>
          val bytes = java.util.Base64.getDecoder.decode(k)
          require(bytes.length == 32, "SCALAS3_MASTER_KEY must be 32 bytes encoded in base64")
          bytes
        },
        trustedProxies = trustedProxies
      ),
      rpcAdvertise = env("SCALAS3_RPC_ADVERTISE").map(parseHostPort),
      peers = env("SCALAS3_PEERS").toList.flatMap(_.split(',').toList).map(_.trim).filter(_.nonEmpty).map { p =>
        val Array(id, addr) = p.split("=", 2): @unchecked
        id.trim -> parseHostPort(addr.trim)
      }.toMap,
      bootstrap = env("SCALAS3_BOOTSTRAP").forall(_.toBooleanOption.getOrElse(true)),
      zone = env("SCALAS3_ZONE").getOrElse("default"),
      copies = env("SCALAS3_COPIES").flatMap(_.toIntOption).getOrElse(3),
      replicas = env("SCALAS3_REPLICAS").flatMap(_.toIntOption).getOrElse(3),
      diskCopies = env("SCALAS3_DISK_COPIES").flatMap(_.toIntOption),
      adminListen = env("SCALAS3_ADMIN_LISTEN").map(parseHostPort),
      gcInterval = env("SCALAS3_GC_INTERVAL").fold(NodeConfig().gcInterval)(duration("SCALAS3_GC_INTERVAL", _)),
      gcGrace = env("SCALAS3_GC_GRACE").fold(NodeConfig().gcGrace)(duration("SCALAS3_GC_GRACE", _)),
      repairInterval = env("SCALAS3_REPAIR_INTERVAL").fold(NodeConfig().repairInterval)(duration("SCALAS3_REPAIR_INTERVAL", _)),
      scrubInterval = env("SCALAS3_SCRUB_INTERVAL").fold(NodeConfig().scrubInterval)(duration("SCALAS3_SCRUB_INTERVAL", _)),
      adminToken = env("SCALAS3_ADMIN_TOKEN"),
      adminKey = for
        ak <- env("SCALAS3_ACCESS_KEY")
        sk <- env("SCALAS3_SECRET_KEY")
      yield (ak, sk)
    )
    // ノード間 TLS：鍵ストアの指定があればそれを、なければ自動発行（CA のディレクトリか参加トークン）を使う
    val auto = AutoTls.Settings(
      caDir = env("SCALAS3_TLS_CA_DIR").map(java.nio.file.Paths.get(_)),
      joinToken = env("SCALAS3_JOIN_TOKEN").map(t => scalas3.pki.JoinToken.parse(t).fold(e => throw IllegalArgumentException(e), identity)),
      joinAddress = env("SCALAS3_JOIN_ADDRESS").map(parseHostPort),
      joinListen = parseHostPort(env("SCALAS3_JOIN_LISTEN").getOrElse("0.0.0.0:3902")),
      days = env("SCALAS3_TLS_CERT_DAYS").flatMap(_.toIntOption).getOrElse(3 * 365)
    )
    val transport: cats.effect.Resource[IO, scalas3.runtime.Transport] =
      env("SCALAS3_TLS_KEYSTORE") match
        case Some(ks) =>
          // 鍵ストアを差し替えれば、新しい接続から新しい証明書を使う
          scalas3.runtime.TlsTransport.reloading(
            java.nio.file.Paths.get(ks),
            IO.pure(env("SCALAS3_TLS_PASSWORD").getOrElse("")),
            env("SCALAS3_TLS_TRUSTSTORE").map(java.nio.file.Paths.get(_))
          )
        case None =>
          cats.effect.Resource.eval(AutoTls.ensure(data.toNioPath, cfg.nodeId, auto)).flatMap {
            case None => cats.effect.Resource.pure(scalas3.runtime.TcpTransport)
            case Some(m) =>
              // CA を持つノードは、後から加わるノードのために参加の受け付けを開く
              val joinServer = auto.caDir.fold(cats.effect.Resource.unit[IO])(ca =>
                AutoTls.joinServer(auto.joinListen, m, ca).evalMap(a => IO.println(s"[scalas3] join endpoint on $a")).void
              )
              // 期限が近づいたら証明書を更新し、書き換えた鍵ストアを読み直す
              joinServer >> AutoTls.renewing(data.toNioPath, cfg.nodeId, auto, m) >>
                scalas3.runtime.TlsTransport.reloading(m.keystore, IO.pure(m.password), Some(m.truststore))
          }
    transport.flatMap(t => S3Node.resource(cfg, transport = t)).use { node =>
      IO.println(s"[scalas3] node ${cfg.nodeId} serving S3 on ${node.s3Address}") >>
        node.adminAddress.traverse_(a => IO.println(s"[scalas3] admin API on $a")) >>
        IO.whenA(cfg.adminKey.isEmpty)(IO.println("[scalas3] warning: SCALAS3_ACCESS_KEY / SCALAS3_SECRET_KEY are not set; no admin key was created")) >>
        IO.never
    }.as(ExitCode.Success)
