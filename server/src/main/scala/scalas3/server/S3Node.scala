package scalas3.server

import cats.effect.{IO, Ref, Resource}
import cats.syntax.all.*
import com.comcast.ip4s.{Host, IpAddress, Port, SocketAddress}
import fs2.io.file.Path
import org.http4s.ember.server.EmberServerBuilder
import scalas3.block.{BlockStore, BlockStoreConfig, ShardStore}
import scalas3.cluster.*
import scalas3.meta.{MetaConfig, MetaNode}
import scalas3.lsm.LsmConfig
import scalas3.raft.NodeId
import scalas3.rpc.{RpcClient, RpcServer}
import scalas3.runtime.{FileSystem, NioFileSystem, NodeAddress, SocketOptions, TcpTransport, Transport}
import scalas3.s3.*

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.*

/** `host:port` を読む（IPv6 は最後の : で分ける）。 */
private[server] def parseHostPort(s: String): NodeAddress =
  val i = s.lastIndexOf(':')
  NodeAddress(s.take(i), s.drop(i + 1).toInt)

final case class NodeConfig(
    nodeId: NodeId = "node1",
    dataDir: Path = Path("data"),
    disks: Vector[Path] = Vector.empty, // 空なら dataDir/disk0
    s3Host: String = "0.0.0.0",
    s3Port: Int = 3900,
    /** 公開ポートの HTTP サーバー。Netty は 100-continue に自分で応える。Ember は応えないので中継を前に置く。 */
    httpServer: HttpServerKind = HttpServerKind.Netty,
    /** 公開ポートを HTTPS にするときの証明書と鍵（Netty のときだけ）。 */
    publicTls: Option[PublicTls] = None,
    rpcListen: NodeAddress = NodeAddress("127.0.0.1", 3901),
    /** 外から見た自分の RPC アドレス（他ノードがここへ接続する）。None なら rpcListen。 */
    rpcAdvertise: Option[NodeAddress] = None,
    /** 他のノード。初期メンバーなら全員分、後から参加するなら既存ノードのどれか。 */
    peers: Map[NodeId, NodeAddress] = Map.empty,
    /** クラスタを作る初期メンバーなら true。後から参加するノードは false。 */
    bootstrap: Boolean = true,
    zone: String = "default",
    weight: Int = 100,
    s3: S3Config = S3Config(),
    /** ブロックを何ノードに置くか。 */
    copies: Int = 3,
    /** メタデータのレンジを何ノードで複製するか。 */
    replicas: Int = 3,
    /** ノード内で何台のディスクに複製するか。None なら単一ノードは 2、複数ノードは 1。 */
    diskCopies: Option[Int] = None,
    adminKey: Option[(String, String)] = None,
    gcInterval: FiniteDuration = 1.hour,
    gcGrace: FiniteDuration = 1.hour,
    repairInterval: FiniteDuration = 10.minutes,
    scrubInterval: FiniteDuration = 7.days,
    layoutRefresh: FiniteDuration = 5.seconds,
    lifecycleInterval: FiniteDuration = 1.hour,
    /** ほかのノードの死活の見張り（Down は表示と警告だけで、データは動かさない）。 */
    health: HealthConfig = HealthConfig(),
    replicationInterval: FiniteDuration = 30.seconds,
    /** Admin API の待ち受け（token と両方あるときだけ起動する）。 */
    adminListen: Option[NodeAddress] = None,
    adminToken: Option[String] = None,
    meta: MetaConfig = MetaConfig(splitBoundary = S3Keys.splitBoundary),
    lsm: LsmConfig = LsmConfig.forHeap(),
    seed: Long = 0L
):
  def advertise: NodeAddress = rpcAdvertise.getOrElse(rpcListen)

/** HTTP を除いた 1 ノード分の中身（テストではシミュレーションのネットワークとディスクの上で動かす）。 */
final case class CoreNode(
    id: NodeId,
    meta: MetaNode,
    local: BlockStore,
    blocks: ClusterBlocks,
    localShards: ShardStore,
    shards: ClusterShards,
    store: ObjectStore,
    data: DataPath,
    layout: Ref[IO, Layout],
    addresses: AtomicReference[Map[NodeId, NodeAddress]],
    health: Health
):
  /** 修復を 1 回行う。 */
  def repair: IO[(Repairer.Report, ShardRepairer.Report)] =
    (Repairer.run(id, local, blocks, layout.get), ShardRepairer.run(id, localShards, shards, layout.get)).tupled

  /** ブロックの GC を 1 回行う。 */
  def gc(grace: FiniteDuration): IO[BlockGc.Report] = BlockGc.run(store, local, grace, Some(localShards))

  /** このノードのブロックとシャードを 1 回検証する。 */
  def scrub = (local.scrub, localShards.scrub).tupled

  /** システムレンジからレイアウトを読み直し、アドレス帳も更新する。 */
  def refreshLayout: IO[Unit] =
    Layout.get(meta.client).flatMap(_.traverse_ { l =>
      layout.set(l) >> IO(addresses.updateAndGet(_ ++ l.nodes.view.mapValues(_.address))).void
    })

object CoreNode:

  def resource(cfg: NodeConfig, fs: FileSystem, transport: Transport): Resource[IO, CoreNode] =
    val disks = if cfg.disks.isEmpty then Vector(cfg.dataDir / "disk0") else cfg.disks
    val self = NodeInfo(cfg.advertise, cfg.zone, cfg.weight, NodeState.Active)
    // 初期メンバーは全員が同じ設定を持つので、同じ初期レイアウトを書く（ゾーンと重みは後から変えられる）
    val initialLayout =
      if cfg.bootstrap then
        Layout(1, cfg.peers.map((id, a) => id -> NodeInfo(a, "default", 100, NodeState.Active)) + (cfg.nodeId -> self.copy(zone = "default", weight = 100)))
      else Layout(0, cfg.peers.map((id, a) => id -> NodeInfo(a, "default", 100, NodeState.Active)) + (cfg.nodeId -> self))
    val members = if cfg.bootstrap then initialLayout.nodes.keySet else cfg.peers.keySet
    val diskCopies = cfg.diskCopies.getOrElse(if initialLayout.nodes.size <= 1 then 2 else 1)
    val addresses = AtomicReference(initialLayout.nodes.view.mapValues(_.address).toMap)
    for
      layoutRef <- Resource.eval(Ref.of[IO, Layout](initialLayout))
      rpc <- RpcClient(transport)
      // 死活の見張りはメタデータより後に始まるので、参照を後から入れる
      healthRef = AtomicReference[Option[Health]](None)
      metaCfg = cfg.meta.copy(
        placement = (_, d) => layoutRef.get.map(l => Option.when(l.active.nonEmpty)(Placement.forRange(d.id, l, math.min(cfg.replicas, l.active.size)))),
        // 全ノードがレンジの統合に対応している（古い版が残っていない）ときだけ統合する
        mergeAllowed = layoutRef.get.map(l => healthRef.get.exists(_.supports(l.nodes.keySet, Health.MergeFeature)))
      )
      meta <- MetaNode.start(
        cfg.nodeId,
        fs,
        cfg.dataDir / "meta",
        rpc,
        n => addresses.get.get(n),
        members,
        cfg.bootstrap,
        layoutRef.get.map(_.nodes.keySet),
        metaCfg,
        lsmConfig = cfg.lsm,
        seed = cfg.seed,
        bootstrapExtra = if cfg.bootstrap then Seq(Layout.Key -> Layout.encode(initialLayout)) else Nil
      )
      local <- BlockStore(fs, disks, BlockStoreConfig(copies = diskCopies))
      health <- Health.start(cfg.nodeId, rpc, layoutRef.get, cfg.health)
      _ = healthRef.set(Some(health))
      blocks = ClusterBlocks(cfg.nodeId, local, rpc, layoutRef.get, cfg.copies, isDown = health.isDown)
      localShards <- ShardStore(fs, disks)
      shards = ClusterShards(cfg.nodeId, localShards, rpc, layoutRef.get, isDown = health.isDown)
      _ <- RpcServer.serve(transport, cfg.rpcListen, meta.rpcHandler ++ BlockRpc.handler(local) ++ ShardRpc.handler(localShards) ++ Health.handler)
      store = ObjectStore(meta.client, cfg.s3.masterKey, cfg.s3.metadataCacheTtl)
      // 長いアップロードの間は、GC の猶予期間の 1/3 ごとに、書いたブロックの更新時刻を進め直す
      data = DataPath(blocks, Some(shards), cfg.s3.blockSize, cfg.s3.inlineThreshold, keepAliveInterval = cfg.gcGrace / 3)
      node = CoreNode(cfg.nodeId, meta, local, blocks, localShards, shards, store, data, layoutRef, addresses, health)
      _ <- (IO.sleep(cfg.layoutRefresh) >> node.refreshLayout.attempt).foreverM.background
      // 自分をレイアウトに加える（初期メンバーなら、ゾーンや重みを設定どおりに直す）
      // 過半数がそろうまで書けないので、成功するまでバックグラウンドで繰り返す
      _ <- join(node, cfg, self).background
      _ <- periodic(cfg.repairInterval, "repair", node.repair)
      _ <- periodic(cfg.scrubInterval, "scrub", node.scrub)
      _ <- periodic(cfg.gcInterval, "gc", node.gc(cfg.gcGrace))
    yield node

  /** every ごとに job を行い、結果をログに出す（失敗しても続ける）。when が false の回（確かめられなかった回も）は飛ばす。 */
  private[server] def periodic(every: FiniteDuration, what: String, job: IO[Any], when: IO[Boolean] = IO.pure(true)): Resource[IO, Unit] =
    (IO.sleep(every) >> when.handleError(_ => false).ifM(
      job.attempt.flatMap {
        case Right(r) => IO.println(s"[scalas3] $what: $r")
        case Left(e)  => IO.println(s"[scalas3] $what failed: $e")
      },
      IO.unit
    )).foreverM.background.void

  private def join(node: CoreNode, cfg: NodeConfig, self: NodeInfo): IO[Unit] =
    val once = Layout.get(node.meta.client).flatMap { cur =>
      // 取り外し準備中（Draining）などの状態は管理者の設定を残す
      val wanted = cur.flatMap(_.nodes.get(cfg.nodeId)).fold(self)(n => self.copy(state = n.state))
      if cur.flatMap(_.nodes.get(cfg.nodeId)).contains(wanted) then IO.unit
      else Layout.update(node.meta.client)(l => Right(l.copy(nodes = l.nodes.updated(cfg.nodeId, wanted)))).void
    } >> node.refreshLayout
    once.handleErrorWith(_ => IO.sleep(500.millis) >> join(node, cfg, self))

final case class RunningNode(s3Address: NodeAddress, core: CoreNode, adminAddress: Option[NodeAddress], jobs: AdminApi.Jobs):
  def store: ObjectStore = core.store
  def blocks: BlockStore = core.local

/** 公開ポートの HTTP サーバーの種類。 */
enum HttpServerKind:
  case Netty, Ember

/** 1 ノードを起動する（S3 の HTTP を含む）。 */
object S3Node:

  def resource(cfg: NodeConfig, fs: FileSystem = NioFileSystem, transport: Transport = TcpTransport): Resource[IO, RunningNode] =
    for
      core <- CoreNode.resource(cfg, fs, transport)
      _ <- Resource.eval(waitReady(core.store, cfg))
      metrics = Metrics()
      dispatcher <- cats.effect.std.Dispatcher.sequential[IO]
      _ <- Resource.eval(IO(registerGauges(metrics, core, dispatcher)))
      remote <- RemoteS3.resource
      service = S3Service(core.store, core.data, cfg.s3)
      jobs: AdminApi.Jobs = Map(
        "repair" -> core.repair,
        "gc" -> core.gc(cfg.gcGrace),
        "scrub" -> core.scrub,
        "lifecycle" -> IO.realTime.flatMap(now => Lifecycle.run(core.store, now.toMillis)),
        "replication" -> Replicator.run(core.store, core.data, cfg.s3.masterKey, remote)
      )
      // ライフサイクルと複製は、システムレンジのリーダーのノードだけが行う（重複を避ける）
      _ <- leaderOnly(core, cfg.lifecycleInterval, "lifecycle", jobs("lifecycle"))
      _ <- leaderOnly(core, cfg.replicationInterval, "replication", jobs("replication"))
      host <- Resource.eval(IO.fromOption(Host.fromString(cfg.s3Host))(IllegalArgumentException(s"bad host ${cfg.s3Host}")))
      port <- Resource.eval(IO.fromOption(Port.fromInt(cfg.s3Port))(IllegalArgumentException(s"bad port ${cfg.s3Port}")))
      public <- publicServer(cfg, host, port, metrics.instrument(service.app))
      admin <- (cfg.adminListen, cfg.adminToken).tupled.traverse { (addr, token) =>
        EmberServerBuilder
          .default[IO]
          .withHost(Host.fromString(addr.host).get)
          .withPort(Port.fromInt(addr.port).get)
          .withHttpApp(AdminApi(core, token, metrics, jobs).routes.orNotFound)
          .withAdditionalSocketOptions(SocketOptions.default)
          .build
          .map(s => NodeAddress(s.address.getHostString, s.address.getPort))
      }
    yield RunningNode(NodeAddress(public.host.toString, public.port.value), core, admin, jobs)

  /** 公開ポートで S3 の HTTP を受ける。 */
  private def publicServer(cfg: NodeConfig, host: Host, port: Port, app: org.http4s.HttpApp[IO]): Resource[IO, SocketAddress[IpAddress]] =
    cfg.httpServer match
      case HttpServerKind.Netty =>
        NettyHttpServer
          .resource(NettyHttpServer.Config(cfg.s3Host, cfg.s3Port, tls = cfg.publicTls), app)
          .map(a => SocketAddress.fromInetSocketAddress(a).asIpUnsafe)
      case HttpServerKind.Ember if cfg.publicTls.isDefined =>
        Resource.raiseError[IO, SocketAddress[IpAddress], Throwable](
          IllegalArgumentException("HTTPS on the public port requires SCALAS3_HTTP_SERVER=netty")
        )
      case HttpServerKind.Ember =>
        val loopback = Host.fromString("127.0.0.1").get
        for
          server <- EmberServerBuilder
            .default[IO]
            .withHost(loopback)
            .withPort(Port.fromInt(0).get)
            .withHttpApp(app)
            .withAdditionalSocketOptions(SocketOptions.default)
            .withMaxHeaderSize(64 * 1024)
            .withIdleTimeout(5.minutes)
            .withShutdownTimeout(5.seconds)
            .build
          // Ember は Expect: 100-continue に応えないので、応える中継を前に置く（ExpectContinue を参照）
          public <- ExpectContinue.front(host, port, SocketAddress(loopback, Port.fromInt(server.address.getPort).get))
        yield public

  private def leaderOnly(core: CoreNode, every: FiniteDuration, what: String, job: IO[Any]): Resource[IO, Unit] =
    val isLeader = core.meta.ranges.replica(0).flatMap(_.traverse(_.node.status)).map(_.exists(_.role == scalas3.raft.Role.Leader))
    CoreNode.periodic(every, what, job, when = isLeader)

  private def registerGauges(m: Metrics, core: CoreNode, d: cats.effect.std.Dispatcher[IO]): Unit =
    m.gauge("scalas3_ranges") { () =>
      val states = d.unsafeRunSync(core.meta.ranges.all.flatMap(_.traverse(_.node.status)))
      states.groupBy(_.role.toString).toList.map((role, xs) => (Map("role" -> role), xs.size.toDouble))
    }
    m.gauge("scalas3_layout_nodes") { () =>
      val l = d.unsafeRunSync(core.layout.get)
      l.nodes.values.groupBy(_.state.toString).toList.map((st, xs) => (Map("state" -> st), xs.size.toDouble))
    }
    // このノードから見た、ほかのノードの死活（1 = 応答あり、0 = Down）。アラートに使う
    m.gauge("scalas3_node_up") { () =>
      core.health.snapshot.toList.sortBy(_._1).map((n, h) => (Map("node" -> n), if h.up then 1.0 else 0.0))
    }

  /** メタデータが書けるようになるまで待ち、管理者キーを用意する。 */
  private def waitReady(store: ObjectStore, cfg: NodeConfig): IO[Unit] =
    val adminKey = cfg.adminKey.map((ak, sk) => AccessKey(ak, sk, "admin", admin = true, Map.empty))
    val once = adminKey.fold(store.listBuckets.void)(store.ensureAccessKey)
    def loop(n: Int): IO[Unit] = once.handleErrorWith(e => if n <= 0 then IO.raiseError(e) else IO.sleep(200.millis) >> loop(n - 1))
    loop(150)
