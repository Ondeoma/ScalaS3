package scalas3.meta

import cats.effect.{IO, Resource}
import cats.syntax.all.*
import fs2.io.file.Path
import scalas3.lsm.{Lsm, LsmConfig}
import scalas3.raft.{NodeId, RaftRpc}
import scalas3.rpc.{RpcClient, RpcHandler}
import scalas3.runtime.{FileSystem, NodeAddress}

/** 1 ノード分のメタデータ層。RPC サーバーは呼び出し側が rpcHandler を使って立てる。 */
final class MetaNode private (val id: NodeId, val lsm: Lsm, val ranges: RangeManager, val client: MetaClient):
  def rpcHandler: RpcHandler =
    RaftRpc.handler(g =>
      ranges.replica(g).flatTap(r => ranges.onUnknownGroup(g).whenA(r.isEmpty)).map(_.map(_.node))
    ) ++ MetaService.handler(ranges)

object MetaNode:
  /** @param members  クラスタの初期メンバー（bootstrap のときのレンジ 0 と 1 のレプリカ）
    * @param knownNodes システムレンジを探すときに問い合わせるノード（レイアウトから更新される）
    * @param bootstrap 初期メンバーなら true（初回起動時にレンジ 0 と 1 を作る）
    */
  def start(
      id: NodeId,
      fs: FileSystem,
      dir: Path,
      rpc: RpcClient,
      addressOf: NodeId => Option[NodeAddress],
      members: Set[NodeId],
      bootstrap: Boolean,
      knownNodes: IO[Set[NodeId]],
      config: MetaConfig = MetaConfig(),
      lsmConfig: LsmConfig = LsmConfig(),
      seed: Long = 0L,
      bootstrapExtra: Seq[(scodec.bits.ByteVector, scodec.bits.ByteVector)] = Nil
  ): Resource[IO, MetaNode] =
    for
      lsm <- Lsm.open(fs, dir / "lsm", lsmConfig)
      _ <- Resource.eval(if bootstrap then RangeManager.bootstrap(lsm, members, bootstrapExtra) else IO.unit)
      rm <- RangeManager(id, fs, dir, lsm, RaftRpc.transport(rpc, addressOf), config, seed)
      _ <- Resource.eval(rm.recover)
      client <- Resource.eval(MetaClient(id, rpc, addressOf, rm, knownNodes.map(_ ++ members), config.requestTimeout))
      _ <- Resource.eval(rm.attach(client))
      _ <- (IO.sleep(config.maintenanceInterval) >> rm.maintain(client).attempt).foreverM.background
    yield new MetaNode(id, lsm, rm, client)
