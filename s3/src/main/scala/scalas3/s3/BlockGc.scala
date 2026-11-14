package scalas3.s3

import cats.effect.IO
import cats.syntax.all.*
import scalas3.block.{BlockId, BlockStore, ShardStore}

import scala.collection.mutable
import scala.concurrent.duration.*

/** ブロックの GC（マーク＆スイープ）。
  *
  *   1. マーク：メタデータを全部読み、オブジェクトとパートが参照するブロックを集める。
  *   2. スイープ：「マーク開始時刻 − 猶予期間」より古く、参照も使用中でもないブロックを消す。
  *
  * 新しく書かれた・共有されたブロックは更新時刻が進むので、マークの途中で参照が移っても消されない。
  */
object BlockGc:
  final case class Report(referenced: Int, scanned: Int, deleted: Int)

  def run(store: ObjectStore, blocks: BlockStore, grace: FiniteDuration, shards: Option[ShardStore] = None): IO[Report] =
    for
      start <- IO.realTime
      referenced <- store.referencedBlocks.compile.fold(mutable.HashSet.empty[BlockId])((s, id) => { s += id; s })
      cutoff = start.toMillis - grace.toMillis
      results <- blocks.list.evalMap((id, disk) => blocks.deleteIfUnused(id, disk, cutoff, referenced.contains)).compile.toVector
      shardResults <- shards.fold(IO.pure(Vector.empty[Boolean]))(ss =>
        ss.list.evalMap((stripe, i) => ss.deleteIfUnused(stripe, i, cutoff, referenced.contains)).compile.toVector
      )
      // 消した分の領域を回収する（ゴミの多いボリュームを詰め直す）
      _ <- blocks.compact
      _ <- shards.traverse_(_.compact)
    yield Report(referenced.size, results.size + shardResults.size, results.count(identity) + shardResults.count(identity))
