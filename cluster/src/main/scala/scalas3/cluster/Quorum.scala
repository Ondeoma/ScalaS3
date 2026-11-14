package scalas3.cluster

import cats.effect.{Deferred, IO, Ref}
import cats.syntax.all.*

/** 並行に書いて、過半数に届いた（または届かないと決まった）時点で返す。 */
private[cluster] object Quorum:

  /** 成功した数を返す。need に届くか、届かないと決まったら返し、残りの書き込みは裏で続ける（タイムアウトで必ず終わる）。
    * 全員の返事を待つと、応答のないノード（落ちたが切断が届かない）が 1 台あるだけで、過半数に届いていても止まった。
    */
  def write(writes: List[IO[Either[Throwable, Unit]]], need: Int): IO[Int] =
    if writes.isEmpty then IO.pure(0)
    else
      val total = writes.size
      for
        decided <- Deferred[IO, Int]
        counts <- Ref.of[IO, (Int, Int)]((0, 0))
        _ <- writes.traverse_ { w =>
          w.attempt.map(_.flatten).flatMap { r =>
            counts.modify { (ok, failed) =>
              val next = if r.isRight then (ok + 1, failed) else (ok, failed + 1)
              (next, next)
            }.flatMap { (ok, failed) =>
              decided.complete(ok).void.whenA(ok >= need || failed > total - need || ok + failed == total)
            }
          }.start
        }
        ok <- decided.get
      yield ok
