package scalas3.sim

import cats.effect.IO
import cats.effect.testkit.TestControl

import java.util.Base64

/** 決定的シミュレーションの実行。
  *
  * TestControl は、同じ時刻に実行される処理の順序を乱数で入れ替える（いろいろな実行順を試すため）。
  * 種を渡さないと実行のたびに違う種になり、結果が再現できない。ここでは種を必ず指定する。
  *   - 既定の種は固定（毎回同じ実行順）
  *   - 環境変数 SCALAS3_SIM_SEED（またはシステムプロパティ scalas3.sim.seed）で別の種を試せる（CI で種を変えながら回すとよい）
  *   - 失敗したら種を表示するので、同じ種で再現できる
  */
object Sim:
  val DefaultSeed: Long = 20261107L

  def seed: Long =
    sys.props.get("scalas3.sim.seed").orElse(sys.env.get("SCALAS3_SIM_SEED")).flatMap(_.toLongOption).getOrElse(DefaultSeed)

  def run[A](program: IO[A], seed: Long = Sim.seed): IO[A] =
    val encoded = Base64.getEncoder.encodeToString(seed.toString.getBytes("UTF-8"))
    TestControl.executeEmbed(program, seed = Some(encoded)).onError(e =>
      IO.println(s"[sim] failed with SCALAS3_SIM_SEED=$seed: $e")
    )
