package scalas3.server

/** サーバーの起動口。cats-effect の設定は IO を最初に使う前に読まれるので、ここで既定値を決めてから Main を呼ぶ。
  *
  * ファイバーの追跡（エラー時に非同期のスタックトレースを出すための記録）を既定で切る。小さな PUT で CPU の
  * 最大の消費になっており、切ると 4 KiB の PUT が約 35% 速くなった（docs/benchmark.md）。調査のときは
  * `-Dcats.effect.tracing.mode=cached` で戻せる。どちらも -D で指定すればそちらが優先される。
  */
object Launcher:
  def main(args: Array[String]): Unit =
    setDefault("cats.effect.tracing.mode", "none")
    // ヒープ上のバッファをソケットに書くとき JDK が作る一時的なダイレクトバッファは、スレッドごとに使い回される。
    // 大きな本体を書くと、その大きさのものが多数のスレッドに残り、ヒープの外が数百 MB 膨らむ（Linux で測定）
    setDefault("jdk.nio.maxCachedBufferSize", "262144")
    Main.main(args)

  /** -D で指定されていなければ value にする。 */
  private def setDefault(name: String, value: String): Unit =
    if System.getProperty(name) == null then System.setProperty(name, value): Unit
