ThisBuild / scalaVersion := "3.9.0"
ThisBuild / organization := "dev.scalas3"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / licenses := Seq("MIT" -> url("https://opensource.org/license/mit"))

ThisBuild / scalacOptions ++= Seq(
  "-deprecation",
  "-feature",
  "-unchecked",
  "-Wunused:all",
  "-Wvalue-discard",
  "-source:3.7"
)

ThisBuild / Test / fork := true
ThisBuild / Test / parallelExecution := true

lazy val V = new {
  val catsEffect = "3.7.1"
  val fs2 = "3.14.0"
  val scodecBits = "1.2.5"
  val scodec = "2.3.3"
  val munit = "1.3.1"
  val munitCE = "2.2.1"
  val http4s = "0.23.38"
  val circe = "0.14.17"
  val netty = "4.2.18.Final"
  val bouncyCastle = "1.86"
}

lazy val testDeps = Seq(
  "org.scalameta" %% "munit-scalacheck" % V.munit % Test,
  "org.typelevel" %% "munit-cats-effect" % V.munitCE % Test,
  "org.typelevel" %% "cats-effect-testkit" % V.catsEffect % Test
)

/** fat jar の重複ファイルの扱い（server と cli で共通）。 */
def commonMergeStrategy(fallback: String => sbtassembly.MergeStrategy): String => sbtassembly.MergeStrategy = {
  case PathList("META-INF", "versions", _, "module-info.class") => MergeStrategy.discard
  // Bouncy Castle の各 jar にある OSGi の情報（実行には使わない）
  case PathList("META-INF", "versions", _, "OSGI-INF", _*)      => MergeStrategy.discard
  case PathList("OSGI-INF", _*)                                 => MergeStrategy.discard
  case PathList("module-info.class")                           => MergeStrategy.discard
  case PathList("META-INF", "io.netty.versions.properties")    => MergeStrategy.first
  case x                                                       => fallback(x)
}

lazy val root = (project in file("."))
  .aggregate(core, runtime, sim, rpc, lsm, raft, meta, block, cluster, s3, server, cli, pki)
  .settings(name := "scalas3", publish / skip := true)

/** 共通の型、バイト列ユーティリティ、チェックサム。 */
lazy val core = project
  .settings(
    name := "scalas3-core",
    libraryDependencies ++= Seq(
      "org.typelevel" %% "cats-core" % "2.13.0",
      "org.scodec" %% "scodec-bits" % V.scodecBits
    ) ++ testDeps
  )

/** 時計・乱数・ディスク・ネットワークの抽象化と本番実装。 */
lazy val runtime = project
  .dependsOn(core)
  .settings(
    name := "scalas3-runtime",
    libraryDependencies ++= Seq(
      "org.typelevel" %% "cats-effect" % V.catsEffect,
      "co.fs2" %% "fs2-core" % V.fs2,
      "co.fs2" %% "fs2-io" % V.fs2
    ) ++ testDeps
  )

/** 決定的シミュレーション用の実装（SimFileSystem, SimNetwork）。 */
lazy val sim = project
  .dependsOn(runtime)
  .settings(
    name := "scalas3-sim",
    libraryDependencies ++= Seq("org.typelevel" %% "cats-effect-testkit" % V.catsEffect) ++ testDeps
  )

/** ノード間 RPC（フレーミング、多重化）。 */
lazy val rpc = project
  .dependsOn(runtime, sim % "test->compile")
  .settings(
    name := "scalas3-rpc",
    libraryDependencies ++= Seq(
      "org.scodec" %% "scodec-core" % V.scodec
    ) ++ testDeps
  )

/** 自前の LSM-Tree。 */
lazy val lsm = project
  .dependsOn(runtime, sim % "test->compile")
  .settings(
    name := "scalas3-lsm",
    libraryDependencies ++= testDeps
  )

/** Raft（純粋なコア、永続化、ドライバ）。 */
lazy val raft = project
  .dependsOn(core, runtime, rpc, sim % "test->compile")
  .settings(
    name := "scalas3-raft",
    libraryDependencies ++= testDeps
  )

/** Multi-Raft によるメタデータ層（レンジ、分割、ルーティング）。 */
lazy val meta = project
  .dependsOn(lsm, raft, rpc, sim % "test->compile")
  .settings(
    name := "scalas3-meta",
    libraryDependencies ++= testDeps
  )

/** データブロックの保存（ディスク管理、複製、GC）。 */
lazy val block = project
  .dependsOn(runtime, lsm, sim % "test->compile")
  .settings(
    name := "scalas3-block",
    libraryDependencies ++= testDeps
  )

/** クラスタ（レイアウト、配置、ノード間のブロック複製、修復）。 */
lazy val cluster = project
  .dependsOn(meta, block, rpc, sim % "test->compile")
  .settings(
    name := "scalas3-cluster",
    libraryDependencies ++= testDeps
  )

/** S3 API（SigV4、XML、オブジェクトのメタデータ、HTTP ルーティング）。 */
lazy val s3 = project
  .dependsOn(meta, block, cluster, sim % "test->compile")
  .settings(
    name := "scalas3-s3",
    libraryDependencies ++= Seq(
      "org.http4s" %% "http4s-ember-server" % V.http4s,
      "org.http4s" %% "http4s-dsl" % V.http4s,
      "org.http4s" %% "http4s-ember-client" % V.http4s,
      "org.http4s" %% "http4s-circe" % V.http4s,
      "org.scala-lang.modules" %% "scala-xml" % "2.5.0",
      "io.circe" %% "circe-parser" % V.circe
    ) ++ testDeps
  )

/** 起動と組み立て。 */
lazy val server = project
  .dependsOn(s3, pki, sim % "test->compile")
  .settings(
    name := "scalas3-server",
    // 設定ファイル（HOCON、設計 D4）
    libraryDependencies += "com.typesafe" % "config" % "1.4.9",
    // 公開ポートの HTTP サーバー（NettyHttpServer。Netty の HTTP の部品の上に http4s を自前で繋ぐ）
    // Netty のネイティブ転送（Linux は epoll、macOS は kqueue）。無い環境では NIO にする
    libraryDependencies ++= Seq(
      "io.netty" % "netty-codec-http" % V.netty,
      "io.netty" % "netty-transport-classes-epoll" % V.netty,
      "io.netty" % "netty-transport-classes-kqueue" % V.netty,
      "io.netty" % "netty-transport-native-epoll" % V.netty classifier "linux-x86_64",
      "io.netty" % "netty-transport-native-epoll" % V.netty classifier "linux-aarch_64",
      "io.netty" % "netty-transport-native-kqueue" % V.netty classifier "osx-x86_64",
      "io.netty" % "netty-transport-native-kqueue" % V.netty classifier "osx-aarch_64"
    ),
    Compile / mainClass := Some("scalas3.server.Launcher"),
    run / fork := true,
    assembly / assemblyJarName := "scalas3-server.jar",
    assembly / assemblyMergeStrategy := commonMergeStrategy((assembly / assemblyMergeStrategy).value),
    libraryDependencies ++= Seq(
      "ch.qos.logback" % "logback-classic" % "1.5.18" % Runtime,
      "software.amazon.awssdk" % "s3" % "2.39.6" % Test
    ) ++ testDeps
  )

/** ノード間 TLS の証明書（内部 CA、署名要求、参加トークン）。CLI とサーバーで使う。 */
lazy val pki = project
  .settings(
    name := "scalas3-pki",
    libraryDependencies ++= Seq(
      "org.bouncycastle" % "bcpkix-jdk18on" % V.bouncyCastle,
      "org.bouncycastle" % "bcprov-jdk18on" % V.bouncyCastle
    ) ++ testDeps
  )

/** 管理 CLI（Admin API のクライアント）。native-image で配布する想定。 */
lazy val cli = project
  .dependsOn(pki)
  .settings(
    name := "scalas3-cli",
    Compile / mainClass := Some("scalas3.cli.Main"),
    assembly / assemblyJarName := "scalas3-cli.jar",
    assembly / assemblyMergeStrategy := commonMergeStrategy((assembly / assemblyMergeStrategy).value),
    libraryDependencies ++= Seq(
      "com.monovore" %% "decline-effect" % "2.6.2",
      "org.slf4j" % "slf4j-nop" % "1.7.36",
      "org.http4s" %% "http4s-ember-client" % V.http4s,
      "org.http4s" %% "http4s-circe" % V.http4s,
      "io.circe" %% "circe-parser" % V.circe
    ) ++ testDeps
  )
