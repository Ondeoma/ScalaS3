package scalas3.cli

import cats.effect.{ExitCode, IO}
import cats.syntax.all.*
import com.monovore.decline.*
import com.monovore.decline.effect.*
import io.circe.Json
import io.circe.syntax.*
import org.http4s.*
import org.http4s.circe.*
import org.http4s.client.Client
import org.http4s.ember.client.EmberClientBuilder
import org.http4s.headers.Authorization
import scalas3.pki.{JoinToken, Pki}

import java.nio.file.{Files, Paths}

/** scalas3 の管理 CLI。Admin API（SCALAS3_ADMIN_ENDPOINT, SCALAS3_ADMIN_TOKEN）を呼ぶ。 */
object Main
    extends CommandIOApp(
      name = "scalas3",
      header = "scalas3 の管理 CLI（Admin API のクライアント）",
      version = "0.1.0"
    ):

  private final case class Conn(endpoint: Uri, token: String)

  private val conn: Opts[Conn] =
    (
      Opts.option[String]("endpoint", "Admin API の URL").orElse(Opts.env[String]("SCALAS3_ADMIN_ENDPOINT", "Admin API の URL")).withDefault("http://127.0.0.1:3903"),
      Opts.option[String]("token", "Admin API のトークン").orElse(Opts.env[String]("SCALAS3_ADMIN_TOKEN", "Admin API のトークン"))
    ).mapN((e, t) => Conn(Uri.unsafeFromString(e.stripSuffix("/")), t))

  /** Admin API の呼び出し。 */
  private final case class Call(method: Method, path: String, body: Option[Json])

  private def call(m: Method, path: String, body: Option[Json] = None) = Call(m, path, body)

  private val layout = Opts.subcommand("layout", "クラスタのレイアウト")(
    Opts.subcommand("show", "レイアウトを表示する")(Opts(call(Method.GET, "/v1/layout"))) orElse
      Opts.subcommand("set", "ノードの設定を変える（ゾーン、重み、状態、アドレス）")(
        (
          Opts.argument[String]("node"),
          Opts.option[String]("zone", "ゾーン").orNone,
          Opts.option[Int]("weight", "重み").orNone,
          Opts.option[String]("state", "Active または Draining").orNone,
          Opts.option[String]("address", "RPC のアドレス host:port").orNone
        ).mapN { (n, z, w, s, a) =>
          call(Method.PUT, s"/v1/layout/nodes/$n", Some(Json.obj("zone" -> z.asJson, "weight" -> w.asJson, "state" -> s.asJson, "address" -> a.asJson).dropNullValues))
        }
      ) orElse
      Opts.subcommand("remove", "ノードを外す（先に Draining にしてデータを移しておくこと）")(
        Opts.argument[String]("node").map(n => call(Method.DELETE, s"/v1/layout/nodes/$n"))
      )
  )

  private val key = Opts.subcommand("key", "アクセスキー")(
    Opts.subcommand("list", "一覧")(Opts(call(Method.GET, "/v1/keys"))) orElse
      Opts.subcommand("create", "作る（秘密鍵はこのときだけ表示される）")(
        (Opts.argument[String]("name"), Opts.flag("admin", "管理者キーにする").orFalse).mapN((n, a) =>
          call(Method.POST, "/v1/keys", Some(Json.obj("name" -> n.asJson, "admin" -> a.asJson)))
        )
      ) orElse
      Opts.subcommand("delete", "消す")(Opts.argument[String]("id").map(i => call(Method.DELETE, s"/v1/keys/$i"))) orElse
      Opts.subcommand("grant", "バケットの権限を与える（どれも付けなければ外す）")(
        (
          Opts.argument[String]("id"),
          Opts.argument[String]("bucket"),
          Opts.flag("read", "読み取り").orFalse,
          Opts.flag("write", "書き込み").orFalse,
          Opts.flag("owner", "所有者").orFalse
        ).mapN((i, b, r, w, o) =>
          call(Method.PUT, s"/v1/keys/$i/grants", Some(Json.obj("bucket" -> b.asJson, "read" -> r.asJson, "write" -> w.asJson, "owner" -> o.asJson)))
        )
      )
  )

  private val remote = Opts.subcommand("remote", "クラスタ間複製の転送先")(
    Opts.subcommand("list", "一覧")(Opts(call(Method.GET, "/v1/remotes"))) orElse
      Opts.subcommand("set", "登録する")(
        (
          Opts.argument[String]("name"),
          Opts.option[String]("endpoint", "S3 互換エンドポイントの URL"),
          Opts.option[String]("access-key", "アクセスキー"),
          Opts.option[String]("secret-key", "秘密鍵"),
          Opts.option[String]("region", "リージョン").withDefault("us-east-1")
        ).mapN((n, e, a, s, r) =>
          call(Method.PUT, s"/v1/remotes/$n", Some(Json.obj("endpoint" -> e.asJson, "accessKey" -> a.asJson, "secretKey" -> s.asJson, "region" -> r.asJson)))
        )
      ) orElse
      Opts.subcommand("delete", "消す")(Opts.argument[String]("name").map(n => call(Method.DELETE, s"/v1/remotes/$n")))
  )

  private val job = Opts.subcommand("job", "管理ジョブをすぐに実行する")(
    Opts.argument[String]("repair|gc|scrub|lifecycle|replication").map(j => call(Method.POST, s"/v1/jobs/$j"))
  )

  private val cert: Opts[IO[ExitCode]] = Opts.subcommand("cert", "ノード間 TLS の証明書")(
    Opts.subcommand("init-ca", "内部 CA と参加トークンを作る。初期メンバーに SCALAS3_TLS_CA_DIR を、後から加わるノードに参加トークンを渡す（docs/tls.md）")(
      (
        Opts.option[String]("dir", "出力するディレクトリ（ca.crt、ca.key、join.secret）").withDefault("scalas3-ca"),
        Opts.option[String]("cn", "CA の名前").withDefault("scalas3 internal CA"),
        Opts.option[Int]("days", "有効日数").withDefault(3650)
      ).mapN(certInitCa)
    ) orElse
      Opts.subcommand("create", "自己署名の鍵ストア（PKCS12）を作る（JDK の keytool を使う）。全ノードで同じものを使えば、相互に信頼できる")(
        (
          Opts.option[String]("out", "出力するファイル").withDefault("cluster.p12"),
          Opts.option[String]("password", "パスワード"),
          Opts.option[String]("cn", "証明書の CN").withDefault("scalas3-cluster"),
          Opts.option[Int]("days", "有効日数").withDefault(3650)
        ).mapN(certCreate)
      )
  )

  private val commands: Opts[Call] =
    Opts.subcommand("status", "ノードとレンジの状態")(Opts(call(Method.GET, "/v1/status"))) orElse layout orElse key orElse
      Opts.subcommand("bucket", "バケット")(Opts.subcommand("list", "一覧")(Opts(call(Method.GET, "/v1/buckets")))) orElse
      remote orElse job

  /** cert は Admin API に接続しない。それ以外は接続先とトークンが要る。 */
  def main: Opts[IO[ExitCode]] =
    cert orElse (conn, commands).mapN((cn, c) => EmberClientBuilder.default[IO].build.use(run(_, cn, c)))

  private def run(client: Client[IO], cn: Conn, c: Call): IO[ExitCode] =
    val req = Request[IO](c.method, cn.endpoint.withPath(Uri.Path.unsafeFromString(c.path)))
      .putHeaders(Authorization(Credentials.Token(AuthScheme.Bearer, cn.token)))
    client.run(c.body.fold(req)(req.withEntity(_))).use { resp =>
      resp.bodyText.compile.string.flatMap { text =>
        val pretty = io.circe.parser.parse(text).fold(_ => text, _.spaces2)
        if resp.status.isSuccess then IO.println(if pretty.isEmpty then "OK" else pretty).as(ExitCode.Success)
        else IO.consoleForIO.errorln(s"${resp.status.code}: $pretty").as(ExitCode.Error)
      }
    }

  private def certInitCa(dirName: String, cn: String, days: Int): IO[ExitCode] = IO.blocking {
    val dir = Paths.get(dirName)
    if Files.exists(dir.resolve("ca.key")) then
      System.err.println(s"${dir.resolve("ca.key")} は既にあります（上書きしません）")
      ExitCode.Error
    else
      val (kp, cert) = Pki.createCa(cn, days)
      val secret = Pki.newSecret()
      Files.createDirectories(dir)
      Files.writeString(dir.resolve("ca.crt"), Pki.certPem(cert))
      Pki.writePrivate(dir.resolve("ca.key"), Pki.keyPem(kp.getPrivate))
      Pki.writePrivate(dir.resolve("join.secret"), secret + "\n")
      println(s"内部 CA を $dir に作りました。")
      println(s"  初期メンバー：このディレクトリを置き、SCALAS3_TLS_CA_DIR=$dir を設定する")
      println(s"  後から加わるノード：SCALAS3_JOIN_TOKEN と SCALAS3_JOIN_ADDRESS（CA を持つノードの参加の受け付け、既定のポートは 3902）を設定する")
      println(s"参加トークン：${JoinToken(secret, Pki.sha256Hex(cert)).render}")
      ExitCode.Success
  }

  private def certCreate(out: String, password: String, cn: String, days: Int): IO[ExitCode] = IO.blocking {
    // ネイティブのバイナリ（native-image）には java.home が無いので、JAVA_HOME か PATH の keytool を使う
    val keytool = Option(System.getProperty("java.home")).orElse(sys.env.get("JAVA_HOME")).filter(_.nonEmpty)
      .map(h => Paths.get(h, "bin", "keytool"))
      .filter(Files.isExecutable(_))
      .fold("keytool")(_.toString)
    val proc = new ProcessBuilder(
      keytool, "-genkeypair", "-alias", "node", "-keyalg", "EC", "-groupname", "secp256r1", "-validity", days.toString,
      "-dname", s"CN=$cn", "-keystore", out, "-storetype", "PKCS12", "-storepass", password, "-keypass", password
    ).inheritIO().start()
    if proc.waitFor() == 0 then
      println(s"$out を作りました。全ノードに配り、SCALAS3_TLS_KEYSTORE と SCALAS3_TLS_PASSWORD を設定してください。")
      ExitCode.Success
    else ExitCode.Error
  }
