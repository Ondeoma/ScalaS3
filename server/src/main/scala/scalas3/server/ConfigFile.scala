package scalas3.server

import com.typesafe.config.{Config, ConfigFactory, ConfigValueType}

import scala.jdk.CollectionConverters.*

/** 設定ファイル（HOCON、設計 D4）。docs/scalas3.conf.example を参照。
  *
  * 項目は環境変数と 1 対 1 に対応させ、読んだ値を環境変数と同じ形の文字列にして使う（解釈は環境変数と共通）。
  * 優先順は「環境変数 → 設定ファイル → 既定値」。知らない項目があれば、書き間違いとみなして起動を止める。
  */
object ConfigFile:

  /** 環境変数 → 設定ファイルの項目（`scalas3.` の下）。 */
  val keys: Vector[(String, String)] = Vector(
    "SCALAS3_NODE_ID" -> "node-id",
    "SCALAS3_DATA" -> "data",
    "SCALAS3_DISKS" -> "disks",
    "SCALAS3_PEERS" -> "peers",
    "SCALAS3_BOOTSTRAP" -> "bootstrap",
    "SCALAS3_ZONE" -> "zone",
    "SCALAS3_COPIES" -> "copies",
    "SCALAS3_REPLICAS" -> "replicas",
    "SCALAS3_DISK_COPIES" -> "disk-copies",
    "SCALAS3_MASTER_KEY" -> "master-key",
    "SCALAS3_RPC_LISTEN" -> "rpc.listen",
    "SCALAS3_RPC_ADVERTISE" -> "rpc.advertise",
    "SCALAS3_S3_LISTEN" -> "s3.listen",
    "SCALAS3_REGION" -> "s3.region",
    "SCALAS3_DOMAIN" -> "s3.domain",
    "SCALAS3_HTTP_SERVER" -> "s3.http-server",
    "SCALAS3_S3_TLS_CERT" -> "s3.tls.cert",
    "SCALAS3_S3_TLS_KEY" -> "s3.tls.key",
    "SCALAS3_BLOCK_SIZE" -> "s3.block-size",
    "SCALAS3_INLINE_THRESHOLD" -> "s3.inline-threshold",
    "SCALAS3_STORAGE_CLASSES" -> "s3.storage-classes",
    "SCALAS3_TRUSTED_PROXIES" -> "s3.trusted-proxies.addresses",
    "SCALAS3_TRUSTED_PROXY_HOPS" -> "s3.trusted-proxies.hops",
    "SCALAS3_CLIENT_IP_HEADER" -> "s3.trusted-proxies.client-ip-header",
    "SCALAS3_ACCESS_KEY" -> "admin.access-key",
    "SCALAS3_SECRET_KEY" -> "admin.secret-key",
    "SCALAS3_ADMIN_LISTEN" -> "admin.listen",
    "SCALAS3_ADMIN_TOKEN" -> "admin.token",
    "SCALAS3_TLS_KEYSTORE" -> "tls.keystore",
    "SCALAS3_TLS_PASSWORD" -> "tls.password",
    "SCALAS3_TLS_TRUSTSTORE" -> "tls.truststore",
    "SCALAS3_TLS_CA_DIR" -> "tls.ca-dir",
    "SCALAS3_JOIN_TOKEN" -> "tls.join-token",
    "SCALAS3_JOIN_ADDRESS" -> "tls.join-address",
    "SCALAS3_JOIN_LISTEN" -> "tls.join-listen",
    "SCALAS3_TLS_CERT_DAYS" -> "tls.cert-days",
    "SCALAS3_GC_INTERVAL" -> "gc.interval",
    "SCALAS3_GC_GRACE" -> "gc.grace",
    "SCALAS3_REPAIR_INTERVAL" -> "repair.interval",
    "SCALAS3_SCRUB_INTERVAL" -> "scrub.interval"
  )

  /** 値が入れ子のオブジェクトになる項目（`名前 = 値` の並びとして読む）。 */
  private val objectKeys = Set("peers", "s3.storage-classes")

  /** ファイルを読み、環境変数名 → 値（環境変数と同じ形の文字列）にする。 */
  def load(text: String): Either[String, Map[String, String]] =
    try
      val root = ConfigFactory.parseString(text).resolve()
      if !root.hasPath("scalas3") then Left("the config file must have a top-level `scalas3` block")
      else
        val c = root.getConfig("scalas3")
        val known = keys.map(_._2).toSet
        val unknown = c.entrySet.asScala.map(_.getKey).filterNot(k => known(k) || objectKeys.exists(o => k.startsWith(o + ".")))
        if unknown.nonEmpty then Left(s"unknown settings in the config file: ${unknown.toList.sorted.mkString(", ")}")
        else Right(keys.flatMap((env, path) => valueOf(c, path).map(env -> _)).toMap)
    catch case e: com.typesafe.config.ConfigException => Left(e.getMessage)

  def loadFile(path: java.nio.file.Path): Either[String, Map[String, String]] =
    if !java.nio.file.Files.exists(path) then Left(s"config file not found: $path")
    else load(java.nio.file.Files.readString(path))

  private def valueOf(c: Config, path: String): Option[String] =
    if !c.hasPath(path) then None
    else
      val v = c.getValue(path)
      v.valueType match
        case ConfigValueType.LIST   => Some(c.getStringList(path).asScala.mkString(","))
        case ConfigValueType.OBJECT => Some(c.getConfig(path).entrySet.asScala.toList.sortBy(_.getKey).map(e => s"${e.getKey.stripPrefix("\"").stripSuffix("\"")}=${e.getValue.unwrapped}").mkString(","))
        case _                      => Some(v.unwrapped.toString)
