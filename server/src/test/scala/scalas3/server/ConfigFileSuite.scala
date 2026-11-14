package scalas3.server

import munit.FunSuite

class ConfigFileSuite extends FunSuite:

  test("設定ファイルの例を読むと、環境変数と同じ形の値になる") {
    // テストの作業ディレクトリはモジュールの中のことがあるので、上へたどって探す
    val file = Iterator.iterate(java.nio.file.Paths.get("").toAbsolutePath)(_.getParent).takeWhile(_ != null)
      .map(_.resolve("docs/scalas3.conf.example")).find(java.nio.file.Files.exists(_)).get
    val text = java.nio.file.Files.readString(file)
    val m = ConfigFile.load(text).fold(e => fail(e), identity)
    assertEquals(m("SCALAS3_NODE_ID"), "n1")
    assertEquals(m("SCALAS3_DISKS"), "/mnt/disk1,/mnt/disk2")
    assertEquals(m("SCALAS3_PEERS"), "n2=10.0.0.2:3901,n3=10.0.0.3:3901")
    assertEquals(m("SCALAS3_BOOTSTRAP"), "true")
    assertEquals(m("SCALAS3_COPIES"), "3")
    assertEquals(m("SCALAS3_STORAGE_CLASSES"), "STANDARD=replica,STANDARD_IA=ec:4:2")
    assertEquals(m("SCALAS3_TRUSTED_PROXIES"), "10.0.0.5")
    assertEquals(m("SCALAS3_TLS_CA_DIR"), "/etc/scalas3/ca")
    assert(!m.contains("SCALAS3_ADMIN_TOKEN"))
  }

  test("知らない項目（書き間違い）はエラーにする") {
    val r = ConfigFile.load("""scalas3 { node-id = "a", replcas = 3, s3 { regoin = "x" } }""")
    assert(r.left.exists(e => e.contains("replcas") && e.contains("s3.regoin")), r.toString)
  }

  test("scalas3 の塊がなければエラー") {
    assert(ConfigFile.load("node-id = a").isLeft)
  }
