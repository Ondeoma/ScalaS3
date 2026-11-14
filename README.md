# scalas3

Scala 3 / cats-effect / fs2 / http4s で実装する、S3 互換の分散オブジェクトストレージです。
設計は [docs/design.md](docs/design.md) を参照してください。

> 開発中です（設計書のマイルストーン M0〜M8 を実装済み。v1.0 に向けた性能の調整や長期運用での検証はこれから）。

## 動かし方

```bash
SCALAS3_DATA=./data \
SCALAS3_ACCESS_KEY=AKIDEXAMPLE0000001 \
SCALAS3_SECRET_KEY=change-me-change-me-change-me-change-me \
sbt "server/run"
```

既定では `0.0.0.0:3900` で S3 API を待ち受けます。設定できる環境変数は `server/src/main/scala/scalas3/server/Main.scala` の先頭にまとめてあります。

```bash
aws --endpoint-url http://127.0.0.1:3900 s3 mb s3://mybucket
aws --endpoint-url http://127.0.0.1:3900 s3 cp ./file s3://mybucket/
```

### 複数ノード

初期メンバー全員に、同じ `SCALAS3_PEERS` を渡して起動します（RPC のアドレス）。

```bash
SCALAS3_NODE_ID=n1 SCALAS3_RPC_LISTEN=10.0.0.1:3901 SCALAS3_ZONE=rack1 \
SCALAS3_PEERS=n1=10.0.0.1:3901,n2=10.0.0.2:3901,n3=10.0.0.3:3901 \
SCALAS3_ACCESS_KEY=... SCALAS3_SECRET_KEY=... sbt "server/run"
```

後から参加するノードは `SCALAS3_BOOTSTRAP=false` にし、`SCALAS3_PEERS` に既存のノードを書きます。起動すると自分をレイアウトに加え、レンジとブロックが自動で移ってきます。

### 主な設定（環境変数）

全項目は `server/src/main/scala/scalas3/server/Main.scala` の先頭にあります。同じ項目を HOCON の設定ファイルにも書けます（`--config /etc/scalas3/scalas3.conf`、例は [docs/scalas3.conf.example](docs/scalas3.conf.example)）。環境変数が優先します。

| 変数 | 内容 |
|---|---|
| `SCALAS3_STORAGE_CLASSES` | 例 `STANDARD=replica,STANDARD_IA=ec:4:2`。`x-amz-storage-class` で選ぶ。AWS CLI のように名前を検査するクライアントからも使えるよう、標準の名前に割り当てるとよい |
| `SCALAS3_MASTER_KEY` | 32 バイトの鍵を base64 で。SSE-S3 とアクセスキーの秘密鍵の暗号化に使う |
| `SCALAS3_TLS_CA_DIR` / `SCALAS3_JOIN_TOKEN` | ノード間通信を相互認証つき TLS にし、証明書を自動で発行する（`scalas3 cert init-ca`、[docs/tls.md](docs/tls.md)） |
| `SCALAS3_TLS_KEYSTORE` / `SCALAS3_TLS_PASSWORD` | 鍵ストアを自分で用意する場合（`scalas3 cert create` で作れる） |
| `SCALAS3_ADMIN_LISTEN` / `SCALAS3_ADMIN_TOKEN` | Admin API（[docs/admin-api.yaml](docs/admin-api.yaml)）と `/metrics` |
| `SCALAS3_S3_TLS_CERT` / `SCALAS3_S3_TLS_KEY` | 公開ポートを HTTPS にする（PEM。Let's Encrypt の `fullchain.pem` と `privkey.pem` をそのまま使え、更新されると読み直す。[docs/tls.md](docs/tls.md)） |
| `SCALAS3_TRUSTED_PROXIES` | 前段のリバースプロキシや CDN のアドレス。送信元 IP をヘッダーから読む（[docs/reverse-proxy.md](docs/reverse-proxy.md)） |

## 管理 CLI

JVM の fat jar（`java -jar cli/target/scala-3.9.0/scalas3-cli.jar`）のほか、GraalVM があればネイティブのバイナリにもできます（`GRAALVM_HOME=… scripts/build-cli-native.sh`）。

```bash
sbt cli/assembly
java -jar cli/target/scala-3.9.0/scalas3-cli.jar --endpoint http://127.0.0.1:3903 --token $SCALAS3_ADMIN_TOKEN status
```

主なコマンド：`status`、`layout show|set|remove`、`key list|create|delete|grant`、`bucket list`、`remote list|set|delete`、`job <repair|gc|scrub|lifecycle|replication>`、`cert create`。

## 配布物

- `sbt server/assembly cli/assembly` で fat jar を作り、[Dockerfile](Dockerfile) でイメージにする。
- Kubernetes 用の Helm チャートは [deploy/helm/scalas3](deploy/helm/scalas3)。Secret（`accessKey`、`secretKey`、`adminToken`）を先に作り、
  ノード間 TLS を使うなら内部 CA の Secret を `tls.caSecret` に渡す（values.yaml のコメントを参照）。
- 版の上げ方（ローリングアップグレード）と、その試験（`it/upgrade/run.sh`）は [docs/upgrade.md](docs/upgrade.md)。

## モジュール

| モジュール | 内容 |
|---|---|
| `core` | バイト列、varint、チェックサム、決定的な乱数 |
| `runtime` | ディスクとネットワークの抽象化（本番実装） |
| `sim` | 決定的シミュレーション用のディスクとネットワーク（電源断、分断） |
| `rpc` | ノード間 RPC |
| `lsm` | 自前の LSM-Tree（WAL なし） |
| `raft` | Raft（純粋なコア、永続化、ドライバ） |
| `meta` | Multi-Raft のメタデータ層（レンジ、分割、ルーティング） |
| `block` | データブロックの保存（複数ディスク） |
| `cluster` | レイアウト、配置、ノード間のブロック複製と EC の断片、修復 |
| `s3` | S3 API（SigV4、XML、オブジェクトのメタデータ、GC） |
| `server` | 組み立てと起動、Admin API、メトリクス |
| `cli` | 管理 CLI |

## テスト

```bash
sbt test
```

ディスクとネットワークの障害を注入する決定的シミュレーションと、AWS SDK for Java v2 を使った結合テストを含みます。

## 検証と性能

- 決定的シミュレーション（`scalas3.sim.Sim`。種は `SCALAS3_SIM_SEED` で変えられる）、線形化可能性の検査、TLA+ の仕様（[spec](spec)）
- ベンチマークの結果と測り方は [docs/benchmark.md](docs/benchmark.md)

## ライセンス

MIT（[LICENSE](LICENSE)）
