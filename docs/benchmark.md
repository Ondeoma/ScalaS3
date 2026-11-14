# ベンチマーク

## 測り方

`server/src/test/scala/scalas3/server/S3Bench.scala`（任意の S3 互換エンドポイントに使える）:

```bash
sbt "server/Test/runMain scalas3.server.S3Bench http://127.0.0.1:3900 <アクセスキー> <秘密鍵> put 4096 32 20"
#   引数: エンドポイント アクセスキー 秘密鍵 put|get オブジェクトの大きさ 並行数 秒数
```

- 各ワーカーは自分のキー（`s<大きさ>-w<番号>-<連番>`）に書く。get は、同じ大きさ・同じ並行数の put で
  各ワーカーが書けた件数（最大 200）を HEAD で調べ、その中から読む
- 署名は SigV4（本体は UNSIGNED-PAYLOAD）、HTTP/1.1 の接続を使い回す
- 数字は 1 回の測定。同じ条件でも ±10% 程度ぶれる
- RPC 単体の速さは `rpc/src/test/scala/scalas3/rpc/RpcBench.scala`（TCP と TLS を比べる）

## 結果（2026-11-07）

### 共通の条件

- マシン: Intel Core i9-12900K、内蔵 SSD、JDK 21、G1
- 4KiB 以下のオブジェクトはメタデータに埋め込まれる（ブロックを書かない）
- クライアントは同じマシン（S3Bench、並行数は表のとおり）

### Garage との比較（Linux、同じ VM の中）

Podman の VM（Fedora CoreOS、12 vCPU、メモリ 2 GB）で、サーバーとクライアントをどちらもコンテナで動かした。
データはコンテナ内（VM のディスク）。

- scalas3: 単一ノード、ディスク 1 台、`-Xmx512m`（VM のメモリが小さいため）
- Garage v1.1.0: 単一ノード、`replication_factor = 1`、LMDB、`metadata_fsync = true`、`data_fsync = true`（scalas3 と同じく、応答の前に永続化する）

| 操作 | 大きさ | 並行数 | scalas3 | Garage |
|---|---|---|---|---|
| PUT | 4 KiB | 32 | **5,857 ops/s**（p50 3.9 ms） | 132 ops/s（p50 249 ms） |
| PUT | 4 KiB | 128 | **5,964 ops/s**（p50 12.2 ms） | 129 ops/s（p50 1,012 ms） |
| GET | 4 KiB | 32 | **13,708 ops/s**（p50 1.9 ms） | 766 ops/s（p50 41.9 ms） |
| PUT | 1 MiB | 8 | **676 MiB/s** | 130 MiB/s |
| GET | 1 MiB | 8 | **646 MiB/s** | 478 MiB/s |
| PUT | 16 MiB | 4 | **702 MiB/s** | 442 MiB/s |
| GET | 16 MiB | 4 | **851 MiB/s** | 536 MiB/s |

注意:

- **Garage は設定を調整していない。** fsync を切った既定の設定（`metadata_fsync = false`、`data_fsync = false`）でも、4KiB の PUT は約 1,000 ops/s、GET は約 750 ops/s だった。
- **Garage の 4KiB GET は、1 件ずつ curl で読んでも約 29 ms かかる。** p50 が 40 ms 前後に張り付くので、Nagle と遅延 ACK の影響の可能性がある（scalas3 も TCP_NODELAY を付けないと同じ症状が出る）。Garage の本来の性能を表していない可能性が高い。
- **MinIO とは比較できなかった。** オープンソース版はアーカイブされ、公式の配布（バイナリもコンテナイメージも）が止まっている（2026-11 時点で `dl.min.io` は 410 Gone）。

### scalas3（macOS、単一ノード）

APFS、ディスク 1 台（`SCALAS3_DISK_COPIES=1`）、fat jar を直接起動、`-Xmx768m`。

| 操作 | 大きさ | 並行数 | ops/s | MiB/s | p50 | p99 |
|---|---|---|---|---|---|---|
| PUT | 4 KiB | 32 | 8,102 | 31.6 | 2.8 ms | 11.7 ms |
| PUT | 4 KiB | 128 | 5,855 | 22.9 | 7.4 ms | 285.1 ms |
| GET | 4 KiB | 32 | 30,559 | 119.4 | 0.9 ms | 4.1 ms |
| PUT | 1 MiB | 8 | 1,058 | 1,058 | 7.0 ms | 14.0 ms |
| GET | 1 MiB | 8 | 1,091 | 1,091 | 6.9 ms | 11.5 ms |
| PUT | 16 MiB | 4 | 57 | 910 | 69.4 ms | 91.3 ms |
| GET | 16 MiB | 4 | 71 | 1,132 | 56.0 ms | 93.0 ms |

並行数 128 の小さな PUT は、測るたびに 5,800〜7,500 ops/s、p99 150〜290 ms とぶれる（LSM の flush とコンパクションの重なり方による）。

### scalas3（macOS、3 ノード）

3 プロセスを同じマシンで動かした（**ディスクは 1 台を共有**。書き込みは 3 倍になり、3 プロセスの fsync が同じ SSD で競合する）。
ゾーンはノードごとに別、メタデータは 3 複製、クライアントは n1 だけに接続。

| 操作 | 大きさ | 並行数 | 複製 ×3 | EC 2+1 | 複製 ×3 ＋ ノード間 mTLS |
|---|---|---|---|---|---|
| PUT | 4 KiB | 32 | 2,995 ops/s（p50 8.0 ms） | 3,296 ops/s | 2,561 ops/s |
| PUT | 4 KiB | 128 | 4,637 ops/s（p50 15.5 ms） | 4,815 ops/s | 3,819 ops/s |
| GET | 4 KiB | 32 | 15,494 ops/s | 14,789 ops/s | 11,549 ops/s |
| PUT | 1 MiB | 8 | 339 MiB/s | 316 MiB/s | 306 MiB/s |
| GET | 1 MiB | 8 | 999 MiB/s | 978 MiB/s | 1,001 MiB/s |
| PUT | 16 MiB | 4 | 388 MiB/s | 465 MiB/s | 397 MiB/s |
| GET | 16 MiB | 4 | 1,164 MiB/s | 623 MiB/s | 1,158 MiB/s |

読み方:

- 大きなオブジェクトの PUT は、同じ SSD に 3 コピー書くので単一ノードのほぼ 1/3。EC 2+1 は書く量が 1.5 倍で済むので速い
- EC の GET は、断片を他のノードからも集めるので複製より遅い
- mTLS は、大きなオブジェクトでは平文とほぼ同じ。小さな PUT・GET は 15〜25% 遅い（暗号化と、ブロッキングの読み書きの受け渡し）

### 公開ポートの HTTP サーバー（macOS、単一ノード）

S3 の公開ポートを受けるサーバーの比較。この節より上の表は、以前の構成（Ember ＋ 中継）で測ったもの。

- 自前の Netty：`NettyHttpServer`、既定
- Ember ＋ 中継：以前の構成（`SCALAS3_HTTP_SERVER=ember`）
- Ember 直結：中継を通さず、内部の Ember に直接つないだ参考値（100-continue に応えないので実運用はできない）

| 操作 | 大きさ | 並行数 | 自前の Netty | Ember ＋ 中継 | Ember 直結 | http4s-netty 0.7.2（試作） |
|---|---|---|---|---|---|---|
| PUT | 4 KiB | 32 | 7,534 ops/s | 8,011 ops/s | 7,623 ops/s | 7,516 ops/s |
| GET | 4 KiB | 32 | **30,101 ops/s** | 25,359 ops/s | 24,882 ops/s | 19,541 ops/s |
| GET | 4 KiB | 128 | **30,539 ops/s** | 25,688 ops/s | 25,304 ops/s | 19,685 ops/s |
| PUT | 1 MiB | 8 | 991 MiB/s | 830 MiB/s | 1,119 MiB/s | 1,035 MiB/s |
| GET | 1 MiB | 8 | **1,280 MiB/s** | 1,048 MiB/s | 1,065 MiB/s | 1,157 MiB/s |
| PUT | 16 MiB | 4 | 950 MiB/s | 912 MiB/s | 1,010 MiB/s | 946 MiB/s |
| GET | 16 MiB | 4 | **1,251 MiB/s** | 1,080 MiB/s | 1,203 MiB/s | 1,126 MiB/s |

小さな PUT は fsync で決まり、サーバーによる差はぶれの範囲。

Linux の VM（epoll）でも、Ember ＋ 中継に比べて GET が速くなった。

| 操作 | 自前の Netty | Ember ＋ 中継 |
|---|---|---|
| 4 KiB GET | 16,069 ops/s | 13,708 ops/s |
| 1 MiB GET | 985 MiB/s | 646 MiB/s |
| 16 MiB GET | 1,186 MiB/s | 851 MiB/s |

確かめたこと:

- `Expect: 100-continue` に、本体を読む前に `100 Continue` を返す（curl）
- 本体を読まずに応答したときは `100 Continue` を送らず、接続を閉じる
- `aws:SourceIp` の条件が効く。LAN からの匿名アクセスを 403 にできた（中継では送信元が常に 127.0.0.1 に見えていた）
- ヒープ 256 MB で 1 GiB の PUT と、50 MB/s に絞った遅い GET が通る。RSS は約 0.5 GB で一定（背圧が効いている）
- AWS SDK のテスト、生のソケットでのプロトコルのテスト（`NettyHttpServerSuite`：keep-alive、先に送られた要求、chunked、HEAD、HTTP/1.0、壊れた要求、途中の切断）

### メモリ

- 単一ノード、`-Xmx512m`、全項目の測定後の RSS は約 0.9 GB
- ヒープの外は約 300 MB。内訳は G1 の管理領域 約 90 MB、スレッドのスタック 約 70 MB、コード・クラス領域など（NMT で測定）
- コンテナではメモリ上限の約半分をヒープにする（Dockerfile は `-XX:MaxRAMPercentage=50`）

### 長時間の負荷（macOS、3 ノード、60 分）

`S3Soak`。PUT 55%・GET 35%・DELETE 10%、1 KiB〜256 KiB、キー 4,000 個を上書きし続ける（GC の間隔 2 分、猶予 1 分）。
約 290 ops/s・21 MiB/s で、p50 2.6〜3.3 ms、p99 15〜36 ms が最後まで変わらなかった。中身の不一致は 0。
常駐メモリは 580〜730 MB、ボリュームは負荷を止めて GC が回ると 946 MB に戻った。

エラーは、同じ条件の 15 分の測定で 0、サーバーの警告も 0。

## 調整で決めた設定

- **バケットとアクセスキーのキャッシュ**（1 秒）：小さな GET がほぼ 2 倍になる。
- **全レンジで 1 本の Raft ログを共有**（C1）：レンジが増えても fsync の回数が増えない。
- **memtable の大きさ**：ヒープの 1/24、flush 待ち 4 枚。2 枚だと並行数 128 の小さな PUT の p99 が約 3 倍になる。
- **一時的なダイレクトバッファ**：ファイルの読み書きを 256 KiB ずつに分け、`jdk.nio.maxCachedBufferSize` を 256 KiB にする。JDK がスレッドごとに抱えるヒープの外のバッファが数百 MB になり、小さな VM で OOM になるため。
- **Raft のログの切り詰め**：状態全体を並べず、LSM の flush で済ませ、裏で行う。ログの大きさ（8 MiB）でも切り詰める。
- **cats-effect のファイバーの追跡**：既定で切る（`Launcher`。小さな PUT が約 35% 速くなる）。調べるときは `-Dcats.effect.tracing.mode=cached` で戻せる。
- **TCP_NODELAY**：すべての TCP ソケットに付ける。付けないと p50 が遅延 ACK の 40 ms に張り付く。
- **Raft の送信**：リーダーは自分の fsync を待たずに Append を送り（Raft 論文 §10.2.1）、同じ相手へのメッセージをまとめて 1 回の RPC にする。前の周までに永続化したエントリのコミットは、その周の fsync を待たずに適用する。
- **ノード間 TLS**：JDK の SSLSocket をブロッキングで使い、届いている分をまとめて読む（fs2 の TLS より大きな転送で 2 倍以上速い。小さな要求は約 10% 遅い）。
- **RPC の送信**：溜まったフレームを最大 256 KiB まとめて 1 回で書く。
- **リーダーが残すログ**：フォロワーが受け取り済みの位置より先は切り詰めない（上限 32 MiB）。少し遅れたフォロワーに、レンジ全体のスナップショットを繰り返し送らないため。
- **公開ポートの HTTP サーバー**：自前の Netty サーバー（C6）。小さな応答はヘッダーと本体を 1 回で書く。
- **SST の組み立て**：バッファを最初から目標の大きさで確保し、最後のコピーを省く。
- **ブロックの保存**：追記型のボリューム（D6）。64 KiB の PUT が約 1.6〜1.8 倍になる。

## まだ測っていないこと

- 複数のマシン（ディスクとネットワークが別）での測定

## 今後の改善候補

- 公開ポートの HTTPS の性能（TLS の処理の分。Netty の JDK の TLS を使っている。OpenSSL（netty-tcnative）にすると速くなる見込み）
- 状態マシンの適用をバッチで LSM に書く（いまのプロファイルでは適用の CPU は小さい。条件付き書き込みが同じバッチ内の前の書き込みを見る必要があり、複雑になるので見送った）
- 3 ノードの小さな PUT（p50 8 ms）。別々のディスクのマシンで測り直してから、fsync 以外の待ちを調べる
