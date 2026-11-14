# TLA+ の仕様

| 仕様 | 内容 | 設定 |
|---|---|---|
| `BlockGc.tla` | ブロックの GC（マーク＆スイープ）と、重複排除・共有・長いアップロードの競合。不変条件は「コミットされた参照が指すブロックは必ずディスクにある」 | `BlockGc.cfg`（実装どおり。成り立つ）、`BlockGcNoTouch.cfg`（共有時に更新時刻を進めない。反例あり）、`BlockGcNoCommitTouch.cfg`（コミット時に進めない。反例あり） |

## 実行

```bash
curl -sLO https://github.com/tlaplus/tlaplus/releases/download/v1.7.4/tla2tools.jar
java -cp tla2tools.jar tlc2.TLC -workers 4 -config BlockGc.cfg BlockGc.tla
```

## コミットの直前に更新時刻を進める理由

`BlockGcNoCommitTouch.cfg` の反例：猶予期間より長くかかったアップロードが、
GC のマークの途中で「もう読み終えたキー」にコミットすると、そのブロックは参照されていないとみなされて消される。
そのため実装は、メタデータをコミットする直前にブロックの更新時刻を進める（`DataPath.touch`）。
