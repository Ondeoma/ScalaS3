# リバースプロキシと CDN の後ろで動かす

scalas3 の公開ポートは、HTTPS を直接受けることもできます（docs/tls.md の「公開ポートの HTTPS」）。ただ、外部への公開は前段のリバースプロキシ（nginx、HAProxy、Traefik など）で行うことも多く、さらにその前に CDN がいることもあります。この文書はその構成のためのものです。

```
クライアント ──▶ （CDN） ──▶ リバースプロキシ ──▶ scalas3
```

この構成では、scalas3 から見た接続の相手はリバースプロキシです。バケットポリシーの `aws:SourceIp`（送信元 IP）と `aws:SecureTransport`（HTTPS か）を正しく評価するには、プロキシが付けるヘッダーから本当の送信元を読む必要があります。

## 設定

| 環境変数 | 内容 |
|---|---|
| `SCALAS3_TRUSTED_PROXIES` | 信頼するプロキシのアドレス。CIDR か IP をカンマ区切りで指定します（例：`10.0.0.0/8,192.168.1.10`）。指定しなければ、接続の相手をそのまま送信元とし、ヘッダーは一切見ません |
| `SCALAS3_TRUSTED_PROXY_HOPS` | 手前から何段目までのプロキシを、アドレスによらず信頼するか。直前の相手を 1 段目と数えます（下の「CDN のアドレスが分からないとき」） |
| `SCALAS3_CLIENT_IP_HEADER` | 送信元を読むヘッダー。`x-forwarded-for`（既定）、`forwarded`（RFC 7239）、または `cf-connecting-ip` のような単一の値のヘッダー |

## 送信元の決め方

1. **直前の相手を確かめる。** 直接つないできた相手が `SCALAS3_TRUSTED_PROXIES` の範囲になければ、ヘッダーは無視します。scalas3 に直接つないで偽のヘッダーを送られても、騙されません。段数の指定があっても、直前の相手は必ずアドレスの範囲で確かめます。
2. **右からたどる。** `X-Forwarded-For: a, b, c` は、経由したプロキシが順に右へ足していった記録です。右（scalas3 に近い側）からたどり、信頼するプロキシを飛ばします。最初に見つかった信頼しないアドレスを送信元とします。クライアントが自分で書いた値は左側に残るだけなので、偽装できません。
3. **全部信頼できるとき。** たどった全員が信頼するプロキシなら、いちばん左のアドレスを送信元とします。
4. **読めない値に当たったとき。** 途中に `unknown` や読めない値があれば、送信元は「分からない」とします。このとき `aws:SourceIp` は空になり、`IpAddress` の条件はどれにも当てはまらず、`NotIpAddress` の条件には当てはまります（安全側）。プロキシのアドレスで代用はしません。代用すると、内部のアドレスを許すポリシーに誤って当てはまるためです。
5. **HTTPS かどうか。** 送信元と同じ段の情報から決めます。
   - `X-Forwarded-Proto` が `X-Forwarded-For` と同じ数だけあれば、同じ位置の値を使います。1 つだけなら、その値を使います。
   - `Forwarded` ヘッダーでは、送信元を選んだ要素の `proto=` を使います。
   - どれにも当てはまらなければ、HTTPS ではないとみなします。

ヘッダーが複数行に分かれていても、届いた順につないで扱います。IPv6（`[2001:db8::1]:4711` のような角かっこやポートつきの形）や、IPv4 を表す IPv6 アドレス（`::ffff:10.0.0.5`）も読めます。

## 構成の例

### リバースプロキシだけ

nginx（10.0.0.5）の後ろで動かす場合です。

```bash
SCALAS3_TRUSTED_PROXIES=10.0.0.5
```

nginx の設定例です（この構成で、署名付きの PUT と GET、100-continue、送信元の判定を確かめてあります）。

```nginx
server {
    listen 443 ssl;
    # ssl_certificate などは省略
    client_max_body_size 0;          # 大きな PUT を制限しない
    proxy_request_buffering off;     # 本体を溜めずに流す
    proxy_buffering off;
    location / {
        proxy_pass http://10.0.0.20:3900;
        proxy_http_version 1.1;
        proxy_set_header Connection "";
        proxy_set_header Host $http_host;   # SigV4 の署名に Host が含まれるので、書き換えずに渡す
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

注意点:

- **Host を書き換えない。** SigV4 の署名には Host ヘッダーが含まれます。書き換えると署名が合わず、403（SignatureDoesNotMatch）になります。パスのエンコードも変えないでください（`proxy_pass` の URL にパスを付けない）。
- **大きな PUT を制限しない。** `client_max_body_size 0` と `proxy_request_buffering off` を指定します。指定しないと、大きな PUT がプロキシのディスクに溜まったり、拒否されたりします。

### CDN → リバースプロキシ（CDN のアドレスの範囲を指定）

CDN が公開しているアドレスの範囲を、リバースプロキシのアドレスと一緒に指定します。

```bash
SCALAS3_TRUSTED_PROXIES=10.0.0.5,198.51.100.0/24,203.0.113.0/24   # 後ろは CDN が公開している範囲（全部並べる）
```

CDN が `X-Forwarded-For: クライアント` を付け、nginx が CDN のアドレスを右に足します。scalas3 は、nginx と CDN を飛ばしてクライアントにたどり着きます。

HTTPS かどうかについて:

- CDN からリバースプロキシへの通信が HTTP のこともあります。その場合、nginx の `$scheme` は `http` になってしまいます。
- CDN が付けた `X-Forwarded-Proto` を使いたいときは、`proxy_set_header X-Forwarded-Proto $http_x_forwarded_proto;` で引き継いでください。
- このとき nginx は、CDN 以外から直接来た接続を受けないようにしておく必要があります（下の「守るべきこと」）。

### CDN のアドレスが分からないとき（段数で指定）

CDN のアドレスの範囲は広く、よく変わります。追いきれないときは段数で指定します。

```bash
SCALAS3_TRUSTED_PROXIES=10.0.0.5        # 直前の相手（リバースプロキシ）は必ず範囲で指定する
SCALAS3_TRUSTED_PROXY_HOPS=2            # リバースプロキシ（1 段目）と CDN（2 段目）を信頼する
```

段数で信頼する場合、リバースプロキシが CDN 以外からの接続も受けると、その接続元が「CDN」として信頼されてしまいます。その結果、`X-Forwarded-For` を偽装できます。リバースプロキシには、CDN からの接続だけを受けるファイアウォールを必ず設けてください。

### CDN が付ける単一の値のヘッダーを使う

```bash
SCALAS3_TRUSTED_PROXIES=10.0.0.5
SCALAS3_CLIENT_IP_HEADER=cf-connecting-ip     # または true-client-ip、x-real-ip など
```

直前の相手（リバースプロキシ）を信頼するときだけ、そのヘッダーの値を送信元とします。リバースプロキシは、CDN 以外から来た接続についてこのヘッダーを消すか、上書きしてください。そうしないと、クライアントが偽の値を送れます。

確かめる手段が多いのは、`X-Forwarded-For` と CDN のアドレスの範囲を組み合わせる方法です。こちらを勧めます。

### Forwarded ヘッダー（RFC 7239）を使うプロキシ

```bash
SCALAS3_TRUSTED_PROXIES=10.0.0.5
SCALAS3_CLIENT_IP_HEADER=forwarded
```

`Forwarded: for="[2001:db8::7]:4711";proto=https, for=198.51.100.10` のような形を読みます。

## 守るべきこと

- **scalas3 の公開ポートを直接公開しない。** リバースプロキシ（と管理用の端末）からだけ届くように、ファイアウォールで制限してください。`SCALAS3_TRUSTED_PROXIES` の判定は偽装を防ぎますが、直接つながると、その接続元が送信元として扱われます。
- **信頼する範囲を広げすぎない。** `0.0.0.0/0` のように広く指定すると、誰でも送信元を偽装できます。
- **ヘルスチェックの扱い。** 信頼するプロキシから `X-Forwarded-For` なしで届いた要求は、プロキシ自身を送信元とします。プロキシ自身のアドレスを許すポリシーを書く場合は、この点に注意してください。
