# 開発環境

bridge を手元で動かして確認するための環境と、その操作 script (`testenv/dev.sh`) の使い方。

構成:

- traQ (3.32.1) + MariaDB + Elasticsearch: docker、`http://localhost:3080`
- Synapse: docker、server name `localhost`、client API は `http://localhost:8008` (host の `127.0.0.1` だけに公開。federation 無し、rate limit 無し、SQLite)
- bridge: host で `go build` して前景で実行 (Synapse から `host.docker.internal:29338` で到達する)

必要なもの: bash、curl、jq、docker (compose v2)、go。libolm のヘッダが無い環境でも動くように、`dev.sh bridge` は `-tags goolm` でビルドする。

## 開発の流れ

```sh
testenv/dev.sh up          # traQ と Synapse を起動 (初回は image の取得と Elasticsearch の起動で数分かかる)
testenv/dev.sh setup       # traQ の OAuth client と user、Matrix の @dev:localhost を作る
testenv/dev.sh bridge      # 別の terminal で。bridge を前景で起動 (log は stdout)
testenv/dev.sh bot help    # @dev として bot に送る。bot の返事は tail で見る
testenv/dev.sh tail
```

`bot` は bridge bot (`@traqbot:localhost`) との management room に送る。room が無ければ作って bot を招待する (bridge を起動してから使う)。

### bot に login する

login の流れは bot の `login` command が案内する。bot が表示する authorize URL を traQ user に承認させ、できた redirect URL を bot に貼る。login 済みの一覧は `list-logins`、解除は `logout <login ID>` (token を traQ 側でも revoke する)。

```sh
testenv/dev.sh bot login                                  # bot が authorize URL を返す
testenv/dev.sh tail                                       # URL を読む
testenv/dev.sh traq-login alice '<authorize URL>'         # alice として承認し、redirect URL を表示する
testenv/dev.sh bot '<redirect URL>'                       # bot に貼る
```

`traq-login` が表示する redirect URL には認可 code が入っている。`dev.sh` が表示する秘密値はこれだけで、bot に貼るために表示する。

### 相手側の操作を再現する

traQ や Matrix の相手側 (別の user) の操作は次で再現する。

```sh
testenv/dev.sh traq alice GET /users/me
testenv/dev.sh traq alice POST /channels '{"name":"dev-ch","parent":null}'
testenv/dev.sh traq alice POST /channels/<channel ID>/messages '{"content":"hi"}'
testenv/dev.sh rooms                                       # @dev が入っている room (room ID と name)
testenv/dev.sh send '<room ID>' 'hello'                    # @dev として room に送り、event ID を表示する
testenv/dev.sh tail '<room ID>'                            # room の最新 20 event を「sender: body」で表示
testenv/dev.sh events '<room ID>' 5                        # 最新 5 event を JSON 1 行ずつ (event ID、type、sender、content)
```

Matrix 側の返信・編集・削除・reaction・既読も `@dev` として送れる。event ID は `$` で始まるので、shell では single quote で囲む。

```sh
ev=$(testenv/dev.sh send '<room ID>' 'hello')              # 送った event の ID
testenv/dev.sh reply '<room ID>' "$ev" 'a reply'           # 返信 (m.in_reply_to)
testenv/dev.sh edit '<room ID>' "$ev" 'hello (edited)'     # 編集 (m.replace)
r=$(testenv/dev.sh react '<room ID>' "$ev" ':<stamp名>:')    # reaction (m.annotation)。key は :stamp名: か絵文字 (*)
testenv/dev.sh redact '<room ID>' "$r"                     # reaction の取り消し (reaction を redact する)
testenv/dev.sh redact '<room ID>' "$ev"                    # message の削除
testenv/dev.sh read '<room ID>' "$ev"                      # 既読 (m.read receipt)
testenv/dev.sh send-raw '<room ID>' '{"msgtype":"m.text","body":"hi bob","format":"org.matrix.custom.html","formatted_body":"hi <a href=\"https://matrix.to/#/@traq_<bob の ID>:localhost\">bob</a>"}'
```

(*) key は traQ に作った custom stamp の `:stamp名:` を使う。`👍` のような Unicode 絵文字の key は絵文字と stamp の対応表ができてから (Task 14) 通る。それまでは「the emoji to stamp table is still being built」の notice で拒否される。

`traq <user> <METHOD> <path> [json]` は毎回 traQ に login し直した session で `/api/v3<path>` を叩き、response body を表示する。user のパスワードは `admin` (user 名 `traq`) が `traq`、それ以外は `<user>-password` (`alice` なら `alice-password`)。

### 片付け

```sh
testenv/dev.sh down        # container と volume を消し、testenv/.dev/ も消す
```

## `dev.sh` の subcommand

| subcommand | 内容 |
| --- | --- |
| `up` | bridge の binary と `cfgedit` をビルドし、設定と registration が無ければ作り、`docker compose -p tmb --profile matrix up -d` で traQ と Synapse を起動して、両方が応答するまで待つ |
| `down` | `docker compose ... down -v` と `testenv/.dev/` の削除 |
| `setup` | traQ: admin (`traq`/`traq`) の OAuth client (public、scope read/write、callback `http://localhost:29399/callback`) と user `alice` / `bob`。Synapse: `@dev:localhost` (パスワード `dev-password`) を `register_new_matrix_user` で作り、access token を保存。client ID を bridge の設定に書く。何度実行しても壊れない |
| `bridge` | `go build -tags goolm` して `testenv/.dev/config.yaml` で前景起動 |
| `bot <text>` | management room に `@dev` として送る |
| `tail [room]` | room (既定は management room) の最新 20 event を `sender: body` で表示 |
| `events <room> [limit]` | room の最新 `limit` (既定 20) event を古い順に JSON 1 行ずつ (`event_id`、`type`、`sender`、`content`、redact 済みなら `redacted: true`) |
| `rooms` | `@dev` が入っている room の `room ID<TAB>name` |
| `send <room> <text>` | `@dev` として room に `m.text` を送り、event ID を表示する |
| `send-raw <room> <content-json>` | 任意の content の `m.room.message` を送り、event ID を表示する (HTML、pill、`m.image` など) |
| `reply <room> <event-id> <text>` | event への返信を送り、event ID を表示する |
| `edit <room> <event-id> <text>` | message の本文を置き換える編集を送り、編集 event の ID を表示する |
| `redact <room> <event-id>` | event を redact し、redaction の event ID を表示する。reaction を redact すると reaction の取り消しになる |
| `react <room> <event-id> <key>` | reaction を送り、reaction の event ID を表示する |
| `read <room> <event-id>` | event までの既読 (`m.read` receipt) を送る |
| `traq-login <user> <authorize-url>` | traQ user の session で authorize URL を開き、同意画面を承認して、redirect URL (`code` と `state` 付き) を表示する |
| `traq <user> <METHOD> <path> [json]` | traQ user の session で REST を叩く |

状態は `testenv/.dev/` (git 管理外) に置く。

| file | 内容 |
| --- | --- |
| `config.yaml` | bridge の設定 |
| `registration.yaml` | bridge の appservice registration |
| `doublepuppet.yaml` | double puppeting 用の registration |
| `bridge.db` | bridge の SQLite |
| `client_id` | traQ の OAuth client ID |
| `matrix_auth_header` | `@dev` の access token (`Authorization: Bearer ...` の 1 行。curl の `-H @file` で使う) |
| `management_room` | management room の room ID |
| `traq-matrix-bridge`、`cfgedit` | ビルドした binary |

token は表示しない。Synapse は registration を起動時にしか読まないので、`up` が (traQ が無くても作れる) 設定と registration を先に作ってから container を起動する。`setup` が埋めるのは `network.oauth.client_id` だけで、registration には影響しない。

## bridge の設定のどこを書き換えるか

`traq-matrix-bridge -e` で生成した設定を、`testenv/cfgedit` (コメントを保ったまま YAML の値を差し替える小さな Go program) で次のとおり書き換える。他の値は生成されたままで、`-g` が `appservice.as_token` / `hs_token` を埋める。

| key | 値 |
| --- | --- |
| `network.server_url` | `http://localhost:3080` |
| `network.oauth.client_id` | `setup` が作った client の ID |
| `bridge.permissions` | `"localhost": admin` のみ |
| `bridge.mute_only_on_create` | `false` |
| `database.type` / `database.uri` | `sqlite3-fk-wal` / `file:<repo>/testenv/.dev/bridge.db?_txlock=immediate` |
| `homeserver.address` / `homeserver.domain` | `http://localhost:8008` / `localhost` |
| `appservice.address` | `http://host.docker.internal:29338` (registration の `url` になる) |
| `appservice.hostname` / `appservice.port` | `0.0.0.0` / `29338` (docker の Synapse から host の bridge に届くよう、全 interface で待ち受ける。理由は下の注意を参照) |
| `double_puppet.servers` | 空 |
| `double_puppet.secrets` | `localhost: "as_token:<doublepuppet.yaml の as_token>"` |
| `encryption.allow` | `false` |
| `backfill.enabled` | `true` |
| `logging` | console に debug (file には書かない) |

double puppeting は mautrix の文書 (https://docs.mau.fi/bridges/general/double-puppeting.html) の appservice 方式。`doublepuppet.yaml` は URL を持たない 2 つ目の registration で、user namespace `@.*:localhost` (exclusive: false) を持ち、Synapse の `app_service_config_files` に `registration.yaml` と一緒に載せている。この `as_token` を使った `whoami?user_id=@dev:localhost` が成功することは確認してあるが、bridge が起動時に確かめたり log に出したりはしない。実際に使えるかどうかは user が login したときに分かる (Task 8)。`as_token` は設定ファイルにだけ書かれ、`cfgedit` には環境変数 (`${DOUBLE_PUPPET_SECRET}` の置換) で渡すので、コマンドラインには出ない。

注意: bridge の appservice listener (`0.0.0.0:29338`) は LAN にも見える。docker の Synapse は host の bridge に `host.docker.internal` (docker bridge network の gateway) 経由で接続するため、`127.0.0.1` だけの待ち受けでは届かない。到達には `hs_token` が要るので、開発用の設定としては許容しているが、信用できないネットワークでは使わない。Synapse の `8008` は loopback だけに公開している。

## テスト

```sh
go vet ./...
go test -race ./...                     # 単体テスト (libolm が無い環境では -tags goolm を付ける)
```

結合テストは traQ (`localhost:3080`) に対して実行する。

```sh
docker compose -p tmbit -f testenv/compose.yaml up -d     # 開発環境を立てていない時だけ。profile を付けないので Synapse は起動しない
go test -tags integration -race -count=1 ./pkg/traq/...
docker compose -p tmbit -f testenv/compose.yaml down -v
```

開発環境と結合テストは同じ port 3080 の traQ を使う。開発環境を立てている間は、上の `docker compose` を使わず、`go test -tags integration ./pkg/traq/...` をその traQ に対して実行する。テストは毎回新しい user とチャンネルを作るので、`alice` / `bob` や開発中のデータとは干渉しない。別の traQ に向けるには環境変数 `TRAQ_TEST_URL` を使う。
