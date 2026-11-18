# 開発環境

bridge を手元で動かして確認するための環境と、その操作 script (`testenv/dev.sh`) の使い方。

構成:

- traQ (3.32.1) + MariaDB + Elasticsearch: docker、`http://localhost:3080`
- Synapse: docker、server name `localhost`、client API は `http://localhost:8008` (host の `127.0.0.1` だけに公開。federation 無し、rate limit 無し、SQLite)
- bridge: host で `go build` して前景で実行 (Synapse から `host.docker.internal:29338` で到達する)

traQ の origin `http://localhost:3080` の host には `.` で区切った TLD が無いが、本文中の bare な `http://localhost:3080/files/<id>` と `http://localhost:3080/messages/<id>` も production の traQ と同じく file と引用として扱う (msgconv は goldmark の linkify が拾わない URL を、設定の origin で始まるものに限って拾う)。`127.0.0.1.nip.io` のような `.` 入りの別名を `network.server_url` に設定する必要は無い。

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

### 検索

bot の `search <query>` が traQ の message を検索し、新しい順に最大 20 件を `#channel  @user  日時  抜粋  link` で返す。bridge 済みの message の link は Matrix の event、そうでなければ traQ の URL。`in:#path` で channel、`from:@name` で user を絞る (`"…"` で空白を含む語を 1 語にできる)。日時は bridge の process の local time zone。traQ の検索は Elasticsearch が index してから効くので、開発用の traQ では message を投稿してから約 70 秒待つ。 抜粋の embed (`@name` など) は表示用の文字列にする。`from:` の user や `in:` の channel が bridge の知る前に作られたものでも、見つからなければ traQ の一覧を 1 回読んで再試行する。

```sh
testenv/dev.sh bot 'search in:#gps/times from:@bob hello'
```

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

(*) key は traQ の stamp の `:stamp名:` か、Unicode stamp の絵文字 (`👍` など)。絵文字の key は、bridge がその絵文字の Unicode stamp をまだ解決していなければ (下の「Unicode stamp」) 「the emoji to stamp table is still being built」の notice で拒否される。traQ 側でその stamp が押された message を bridge すると、その場で解決される。標準の開発環境には Unicode stamp が無いので、絵文字の key は「unknown stamp」になる。

`traq <user> <METHOD> <path> [json]` は毎回 traQ に login し直した session で `/api/v3<path>` を叩き、response body を表示する。user のパスワードは `admin` (user 名 `traq`) が `traq`、それ以外は `<user>-password` (`alice` なら `alice-password`)。

### Unicode stamp

`compose.yaml` の traQ は `--skip-init-emojis` で起動するので、Unicode stamp (twemoji の絵文字) が無い。Unicode stamp を使うときは、環境変数 `TRAQ_INIT_EMOJIS=1` を付けて新しく立てる。

```sh
testenv/dev.sh down                        # 既に立てている時だけ。traQ が絵文字を入れるのは DB を初期化する時だけ
TRAQ_INIT_EMOJIS=1 testenv/dev.sh up       # traQ は twemoji を GitHub から取得して約 1700 個の stamp を作り終えてから応答する (回線次第で数十秒から数分)
testenv/dev.sh setup
```

`TRAQ_INIT_EMOJIS=1` は `testenv/compose.emojis.yaml` を重ねて traQ を `--skip-init-emojis` 無しで起動し、traQ が応答するまで最大 30 分待つ。絵文字の取得に失敗しても traQ はそのまま起動するので、stamp が少ない時は `docker compose -p tmb logs traq` で `failed to install unicode emojis` を確かめる。結合テストと CI は `compose.yaml` だけを使うので影響しない。

bridge は最初に CONNECTED になった login の API で stamp の一覧を DB (`traq_stamp`) に取り込み、Unicode stamp の file 名 (`1f44d.svg` など) から絵文字を 100ms に 1 件ずつ解決する (約 1700 個で 3 分ほど)。進み具合は 500 件ごとに、終わると `The stamp table is complete` が log に出る。解決した表は DB に残るので、再起動後は解決し直さない。その途中でも、bridge する message や reaction に付いた Unicode stamp はその場で解決する。file が無いものや file 名が絵文字でないものは、解決できないものとして DB に記録する。meta の読み込みがそれ以外の理由 (traQ が落ちているなど) で 5 回失敗したものは、いったん飛ばして (`Giving up on the emoji of a Unicode stamp for now, it is retried after the others`) 残りを先に解決し、表を完成させてから背景で backoff を空けて読み直し続ける (`retrying` がその数、待つたびに `Retrying the emoji of the Unicode stamps given up on`、全部解決すると `Resolved the emoji of the Unicode stamps that were retried`)。1 回の読み直しは最初に失敗したところで止め (traQ が落ちている間は 1 回に 1 件しか読まない)、次の回は失敗した stamp の次から読む。DB では未解決のままなので、途中で止めても次の起動で読み直す。読み直しが終わるまでは、Matrix からの絵文字の reaction で表に無いものは「unknown stamp」でなく「the emoji to stamp table is still being built」で拒否され、traQ → Matrix の変換はその stamp を 1 分に 1 回だけその場で読み直す (それまでは `:name:`)。`STAMP_CREATED` / `STAMP_UPDATED` で読み直した Unicode stamp の meta が読めなかったときも同じ扱いになる。

### 片付け

```sh
testenv/dev.sh down        # container と volume を消し、testenv/.dev/ も消す
```

## 通しの確認

release の前と、受信・送信・portal の扱いを変えたときに、開発環境で次の表を全部確かめる。Matrix 側は `@dev`、traQ 側は login する `alice` と、相手役の `bob`。

### 準備

```sh
TRAQ_INIT_EMOJIS=1 testenv/dev.sh up        # reaction の行で 👍 (Unicode stamp) を使う
testenv/dev.sh setup
testenv/.dev/cfgedit testenv/.dev/config.yaml 'bridge/split_portals=true'      # Beeper (bbctl) と同じにする。bridge を初めて起動する前に
testenv/.dev/cfgedit testenv/.dev/config.yaml 'backfill/max_initial_messages=10' 'network/poll/interval=1m'   # 任意: 件数を数えやすく、poll を待つ時間を短くする
```

bridge を起動する前に、traQ 側に portal の元を作る。`e2e` (level 2)、その子の `quiet` (level 1)、購読しない `none` (level 0) を作り、`bob` が `e2e` に 15 件 (`max_initial_messages` より多く)、`quiet` と `none` と `alice` への DM に 1 件ずつ投稿する。

```sh
testenv/dev.sh traq alice GET /users/me                                         # alice の ID
testenv/dev.sh traq alice POST /channels '{"name":"e2e","parent":null}'         # 返る id を E2E とする
testenv/dev.sh traq alice POST /channels "{\"name\":\"quiet\",\"parent\":\"$E2E\"}"   # QUIET
testenv/dev.sh traq alice POST /channels '{"name":"none","parent":null}'        # NONE
testenv/dev.sh traq alice PUT "/users/me/subscriptions/$E2E" '{"level":2}'
testenv/dev.sh traq alice PUT "/users/me/subscriptions/$QUIET" '{"level":1}'
for i in $(seq 15); do testenv/dev.sh traq bob POST "/channels/$E2E/messages" "{\"content\":\"history $i\"}"; done
testenv/dev.sh traq bob POST "/channels/$QUIET/messages" '{"content":"quiet hello"}'
testenv/dev.sh traq bob POST "/channels/$NONE/messages" '{"content":"none hello"}'
testenv/dev.sh traq bob POST "/users/<alice の ID>/messages" '{"content":"dm hello"}'
```

`dev.sh traq` は JSON しか送れないので、カスタムスタンプと file は curl の multipart で作る (session の cookie は使い終わったら消す)。

```sh
curl -sS -c /tmp/bob.jar -H 'Content-Type: application/json' -d '{"name":"bob","password":"bob-password"}' http://localhost:3080/api/v3/login
curl -sS -b /tmp/bob.jar -F name=custom -F 'file=@icon.png;type=image/png' http://localhost:3080/api/v3/stamps    # カスタムスタンプ :custom:
curl -sS -b /tmp/bob.jar -F "channelId=$E2E" -F file=@icon.png http://localhost:3080/api/v3/files                # 画像 (返る id を本文に書く)
curl -sS -b /tmp/bob.jar -F "channelId=$E2E" -F file=@note.txt http://localhost:3080/api/v3/files                # 画像以外の file
rm /tmp/bob.jar
```

その後 `testenv/dev.sh bridge` を別の terminal で起動し、表を上から確かめる。room ID は `testenv/dev.sh rooms`、bridge した event は `testenv/dev.sh events '<room>' <n>`、traQ 側は `testenv/dev.sh traq <user> GET ...` で見る。

### 項目

| 項目 | 確かめること | 操作 |
| --- | --- | --- |
| login | bot の DM で `login` → URL を承認 → redirect の URL を貼る → `Successfully logged in` | `bot login`、`tail`、`traq-login alice '<URL>'`、`bot '<redirect URL>'`、`tail` |
| portal | level 2 / level 1 のチャンネルと DM の room ができ、名前がフルパス (`e2e/quiet`)、level 0 の room はできない。level 1 が mute (double puppet ありのとき) | `rooms`。mute は `@dev` の push rule (`GET /_matrix/client/v3/pushrules/`) の `global.room` に、level 1 の room の `actions: []` があること |
| 受信 | traQ で別 user が投稿・編集・削除 → Matrix に message・edit・redaction | `traq bob POST /channels/$E2E/messages`、`PUT /messages/<id>`、`DELETE /messages/<id>`、`events` |
| スタンプ | 閲覧中 (Matrix で既読を付けた) チャンネルで即時、他のチャンネルと DM は `poll.interval` 以内に reaction が付く・消える。DM の編集・削除も `poll.interval` 以内に届く | `read '<e2e の room>' '<event>'` の後に `traq bob POST /messages/<id>/stamps/<stamp ID>` と `DELETE` (stamp ID は `traq alice GET '/stamps?type=unicode'`)。`quiet` と DM の message にも同じことをし、`poll.interval` 待って `events` |
| 送信 | Matrix から投稿・reply・編集・削除、`@name` の pill が traQ で mention になる | `send`、`reply`、`edit`、`redact`、`send-raw` (pill)。`traq bob GET "/channels/$E2E/messages?limit=5"` で reply が先頭行の引用 URL、pill が `!{"type":"user",...}` になっていること |
| reaction | Matrix の 👍 と `:custom:` が traQ のスタンプになる。未知の key は bot の notice (`Your reaction was not bridged: unknown stamp`) | `react '<room>' '<bob の message>' '👍'`、`':custom:'`、`':nosuchstamp:'`。`traq bob GET /messages/<id>/stamps`。未知の key には bot が `Your reaction was not bridged: unknown stamp` の notice を返す。reaction を `redact` するとスタンプが消える |
| 既読 | Matrix の既読で traQ の未読が消える。traQ で読むと Matrix に既読が付く | `traq alice GET /users/me/unread` → `read` → もう一度 `GET`。逆は、閲覧中でないチャンネル (閲覧中のチャンネルは traQ で未読にならない) に `bob` が投稿し、`traq alice DELETE /users/me/unread/<channel ID>`。下の「既読の見方」 |
| file | traQ の画像と file が Matrix に image / file で届く | 準備で作った file の `http://localhost:3080/files/<id>` を本文に書いて `bob` が投稿し、`events` で `m.image` / `m.file` |
| 過去ログ | 新しい portal に `max_initial_messages` 件入る | `e2e` の room に `history 6` から `history 15` の 10 件 (`max_initial_messages=10` のとき) |
| 再接続 | traQ の container を止めて投稿できない間を挟み、再起動後に bridge を止めていた間の投稿が入る | `docker compose -p tmb stop traq` → bridge の log に `Lost the connection to traQ, reconnecting` → `docker compose -p tmb start traq` → `Connected to the traQ WebSocket` と `Catching up on messages missed while disconnected`、その後の投稿が届く。bridge を止めて `e2e`・`quiet`・DM に投稿し、起動し直すとそれが入る |
| 検索 | `search in:#<path> <語>` が結果を返す | `bot 'search in:#e2e history 1'`、`tail`。投稿から約 70 秒は index されていない |

購読レベルの変更 (level 0 で room が消える、level 1 に下げると後から mute が付く) も確かめるときは、`traq alice PUT /users/me/subscriptions/<id> '{"level":0}'` の後に bridge を起動し直す (自分の購読の変更は次の読み直しまで反映しない)。

### 既読の見方

`@dev` の既読は `/sync` の ephemeral の `m.receipt` に出るが、Synapse は同じ request の `/sync` (since 無し) の応答を短い間 cache するので、続けて叩くと古い値が返る。Synapse の DB を直接読むと確実。

```sh
docker compose -p tmb exec -T synapse python3 -c "
import sqlite3
for r in sqlite3.connect('/data/homeserver.db').execute(\"select room_id, event_id from receipts_linearized where user_id='@dev:localhost' and receipt_type='m.read'\"): print(*r)"
```

## `dev.sh` の subcommand

| subcommand | 内容 |
| --- | --- |
| `up` | bridge の binary と `cfgedit` をビルドし、設定と registration が無ければ作り、`docker compose -p tmb --profile matrix up -d` で traQ と Synapse を起動して、両方が応答するまで待つ。`TRAQ_INIT_EMOJIS=1` を付けると traQ に Unicode stamp を入れる (上の「Unicode stamp」) |
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
