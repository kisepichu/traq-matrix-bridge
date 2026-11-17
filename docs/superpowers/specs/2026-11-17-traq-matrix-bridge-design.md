# traq-matrix-bridge 設計

## 概要

traQ (traP の社内 SNS、OSS) と Matrix をつなぐ puppeting bridge を、mautrix-go の bridgev2 で書く。
traQ のチャンネルと DM を Matrix の room にし、Matrix client (iamb など) から traQ を普段どおりに使えるようにする。
1 つの bridge が扱う traQ server は 1 つ (設定の `network.server_url`)。traQ (`https://q.trap.jp`) にも ex-traQ
(`https://q.ex.trap.jp`) にも、それぞれ bridge を 1 本ずつ動かせば繋げる。homeserver は Matrix の appservice を
登録できるもの (Synapse など) か、Beeper の self-hosted bridge (bbctl) を想定する (「動かし方」の節)。
作者自身の deploy (ex-traQ を Beeper 経由で使う) の手順は作者の環境の repository に置き、ここには書かない。

repository は `kisepichu/traq-matrix-bridge` (public、MIT)。

## 目的

Matrix client から次ができる。

- 未読管理しているチャンネルと DM のメッセージを読み、送り、編集し、消す。
- 未読の一覧を見て、次の未読へ移る (未読は traQ と双方向に同期する)。
- スタンプを付け、外し、誰が押したかを見る。
- 引用元へ飛ぶ。
- メッセージを検索する。
- 画像とファイルを受け取る。

## 対象外 (v1)

- スレッド (traQ の threads API)
- チャンネルツリーを Matrix の space で再現すること (room 名をフルパスにして代える)
- Matrix から新しい DM を始めること、Matrix からのファイル送信
- カスタムスタンプの画像表示 (reaction は `:name:` の文字列にする)
- ピン、クリップ、Qall、typing 通知
- Matrix 側まで含めた自動の end-to-end テスト

## 前提と制約

- **ライセンスは MIT**。依存 (mautrix-go: MPL-2.0、coder/websocket: ISC、goldmark: MIT) は
  すべて MIT の project から使える。mautrix-go のファイルは改変しない (MPL-2.0 の義務は改変したファイルに掛かる)。
- **AGPL の bridge (mautrix-slack、mautrix-discord など) のコードは写さない**。bridgev2 の使い方は
  mautrix-go の interface と godoc から書く。他の bridge は挙動を理解するために読むだけにする。
- end-to-bridge encryption は libolm を使う (mautrix の upstream の build と同じ)。Nix を使わない build のために
  `-tags goolm` (libolm 無し) でも build できることを README に書く。
- Go と依存の version は flake と `go.mod` で固定する。

## 採用方式

- **言語・framework**: Go + mautrix-go bridgev2。bridge 本体 (Matrix との接続、portal、ghost、backfill の
  queue、bot command、provisioning API) は bridgev2 が持ち、この repository は traQ 側の network connector を書く。
- **traQ の REST**: `pkg/traq` に自前の薄い client を書く (`net/http` + `encoding/json`、使う field だけの型)。
  go-traq (OpenAPI の生成物) は使わない。go-traq の model は未知の JSON field があると decode に失敗する
  (`DisallowUnknownFields`) ので、traQ server が field を足すたびに bridge が壊れる。
- **traQ の WebSocket** (`/api/v3/ws`): `coder/websocket` で書く。`Authorization: Bearer` で接続できる。

## repository の構成

```
cmd/traq-matrix-bridge/main.go   mxmain に connector を渡して起動するだけ
pkg/connector/                   bridgev2 の NetworkConnector / NetworkAPI の実装
  connector.go    設定、機能一覧、起動
  login.go        OAuth + PKCE の login flow
  client.go       接続、切断、WebSocket の維持
  sync.go         購読 → portal、未読の同期、DM 一覧
  handle_traq.go  traQ の event → bridgev2 の remote event
  handle_matrix.go Matrix の message / edit / redaction / reaction / read receipt → traQ
  poller.go       activity timeline を見て直近のメッセージを読み直し、差分を出す
  backfill.go     過去ログ
  commands.go     search command
  ids.go          networkid と traQ の ID の対応
pkg/traq/       REST client、WebSocket client、OAuth (PKCE) helper
pkg/msgconv/    traQ の Markdown・埋め込み ↔ Matrix の本文 (副作用の無い関数)
pkg/stampconv/  スタンプ ↔ reaction の key (副作用の無い関数)
testenv/        結合テスト用の traQ (compose.yaml)
flake.nix       package (buildGoModule)、開発環境
```

## ID

| bridgev2 | traQ |
|---|---|
| `UserLoginID` | login した traQ user の UUID |
| `PortalID` | channel の UUID (DM は DM channel の UUID)。DM の PortalKey は receiver に login を入れる |
| `UserID` (ghost) | traQ user の UUID。MXID は bridge 設定の `appservice.username_template` で決まる (例: `traq_{{.}}` なら `@traq_<uuid>:<homeserver の domain>`) |
| `MessageID` | message の UUID |
| reaction の `EmojiID` | stamp の UUID (1 user × 1 stamp で 1 reaction) |

## 設定 (`network:` 節)

```yaml
network:
  # traQ の origin。API は <server_url>/api/v3
  server_url: https://q.trap.jp
  oauth:
    # 運用者がその traQ に作った public client (scope: read write)
    client_id: ""
    # client の callbackUrl と同じ値。到達できなくてよい (login 時に URL を貼り返す)
    redirect_uri: http://localhost:29399/callback
  poll:
    interval: 3m      # 読み直しの間隔
    window: 6h        # 最終書き込みがこの時間内のチャンネルだけ読み直す
    depth: 30         # 1 チャンネルで読み直す直近のメッセージ数
    max_channels: 50  # 1 回に読み直すチャンネル数の上限 (最終書き込みの新しい順)
  view_idle_timeout: 10m          # Matrix で操作が無ければ viewstate を外す
  subscription_sync_interval: 10m # 購読状態を読み直す間隔
```

過去ログの件数は bridgev2 の `backfill` 設定 (`max_initial_messages` など) を使い、`network:` には重複させない。

## login (OAuth 2.0 authorization code + PKCE)

traQ は redirect 先が http(s) 以外なら PKCE (S256) を必須とする。token endpoint は access token だけを返し
(有効期間 `expires_in: 31536000` = 1 年)、refresh token は返さない (traQ 3.32.1 で確認)。

1. login flow `oauth` を始めると、bridge は `code_verifier` と `state` を作り、
   `<server_url>/api/v3/oauth2/authorize?response_type=code&client_id=…&redirect_uri=…&scope=read+write&state=…&code_challenge=…&code_challenge_method=S256`
   を `user_input` step の説明に出す。入力欄は「承認後に開いた URL」1 つ。
2. user がブラウザで承認すると `redirect_uri` に飛ばされる (ページは開けない)。その URL を bot に貼り返す。
3. bridge は `state` を照合し、`code` と `code_verifier` で `/oauth2/token` を叩いて access token を得る。
   `/users/me` で自分の user を引き、`UserLoginID` にする。
4. token は `UserLogin` の metadata (bridge の database) にだけ置く。log に出さない。

`code_verifier` は login 処理の memory にだけあり、外に出ない。貼り返す URL の `code` は verifier 無しでは
token に換えられないので、provisioning API が homeserver などの中継 (Beeper なら hungryserv) を通っても token は漏れない。

token が期限切れになるか、REST / WebSocket が 401 を返したら、bridge state を `BAD_CREDENTIALS` にして user に
再 login を求める。logout では `/oauth2/revoke` で token を無効にする。

## 受信

traQ の WebSocket は event ごとに届く相手が違う。

| event | 届く相手 |
|---|---|
| `MESSAGE_CREATED` | そのチャンネルを閲覧中の session、通知を付けている人、メンションされた人。`timeline_streaming:on` で全 public チャンネル分 |
| `MESSAGE_UPDATED` / `DELETED` | 仕様上は閲覧中の session だけ。**実測 (3.32.1) では `timeline_streaming:on` で public チャンネルの分も届く。DM の分は届かない** |
| `MESSAGE_STAMPED` / `UNSTAMPED` | 閲覧中の session だけ (実測でも、public チャンネル・DM とも viewstate が要る) |
| `MESSAGE_READ` | 自分 |

閲覧中のチャンネルは `viewstate:<channel>:<state>` で 1 session に 1 つだけ指定できる。traQ の通常チャンネルは
すべて public で、private なのは DM だけ。これに合わせて次のように受信する。

- **WebSocket を 1 本**張り、接続直後に `timeline_streaming:on` を送る。新着は全部、public チャンネルの編集・削除も
  これで届く。
  event は ID しか持たないので、本文などは REST で取る。
- **今読んでいるチャンネル**: Matrix で read receipt (または message の送信) があった room のチャンネルに
  `viewstate:<channel>:monitoring` を送る。そのチャンネルの編集・削除・スタンプは WebSocket で届く。
  `view_idle_timeout` の間 Matrix で操作が無ければ `viewstate:null` を送る (traQ の閲覧者の表示に残り続けない)。
- **それ以外のチャンネル** (poller): 新着は全部 bridge に届くので、bridge は portal ごとの最終書き込み時刻を
  知っている (起動時は database の最新 message から、その後は受け取った event から)。`poll.interval` ごとに、
  最終書き込みが `poll.window` 内で、今 viewstate を向けていない portal を新しい順に最大 `poll.max_channels` 件選び、
  `/channels/{id}/messages?limit=<poll.depth>` を読む。DM もこれで対象になる。取れたメッセージと database の
  状態を比べて、次の remote event を出す。
  - 本文か `updatedAt` が変わった → 編集
  - database にあり、取れた範囲 (最古〜最新) の中なのに無い → 削除
  - stamp の (user, stamp) の組が増えた・減った → reaction の追加・削除
  
  `/activity/timeline` は候補選びに使わない。`limit` が最大 50 で、public チャンネルしか返さない (DM が漏れる) ため。
- **再接続**: WebSocket が切れたら指数 backoff で繋ぎ直す。繋がったら、切れていた間の新着を拾うため、
  `/users/me/unread` に出ているチャンネルと `/activity/timeline?per_channel=true` (購読チャンネル) に出ている
  チャンネルのうち、最終書き込みが切断時刻より後の portal について、database の最新 message より後を取り込む。
  `PING` を定期的に送り、応答が無ければ切って繋ぎ直す。
- **user / stamp の変更**: `USER_UPDATED` / `USER_ICON_UPDATED` で ghost の表示名とアイコンを、
  `STAMP_CREATED` / `UPDATED` / `DELETED` で stamp の cache を更新する。

## チャンネルと room

- portal にするのは、購読レベル 1 (未読管理) 以上のチャンネルと DM (`/channels?include-dm=true`)。
  購読状態は接続時と `subscription_sync_interval` ごとに `/users/me/subscriptions` から読む。
- 購読レベル 2 (未読管理 + 通知) は通常の room、1 は通知を切った room (double puppet の room 設定)。
- 購読を 0 にしたチャンネルは portal を消す。購読し直したら作り直し、過去ログを取り込む。
- room 名はフルパス (`#` 無しの `gps/times/kise`)、topic はチャンネルの topic。DM は相手の表示名。
- ghost の表示名は traQ の `displayName`、アイコンは `/public/icon/{name}` (認証不要)。bot user は表示名に `(bot)` を付ける。

## メッセージの変換

### 本文

- **traQ → Matrix**: traQ の Markdown を goldmark (GFM、raw HTML 無効) で HTML にし `formatted_body` に、
  plain text を `body` にする。traQ 固有の記法は次のとおり。
  - 埋め込み `!{"type":"user","raw":"@name","id":"<uuid>"}` → ghost への mention (pill)。
  - `!{"type":"channel",...}` → portal があればその room への link、無ければ traQ の channel URL。
  - `!{"type":"group",...}` → `@group` の文字列。自分が member なら自分への mention として扱う (group の member は cache)。
  - `!!spoiler!!` → Matrix の spoiler。インラインのスタンプ `:name:` と数式は文字のまま。
  - 自分 (user か所属 group) への mention を含むときは `m.mentions` に自分を入れる。
- **Matrix → traQ**: `formatted_body` があれば mautrix-go の HTML parser で Markdown に戻し、ghost (と bridge に login
  している Matrix user) の pill を `@name` にする。無ければ `body` をそのまま送る。
  投稿と編集は `embed: true` で送り、pill から作った `@name` も、pill を使わずに打った `@name` / `#channel` も、traQ に
  埋め込み `!{"type":"user","raw":"@name","id":"<uuid>"}` へ直させる (traQ の web client で打ったときと同じになる)。
  pill を自分で埋め込みにはしない: traQ (3.32.1) は既存の埋め込みの `raw` の中の `@name` も埋め込みに直すので、二重になる。
  traQ の上限 (10000 文字) を超える場合は送らずに error を返す。

### 編集・削除

- Matrix の edit (`m.replace`) → `PUT /messages/{id}`。traQ の `MESSAGE_UPDATED` (と poller の差分) → Matrix の edit。
- Matrix の redaction → `DELETE /messages/{id}` (自分の message だけ)。traQ の `MESSAGE_DELETED` → redaction。

### 引用

- **traQ → Matrix**: 本文中の `<server_url>/messages/<uuid>` と `!{"type":"message",...,"id":"<uuid>"}` について、
  - 同じチャンネルで portal に bridge 済みの message への最初の引用 → Matrix の reply relation にし、本文からその URL を消す。
  - それ以外 → bridge 済みならその Matrix event への link (matrix.to)、未 bridge なら traQ の URL のまま。
- **Matrix → traQ**: reply の相手が bridge 済みの traQ message なら、その URL を本文の先頭行に付ける。

### スタンプ

- **traQ → Matrix**: (user, stamp) ごとに reaction を 1 つ付ける。押した回数は捨てる。
  - Unicode のスタンプ → その絵文字。
  - カスタムスタンプ → 文字列 `:name:`。
- **Matrix → traQ**: reaction の key が
  - 絵文字 → 対応する Unicode スタンプ
  - `:name:` か `name` → その名前のスタンプ
  - どれでもない → 送らずに message status で error を返す
  
  reaction の redaction → `DELETE /messages/{id}/stamps/{stampId}`。
- スタンプの一覧は接続時に `GET /stamps` で読み、cache する。
- **Unicode スタンプと絵文字の対応は traQ 自身から作る**。traQ は Unicode スタンプを twemoji の SVG から作っていて
  (`traQ/utils/twemoji/installer.go`)、名前は JoyPixels の shortname、file 名は codepoint (`1f44d.svg`、
  複数なら `-` 区切り) になる。そこで `isUnicode` なスタンプの `/files/{fileId}/meta` の `name` を
  codepoint として読み、絵文字の文字列に直す。
  - 対応表は connector 自身の database table (stamp ID、名前、`isUnicode`、絵文字) に持つ。
  - 初回の接続時に背景で作る (数千件の meta を間隔を空けて読む)。`STAMP_CREATED` で増えた分を足す。
  - 比べるときは双方の異体字セレクタ (U+FE0F) を取り除く。
  - 対応表ができるまでは、traQ → Matrix は `:name:` で代え、Matrix → traQ の絵文字は error を返す。
  - JoyPixels の emoji.json は OSS の license ではない (JoyPixels Free License) ので、repository に同梱しない。

### ファイル

- 本文中の `<server_url>/files/<uuid>` と `!{"type":"file",...,"id":"<uuid>"}` を見つけたら `/files/{id}/meta` と `/files/{id}` を認証付きで取り、
  画像は `m.image`、それ以外は `m.file` として本文とは別の part で送る。本文からその URL を消す。
  bridgev2 の最大 file size を超えるものは、traQ の URL を残して上げない。
- Matrix からのファイル (`m.image` などの msgtype) は送らずに error を返す。

## 既読

- **Matrix → traQ**: read receipt で `DELETE /users/me/unread/{channelId}` (`readChannel`) を送る。traQ の既読は
  チャンネル単位なので、Matrix で途中まで読んだ場合もチャンネル全体が既読になる。
- **traQ → Matrix**: `MESSAGE_READ` で、その room の最新の bridge 済み message に自分の read receipt を付ける (double puppet)。
- **接続時**: `/users/me/unread` に無い portal は最新 message まで既読にする。

## 検索

- bot command `search <query>`。`in:#<channel path>` と `from:@<name>` を解釈し、残りを語として
  `GET /messages` (`searchMessages`) に渡す。新しい順に 20 件返す。
- 各結果は 1 行: `#channel  @user  日時  抜粋 (80 文字)  link`。link は bridge 済みなら Matrix event、無ければ traQ の URL。
- traQ の検索機能が無効な server (`503`) では、そう返す。

## 過去ログ

- portal を作るときは bridgev2 の `max_initial_messages` 件を取り込む。
- それより前は bridgev2 の backfill queue が `FetchMessages` を呼び、
  `/channels/{id}/messages?until=<anchor の createdAt>&order=desc&limit=…` で遡る。
  backfill queue は、既存の room の履歴に message を差し込める homeserver (Beeper の hungryserv) でだけ動く。
  標準の homeserver (Synapse など) では、room を作ったときの `max_initial_messages` 件までしか入らない。
- 取り込んだ message にもスタンプを reaction として付ける。

## 失敗時の扱い

- REST / WebSocket の 401: `BAD_CREDENTIALS` にし、再 login を求める。
- REST の 429 / 502 / 504: `Retry-After` か backoff で待ってやり直す。ただし POST (投稿、スタンプ) は 429 のときだけやり直す (502 / 504 では traQ が保存済みのことがあり、二重投稿になる)。Matrix からの送信が失敗したら message status で返す。
- WebSocket の切断: bridge state を `TRANSIENT_DISCONNECT` にし、繋ぎ直したら「受信」の再接続の手順で取りこぼしを拾う。
- poller の失敗: log に残し、次の間隔でやり直す。
- 未知の stamp、上限超え、未対応の msgtype: traQ には送らず、理由付きの message status を返す。

## テスト

1. **単体テスト** (`go test ./...`): `pkg/msgconv` (mention・channel link・group・spoiler・引用 → reply の
   判定・Matrix HTML → traQ Markdown)、`pkg/stampconv` (絵文字・`:name:`・未知の key)、検索 query の解釈、
   poller の差分 (編集・削除・stamp 増減・取れた範囲外を削除と誤認しないこと)。
2. **結合テスト** (`go test -tags integration ./pkg/traq/...`): `testenv/compose.yaml` で
   `ghcr.io/traptitech/traq:3.32.1`、MariaDB、Elasticsearch を立てる。テストの中で管理者 (`traq`/`traq`) で
   login し、OAuth client を作り、PKCE で認可して token に換える。その token で投稿・編集・削除、stamp、
   `timeline_streaming` と `viewstate` で届く event (「受信」の表の実測どおりであること)、既読、検索を確かめる。
3. **通しの確認** (手動): 実際の traQ と homeserver に繋ぎ、Matrix client から「目的」の操作一式を試す。

CI (GitHub Actions) は PR ごとに `go vet`、単体テスト、結合テスト (compose) を回す。

## 動かし方 (運用者向け)

README に次を書く。

- **traQ の OAuth client**: 運用者がその traQ に public client を 1 つ作る (scope `read` `write`、
  callback は設定の `redirect_uri`)。作るには traQ の `create_client` 権限が要る。
- **homeserver**: 次のどちらか。
  - Matrix の appservice を登録できる homeserver: `traq-matrix-bridge -e` で設定の例を出し、`-g` で
    registration を作って homeserver に登録する (mautrix の bridge と同じ手順)。
  - Beeper: `bbctl config --type bridgev2 -o config.yaml sh-<name>` で設定を作り、`network:` 節を足す。
- **double puppeting**: 自分の発言を自分の Matrix account の発言として出すこと、自分の既読、通知を切った room
  (購読レベル 1) は double puppeting で動く。bridgev2 の `double_puppet` 設定が要る。
  Beeper では bbctl の生成した設定に入っている。無い場合、自分の発言が ghost として出て、既読と通知の設定が付かない。
- **encryption**: bridgev2 の `encryption` 設定に従う。Beeper は暗号化を必須にしている。
- **build**: `nix build` (flake の package)、または `go build ./cmd/traq-matrix-bridge` (libolm が要る。
  無ければ `-tags goolm`)。
