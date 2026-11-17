# traq-matrix-bridge 設計

## 概要

traQ (traP の社内 SNS、OSS) と Matrix をつなぐ puppeting bridge を、mautrix-go の bridgev2 で書く。
traQ のチャンネルと DM を Matrix の room にし、Matrix client (iamb など) から traQ を普段どおりに使えるようにする。
1 つの bridge が扱う traQ server は 1 つ (設定の `network.server_url`)。最初の利用者は ex-traQ
(`https://q.ex.trap.jp`) を Beeper の self-hosted bridge (bbctl) として leafeon で動かす。

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

- **ライセンスは MIT**。依存 (mautrix-go: MPL-2.0、go-traq: MIT、coder/websocket: ISC、goldmark: MIT) は
  すべて MIT の project から使える。mautrix-go のファイルは改変しない (MPL-2.0 の義務は改変したファイルに掛かる)。
- **AGPL の bridge (mautrix-slack、mautrix-discord など) のコードは写さない**。bridgev2 の使い方は
  mautrix-go の interface と godoc から書く。他の bridge は挙動を理解するために読むだけにする。
- end-to-bridge encryption は libolm を使う (leafeon の mautrix-discord と同じ)。Nix を使わない build のために
  `-tags goolm` (libolm 無し) でも build できることを README に書く。
- Go と依存の version は flake と `go.mod` で固定する。

## 採用方式

- **言語・framework**: Go + mautrix-go bridgev2。bridge 本体 (Matrix との接続、portal、ghost、backfill の
  queue、bot command、provisioning API) は bridgev2 が持ち、この repository は traQ 側の network connector を書く。
- **traQ の REST**: `traPtitech/go-traq` (OpenAPI の生成物) を `pkg/traq` で薄く包む。
- **traQ の WebSocket** (`/api/v3/ws`): go-traq に無いので `coder/websocket` で自前で書く。

## repository の構成

```
cmd/traq-matrix-bridge/main.go   mxmain に connector を渡して起動するだけ
pkg/connector/                   bridgev2 の NetworkConnector / NetworkAPI の実装
  connector.go    設定、機能一覧、起動
  login.go        OAuth + PKCE の login flow
  client.go       接続、切断、token の更新
  sync.go         購読 → portal、未読の同期、DM 一覧
  handle_traq.go  traQ の event → bridgev2 の remote event
  handle_matrix.go Matrix の message / edit / redaction / reaction / read receipt → traQ
  poller.go       activity timeline を見て直近のメッセージを読み直し、差分を出す
  backfill.go     過去ログ
  commands.go     search command
  ids.go          networkid と traQ の ID の対応
pkg/traq/       REST (go-traq)、WebSocket client、OAuth (PKCE) helper
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
| `UserID` (ghost) | traQ user の UUID。MXID は bbctl の `username_template` (例 `@sh-extraq_<uuid>:beeper.local`) |
| `MessageID` | message の UUID |
| reaction の `EmojiID` | stamp の UUID (1 user × 1 stamp で 1 reaction) |

## 設定 (`network:` 節)

```yaml
network:
  # traQ の origin。API は <server_url>/api/v3
  server_url: https://q.ex.trap.jp
  oauth:
    # 運用者がその traQ に作った public client (scope: read write)
    client_id: ""
    # client の callbackUrl と同じ値。到達できなくてよい (login 時に URL を貼り返す)
    redirect_uri: http://localhost:29399/callback
  poll:
    interval: 3m    # activity timeline を見る間隔
    window: 6h      # 最新メッセージがこの時間内のチャンネルだけ読み直す
    depth: 30       # 読み直す直近のメッセージ数
  view_idle_timeout: 10m          # Matrix で操作が無ければ viewstate を外す
  subscription_sync_interval: 10m # 購読状態を読み直す間隔
```

過去ログの件数は bridgev2 の `backfill` 設定 (`max_initial_messages` など) を使い、`network:` には重複させない。

## login (OAuth 2.0 authorization code + PKCE)

traQ は redirect 先が http(s) 以外なら PKCE (S256) を必須とし、`/oauth2/token` は refresh token を返す。

1. login flow `oauth` を始めると、bridge は `code_verifier` と `state` を作り、
   `<server_url>/api/v3/oauth2/authorize?response_type=code&client_id=…&redirect_uri=…&scope=read+write&state=…&code_challenge=…&code_challenge_method=S256`
   を `user_input` step の説明に出す。入力欄は「承認後に開いた URL」1 つ。
2. user がブラウザで承認すると `redirect_uri` に飛ばされる (ページは開けない)。その URL を bot に貼り返す。
3. bridge は `state` を照合し、`code` と `code_verifier` で `/oauth2/token` を叩いて access token と
   refresh token を得る。`/users/me` で自分の user を引き、`UserLoginID` にする。
4. token は `UserLogin` の metadata (bridge の database) にだけ置く。log に出さない。

`code_verifier` は login 処理の memory にだけあり、外に出ない。貼り返す URL の `code` は verifier 無しでは
token に換えられないので、provisioning API が Beeper (hungryserv) を経由しても token は漏れない。

token は期限の 1 日前と、REST が 401 を返したときに refresh する。refresh が失敗したら bridge state を
`BAD_CREDENTIALS` にし、user に再 login を求める。logout では `/oauth2/revoke` で token を無効にする。

## 受信

traQ の WebSocket は event ごとに届く相手が違う。

| event | 届く相手 |
|---|---|
| `MESSAGE_CREATED` | そのチャンネルを閲覧中の session、通知を付けている人、メンションされた人。`timeline_streaming:on` で全 public チャンネル分 |
| `MESSAGE_UPDATED` / `DELETED` / `STAMPED` / `UNSTAMPED` | そのチャンネルを閲覧中の session だけ |
| `MESSAGE_READ` | 自分 |

閲覧中のチャンネルは `viewstate:<channel>:<state>` で 1 session に 1 つだけ指定できる。traQ の通常チャンネルは
すべて public で、private なのは DM だけ。これに合わせて次のように受信する。

- **WebSocket を 1 本**張り、接続直後に `timeline_streaming:on` を送る。新着はこれと DM で全部届く。
  event は ID しか持たないので、本文などは REST で取る。
- **今読んでいるチャンネル**: Matrix で read receipt (または message の送信) があった room のチャンネルに
  `viewstate:<channel>:monitoring` を送る。そのチャンネルの編集・削除・スタンプは WebSocket で届く。
  `view_idle_timeout` の間 Matrix で操作が無ければ `viewstate:null` を送る (traQ の閲覧者の表示に残り続けない)。
- **それ以外のチャンネル** (poller): `poll.interval` ごとに `/activity/timeline?all=true&per_channel=true` を読み、
  最新メッセージが `poll.window` 内で、portal があり、今 viewstate を向けていないチャンネルについて
  `/channels/{id}/messages?limit=<poll.depth>` を読む。取れたメッセージと database の状態を比べて、
  次の remote event を出す。
  - 本文か `updatedAt` が変わった → 編集
  - database にあり、取れた範囲 (最古〜最新) の中なのに無い → 削除
  - stamp の (user, stamp) の組が増えた・減った → reaction の追加・削除
- **再接続**: WebSocket が切れたら指数 backoff で繋ぎ直し、繋がったら poller を 1 回すぐ回して取りこぼしを拾う。
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
- **Matrix → traQ**: `formatted_body` があれば mautrix-go の HTML parser で Markdown に戻し、ghost の pill を
  `!{"type":"user","raw":"@name","id":"<uuid>"}` にする。無ければ `body` をそのまま送る。
  投稿と編集は `embed: true` で送り、pill を使わずに打った `@name` / `#channel` も traQ に埋め込みへ直させる
  (traQ の web client で打ったときと同じになる)。traQ の上限 (10000 文字) を超える場合は送らずに error を返す。

### 編集・削除

- Matrix の edit (`m.replace`) → `PUT /messages/{id}`。traQ の `MESSAGE_UPDATED` (と poller の差分) → Matrix の edit。
- Matrix の redaction → `DELETE /messages/{id}` (自分の message だけ)。traQ の `MESSAGE_DELETED` → redaction。

### 引用

- **traQ → Matrix**: 本文中の `<server_url>/messages/<uuid>` について、
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

- 本文中の `<server_url>/files/<uuid>` を見つけたら `/files/{id}/meta` と `/files/{id}` を認証付きで取り、
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
- それより前は bridgev2 の backfill queue (Beeper 接続時) が `FetchMessages` を呼ぶ。
  `/channels/{id}/messages?until=<anchor の createdAt>&order=desc&limit=…` で遡る。
- 取り込んだ message にもスタンプを reaction として付ける。

## 失敗時の扱い

- REST の 401: token を refresh して 1 回だけやり直す。だめなら `BAD_CREDENTIALS`。
- REST の 429 / 5xx: `Retry-After` か backoff で待ってやり直す。Matrix からの送信が失敗したら message status で返す。
- WebSocket の切断: bridge state を `TRANSIENT_DISCONNECT` にし、繋ぎ直したら poller で取りこぼしを拾う。
- poller の失敗: log に残し、次の間隔でやり直す。
- 未知の stamp、上限超え、未対応の msgtype: traQ には送らず、理由付きの message status を返す。

## テスト

1. **単体テスト** (`go test ./...`): `pkg/msgconv` (mention・channel link・group・spoiler・引用 → reply の
   判定・Matrix HTML → traQ Markdown)、`pkg/stampconv` (絵文字・`:name:`・未知の key)、検索 query の解釈、
   poller の差分 (編集・削除・stamp 増減・取れた範囲外を削除と誤認しないこと)。
2. **結合テスト** (`go test -tags integration ./pkg/traq/...`): `testenv/compose.yaml` で
   `ghcr.io/traptitech/traq:3.32.1`、MariaDB、Elasticsearch を立てる。テストの中で管理者 (`traq`/`traq`) で
   login し、OAuth client を作り、PKCE で認可して token に換える。その token で投稿・編集・削除、stamp、
   `timeline_streaming` と `viewstate` の event、既読、検索、token の refresh を確かめる。
3. **通しの確認** (手動): leafeon で `sh-extraq` として動かし、Beeper 経由で iamb から目的の操作一式を試す。

CI (GitHub Actions) は PR ごとに `go vet`、単体テスト、結合テスト (compose) を回す。

## 配布と deploy (ex-traQ / leafeon)

1. ex-traQ に public の OAuth client を作る (scope `read` `write`、callback `http://localhost:29399/callback`)。
2. WSL で `bbctl config --type bridgev2 -o /tmp/sh-extraq.yaml sh-extraq` を実行し、`network:` 節を足し、
   logging を info + stdout にして agenix で暗号化する。
3. nixos-config-private: flake input に `traq-matrix-bridge` を足し、`hosts/leafeon/matrix-bridges.nix` の
   `bridges` に追加する (既存の DynamicUser + LoadCredential の unit)。status.kisen.one にも出す。
4. deploy し、bot に DM して `login` する。
