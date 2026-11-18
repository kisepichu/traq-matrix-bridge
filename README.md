# traq-matrix-bridge

[traQ](https://github.com/traPtitech/traQ) と Matrix をつなぐ puppeting bridge。[mautrix-go](https://github.com/mautrix/go) の bridgev2 の network connector として書いている。

traQ のチャンネルと DM を Matrix の room にし、Matrix client から traQ を普段どおりに使えるようにする。1 つの bridge が扱う traQ server は 1 つ (設定の `network.server_url`)。traQ (`https://q.trap.jp`) と ex-traQ (`https://q.ex.trap.jp`) の両方に繋ぐなら、bridge を 1 本ずつ動かす。

## できること

- 購読レベル 1 (未読管理) 以上のチャンネルと DM の message を読み、送り、編集し、消す。
- 未読を traQ と双方向に同期する (Matrix で既読を付けると traQ の未読が消え、traQ で読むと Matrix に既読が付く)。
- スタンプを付け、外し、誰が押したかを見る (Matrix の reaction になる)。
- 引用元へ飛ぶ (同じチャンネルの message の引用は Matrix の reply になる)。
- message を検索する (bot の `search` command)。
- 画像とファイルを受け取る。
- 停止中・切断中に投稿された message を、起動・再接続のときに取り込む。

## できないこと

- スレッド (traQ の threads API)
- チャンネルツリーを Matrix の space で再現すること (代わりに room 名をフルパスにする)
- Matrix から新しい DM を始めること、Matrix からのファイル送信 (送ると error が返る)
- カスタムスタンプの画像表示 (reaction は `:name:` の文字列になる)
- ピン、クリップ、Qall、typing 通知
- relay mode (bridgev2 の `set-relay`)。`bridge.relay.enabled` は `false` のままにする

## build

```sh
nix build                                                   # flake の package。result/bin/traq-matrix-bridge (libolm 込み)
go build ./cmd/traq-matrix-bridge                           # libolm (ヘッダとライブラリ) が要る
go build -tags goolm ./cmd/traq-matrix-bridge               # libolm 無し (pure Go の olm 実装)
```

end-to-bridge encryption は libolm を使う。libolm を入れられない環境では `-tags goolm` で build する。

## 動かし方

### 1. traQ の OAuth client を作る

運用者がその traQ に public client を 1 つ作る。作るには traQ の `create_client` 権限が要る。

| 項目 | 値 |
| --- | --- |
| scope | `read` と `write` |
| callback URL | 設定の `network.oauth.redirect_uri` と同じ値 (既定 `http://localhost:29399/callback`)。到達できなくてよい。login のときに user がその URL を bot に貼る |
| confidential | `false` (public client。client secret は使わない) |

traQ の API で作るなら、traQ に login した session で `POST /api/v3/clients` を送る:

```json
{"name": "traq-matrix-bridge", "description": "Matrix bridge", "callbackUrl": "http://localhost:29399/callback", "scopes": ["read", "write"], "confidential": false}
```

返ってきた `id` を `network.oauth.client_id` に書く。

### 2. 設定を作り、homeserver に繋ぐ

次のどちらか。

#### appservice を登録できる homeserver (Synapse など)

mautrix の bridge と同じ手順。

```sh
traq-matrix-bridge -c config.yaml -e                        # 設定の例を config.yaml に書き出す
$EDITOR config.yaml                                         # homeserver、appservice、database、bridge.permissions、network: 節 (下の「設定」)
traq-matrix-bridge -c config.yaml -r registration.yaml -g   # registration.yaml を作り、as_token / hs_token を config.yaml に書き戻す
# registration.yaml を homeserver に登録して homeserver を再起動する (Synapse なら app_service_config_files)
traq-matrix-bridge -c config.yaml      # 起動
```

#### Beeper (bbctl)

```sh
bbctl config --type bridgev2 -o config.yaml sh-<name>       # flag は bridge 名の前に書く
$EDITOR config.yaml                                         # network: 節を足し、下の「bridgev2 の設定」を確かめる
traq-matrix-bridge -c config.yaml                           # 起動 (registration は bbctl が Beeper に登録する)
```

bbctl が生成する設定は `bridge.split_portals: true` で、double puppeting と暗号化 (Beeper では必須) が入っている。

### 3. 設定

#### `network:` 節

```yaml
network:
    # traQ の origin (path 無し)。API は <server_url>/api/v3
    server_url: https://q.trap.jp
    oauth:
        # 1. で作った client の ID
        client_id: ""
        # client の callback URL と同じ値
        redirect_uri: http://localhost:29399/callback
    # 閲覧していないチャンネルと DM を読み直して、編集・削除・スタンプの変更を拾う
    poll:
        interval: 3m      # 読み直しの間隔
        window: 6h        # 最終書き込みがこの時間内のチャンネルだけ読み直す
        depth: 30         # 1 チャンネルで読み直す直近の message 数 (1-200)
        max_channels: 50  # 1 回に読み直すチャンネル数の上限 (最終書き込みの新しい順)
    # Matrix で操作が無ければ、この時間で traQ の viewstate (閲覧中のチャンネル) を外す
    view_idle_timeout: 10m
    # 購読状態を traQ から読み直す間隔
    subscription_sync_interval: 10m
```

#### bridgev2 の設定

`network:` の外 (bridgev2 の設定) で、次を設定するか確かめる。`backfill.enabled` が `false`、`bridge.async_events` が `true` のときは起動時に warning を log に出す。

| key | 値 | 理由 |
| --- | --- | --- |
| `backfill.enabled` | `true` (必須) | `false` だと新しい room に過去ログが入らず、停止中・切断中の message の取り込み (catch-up) も動かない。bridgev2 の既定は `false` |
| `backfill.max_initial_messages` | 控えめに (既定 50) | room を作るときに取り込む件数。最初の login で購読チャンネル全部について取り込む (画像・ファイルのダウンロードとアップロードを含む) |
| `backfill.max_catchup_messages` | 既定 500 | 停止中・切断中の取り込みの、portal ごとの上限。超えた分は古い方から入らない |
| `bridge.async_events` | `false` (必須、既定) | portal ごとに event を 1 つずつ順に処理することを前提にしている (echo の判定、live の message より先に catch-up を処理すること) |
| `bridge.mute_only_on_create` | `false` | `true` (既定) だと購読レベル 1 の mute を room を作るときにしか付けず、後で購読レベルを変えても反映しない |
| `bridge.split_portals` | どちらでも | `false` なら公開チャンネルの room を login で共有し、`true` (bbctl の既定) なら login ごとに別の room にする。bridgev2 の注意どおり、portal がある状態で切り替えない |
| `bridge.relay.enabled` | `false` (既定) | relay mode は対応していない |
| `bridge.permissions` | 使わせる user / server に `user` 以上 | bridgev2 の標準 |
| `double_puppet` | 設定する | 下の「double puppeting」 |
| `encryption` | 必要に応じて | bridgev2 の `encryption` 設定に従う。Beeper は必須 |

#### double puppeting

自分の発言を自分の Matrix account の発言として出すこと、自分の既読、購読レベル 1 の room の mute (通知を切る push rule) は double puppeting で動く。bridgev2 の `double_puppet` 設定が要る (appservice 方式の手順は https://docs.mau.fi/bridges/general/double-puppeting.html)。Beeper では bbctl が生成した設定に入っている。無い場合、自分の発言が ghost として出て、既読と mute が付かない。

## login

bridge の bot (appservice 方式の既定は `@traqbot:<homeserver の domain>`) との DM (management room) で:

1. `login` を送る。bot が traQ の authorize URL を返す。
2. その URL を browser で開き、traQ で bridge を承認する。
3. browser は callback URL (`http://localhost:29399/callback?code=...&state=...`) に移り、ページは開けない。その address bar の URL を全部コピーして bot に貼る。
4. bot が `Successfully logged in as @<traQ の ID>` と返せば完了。room が作られ、過去ログが入る。

貼る URL には認可 code が入っていて、management room の履歴に残る。code は bridge の memory にだけある PKCE の `code_verifier` が無いと token に換えられないので、room を読める人や homeserver (Beeper の hungryserv など) からは token は漏れない。

traQ は refresh token を出さず、access token の有効期間は 1 年。token が切れるか traQ が 401 を返すと、bridge state が `BAD_CREDENTIALS` になるので、もう一度 `login` する。login の一覧は `list-logins`、解除は `logout <login ID>` (traQ 側でも token を revoke する)。

## 使い方と挙動

### チャンネルと room

- room にするのは、購読レベル 1 (未読管理) 以上のチャンネルと DM。room 名はチャンネルのフルパス (`gps/times/kise`)、DM は相手の表示名。
- 購読レベル 2 (未読管理 + 通知) は通常の room、1 は通知を切った room (double puppeting が要る)。
- 購読を 0 にしたチャンネルは、その room に他の login がいなければ room を消す (`bridge.split_portals: false` で複数の login が公開チャンネルの portal を共有しているときは、他の login のために room が残る)。room の Matrix の履歴と bridge の記録も消える。購読し直すと room を作り直し、`max_initial_messages` 件だけ取り込み直す。1 回の読み直しで公開チャンネルの room の半分より多く (かつ 5 件以上) が消える応答は、traQ の一時的な異常とみなしてその回は消さず、warning を log に出す。購読が 1 件もない snapshot で公開 portal が消える場合も同様に保留する。次の読み直しでも同じなら消す。
- 購読状態は接続時と `subscription_sync_interval` ごとに読む。チャンネルの変更 event (`CHANNEL_UPDATED` / `DELETED`) でも読み直す (前回から 30 秒は空ける)。traQ で自分の購読レベルを変えたことは、次の読み直しか再接続のときに反映する。
- DM の room は遅延して作る。接続時に作るのは未読のある DM だけで、他は最初の新着で作る (traQ は DM のページを開いただけで DM チャンネルを作るため)。
- room の作成・更新は 1 秒に 4 件までに抑える。購読の多い account では、最初の login から全部の room ができるまで時間がかかる。

### 閲覧中のチャンネル

Matrix で room に既読を付ける、message を送る、reaction を付け外しすると、bridge は traQ の viewstate (閲覧中のチャンネル) をその room のチャンネルに向ける。1 login で同時に 1 チャンネルだけで、Matrix で操作が無いまま `view_idle_timeout` (既定 10 分) 経つと外す。

- 閲覧中のチャンネルの編集・削除・スタンプは WebSocket ですぐ届く。
- **閲覧中のチャンネルに来た新着は、traQ では未読にならない** (traQ が閲覧中の session があるチャンネルを未読にしないため)。Matrix で見ているチャンネルを traQ の web client で後から開いても、その間の message は未読として出ない。
- traQ の閲覧者の一覧にも、その間は自分が出る。

### 閲覧していないチャンネルと DM

- 新着は全部すぐ届く。公開チャンネルの編集・削除もすぐ届く。
- DM の編集・削除と、閲覧していないチャンネルと DM のスタンプは WebSocket で届かないので、poller が拾う。`poll.interval` ごとに、最終書き込みが `poll.window` 内のチャンネルを新しい順に最大 `poll.max_channels` 件、直近 `poll.depth` 件の message を読み直して差分を出す。この範囲の外の変更 (古い message の編集・削除・スタンプ、静かなチャンネルの変更) は bridge されない。WebSocket が切れていた間の削除も、この範囲の中のものしか拾わない。

### 停止中・切断中の message

起動時と WebSocket の再接続時に、未読のあるチャンネル、切断中に投稿のあったチャンネル、全 DM について、bridge が知る最新の message より後を取り込む (`backfill.enabled: true` が要る)。1 portal で `backfill.max_catchup_messages` 件を超えた分は古い方から入らない。

### 既読

- Matrix で既読を付けると、traQ のチャンネル全体が既読になる (traQ の既読はチャンネル単位なので、途中まで読んだときも全部既読になる)。
- traQ で読むと、その room の最新の message に自分の既読が付く (double puppeting が要る)。

### スタンプ

- traQ → Matrix: (user, stamp) ごとに reaction 1 つ。Unicode スタンプはその絵文字、カスタムスタンプは `:name:`。押した回数は捨てる。
- Matrix → traQ: reaction の key が絵文字なら対応する Unicode スタンプ、`:name:` か `name` ならその名前のスタンプ。どれでもなければ送らず、理由を返す (`Your reaction was not bridged: unknown stamp: ...`)。
- Unicode スタンプと絵文字の対応表は、最初の login の後に bridge が traQ から作る (stamp ごとに 100ms 空けて file の meta を読む。約 1700 個で約 3 分、q.trap.jp のようにスタンプの多い server ではもっとかかる)。終わると log に `The stamp table is complete` が出る。表は database に残るので、作るのは 1 回だけ。
  - 作っている間も、traQ → Matrix は出会った stamp をその場で解決するので絵文字になる。
  - Matrix → traQ では、まだ解決していない絵文字の reaction は `the emoji to stamp table is still being built` で拒否される。

### ファイル

traQ の画像は `m.image`、それ以外は `m.file` として本文とは別の message で届く。homeserver の upload 上限を超えるものは、traQ の URL を本文に残す。

## 検索

bot の `search <query>` が traQ の message を検索し、新しい順に最大 20 件を返す。management room では prefix なしで使えるが、それ以外の room では `!traq search <query>` と prefix を付ける (既定の prefix は `!traq`)。portal では、その room の login で検索する。

```
search in:#gps/times from:@bob hello
search "two words" from:@alice
```

- `in:#<チャンネルのフルパス>` でチャンネル、`from:@<ID>` で投稿者を絞る。残りが検索語 (`"..."` で空白を含む語を 1 語にできる)。
- 1 件 1 行: `#チャンネル  @user  日時  抜粋  link`。DM の hit は `#チャンネル` の代わりに `DM` と出す。link は bridge 済みの message なら Matrix の event、そうでなければ traQ の URL。
- 日時は bridge の process の time zone で出す。container で動かすなら `TZ` (例: `TZ=Asia/Tokyo`) を設定する。
- traQ の検索は Elasticsearch が index してから効くので、投稿直後の message は出ないことがある。検索機能が無効な traQ ではそう返す。

## 開発

開発環境 (traQ + Synapse の docker と `testenv/dev.sh`)、テスト、通しの確認の手順は [docs/development.md](docs/development.md)。設計は [docs/superpowers/specs/2026-11-17-traq-matrix-bridge-design.md](docs/superpowers/specs/2026-11-17-traq-matrix-bridge-design.md)。

## License

MIT ([LICENSE](LICENSE))。依存の mautrix-go は MPL-2.0 (改変していない)。
