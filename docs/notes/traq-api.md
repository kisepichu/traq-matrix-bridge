<!--
調査メモ (英語のまま)。この repository は go-traq を使わない (未知の JSON field で decode に失敗するため、
`pkg/traq` に自前の client を書く)。go-traq 固有の記述 (§1 の Configuration、§2 の呼び出し方) は読み飛ばしてよい。
HTTP の振る舞い、model の field、WebSocket、埋め込みの形式 (§2 の query / body の表、§3、§5、§6) は有効。

traQ 3.32.1 (ghcr.io/traptitech/traq:3.32.1) での実測 (2026-11-17) — この memo の記述と食い違うところ:
- POST /oauth2/token (authorization_code + PKCE、public client) は `{"access_token", "token_type":"Bearer",
  "expires_in":31536000, "scope":null}` を返す。refresh_token は返らない。
- WebSocket で `timeline_streaming:on` だけ (viewstate なし) の session にも、public チャンネルの
  MESSAGE_UPDATED / MESSAGE_DELETED は届く (他人の message でも)。MESSAGE_STAMPED / UNSTAMPED は届かない。
- DM では、viewstate なしの session に届くのは MESSAGE_CREATED (と新しい DM の CHANNEL_CREATED) だけ。
  MESSAGE_UPDATED / DELETED / STAMPED は届かない。
- `viewstate:<channel>:monitoring` を送ると、そのチャンネルの STAMPED / UNSTAMPED も届く。
- `GET /users/me/unread` には DM も出る (購読していなくても)。
- `GET /authorize` は consent 画面 (`<origin>/consent?...`) へ 302 し、`POST /oauth2/authorize/decide`
  (form `submit=approve`、同じ cookie) が `redirect_uri?code=...&state=...` へ 302 する。
-->

# go-traq digest for a traQ ⇄ Matrix bridge

Sources checked:
- go-traq clone `/tmp/research/go-traq` at commit `4133eadfad06851b9147ab023b376baa1551c61b`.
- Spec `/tmp/traq-v3.yaml`. The spec decides HTTP semantics.
- traQ server sources, fetched raw from `github.com/traPtitech/traQ@master`: `utils/message/{embedded,parser,replacer,nonce}.go`, `router/v3/router.go`, `router/middlewares/{user_authenticate,rate_limit}.go`, `cmd/root.go`, `cmd/config.go`.

Lines marked `[INFERENCE]` were not read directly from a source.

---

## 1. Module, configuration, auth

`go.mod`:
```
module github.com/traPtitech/go-traq
go 1.22
toolchain go1.22.2
require ( golang.org/x/oauth2 v0.20.0; gopkg.in/validator.v2 v2.0.1 )
```
Import it as `traq "github.com/traPtitech/go-traq"` (package `traq`).

### Configuration (`configuration.go`)
```go
type ServerConfiguration struct { URL string; Description string; Variables map[string]ServerVariable }
type ServerConfigurations []ServerConfiguration
type Configuration struct {
	Host             string            `json:"host,omitempty"`
	Scheme           string            `json:"scheme,omitempty"`
	DefaultHeader    map[string]string `json:"defaultHeader,omitempty"`
	UserAgent        string            `json:"userAgent,omitempty"`
	Debug            bool              `json:"debug,omitempty"`   // httputil.DumpRequestOut/DumpResponse to log
	Servers          ServerConfigurations
	OperationServers map[string]ServerConfigurations
	HTTPClient       *http.Client
}
func NewConfiguration() *Configuration            // Servers default: [q.trap.jp/api/v3, q-dev.trapti.tech/api/v3, localhost:3000/api/v3]
func (c *Configuration) AddDefaultHeader(key string, value string)
func NewAPIClient(cfg *Configuration) *APIClient  // cfg.HTTPClient nil → http.DefaultClient
```
- Request URLs are built from `Servers[idx].URL`. `idx` defaults to 0 and can be overridden with context keys `traq.ContextServerIndex` (int), `traq.ContextOperationServerIndices` (map[string]int), and `traq.ContextServerVariables`.
- **To use an arbitrary server, replace `Servers`** with a single entry whose URL includes `/api/v3`.

`APIClient` service fields (`client.go:47-93`). The suffix is `API`, not `Api`:
`ActivityAPI, AuthenticationAPI, BotAPI, ChannelAPI, ClipAPI, FileAPI, GroupAPI, MeAPI, MessageAPI, NotificationAPI, Oauth2API, OgpAPI, PinAPI, PublicAPI, QallAPI, StampAPI, StarAPI, UserAPI, UserTagAPI, WebhookAPI, WebrtcAPI`.

### Auth (`configuration.go:31-47`, `client.go:476-503`)
```go
ContextOAuth2      = contextKey("token")        // value: oauth2.TokenSource → tok.Token(); latestToken.SetAuthHeader(req)
ContextAccessToken = contextKey("accesstoken")  // value: string → req.Header.Add("Authorization", "Bearer "+auth)
```
Order inside `prepareRequest`:
1. `ContextOAuth2`
2. `ContextAccessToken`
3. `cfg.DefaultHeader`, applied with `Header.Add`

So if you also put `Authorization` in `DefaultHeader`, the request carries two `Authorization` headers. The server reads `Header.Get`, which returns the first one.

For refreshable tokens, use `ContextOAuth2` with an `oauth2.TokenSource` (`golang.org/x/oauth2`).

Server-side behaviour (`router/middlewares/user_authenticate.go`):
- The `Authorization` header must be `Bearer <token>`; any other scheme gives 401 `invalid authorization scheme`.
- An unknown or expired token gives 401 `invalid token`.
- With no header, the server falls back to the `r_session` cookie.

### Example
```go
cfg := traq.NewConfiguration()
cfg.Servers = traq.ServerConfigurations{{URL: "https://traq.example.com/api/v3"}}
cfg.HTTPClient = &http.Client{Timeout: 30 * time.Second}
cfg.UserAgent = "traq-matrix-bridge/0.1"
c := traq.NewAPIClient(cfg)
ctx := context.WithValue(context.Background(), traq.ContextAccessToken, accessToken)
me, resp, err := c.MeAPI.GetMe(ctx).Execute()
if err != nil { log.Fatalf("getMe: %v (status %v)", err, statusOf(resp)) }
fmt.Println(me.Id, me.Name)
```

### Helpers (`utils.go`)
- `traq.PtrBool`, `traq.PtrInt32`, `traq.PtrString`.
- `NullableString`: `.Get() *string`, `.IsSet() bool`, `NewNullableString(*string)`. The same pattern exists for `NullableTime`, `NullableFileInfoThumbnail`, and others.
- Generated getters return the zero value when a field is nil or null, e.g. `(*Message).GetThreadId() string` and `GetThreadIdOk() (*string, bool)`.

### ⚠ Strict decoding
`UnmarshalJSON` is strict in 116 of the 148 `model_*.go` files, including `Message` (`model_message.go:350-392`):
- Every required property must be present, otherwise you get `no value given for required property X`.
- The decoder uses `decoder.DisallowUnknownFields()`.

So **if a traQ server adds a JSON field that this go-traq version doesn't know, decoding fails**. Execute then returns a `*GenericOpenAPIError` carrying the raw body, and `resp.StatusCode` is still 2xx.

I diffed the spec's schemas against the go-traq structs for every model listed in §3; they matched. Fallback: decode `resp.Body` yourself. It has already been re-wrapped as `io.NopCloser(bytes.NewBuffer(body))`, so it can be read again.

---

## 2. Endpoints → go-traq calls

Common pattern: `c.<Svc>.<Op>(ctx, pathArgs...).<Setter>(v)...Execute()`. The `XExecute(r)` variants are equivalent.

Return shapes:
- JSON endpoints return `(T, *http.Response, error)`.
- 204 endpoints return `(*http.Response, error)`.

Request type names are `<Svc>API<Op>Request`, e.g. `MessageAPIGetMessagesRequest`. Many operations exist in two services with identical behaviour. Below I list one service and note the other in parentheses.

| Endpoint | Call | Setters (query/body) | Returns |
|---|---|---|---|
| GET /users/me | `MeAPI.GetMe(ctx)` | – | `*MyUserDetail` |
| GET /users | `UserAPI.GetUsers(ctx)` | `.IncludeSuspended(bool)` (`include-suspended`, default false), `.Name(string)` | `[]User` |
| GET /users/{userId} | `UserAPI.GetUser(ctx, userId)` | – | `*UserDetail` (404) |
| GET /channels | `ChannelAPI.GetChannels(ctx)` | `.IncludeDm(bool)` (`include-dm`, default false), `.Path(string)` | `*ChannelList` |
| GET /channels/{id} | `ChannelAPI.GetChannel(ctx, channelId)` | – | `*Channel` |
| GET /users/me/subscriptions | `MeAPI.GetMyChannelSubscriptions(ctx)` (also NotificationAPI) | – | `[]UserSubscribeState` |
| GET /users/me/unread | `MeAPI.GetMyUnreadChannels(ctx)` (also NotificationAPI) | – | `[]UnreadChannel` |
| DELETE /users/me/unread/{channelId} | `MeAPI.ReadChannel(ctx, channelId)` (also NotificationAPI) | – | 204 |
| GET /channels/{channelId}/messages | `MessageAPI.GetMessages(ctx, channelId)` (also ChannelAPI) | `.Limit(int32)`, `.Offset(int32)`, `.Since(time.Time)`, `.Until(time.Time)`, `.Inclusive(bool)`, `.Order(string)` | `[]Message` (400/404) |
| POST /channels/{channelId}/messages | `MessageAPI.PostMessage(ctx, channelId)` (also ChannelAPI) | `.PostMessageRequest(traq.PostMessageRequest{...})` | `*Message` (201; 400/404) |
| PUT /messages/{messageId} | `MessageAPI.EditMessage(ctx, messageId)` | `.PostMessageRequest(...)` | 204 (400/403/404) |
| DELETE /messages/{messageId} | `MessageAPI.DeleteMessage(ctx, messageId)` | – | 204 (403/404) |
| GET /messages/{messageId} | `MessageAPI.GetMessage(ctx, messageId)` | – | `*Message` (404) |
| GET /users/{userId}/dm-channel | `UserAPI.GetUserDMChannel(ctx, userId)` (also ChannelAPI) | – | `*DMChannel` |
| POST /users/{userId}/messages | `MessageAPI.PostDirectMessage(ctx, userId)` (also UserAPI) | `.PostMessageRequest(...)` | `*Message` (201) |
| GET /messages | `MessageAPI.SearchMessages(ctx)` | see below | `*MessageSearchResult` (400/503) |
| GET /stamps | `StampAPI.GetStamps(ctx)` | `.IncludeUnicode(bool)` (deprecated, default true), `.Type_(string)` (`type`: `unicode`\|`original`) | `[]StampWithThumbnail` |
| GET /stamps/{stampId} | `StampAPI.GetStamp(ctx, stampId)` | – | `*Stamp` |
| POST /messages/{mid}/stamps/{sid} | `MessageAPI.AddMessageStamp(ctx, messageId, stampId)` (also StampAPI) | `.PostMessageStampRequest(traq.PostMessageStampRequest{Count: n})` (body optional) | 204 (400/404) |
| DELETE /messages/{mid}/stamps/{sid} | `MessageAPI.RemoveMessageStamp(ctx, messageId, stampId)` (also StampAPI) | `.UserIds([]string)` | 204 (400/403/404) |
| GET /messages/{mid}/stamps | `MessageAPI.GetMessageStamps(ctx, messageId)` (also StampAPI) | – | `[]MessageStamp` |
| GET /files/{fileId}/meta | `FileAPI.GetFileMeta(ctx, fileId)` | – | `*FileInfo` (403/404) |
| GET /files/{fileId} | `FileAPI.GetFile(ctx, fileId)` | `.Dl(int32)` (deprecated, no effect) | `*os.File` (403/404) |
| GET /files/{fileId}/thumbnail | `FileAPI.GetThumbnailImage(ctx, fileId)` | – | `*os.File` |
| POST /files | `FileAPI.PostFile(ctx)` | `.File(*os.File)`, `.ChannelId(string)` (multipart) | `*FileInfo` (201; 411/413) |
| GET /activity/timeline | `ActivityAPI.GetActivityTimeline(ctx)` | `.Limit(int32)` (1–50, default 50), `.All(bool)` (default false), `.PerChannel(bool)` (`per_channel`, default false) | `[]ActivityTimelineMessage` |
| GET /public/icon/{username} | `PublicAPI.GetPublicUserIcon(ctx, username)` | – | `*os.File` (image/jpeg\|gif\|png; 404) |
| GET /users/{userId}/icon | `UserAPI.GetUserIcon(ctx, userId)` | – | `*os.File` |
| GET /groups | `GroupAPI.GetUserGroups(ctx)` | – | `[]UserGroup` |
| GET /groups/{groupId} | `GroupAPI.GetUserGroup(ctx, groupId)` | – | `*UserGroup` (404) |
| GET /groups/{groupId}/members | `GroupAPI.GetUserGroupMembers(ctx, groupId)` | – | `[]UserGroupMember` (404) |
| POST /oauth2/token | `Oauth2API.PostOAuth2Token(ctx)` | `.GrantType(s)` (required), `.Code`, `.RedirectUri`, `.ClientId`, `.CodeVerifier`, `.Username`, `.Password`, `.Scope`, `.RefreshToken`, `.ClientSecret` (all `string`) | `*OAuth2Token` (200; 400/403) |
| POST /oauth2/revoke | `Oauth2API.RevokeOAuth2Token(ctx)` | `.Token(string)` (required) | 200, no body |
| POST /clients | `Oauth2API.CreateClient(ctx)` | `.PostClientRequest(traq.PostClientRequest{...})` | `*OAuth2ClientDetail` (201; 400) |

Exact signatures for the most-used operations (from `api_message.go`, `api_file.go`, `api_oauth2.go`):
```go
func (a *MessageAPIService) GetMessages(ctx context.Context, channelId string) MessageAPIGetMessagesRequest
func (r MessageAPIGetMessagesRequest) Limit(limit int32) MessageAPIGetMessagesRequest
func (r MessageAPIGetMessagesRequest) Offset(offset int32) MessageAPIGetMessagesRequest
func (r MessageAPIGetMessagesRequest) Since(since time.Time) MessageAPIGetMessagesRequest
func (r MessageAPIGetMessagesRequest) Until(until time.Time) MessageAPIGetMessagesRequest
func (r MessageAPIGetMessagesRequest) Inclusive(inclusive bool) MessageAPIGetMessagesRequest
func (r MessageAPIGetMessagesRequest) Order(order string) MessageAPIGetMessagesRequest
func (a *MessageAPIService) GetMessagesExecute(r MessageAPIGetMessagesRequest) ([]Message, *http.Response, error)
func (a *MessageAPIService) PostMessageExecute(r MessageAPIPostMessageRequest) (*Message, *http.Response, error)
func (a *MessageAPIService) EditMessageExecute(r MessageAPIEditMessageRequest) (*http.Response, error)
func (a *MessageAPIService) PostDirectMessage(ctx context.Context, userId string) MessageAPIPostDirectMessageRequest
func (a *MessageAPIService) AddMessageStamp(ctx context.Context, messageId string, stampId string) MessageAPIAddMessageStampRequest
func (a *FileAPIService) GetFileExecute(r FileAPIGetFileRequest) (*os.File, *http.Response, error)
func (a *Oauth2APIService) PostOAuth2TokenExecute(r Oauth2APIPostOAuth2TokenRequest) (*OAuth2Token, *http.Response, error)
func (a *Oauth2APIService) CreateClientExecute(r Oauth2APICreateClientRequest) (*OAuth2ClientDetail, *http.Response, error)
```

### getMessages query (spec)
| Param | Type | Rule / default |
|---|---|---|
| `limit` | int | 1–200 |
| `offset` | int | default 0 |
| `since` | date-time | default `0000-01-01T00:00:00.000000Z` |
| `until` | date-time | – |
| `inclusive` | bool | default false; includes the range endpoints |
| `order` | `asc`\|`desc` | default `desc` |

### postMessage / editMessage / postDirectMessage body
`PostMessageRequest{Content string; Embed *bool; Nonce *string}`. Spec rules:
- `content`: 1–10000 characters.
- `embed`: default false; "メンション・チャンネルリンクを自動埋め込みするか". With `embed:true` the server runs the `Replacer` from §6 on `@name` / `#path`.
- `nonce`: `^[a-zA-Z0-9_-]{1,32}$`; "メッセージ送信の確認に使うことができる任意の識別子(投稿でのみ使用可)".

Posting to, or editing in, an archived channel is not allowed. You can only edit your own messages. You can delete your own messages and those of webhooks/BOTs you manage.

Use the nonce for echo suppression: it comes back in `Message.Nonce` on the POST response. Whether it also appears when the message is later fetched via GET is `[INFERENCE: probably not; the spec marks it 投稿でのみ使用可]`.

Constructor: `traq.NewPostMessageRequest(content string) *PostMessageRequest`. go-traq does **not** reject a missing body client-side; the server returns 400.

### searchMessages query (`MessageAPISearchMessagesRequest` setters)
| Setter | Query | Type | Notes |
|---|---|---|---|
| `.Word(string)` | `word` | string | Elasticsearch Simple-Query-String syntax |
| `.After(time.Time)` | `after` | date-time | |
| `.Before(time.Time)` | `before` | date-time | |
| `.In(string)` | `in` | uuid | channel |
| `.To([]string)` | `to` | uuid[] | mentioned user or group |
| `.From([]string)` | `from` | uuid[] | poster user, or a group the poster belongs to |
| `.Citation(string)` | `citation` | uuid | cited message |
| `.Bot(bool)` | `bot` | bool | |
| `.HasURL(bool)` | `hasURL` | bool | |
| `.HasAttachments(bool)` | `hasAttachments` | bool | |
| `.HasImage(bool)` | `hasImage` | bool | |
| `.HasVideo(bool)` | `hasVideo` | bool | |
| `.HasAudio(bool)` | `hasAudio` | bool | |
| `.Limit(int32)` | `limit` | int | 1–100, default 20 |
| `.Offset(int32)` | `offset` | int | 0–9999; limit+offset ≤ 10000 |
| `.Sort(string)` | `sort` | enum | `createdAt`\|`-createdAt`\|`updatedAt`\|`-updatedAt`, default `-createdAt` |

The 503 response means the search engine is unavailable.

Result type (`model_message_search_result.go`):
```go
type MessageSearchResult struct { TotalHits int64 `json:"totalHits"`; Hits []Message `json:"hits"` }
```

### Stamps
- **GET /stamps:** `type` omitted returns all stamps. `include-unicode` is deprecated in favour of `type`.
- **AddMessageStamp:** body `PostMessageStampRequest{Count int32}`, with `count` 1–100 and required inside the body. The body itself is not required by the spec (`requestBody.required` is unset).
- **RemoveMessageStamp `userIds`:** if omitted, removes your own stamp. Only bots may pass other users' IDs, and only on their own messages.

### Files
`GetFile`, `GetThumbnailImage`, `GetUserIcon` and `GetPublicUserIcon` all return `*os.File`. Mechanics (`client.go:506-535` and every Execute):
1. `io.ReadAll(resp.Body)`: the **whole body is buffered in memory**.
2. Status ≥ 300 → error, no file.
3. `decode(&localVarReturnValue, ...)` with `**os.File` → `os.CreateTemp("", "HttpClientFile")`, write bytes, `Seek(0)`.

The caller must `f.Close(); os.Remove(f.Name())`. An empty body gives a `nil` file. go-traq has **no streaming**. To stream large files, issue your own request with the same client:
```go
req, _ := http.NewRequestWithContext(ctx, "GET", base+"/files/"+id, nil)
req.Header.Set("Authorization", "Bearer "+tok); resp, err := cfg.HTTPClient.Do(req) // stream resp.Body
```
- `GET /files/{id}` responds `application/octet-stream` with `Content-Disposition` (the `dl` param no longer matters).
- 403 means no access to the file.
- Upload: `POST /files` is multipart with `file` and `channelId`. You can't upload to archived channels. 411/413 are length-required / too large.

### Activity timeline
Returns recent public-channel posts, newest first. With `all` not true, only subscribed channels are included.

### OAuth2
- **POST /oauth2/token** is `application/x-www-form-urlencoded` in both the spec (schema `PostOAuth2Token`) and go-traq (`api_oauth2.go`, `localVarHTTPContentTypes := []string{"application/x-www-form-urlencoded"}`). Form keys: `grant_type` (required), `code`, `redirect_uri`, `client_id`, `code_verifier`, `username`, `password`, `scope`, `refresh_token`, `client_secret`.
- **Grant types are not enumerated in the spec.** For authorization_code + PKCE: `GrantType("authorization_code").Code(c).RedirectUri(u).ClientId(id).CodeVerifier(v)`. For refresh: `GrantType("refresh_token").RefreshToken(rt).ClientId(id)`, plus `.ClientSecret` for confidential clients.
- **Spec security schemes** (components):
  - `OAuth2` authorizationCode: `authorizationUrl: /api/v3/oauth2/authorize`, `tokenUrl: /api/v3/oauth2/token`, scopes `read`, `write`, `manage_bot`, `qall`.
  - `bearerAuth` (http bearer).
  - `cookieAuth` (`r_session`).
  - Global `security: [OAuth2, bearerAuth]`.
- `golang.org/x/oauth2` can also be used directly with `AuthStyle: oauth2.AuthStyleInParams`. `[INFERENCE: traQ accepts client_id in the form body; go-traq sends it there.]`
- **POST /oauth2/revoke** is form-urlencoded, `token` (required): "無効化するOAuth2トークンまたはOAuth2リフレッシュトークン". Responds 200.
- **POST /clients** takes a JSON `PostClientRequest` (below) and returns 201 `OAuth2ClientDetail`, which includes `Secret`. The user needs the `create_client` permission.

### WebSocket in go-traq
`NotificationAPI.Ws(ctx)` exists (`api_notification.go:1295`), but it is a plain HTTP GET with no Upgrade handling. Don't use it; use a WebSocket library (§5).

---

## 3. Models (exact fields; `model_<snake>.go`)

All timestamps are `time.Time` (RFC 3339). `NullableX` means the JSON key is required but may be `null`. `*T` with `omitempty` means optional.

```go
type Message struct {                     // model_message.go
	Id        string         `json:"id"`
	UserId    string         `json:"userId"`
	ChannelId string         `json:"channelId"`
	Content   string         `json:"content"`
	CreatedAt time.Time      `json:"createdAt"`
	UpdatedAt time.Time      `json:"updatedAt"`
	Pinned    bool           `json:"pinned"`
	Stamps    []MessageStamp `json:"stamps"`
	ThreadId  NullableString `json:"threadId"`              // required, nullable
	Nonce     *string        `json:"nonce,omitempty" validate:"regexp=^[a-zA-Z0-9_-]{1,32}$"`
}
type MessageStamp struct {                // model_message_stamp.go
	UserId    string    `json:"userId"`
	StampId   string    `json:"stampId"`
	Count     int32     `json:"count"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}
type Stamp struct {                       // model_stamp.go
	Id        string    `json:"id"`
	Name      string    `json:"name" validate:"regexp=^[a-zA-Z0-9_-]{1,32}$"`
	CreatorId string    `json:"creatorId"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
	FileId    string    `json:"fileId"`
	IsUnicode bool      `json:"isUnicode"`
}
// StampWithThumbnail = Stamp fields + HasThumbnail bool `json:"hasThumbnail"`   (model_stamp_with_thumbnail.go)

type User struct {                        // model_user.go
	Id          string           `json:"id"`
	Name        string           `json:"name" validate:"regexp=^([a-zA-Z0-9_-]{1,32}|Webhook#[a-zA-Z0-9_-]{22})$"`
	DisplayName string           `json:"displayName"`
	IconFileId  string           `json:"iconFileId"`
	Bot         bool             `json:"bot"`
	State       UserAccountState `json:"state"`
	UpdatedAt   time.Time        `json:"updatedAt"`
}
type UserAccountState int32  // USERACCOUNTSTATE_deactivated=0, _active=1, _suspended=2

type UserDetail struct {                  // model_user_detail.go
	Id          string           `json:"id"`
	State       UserAccountState `json:"state"`
	Bot         bool             `json:"bot"`
	IconFileId  string           `json:"iconFileId"`
	DisplayName string           `json:"displayName"`
	Name        string           `json:"name"`
	TwitterId   string           `json:"twitterId" validate:"regexp=^[a-zA-Z0-9_]{0,15}$"`
	LastOnline  NullableTime     `json:"lastOnline"`
	UpdatedAt   time.Time        `json:"updatedAt"`
	Tags        []UserTag        `json:"tags"`
	Groups      []string         `json:"groups"`      // group UUIDs
	Bio         string           `json:"bio"`
	HomeChannel NullableString   `json:"homeChannel"`
}
type MyUserDetail struct {                // model_my_user_detail.go
	Id          string           `json:"id"`
	Bio         string           `json:"bio"`
	Groups      []string         `json:"groups"`
	Tags        []UserTag        `json:"tags"`
	UpdatedAt   time.Time        `json:"updatedAt"`
	LastOnline  NullableTime     `json:"lastOnline"`
	TwitterId   string           `json:"twitterId"`
	Name        string           `json:"name"`
	DisplayName string           `json:"displayName"`
	IconFileId  string           `json:"iconFileId"`
	Bot         bool             `json:"bot"`
	State       UserAccountState `json:"state"`
	Permissions []UserPermission `json:"permissions"`
	HomeChannel NullableString   `json:"homeChannel"`
}
```
`UserPermission` is a `string` enum with constants `USERPERMISSION_*` in `model_user_permission.go`. Values relevant to the bridge: `get_me`, `get_user`, `get_channel`, `get_message`, `post_message`, `edit_message`, `delete_message`, `get_stamp`, `add_message_stamp`, `remove_message_stamp`, `upload_file`, `download_file`, `get_unread`, `delete_unread`, `get_channel_subscription`, `connect_notification_stream`, `get_user_group`, `create_client`.

```go
type Channel struct {                     // model_channel.go
	Id       string         `json:"id"`
	ParentId NullableString `json:"parentId"`   // null = root
	Archived bool           `json:"archived"`
	Force    bool           `json:"force"`      // forced-notification channel
	Topic    string         `json:"topic"`
	Name     string         `json:"name" validate:"regexp=^[a-zA-Z0-9-_]{1,20}$"`
	Children []string       `json:"children"`   // child channel UUIDs
}
type ChannelList struct {                 // model_channel_list.go
	Public []Channel   `json:"public"`
	Dm     []DMChannel `json:"dm,omitempty"`   // only present with include-dm=true
}
type DMChannel struct { Id string `json:"id"`; UserId string `json:"userId"` }  // UserId = the other party
type UnreadChannel struct {               // model_unread_channel.go
	ChannelId       string    `json:"channelId"`
	Count           int32     `json:"count"`
	Noticeable      bool      `json:"noticeable"`
	Since           time.Time `json:"since"`
	UpdatedAt       time.Time `json:"updatedAt"`
	OldestMessageId string    `json:"oldestMessageId"`
}
type UserSubscribeState struct {          // model_user_subscribe_state.go
	ChannelId string                `json:"channelId"`
	Level     ChannelSubscribeLevel `json:"level"`
}
type ChannelSubscribeLevel int32
// CHANNELSUBSCRIBELEVEL_none=0 (無し), _subscribed=1 (未読管理), _notified=2 (未読管理+通知)

type ActivityTimelineMessage struct {     // model_activity_timeline_message.go — no stamps/pinned/threadId
	Id        string    `json:"id"`
	UserId    string    `json:"userId"`
	ChannelId string    `json:"channelId"`
	Content   string    `json:"content"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}
type FileInfo struct {                    // model_file_info.go
	Id              string                    `json:"id"`
	Name            string                    `json:"name"`
	Mime            string                    `json:"mime"`
	Size            int64                     `json:"size"`
	Md5             string                    `json:"md5"`
	IsAnimatedImage bool                      `json:"isAnimatedImage"`
	CreatedAt       time.Time                 `json:"createdAt"`
	Thumbnails      []ThumbnailInfo           `json:"thumbnails"`
	Thumbnail       NullableFileInfoThumbnail `json:"thumbnail"`   // deprecated in spec
	ChannelId       NullableString            `json:"channelId"`
	UploaderId      NullableString            `json:"uploaderId"`
}
type ThumbnailInfo struct {               // model_thumbnail_info.go
	Type   ThumbnailType `json:"type"`     // "image" | "waveform"
	Mime   string        `json:"mime"`
	Width  *int32        `json:"width,omitempty"`
	Height *int32        `json:"height,omitempty"`
}
type FileInfoThumbnail struct { Mime string `json:"mime"`; Width *int32 `json:"width,omitempty"`; Height *int32 `json:"height,omitempty"` }

type OAuth2Token struct {                 // model_o_auth2_token.go
	AccessToken  string  `json:"access_token"`
	TokenType    string  `json:"token_type"`
	ExpiresIn    *int32  `json:"expires_in,omitempty"`
	RefreshToken *string `json:"refresh_token,omitempty"`
	Scope        *string `json:"scope,omitempty"`
	IdToken      *string `json:"id_token,omitempty"`
}
type PostClientRequest struct {           // model_post_client_request.go
	Name         string        `json:"name"`
	CallbackUrl  string        `json:"callbackUrl"`
	Scopes       []OAuth2Scope `json:"scopes"`  // OAUTH2SCOPE_OPENID "openid", _PROFILE "profile", _READ "read", _WRITE "write", _MANAGE_BOT "manage_bot", _QALL "qall"
	Description  string        `json:"description"`
	Confidential *bool         `json:"confidential,omitempty"`
}
type OAuth2ClientDetail struct {          // model_o_auth2_client_detail.go
	Id           string        `json:"id"`
	DeveloperId  string        `json:"developerId"`
	Description  string        `json:"description"`
	Name         string        `json:"name"`
	Scopes       []OAuth2Scope `json:"scopes"`
	CallbackUrl  string        `json:"callbackUrl"`
	Secret       string        `json:"secret"`
	Confidential bool          `json:"confidential"`
}
type UserGroup struct {                   // model_user_group.go
	Id          string            `json:"id"`
	Name        string            `json:"name"`
	Description string            `json:"description"`
	Type        string            `json:"type"`
	Icon        string            `json:"icon"`
	Members     []UserGroupMember `json:"members"`
	CreatedAt   time.Time         `json:"createdAt"`
	UpdatedAt   time.Time         `json:"updatedAt"`
	Admins      []string          `json:"admins"`
}
type UserGroupMember struct { Id string `json:"id"`; Role string `json:"role"` }  // Id = user UUID; role maxLength 100
type PostMessageStampRequest struct { Count int32 `json:"count"` }                // 1..100
```

---

## 4. Errors

From `client.go:693-715` and every `*Execute`:
```go
type GenericOpenAPIError struct { body []byte; error string; model interface{} }
func (e GenericOpenAPIError) Error() string       // = resp.Status, e.g. "404 Not Found", or the decode error text
func (e GenericOpenAPIError) Body() []byte        // raw response body
func (e GenericOpenAPIError) Model() interface{}  // nil for traQ (spec defines no error schemas)
```

How each failure is reported:
- **Transport error:** `err` comes straight from `HTTPClient.Do`. `resp` may be nil.
- **`StatusCode >= 300`:** `err = &GenericOpenAPIError{body, resp.Status}`. `resp` is non-nil and `resp.Body` can be read again.
- **JSON decode failure on 2xx** (including unknown fields, see §1): `&GenericOpenAPIError{body, decodeErr}`.
- **Missing required setter** (e.g. `grantType`): `reportError(...)`, a plain `error`, with `resp == nil`.

Detection:
```go
func statusOf(resp *http.Response) int { if resp == nil { return 0 }; return resp.StatusCode }
_, resp, err := c.MessageAPI.GetMessage(ctx, id).Execute()
var apiErr *traq.GenericOpenAPIError
if errors.As(err, &apiErr) && resp != nil {
	switch resp.StatusCode {
	case 401: // token invalid/expired → refresh or re-login
	case 404: // gone/no access
	case 429: ra := resp.Header.Get("Retry-After"); _ = ra // may be empty
	case 503: // e.g. searchMessages: search engine unavailable
	}
	log.Printf("traq %d: %s", resp.StatusCode, apiErr.Body())
}
```

Rate limiting is not in the spec: it contains no `429` and no `Retry-After`. The server source does rate-limit (`router/v3/router.go:103`):
```go
api := e.Group("/v3", middlewares.UserAuthenticate(...), middlewares.RateLimit(rate.Limit(100), 1000, time.Minute))
```
- Uses Echo's `RateLimiterMemoryStore`: 100 req/s, burst 1000, keyed by user ID + IP.
- `[INFERENCE]` Echo's default deny handler returns **429 with no `Retry-After`**. Handle 429 with backoff and fall back when the header is absent.
- Error bodies are Echo JSON, roughly `{"message":"..."}` `[INFERENCE]`.

---

## 5. WebSocket `/api/v3/ws` (spec `paths./ws`, operationId `ws`)

### Auth
- The `/ws` operation has no `security` override, so the global `security: [OAuth2, bearerAuth]` applies.
- Server: `api.GET("/ws", echo.WrapHandler(h.WS), requires(permission.ConnectNotificationStream), blockBot)` (`router/v3/router.go:416`) inside the group that uses `UserAuthenticate`.
- **`Authorization: Bearer <token>` on the upgrade request is accepted.** Server-side Go WS libraries can set it, e.g. `websocket.Dial(ctx, url, &websocket.DialOptions{HTTPHeader: h})` or gorilla `Dialer.Dial(url, header)`.
- The token needs the `connect_notification_stream` permission.
- **Bot users are blocked** (`blockBot`); bots use `/api/v3/bots/ws` instead.
- URL: `wss://<host>/api/v3/ws`.

### Commands (verbatim from the spec)
> `コマンド:引数1:引数2:...`のような形式のTextMessageをサーバーに送信することで、このWebSocketセッションに対する設定が実行できる。

**`viewstate`**: sets the channel this session is viewing, i.e. the channel it receives events for. Currently only one channel per session.
```
viewstate:{チャンネルID}:{閲覧状態}
```
- チャンネルID: target channel ID.
- 閲覧状態: `none`, `monitoring`, `editing`.
- Before the first `viewstate`, or after sending `viewstate:null` or `viewstate:`, the session is viewing no channel.

**`rtcstate`**: changes your own WebRTC state. It can't be changed while another connection holds the state, and it resets when the connection drops.
```
rtcstate:{チャンネルID}:({状態}:{セッションID})*
```

**`timeline_streaming`**: whether to receive `MESSAGE_CREATED` for **all public channels**. Initial state is `off`.
```
timeline_streaming:(on|off|true|false)
```

**`ping`**: the server replies with a `PING` event, `{"type":"PING","body":null}`.

### Received events
Events arrive as TextMessage JSON `{"type": "...", "body": {...}}`, e.g. `{"type":"USER_ONLINE","body":{"id":"7dd8e07f-7f5d-4331-9176-b56a4299768b"}}`. Body fields below are verbatim key names. The scope column (対象) says who receives each event.

| type | 対象 | body |
|---|---|---|
| `USER_JOINED` | 全員 | `id` |
| `USER_UPDATED` | 全員 | `id` |
| `USER_TAGS_UPDATED` | 全員 | `id`, `tag_id` |
| `USER_ICON_UPDATED` | 全員 | `id` |
| `USER_WEBRTC_STATE_CHANGED` | 全員 | `user_id`, `channel_id`, `sessions[]{state, sessionId}` |
| `USER_VIEWSTATE_CHANGED` | all WS sessions of that user | `view_states[]{key, channel_id, state}` |
| `USER_ONLINE` | 全員 | `id` |
| `USER_OFFLINE` | 全員 | `id`, `last_online` |
| `USER_GROUP_CREATED` / `USER_GROUP_UPDATED` / `USER_GROUP_DELETED` | 全員 | `id` |
| `CHANNEL_CREATED` / `CHANNEL_UPDATED` / `CHANNEL_DELETED` | everyone who can see the channel | `id`, `dm_user_id` (DM only) |
| `CHANNEL_STARED` / `CHANNEL_UNSTARED` | 自分 | `id` |
| `CHANNEL_VIEWERS_CHANGED` | users viewing the channel | `id`, `viewers[]{userId, state, updatedAt}` |
| `CHANNEL_SUBSCRIBERS_CHANGED` | users viewing the channel | `id` |
| `MESSAGE_CREATED` | viewers of the channel, users with notifications on it, mentioned users | `id`, `is_citing` (whether the message cites one of the WS user's own posts) |
| `MESSAGE_UPDATED` | users viewing the channel | `id` |
| `MESSAGE_DELETED` | users viewing the channel | `id` |
| `MESSAGE_STAMPED` | users viewing the channel | `message_id`, `user_id`, `stamp_id`, `count` (that user's count), `created_at` (when that user first added this stamp to this message) |
| `MESSAGE_UNSTAMPED` | users viewing the channel | `message_id`, `user_id`, `stamp_id` |
| `MESSAGE_PINNED` / `MESSAGE_UNPINNED` | users viewing the channel | `message_id`, `channel_id` |
| `MESSAGE_READ` | 自分 | `id` (channel read) |
| `STAMP_CREATED` / `STAMP_UPDATED` / `STAMP_DELETED` | 全員 | `id` |
| `STAMP_PALETTE_CREATED` / `_UPDATED` / `_DELETED` | 自分 | `id` |
| `CLIP_FOLDER_CREATED` / `_UPDATED` / `_DELETED` | 自分 | `id` |
| `CLIP_FOLDER_MESSAGE_DELETED` / `CLIP_FOLDER_MESSAGE_ADDED` | 自分 | `folder_id`, `message_id` |
| `QALL_ROOM_STATE_CHANGED` | 全員 | `roomStates[]{roomId, participants[]{identity, name, joinedAt, attributes, canPublish}, isWebinar, metadata}` |
| `QALL_SOUNDBOARD_ITEM_CREATED` | 全員 | `sound_id`, `name`, `creator_id` |
| `QALL_SOUNDBOARD_ITEM_DELETED` | 全員 | `sound_id` |
| `PING` | reply to `ping` | `null` |

Bridge implications:
- `MESSAGE_UPDATED`, `MESSAGE_DELETED`, `MESSAGE_STAMPED`, `MESSAGE_UNSTAMPED`, `MESSAGE_PINNED` and `MESSAGE_UNPINNED` only reach sessions **viewing** the channel. With one `viewstate` per session, catching edits, deletes and stamps across many channels needs one WS session per channel, or polling. `[INFERENCE: that is the only way to get these events; nothing besides viewstate controls them.]`
- `MESSAGE_CREATED` for every public channel only needs `timeline_streaming:on`. DMs and mentions arrive regardless. The body has IDs only, so call `GetMessage` to get the content.
- The spec root `x-traq-codegen-schemas` contains only a few event schemas: `UserOfflineEvent`, `QallRoomStateChangedEvent`, `QallSoundboardItemCreatedEvent`, `QallSoundboardItemDeletedEvent`.

---

## 6. traQ message embedding format

Source: `github.com/traPtitech/traQ/utils/message/`. The spec only says `embed: メンション・チャンネルリンクを自動埋め込みするか`, and go-traq has no docs on it.

### JSON embeds `!{...}`
Regex (`parser.go`):
```go
embJSONRegex = regexp.MustCompile(`(?m)!({(?:[ \t\n]*"(?:[^"]|\\.)*"[ \t\n]*:[ \t\n]*"(?:[^"]|\\.)*",)*(?:[ \t\n]*"(?:[^"]|\\.)*"[ \t\n]*:[ \t\n]*"(?:[^"]|\\.)*")})`)
```
- The embed is a flat JSON object of **string→string pairs only**, prefixed with `!`. The server unmarshals `s[1:]`.
- Fields (`embedded.go`):
  ```go
  type EmbeddedInfo struct { Raw string `json:"raw"`; Type string `json:"type"`; ID string `json:"id"` }
  ```
- Types handled by `Parse` (`parser.go`):

| `type` | Collected into | Plain text |
|---|---|---|
| `user` | `Mentions` | `raw` |
| `group` | `GroupMentions` | `raw` |
| `channel` | `ChannelLink` | `raw` |
| `file` | `Attachments` | `[添付ファイル]` |
| `message` | `Citation` | `[引用メッセージ]` |

- An unknown type, or JSON/UUID that fails to parse, leaves the text unchanged.
- `ExtractEmbedding` gives the same plain text and also replaces `\n` with a space.

Canonical forms produced by the server `Replacer` (`replacer.go`) when `embed:true`:
```
!{"type":"user","raw":"@name","id":"<uuid>"}
!{"type":"group","raw":"@group","id":"<uuid>"}
!{"type":"channel","raw":"#a/b/c","id":"<uuid>"}
```
`raw` keeps the original token, including `@` or `#`, possibly full-width `＠`/`＃`.

### Replacer regexes (`replacer.go`)
```go
mentionRegex    = regexp.MustCompile(`:?[@＠]([^\s@＠]{0,31}[^\s@＠:])`)
userStartsRegex = regexp.MustCompile(`^[@＠]([a-zA-Z0-9_-]{1,32})`)
channelRegex    = regexp.MustCompile(`[#＃]([a-zA-Z0-9_/-]+)`)
```
Rules:
- Matches with a leading `:` are skipped (stamp syntax `:name:`).
- Name lookup is lower-cased. Order: user, then group, then a prefix-user fallback via `userStartsRegex`.
- Channel paths are lower-cased, e.g. `#gps/times/foo`.
- Nothing is replaced inside code blocks fenced with matching backtick counts, inside `$$` LaTeX blocks, or inside inline `` `...` `` / `$...$` spans.
- Channels are replaced before mentions (`replaceMention(replaceChannel(m))`).

A bridge sending Matrix → traQ can either:
- set `embed:true` and send `@name` / `#path`, or
- pre-build `!{...}` itself. Stamps/emoji in traQ markdown are written `:stamp_name:`.

### URL embeds (`parser.go`)
```go
const embURLRegexFragment = `/(files|messages)/[\da-f]{8}-[\da-f]{4}-[\da-f]{4}-[\da-f]{4}-[\da-f]{12}`
embURLRegex  = regexp.MustCompile("http://localhost:3000" + embURLRegexFragment)
func SetOrigin(origin string) { embURLRegex = regexp.MustCompile(strings.ReplaceAll(origin, ".", `\.`) + embURLRegexFragment) }
```
- `SetOrigin(c.Origin)` is called in `cmd/root.go:66`. `Origin` is the server-origin config (`cmd/config.go:45-46`, default `http://localhost:3000`), e.g. `https://q.trap.jp`, **without `/api/v3`**.
- `<origin>/files/<uuid>` → attachment, plain `[添付ファイル]`. The bridge attaches files by uploading with `POST /files` and appending the file URL to the content.
- `<origin>/messages/<uuid>` → citation (quote), plain `[引用メッセージ]`.
- Only lowercase hex UUIDs match.

### Other server notes
- `NonceManager` (`nonce.go`) is an in-memory string set. `NonceChecker` returns false on a duplicate.
- Spoilers use `!!text!!` (`spoiler.go`, tokenizes runs of `!!`). `FillSpoiler` masks them in notification text.
