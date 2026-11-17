# traq-matrix-bridge Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** traQ のチャンネルと DM を Matrix の room として読み書きできる puppeting bridge (mautrix-go bridgev2 の network connector) を作る。

**Architecture:** bridge 本体 (Matrix 接続、portal、ghost、backfill queue、bot command) は bridgev2 が持ち、この repository は traQ 側を書く。traQ の REST は自前の薄い client (go-traq は未知の field で decode に失敗するため使わない)、WebSocket は coder/websocket。変換 (`pkg/msgconv`、`pkg/stampconv`) は副作用の無い関数にして単体テストし、traQ client (`pkg/traq`) は compose で立てた本物の traQ に対して結合テストする。

**Tech Stack:** Go、maunium.net/go/mautrix v0.33.0 (bridgev2、MPL-2.0)、github.com/coder/websocket (ISC)、github.com/yuin/goldmark (MIT)、Nix flake (buildGoModule)、GitHub Actions、docker compose (traQ 3.32.1)

Spec: `docs/superpowers/specs/2026-11-17-traq-matrix-bridge-design.md`

## Global Constraints

- License は MIT。依存に AGPL/GPL のものを入れない。mautrix-go のファイルを改変・vendor しない。
- AGPL の bridge (mautrix-slack、mautrix-discord など) のコードを写さない。bridgev2 の使い方は mautrix-go の interface と godoc から書く。
- module path は `github.com/kisepichu/traq-matrix-bridge`。binary 名は `traq-matrix-bridge`。
- end-to-bridge encryption は libolm (cgo)。`-tags goolm` でも build できること。
- traQ の API は `<server_url>/api/v3`。WebSocket は `<server_url>/api/v3/ws` (`wss` / `ws` は server_url の scheme から)。
- access token と PKCE の `code_verifier` を log や error message に出さない。token は `UserLogin` の metadata にだけ置く。traQ は refresh token を返さない (有効期間 1 年)。
- 投稿と編集は `embed: true` で送る。本文の上限は 10000 文字。
- コード中のコメント、log message、bot の返答は英語。README と `docs/` は日本語 (`docs/notes/` の調査メモは英語のまま)。commit message は Conventional Commits (英語)。
- 設定 `network:` の key と既定値は spec の「設定」節のとおり: `server_url`、`oauth.client_id`、`oauth.redirect_uri` (既定 `http://localhost:29399/callback`)、`poll.interval` (3m)、`poll.window` (6h)、`poll.depth` (30)、`poll.max_channels` (50)、`view_idle_timeout` (10m)、`subscription_sync_interval` (10m)。
- 各 task は `main` から branch を切り、PR を出して merge する。
- bridgev2 の API は `docs/notes/bridgev2-v0.33.0.md`、traQ の API と WebSocket は `docs/notes/traq-api.md` (冒頭の実測を優先) を見る。どちらも要約なので、使う前に module cache の source (`go env GOMODCACHE`) か `/tmp/traq-v3.yaml` 相当の OpenAPI (`https://raw.githubusercontent.com/traPtitech/traQ/v3.32.1/docs/v3-api.yaml`) で確かめる。
- 単体テストは `go test -race ./...` で、network にも docker にも触らない。traQ に繋ぐテストは build tag `integration` を付け、`TRAQ_TEST_URL` (既定 `http://localhost:3080`) の traQ を使う。

Task の依存: 1 → (2, 3, 6, 7 は並行できる) → 4, 5 (3 の後) → 8 (1, 3 の後) → 9 (5, 8) → 10 (2, 6, 9) → 11 (7, 9) → 12, 13, 14, 15 (10 の後) → 16。connector の task (8 以降) は同じ file を触るので 1 つずつ進める。

---

### Task 1: 雛形 (起動できる binary、設定、Nix、CI)

**Files:**
- Create: `go.mod`、`go.sum`
- Create: `cmd/traq-matrix-bridge/main.go`
- Create: `pkg/connector/connector.go`、`pkg/connector/config.go`、`pkg/connector/example-config.yaml`
- Test: `pkg/connector/config_test.go`
- Create: `flake.nix`、`flake.lock`
- Create: `.github/workflows/ci.yml`
- Modify: `.gitignore`

**Interfaces:**
- Consumes: `maunium.net/go/mautrix v0.33.0` (`bridgev2`、`bridgev2/matrix/mxmain`)、`go.mau.fi/util/configupgrade`。API の要約は `docs/notes/bridgev2-v0.33.0.md` の §1、§9 (実物は module cache の source で確かめる)。
- Produces:

```go
package connector

type NetworkConfig struct {
	ServerURL string      `yaml:"server_url"`
	OAuth     OAuthConfig `yaml:"oauth"`
	Poll      PollConfig  `yaml:"poll"`
	ViewIdleTimeout          time.Duration `yaml:"view_idle_timeout"`
	SubscriptionSyncInterval time.Duration `yaml:"subscription_sync_interval"`
}
type OAuthConfig struct {
	ClientID    string `yaml:"client_id"`
	RedirectURI string `yaml:"redirect_uri"`
}
type PollConfig struct {
	Interval    time.Duration `yaml:"interval"`
	Window      time.Duration `yaml:"window"`
	Depth       int           `yaml:"depth"`
	MaxChannels int           `yaml:"max_channels"`
}

// TraQConnector implements bridgev2.NetworkConnector (+ ConfigValidatingNetwork).
type TraQConnector struct {
	Bridge *bridgev2.Bridge
	Config NetworkConfig
}

func (c *NetworkConfig) Validate() error // server_url: absolute http(s) URL without path; client_id non-empty; redirect_uri absolute URL; durations > 0; 1 <= depth <= 200; max_channels >= 1
```

決まり:

- `GetName()` は `bridgev2.BridgeName{DisplayName: "traQ", NetworkURL: "https://github.com/traPtitech/traQ", NetworkID: "traq", BeeperBridgeType: "github.com/kisepichu/traq-matrix-bridge", DefaultPort: 29338, DefaultCommandPrefix: "!traq"}`。NetworkIcon は空。config 読み込み前に呼ばれるので `Config` を参照しない。
- `example-config.yaml` は `network:` を書かずに最上位から書く (mxmain が 4 文字字下げして `network:` の下に入れる)。既定値は Global Constraints のとおり。`server_url` と `client_id` は空 (`""`)。
- upgrader は `up.SimpleUpgrader` で、全 key を `helper.Copy` する (duration は `up.Str`、数値は `up.Int`)。
- `ValidateConfig()` は `Config.Validate()` を返す。
- この task の時点では login flow は空 (`GetLoginFlows` は nil)、`LoadUserLogin` は `errors.New("traq: logins are not implemented yet")` を返す。Task 7 で置き換える。
- `main.go` は `mxmain.BridgeMain{Name: "traq-matrix-bridge", URL: "https://github.com/kisepichu/traq-matrix-bridge", Description: "A Matrix bridge for traQ.", Version: "0.1.0", Connector: &connector.TraQConnector{}}` と `InitVersion(Tag, Commit, BuildTime)`。`Tag`、`Commit`、`BuildTime` は `-ldflags -X` で埋める。
- Go は mautrix-go v0.33.0 の要求 (1.26 以上) に合わせ、`go.mod` を `go 1.26` にする。
- `flake.nix`: input は `nixpkgs` (nixos-unstable) だけ。`packages.x86_64-linux.default` / `aarch64-linux` / `x86_64-darwin` / `aarch64-darwin` を `buildGoModule` で作る (`subPackages = [ "cmd/traq-matrix-bridge" ]`、`buildInputs = [ olm ]`、`ldflags` に `-X main.Tag=v${version}` など)。olm は nixpkgs で insecure なので、`import nixpkgs { inherit system; config.permittedInsecurePackages = [ "olm-3.2.16" ]; }` で読む。`devShells.default` に go、gopls、olm、pkg-config。`checks` に package。
- CI (`.github/workflows/ci.yml`、`pull_request` と `push: main`): job `go` (ubuntu-latest、`actions/setup-go` で `go-version-file: go.mod`、`sudo apt-get install -y libolm-dev`、`go vet ./...`、`go test -race ./...`)、job `goolm` (`go build -tags goolm ./...`、libolm 無し)、job `nix` (`cachix/install-nix-action`、`nix build`、`nix flake check`)。結合テストの job は Task 4 で足す。
- `.gitignore` に `/result`、`/config.yaml`、`/registration.yaml`、`*.db`、`*.db-*`、`/logs/`、`/.superpowers/` を足す。

- [ ] **Step 1: 失敗するテストを書く** (`pkg/connector/config_test.go`)

```go
package connector

import (
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

func validConfig() NetworkConfig {
	return NetworkConfig{
		ServerURL:                "https://q.trap.jp",
		OAuth:                    OAuthConfig{ClientID: "cid", RedirectURI: "http://localhost:29399/callback"},
		Poll:                     PollConfig{Interval: 3 * time.Minute, Window: 6 * time.Hour, Depth: 30, MaxChannels: 50},
		ViewIdleTimeout:          10 * time.Minute,
		SubscriptionSyncInterval: 10 * time.Minute,
	}
}

func TestExampleConfigDecodesToDefaults(t *testing.T) {
	var c NetworkConfig
	if err := yaml.Unmarshal([]byte(ExampleConfig), &c); err != nil {
		t.Fatal(err)
	}
	want := validConfig()
	want.ServerURL, want.OAuth.ClientID = "", ""
	if c != want {
		t.Errorf("example config = %+v, want %+v", c, want)
	}
}

func TestValidate(t *testing.T) {
	if err := validConfig().Validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	cases := map[string]func(*NetworkConfig){
		"empty server_url":    func(c *NetworkConfig) { c.ServerURL = "" },
		"server_url with path": func(c *NetworkConfig) { c.ServerURL = "https://q.trap.jp/api/v3" },
		"server_url not http":  func(c *NetworkConfig) { c.ServerURL = "ftp://q.trap.jp" },
		"empty client_id":      func(c *NetworkConfig) { c.OAuth.ClientID = "" },
		"relative redirect":    func(c *NetworkConfig) { c.OAuth.RedirectURI = "/callback" },
		"zero poll interval":   func(c *NetworkConfig) { c.Poll.Interval = 0 },
		"depth over 200":       func(c *NetworkConfig) { c.Poll.Depth = 201 },
		"no channels":          func(c *NetworkConfig) { c.Poll.MaxChannels = 0 },
		"zero idle timeout":    func(c *NetworkConfig) { c.ViewIdleTimeout = 0 },
	}
	for name, mutate := range cases {
		c := validConfig()
		mutate(&c)
		if err := c.Validate(); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestServerURLTrailingSlashIsAccepted(t *testing.T) {
	c := validConfig()
	c.ServerURL = "https://q.trap.jp/"
	if err := c.Validate(); err != nil {
		t.Errorf("trailing slash: %v", err)
	}
}
```

- [ ] **Step 2: 失敗を確かめる** — Run: `go test ./pkg/connector/` → Expected: FAIL (undefined)
- [ ] **Step 3: 実装する** — 上の決まりのとおりに全 file を書く。`go mod tidy` で依存を揃える。
- [ ] **Step 4: 確かめる**
  - `go test -race ./...` → PASS
  - `go build -tags goolm ./...` → 成功
  - `go run ./cmd/traq-matrix-bridge -e -c /tmp/tmb-example.yaml && grep -A12 '^network:' /tmp/tmb-example.yaml` → `network:` の下に `server_url`、`oauth`、`poll`、`view_idle_timeout`、`subscription_sync_interval` が出る
  - `nix build && ./result/bin/traq-matrix-bridge -v` → version が出る (`vendorHash` は最初の失敗で出た値に直す)
- [ ] **Step 5: commit** — `git commit -m "feat: scaffold the bridge binary, config, Nix flake and CI"`

### Task 2: スタンプと reaction の対応 (`pkg/stampconv`)

**Files:**
- Create: `pkg/stampconv/stampconv.go`
- Test: `pkg/stampconv/stampconv_test.go`

**Interfaces:**
- Consumes: なし (標準ライブラリだけ)
- Produces (後の task が使う):
  - `type Stamp struct { ID, Name string; Unicode bool; Emoji string }`
  - `func EmojiFromFileName(name string) (string, bool)`
  - `func Normalize(s string) string`
  - `type Table struct` (goroutine safe) と `func NewTable(stamps []Stamp, complete bool) *Table`
  - `func (t *Table) Put(s Stamp)`、`Delete(id string)`、`SetComplete(bool)`、`ByID(id string) (Stamp, bool)`
  - `func (t *Table) Resolve(key string) (Stamp, error)`
  - `func ReactionKey(s Stamp) string`
  - `var ErrUnknownStamp`、`var ErrEmojiTableIncomplete`

背景: traQ の Unicode スタンプは twemoji の SVG から作られ、file 名が codepoint (`1f44d.svg`、複数なら `-` 区切り) になっている。スタンプ名は `^[a-zA-Z0-9_-]{1,32}$`。reaction の key は「絵文字」「`:name:`」「`name`」のどれか。

- [ ] **Step 1: 失敗するテストを書く**

```go
package stampconv

import (
	"errors"
	"testing"
)

func TestEmojiFromFileName(t *testing.T) {
	cases := []struct {
		in   string
		want string
		ok   bool
	}{
		{"1f44d.svg", "👍", true},
		{"1f468-200d-1f4bb.svg", "👨\u200d💻", true},
		{"2764.svg", "❤", true},
		{"00a9.svg", "©", true},
		{"1f44d.png", "", false},
		{"thumbsup.svg", "", false},
		{".svg", "", false},
		{"110000.svg", "", false}, // beyond the Unicode range
	}
	for _, c := range cases {
		got, ok := EmojiFromFileName(c.in)
		if got != c.want || ok != c.ok {
			t.Errorf("EmojiFromFileName(%q) = %q, %v; want %q, %v", c.in, got, ok, c.want, c.ok)
		}
	}
}

func TestNormalizeRemovesVariationSelector(t *testing.T) {
	if got := Normalize("❤️"); got != "❤" {
		t.Errorf("Normalize = %q, want %q", got, "❤")
	}
	if got := Normalize("👍"); got != "👍" {
		t.Errorf("Normalize changed an emoji without FE0F: %q", got)
	}
}

func newTestTable(complete bool) *Table {
	return NewTable([]Stamp{
		{ID: "s-thumbsup", Name: "thumbsup", Unicode: true, Emoji: "👍"},
		{ID: "s-heart", Name: "heart", Unicode: true, Emoji: "❤"},
		{ID: "s-unresolved", Name: "zzz", Unicode: true},
		{ID: "s-custom", Name: "kise-custom", Unicode: false},
	}, complete)
}

func TestResolve(t *testing.T) {
	tbl := newTestTable(true)
	cases := []struct {
		key    string
		wantID string
	}{
		{":thumbsup:", "s-thumbsup"},
		{"thumbsup", "s-thumbsup"},
		{"👍", "s-thumbsup"},
		{"❤️", "s-heart"},
		{"❤", "s-heart"},
		{":kise-custom:", "s-custom"},
		{"kise-custom", "s-custom"},
	}
	for _, c := range cases {
		got, err := tbl.Resolve(c.key)
		if err != nil || got.ID != c.wantID {
			t.Errorf("Resolve(%q) = %+v, %v; want ID %q", c.key, got, err, c.wantID)
		}
	}
}

func TestResolveUnknown(t *testing.T) {
	tbl := newTestTable(true)
	for _, key := range []string{":nope:", "nope", "🦄", ""} {
		if _, err := tbl.Resolve(key); !errors.Is(err, ErrUnknownStamp) {
			t.Errorf("Resolve(%q) err = %v, want ErrUnknownStamp", key, err)
		}
	}
}

func TestResolveEmojiWhileTableIncomplete(t *testing.T) {
	tbl := newTestTable(false)
	if _, err := tbl.Resolve("🦄"); !errors.Is(err, ErrEmojiTableIncomplete) {
		t.Errorf("err = %v, want ErrEmojiTableIncomplete", err)
	}
	// Names still resolve while the emoji table is being built.
	if got, err := tbl.Resolve(":thumbsup:"); err != nil || got.ID != "s-thumbsup" {
		t.Errorf("Resolve(:thumbsup:) = %+v, %v", got, err)
	}
	tbl.SetComplete(true)
	if _, err := tbl.Resolve("🦄"); !errors.Is(err, ErrUnknownStamp) {
		t.Errorf("after SetComplete err = %v, want ErrUnknownStamp", err)
	}
}

func TestPutAndDeleteUpdateAllIndexes(t *testing.T) {
	tbl := newTestTable(true)
	tbl.Put(Stamp{ID: "s-thumbsup", Name: "good", Unicode: true, Emoji: "👍"}) // renamed
	if _, err := tbl.Resolve("thumbsup"); !errors.Is(err, ErrUnknownStamp) {
		t.Errorf("old name still resolves after rename: %v", err)
	}
	if got, err := tbl.Resolve("good"); err != nil || got.ID != "s-thumbsup" {
		t.Errorf("new name = %+v, %v", got, err)
	}
	tbl.Delete("s-thumbsup")
	if _, ok := tbl.ByID("s-thumbsup"); ok {
		t.Error("ByID still finds a deleted stamp")
	}
	if _, err := tbl.Resolve("👍"); !errors.Is(err, ErrUnknownStamp) {
		t.Errorf("emoji of a deleted stamp still resolves: %v", err)
	}
}

func TestReactionKey(t *testing.T) {
	cases := []struct {
		s    Stamp
		want string
	}{
		{Stamp{Name: "thumbsup", Unicode: true, Emoji: "👍"}, "👍"},
		{Stamp{Name: "zzz", Unicode: true}, ":zzz:"},
		{Stamp{Name: "kise-custom"}, ":kise-custom:"},
	}
	for _, c := range cases {
		if got := ReactionKey(c.s); got != c.want {
			t.Errorf("ReactionKey(%+v) = %q, want %q", c.s, got, c.want)
		}
	}
}
```

- [ ] **Step 2: 失敗を確かめる**

Run: `go test ./pkg/stampconv/`
Expected: FAIL (undefined: EmojiFromFileName など)

- [ ] **Step 3: 実装する**

```go
// Package stampconv maps traQ stamps to Matrix reaction keys and back.
package stampconv

import (
	"errors"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"unicode/utf8"
)

// Stamp is a traQ stamp as the bridge needs it.
type Stamp struct {
	ID      string
	Name    string
	Unicode bool
	// Emoji is the emoji text of a Unicode stamp, or "" until its file name has been resolved.
	Emoji string
}

var (
	ErrUnknownStamp         = errors.New("unknown stamp")
	ErrEmojiTableIncomplete = errors.New("the emoji to stamp table is still being built")
)

var namePattern = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,32}$`)

// EmojiFromFileName converts a twemoji file name such as "1f44d.svg" or "1f468-200d-1f4bb.svg" into emoji text.
func EmojiFromFileName(name string) (string, bool) {
	base, ok := strings.CutSuffix(name, ".svg")
	if !ok || base == "" {
		return "", false
	}
	var b strings.Builder
	for _, part := range strings.Split(base, "-") {
		cp, err := strconv.ParseUint(part, 16, 32)
		if err != nil || cp > utf8.MaxRune || !utf8.ValidRune(rune(cp)) {
			return "", false
		}
		b.WriteRune(rune(cp))
	}
	return b.String(), true
}

// Normalize removes U+FE0F (variation selector-16) so that emoji with and without it compare equal.
func Normalize(s string) string {
	return strings.ReplaceAll(s, "\uFE0F", "")
}

// ReactionKey returns the Matrix reaction key of a stamp: the emoji of a resolved Unicode stamp, otherwise ":name:".
func ReactionKey(s Stamp) string {
	if s.Unicode && s.Emoji != "" {
		return s.Emoji
	}
	return ":" + s.Name + ":"
}

// Table indexes stamps by ID, name and normalized emoji. It is safe for concurrent use.
type Table struct {
	mu       sync.RWMutex
	byID     map[string]Stamp
	byName   map[string]string // name -> ID
	byEmoji  map[string]string // normalized emoji -> ID
	complete bool
}

// NewTable builds a table. complete reports whether every Unicode stamp already has its Emoji resolved.
func NewTable(stamps []Stamp, complete bool) *Table {
	t := &Table{
		byID:     make(map[string]Stamp, len(stamps)),
		byName:   make(map[string]string, len(stamps)),
		byEmoji:  make(map[string]string, len(stamps)),
		complete: complete,
	}
	for _, s := range stamps {
		t.putLocked(s)
	}
	return t
}

func (t *Table) putLocked(s Stamp) {
	t.deleteLocked(s.ID)
	t.byID[s.ID] = s
	t.byName[s.Name] = s.ID
	if s.Unicode && s.Emoji != "" {
		t.byEmoji[Normalize(s.Emoji)] = s.ID
	}
}

func (t *Table) deleteLocked(id string) {
	old, ok := t.byID[id]
	if !ok {
		return
	}
	delete(t.byID, id)
	if t.byName[old.Name] == id {
		delete(t.byName, old.Name)
	}
	if old.Emoji != "" && t.byEmoji[Normalize(old.Emoji)] == id {
		delete(t.byEmoji, Normalize(old.Emoji))
	}
}

// Put adds or replaces a stamp.
func (t *Table) Put(s Stamp) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.putLocked(s)
}

// Delete removes a stamp by ID.
func (t *Table) Delete(id string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.deleteLocked(id)
}

// SetComplete marks whether every Unicode stamp has its Emoji resolved.
func (t *Table) SetComplete(complete bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.complete = complete
}

// ByID looks up a stamp by its traQ ID.
func (t *Table) ByID(id string) (Stamp, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	s, ok := t.byID[id]
	return s, ok
}

// Resolve finds the stamp for a Matrix reaction key: an emoji, ":name:" or "name".
func (t *Table) Resolve(key string) (Stamp, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	name := strings.TrimSuffix(strings.TrimPrefix(key, ":"), ":")
	if namePattern.MatchString(name) {
		if id, ok := t.byName[name]; ok {
			return t.byID[id], nil
		}
		return Stamp{}, ErrUnknownStamp
	}
	if key == "" {
		return Stamp{}, ErrUnknownStamp
	}
	if id, ok := t.byEmoji[Normalize(key)]; ok {
		return t.byID[id], nil
	}
	if !t.complete {
		return Stamp{}, ErrEmojiTableIncomplete
	}
	return Stamp{}, ErrUnknownStamp
}
```

- [ ] **Step 4: 通ることを確かめる**

Run: `go test -race ./pkg/stampconv/`
Expected: PASS

- [ ] **Step 5: commit**

```bash
git add pkg/stampconv
git commit -m "feat(stampconv): map traQ stamps to Matrix reaction keys"
```

### Task 3: traQ の REST client と OAuth (`pkg/traq`)

**Files:**
- Create: `pkg/traq/client.go` (Client、共通の request 処理、error)
- Create: `pkg/traq/types.go` (使う field だけの型)
- Create: `pkg/traq/api.go` (endpoint ごとの method)
- Create: `pkg/traq/oauth.go` (PKCE、authorize URL、redirect の解析、token の交換・無効化)
- Test: `pkg/traq/client_test.go`、`pkg/traq/oauth_test.go` (`net/http/httptest`)

**Interfaces:**
- Consumes: 標準ライブラリだけ (go-traq は使わない)
- Produces:

```go
package traq

type Client struct{ /* unexported */ }

// NewClient: serverURL is the traQ origin ("https://q.trap.jp"); requests go to <serverURL>/api/v3
// with "Authorization: Bearer <accessToken>".
func NewClient(serverURL string, httpClient *http.Client, accessToken, userAgent string) (*Client, error)
func (c *Client) ServerURL() *url.URL // the origin, without /api/v3

type HTTPError struct {
	Status  int
	Message string // the "message" field of traQ's JSON error body, or the raw body
}
func (e *HTTPError) Error() string
func IsStatus(err error, status int) bool

// types.go
type Message struct {
	ID        string         `json:"id"`
	UserID    string         `json:"userId"`
	ChannelID string         `json:"channelId"`
	Content   string         `json:"content"`
	CreatedAt time.Time      `json:"createdAt"`
	UpdatedAt time.Time      `json:"updatedAt"`
	Stamps    []MessageStamp `json:"stamps"`
}
type MessageStamp struct {
	UserID    string    `json:"userId"`
	StampID   string    `json:"stampId"`
	Count     int       `json:"count"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}
type User struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	DisplayName string    `json:"displayName"`
	IconFileID  string    `json:"iconFileId"`
	Bot         bool      `json:"bot"`
	State       int       `json:"state"` // 0 deactivated, 1 active, 2 suspended
	UpdatedAt   time.Time `json:"updatedAt"`
}
type Me struct {
	User
	Groups      []string `json:"groups"`
	Permissions []string `json:"permissions"`
}
type Channel struct {
	ID       string   `json:"id"`
	ParentID *string  `json:"parentId"`
	Archived bool     `json:"archived"`
	Force    bool     `json:"force"`
	Topic    string   `json:"topic"`
	Name     string   `json:"name"`
	Children []string `json:"children"`
}
type DMChannel struct {
	ID     string `json:"id"`
	UserID string `json:"userId"` // the other party
}
type ChannelList struct {
	Public []Channel   `json:"public"`
	DM     []DMChannel `json:"dm"`
}
type Subscription struct {
	ChannelID string `json:"channelId"`
	Level     int    `json:"level"` // 0 none, 1 unread tracking, 2 unread tracking + notifications
}
type UnreadChannel struct {
	ChannelID       string    `json:"channelId"`
	Count           int       `json:"count"`
	Noticeable      bool      `json:"noticeable"`
	Since           time.Time `json:"since"`
	UpdatedAt       time.Time `json:"updatedAt"`
	OldestMessageID string    `json:"oldestMessageId"`
}
type Stamp struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	FileID    string `json:"fileId"`
	IsUnicode bool   `json:"isUnicode"`
}
type FileInfo struct {
	ID              string `json:"id"`
	Name            string `json:"name"`
	Mime            string `json:"mime"`
	Size            int64  `json:"size"`
	IsAnimatedImage bool   `json:"isAnimatedImage"`
}
type ActivityMessage struct {
	ID        string    `json:"id"`
	UserID    string    `json:"userId"`
	ChannelID string    `json:"channelId"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}
type SearchResult struct {
	TotalHits int64     `json:"totalHits"`
	Hits      []Message `json:"hits"`
}
type MessagesQuery struct {
	Limit     int        // 1-200
	Since     *time.Time
	Until     *time.Time
	Inclusive bool
	Order     string // "asc" or "desc" (default "desc")
}
type SearchQuery struct {
	Word   string
	In     string   // channel ID
	From   []string // user IDs
	Limit  int      // 1-100
	Offset int
}

// api.go (paths are relative to /api/v3)
func (c *Client) Me(ctx context.Context) (*Me, error)                                    // GET /users/me
func (c *Client) Users(ctx context.Context) ([]User, error)                              // GET /users?include-suspended=true
func (c *Client) User(ctx context.Context, userID string) (*User, error)                 // GET /users/{userId}
func (c *Client) Channels(ctx context.Context) (*ChannelList, error)                     // GET /channels?include-dm=true
func (c *Client) Channel(ctx context.Context, channelID string) (*Channel, error)        // GET /channels/{channelId}
func (c *Client) Subscriptions(ctx context.Context) ([]Subscription, error)              // GET /users/me/subscriptions
func (c *Client) Unread(ctx context.Context) ([]UnreadChannel, error)                    // GET /users/me/unread
func (c *Client) ReadChannel(ctx context.Context, channelID string) error                // DELETE /users/me/unread/{channelId}
func (c *Client) Messages(ctx context.Context, channelID string, q MessagesQuery) (msgs []Message, more bool, err error) // GET /channels/{channelId}/messages; more = header X-TRAQ-MORE
func (c *Client) Message(ctx context.Context, messageID string) (*Message, error)        // GET /messages/{messageId}
func (c *Client) PostMessage(ctx context.Context, channelID, content string) (*Message, error) // POST /channels/{channelId}/messages {"content", "embed": true}
func (c *Client) EditMessage(ctx context.Context, messageID, content string) error       // PUT /messages/{messageId} {"content", "embed": true}
func (c *Client) DeleteMessage(ctx context.Context, messageID string) error              // DELETE /messages/{messageId}
func (c *Client) AddStamp(ctx context.Context, messageID, stampID string) error          // POST /messages/{messageId}/stamps/{stampId} {"count": 1}
func (c *Client) RemoveStamp(ctx context.Context, messageID, stampID string) error       // DELETE /messages/{messageId}/stamps/{stampId}
func (c *Client) Stamps(ctx context.Context) ([]Stamp, error)                            // GET /stamps
func (c *Client) Stamp(ctx context.Context, stampID string) (*Stamp, error)              // GET /stamps/{stampId}
func (c *Client) FileMeta(ctx context.Context, fileID string) (*FileInfo, error)         // GET /files/{fileId}/meta
func (c *Client) DownloadFile(ctx context.Context, fileID string) (io.ReadCloser, error) // GET /files/{fileId}, streamed; caller closes
func (c *Client) PublicIcon(ctx context.Context, username string) (data []byte, mime string, err error) // GET /public/icon/{username}
func (c *Client) Search(ctx context.Context, q SearchQuery) (*SearchResult, error)       // GET /messages
func (c *Client) ActivityTimeline(ctx context.Context, perChannel bool) ([]ActivityMessage, error) // GET /activity/timeline?limit=50&all=false&per_channel=...

// oauth.go
type OAuthConfig struct {
	ServerURL   string // origin, e.g. "https://q.trap.jp"
	ClientID    string
	RedirectURI string
	HTTPClient  *http.Client
}
type PKCE struct{ Verifier, Challenge, State string }
type Token struct {
	AccessToken string
	ExpiresAt   time.Time // zero if the server did not send expires_in
}
func NewPKCE() (PKCE, error)                                          // 32 random bytes each, base64url without padding; Challenge = base64url(sha256(Verifier))
func (c OAuthConfig) AuthorizeURL(p PKCE) string                      // <origin>/api/v3/oauth2/authorize?response_type=code&client_id&redirect_uri&scope=read write&state&code_challenge&code_challenge_method=S256
func ParseRedirect(redirected, expectedState string) (code string, err error)
func (c OAuthConfig) Exchange(ctx context.Context, code, verifier string) (Token, error)       // POST /oauth2/token, form: grant_type=authorization_code, code, redirect_uri, client_id, code_verifier
func (c OAuthConfig) Revoke(ctx context.Context, token string) error                           // POST /oauth2/revoke, form: token
var ErrStateMismatch, ErrNoCode error
```

振る舞いの決まり:

- JSON は `encoding/json` の既定 (未知の field は無視) で decode する。`DisallowUnknownFields` を使わない。
- `Authorization: Bearer <token>`。401 はやり直さずに `*HTTPError` で返す (呼び出し側が再 login を求める)。
- 429、502、504 は最大 3 回やり直す。待ち時間は `Retry-After` (秒) があればそれ、無ければ 1s → 2s → 4s。`ctx` が切れたら待たずに返す。503 はやり直さずに `*HTTPError` で返す (search の「検索が無効」を呼び出し側が判定するため)。
- 2xx 以外は `*HTTPError{Status, Message}`。`Message` は body の JSON `{"message": "..."}` か、無ければ body の先頭 200 byte。
- `ParseRedirect` は URL の query の `state` が `expectedState` と違えば `ErrStateMismatch`、`error` があれば `fmt.Errorf("authorization denied: %s", error)`、`code` が無ければ `ErrNoCode`。前後の空白は取り除く。
- token の `expires_in` は秒。`ExpiresAt = now + expires_in`。
- 平文の token を error message や log に入れない。

- [ ] **Step 1: 失敗するテストを書く** (`pkg/traq/client_test.go`)

```go
package traq

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestUnknownFieldsAreIgnored(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v3/messages/m1" {
			t.Errorf("path = %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"m1","userId":"u1","channelId":"c1","content":"hi","createdAt":"2026-01-02T03:04:05Z","updatedAt":"2026-01-02T03:04:05Z","stamps":[],"threadId":null,"aBrandNewField":{"x":1}}`))
	}))
	defer srv.Close()
	c, err := NewClient(srv.URL, srv.Client(), "t", "test")
	if err != nil {
		t.Fatal(err)
	}
	m, err := c.Message(context.Background(), "m1")
	if err != nil {
		t.Fatalf("Message: %v", err)
	}
	if m.ID != "m1" || m.Content != "hi" || !m.CreatedAt.Equal(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)) {
		t.Errorf("decoded %+v", m)
	}
}

func TestBearerTokenAnd401(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Header.Get("Authorization") != "Bearer good" {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"message":"invalid token"}`))
			return
		}
		_, _ = w.Write([]byte(`{"id":"me","name":"kise","displayName":"Kise","groups":["g1"],"permissions":["get_me"]}`))
	}))
	defer srv.Close()
	good, _ := NewClient(srv.URL, srv.Client(), "good", "test")
	me, err := good.Me(context.Background())
	if err != nil || me.Name != "kise" || me.ID != "me" || len(me.Groups) != 1 {
		t.Fatalf("Me = %+v, %v", me, err)
	}
	bad, _ := NewClient(srv.URL, srv.Client(), "bad", "test")
	calls.Store(0)
	_, err = bad.Me(context.Background())
	var he *HTTPError
	if !IsStatus(err, http.StatusUnauthorized) || !errors.As(err, &he) || he.Message != "invalid token" {
		t.Fatalf("err = %v, want 401 HTTPError with the server message", err)
	}
	if calls.Load() != 1 {
		t.Errorf("401 was retried: %d calls", calls.Load())
	}
	if strings.Contains(err.Error(), "bad") {
		t.Error("error message leaks the token")
	}
}

func TestRetriesRateLimit(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		_, _ = w.Write([]byte(`[]`))
	}))
	defer srv.Close()
	c, _ := NewClient(srv.URL, srv.Client(), "t", "test")
	if _, err := c.Stamps(context.Background()); err != nil {
		t.Fatalf("Stamps: %v", err)
	}
	if calls.Load() != 2 {
		t.Errorf("calls = %d, want 2", calls.Load())
	}
}

func TestServiceUnavailableIsNotRetried(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()
	c, _ := NewClient(srv.URL, srv.Client(), "t", "test")
	_, err := c.Search(context.Background(), SearchQuery{Word: "x", Limit: 20})
	if !IsStatus(err, http.StatusServiceUnavailable) || calls.Load() != 1 {
		t.Errorf("err = %v, calls = %d", err, calls.Load())
	}
}

func TestMessagesQueryAndMoreHeader(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if r.URL.Path != "/api/v3/channels/c1/messages" || q.Get("limit") != "30" || q.Get("order") != "asc" ||
			q.Get("since") != "2026-01-02T03:04:05Z" || q.Get("until") != "" {
			t.Errorf("request %s?%s", r.URL.Path, r.URL.RawQuery)
		}
		w.Header().Set("X-TRAQ-MORE", "true")
		_, _ = w.Write([]byte(`[]`))
	}))
	defer srv.Close()
	c, _ := NewClient(srv.URL, srv.Client(), "t", "test")
	since := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	_, more, err := c.Messages(context.Background(), "c1", MessagesQuery{Limit: 30, Since: &since, Order: "asc"})
	if err != nil || !more {
		t.Errorf("more = %v, err = %v", more, err)
	}
}

func TestPostMessageSendsEmbed(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		if r.Method != http.MethodPost || body["content"] != "hello" || body["embed"] != true {
			t.Errorf("%s body = %v", r.Method, body)
		}
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"id":"m2","userId":"u1","channelId":"c1","content":"hello","createdAt":"2026-01-02T03:04:05Z","updatedAt":"2026-01-02T03:04:05Z","stamps":[]}`))
	}))
	defer srv.Close()
	c, _ := NewClient(srv.URL, srv.Client(), "t", "test")
	m, err := c.PostMessage(context.Background(), "c1", "hello")
	if err != nil || m.ID != "m2" {
		t.Errorf("PostMessage = %+v, %v", m, err)
	}
}
```

`pkg/traq/oauth_test.go`:

```go
package traq

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"
)

func TestPKCEChallengeIsS256OfVerifier(t *testing.T) {
	p, err := NewPKCE()
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256([]byte(p.Verifier))
	if p.Challenge != base64.RawURLEncoding.EncodeToString(sum[:]) {
		t.Error("challenge is not base64url(sha256(verifier))")
	}
	if len(p.Verifier) < 43 || p.State == "" || p.State == p.Verifier {
		t.Errorf("weak PKCE values: %+v", p)
	}
}

func TestAuthorizeURL(t *testing.T) {
	cfg := OAuthConfig{ServerURL: "https://q.example.jp", ClientID: "cid", RedirectURI: "http://localhost:29399/callback"}
	u, err := url.Parse(cfg.AuthorizeURL(PKCE{Verifier: "v", Challenge: "ch", State: "st"}))
	if err != nil {
		t.Fatal(err)
	}
	q := u.Query()
	if u.Scheme+"://"+u.Host+u.Path != "https://q.example.jp/api/v3/oauth2/authorize" ||
		q.Get("response_type") != "code" || q.Get("client_id") != "cid" ||
		q.Get("redirect_uri") != "http://localhost:29399/callback" || q.Get("scope") != "read write" ||
		q.Get("state") != "st" || q.Get("code_challenge") != "ch" || q.Get("code_challenge_method") != "S256" {
		t.Errorf("authorize URL = %s", u)
	}
}

func TestParseRedirect(t *testing.T) {
	code, err := ParseRedirect("  http://localhost:29399/callback?code=abc&state=st \n", "st")
	if err != nil || code != "abc" {
		t.Errorf("code = %q, err = %v", code, err)
	}
	if _, err := ParseRedirect("http://localhost:29399/callback?code=abc&state=other", "st"); !errors.Is(err, ErrStateMismatch) {
		t.Errorf("state mismatch err = %v", err)
	}
	if _, err := ParseRedirect("http://localhost:29399/callback?state=st", "st"); !errors.Is(err, ErrNoCode) {
		t.Errorf("missing code err = %v", err)
	}
	if _, err := ParseRedirect("http://localhost:29399/callback?error=access_denied&state=st", "st"); err == nil {
		t.Error("denied authorization was accepted")
	}
}

func TestExchangeSendsPKCEForm(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Fatal(err)
		}
		f := r.PostForm
		if r.URL.Path != "/api/v3/oauth2/token" || f.Get("grant_type") != "authorization_code" || f.Get("code") != "abc" ||
			f.Get("code_verifier") != "ver" || f.Get("client_id") != "cid" || f.Get("redirect_uri") != "http://localhost:29399/callback" {
			t.Errorf("token request %s %v", r.URL.Path, f)
		}
		w.Header().Set("Content-Type", "application/json")
		// traQ 3.32.1 returns no refresh_token and scope: null
		_, _ = w.Write([]byte(`{"access_token":"at","token_type":"Bearer","expires_in":31536000,"scope":null}`))
	}))
	defer srv.Close()
	cfg := OAuthConfig{ServerURL: srv.URL, ClientID: "cid", RedirectURI: "http://localhost:29399/callback", HTTPClient: srv.Client()}
	tok, err := cfg.Exchange(context.Background(), "abc", "ver")
	if err != nil || tok.AccessToken != "at" || time.Until(tok.ExpiresAt) < 364*24*time.Hour {
		t.Errorf("token = %+v, err = %v", tok, err)
	}
}
```

- [ ] **Step 2: 失敗を確かめる** — Run: `go test ./pkg/traq/` → Expected: FAIL (undefined)
- [ ] **Step 3: 実装する** — 上の interface と「振る舞いの決まり」のとおり。endpoint の path と query は interface のコメントのとおり。
- [ ] **Step 4: 通ることを確かめる** — Run: `go test -race ./pkg/traq/` → Expected: PASS
- [ ] **Step 5: commit** — `git commit -m "feat(traq): add REST and OAuth clients for the traQ API"`

### Task 4: 結合テスト用の traQ (`testenv/`) と REST・OAuth の結合テスト

**Files:**
- Create: `testenv/compose.yaml`
- Create: `pkg/traq/integration_helpers_test.go` (`//go:build integration`)
- Create: `pkg/traq/integration_test.go` (`//go:build integration`)
- Modify: `.github/workflows/ci.yml` (job `integration`)

**Interfaces:**
- Consumes: Task 3 の `traq.Client`、`traq.OAuthConfig`
- Produces (Task 5 の結合テストが使う、test file の中だけ):

```go
// integration_helpers_test.go
func testServerURL() string                                   // $TRAQ_TEST_URL or "http://localhost:3080"
func waitForServer(t *testing.T)                              // GET /api/v3/version until 200, up to 120s
type session struct{ http *http.Client }                      // cookie jar, CheckRedirect = http.ErrUseLastResponse
func loginSession(t *testing.T, name, password string) *session // POST /api/v3/login {"name","password"}
func ensureUser(t *testing.T, admin *session, name string) string // POST /api/v3/users {"name","password":name+"-password"} as admin; 409 → look up; returns user ID
func newOAuthClient(t *testing.T, admin *session) traq.OAuthConfig // POST /api/v3/clients {name,description,callbackUrl:"http://localhost:29399/callback",scopes:["read","write"],confidential:false}
func authorize(t *testing.T, s *session, cfg traq.OAuthConfig) traq.Token // NewPKCE → GET AuthorizeURL (302 /consent) → POST /api/v3/oauth2/authorize/decide submit=approve (302 redirect_uri?code&state) → ParseRedirect → Exchange
func apiFor(t *testing.T, s *session, cfg traq.OAuthConfig) (*traq.Client, *traq.Me) // authorize + NewClient + Me
func newChannel(t *testing.T, s *session) string              // POST /api/v3/channels {"name":"it-<random 8 hex>","parent":null}
func subscribe(t *testing.T, s *session, channelID string, level int) // PUT /api/v3/users/me/subscriptions/{channelId} {"level"}
func createStamp(t *testing.T, s *session, name string) string // POST /api/v3/stamps multipart name + file (1x1 PNG, embedded bytes)
func uploadFile(t *testing.T, s *session, channelID string, name string, data []byte) string // POST /api/v3/files multipart file + channelId
func dmChannel(t *testing.T, s *session, userID string) string // GET /api/v3/users/{userId}/dm-channel → id
```

- `testenv/compose.yaml` は次のとおり (traQ 3.32.1 で動作を確かめた構成)。project 名は呼ぶ側が `-p` で付ける。

```yaml
services:
  traq:
    image: ghcr.io/traptitech/traq:3.32.1
    environment:
      TRAQ_ORIGIN: http://localhost:3080
      TRAQ_MARIADB_HOST: mariadb
      TRAQ_ES_URL: http://es:9200
      TRAQ_ALLOWSIGNUP: "true"
    command: ["serve", "--dev", "--skip-init-emojis"]
    ports:
      - "3080:3000"
    depends_on:
      mariadb:
        condition: service_healthy
      es:
        condition: service_healthy
  mariadb:
    image: mariadb:10.11.19
    environment:
      MYSQL_ROOT_PASSWORD: password
      MYSQL_DATABASE: traq
    command: mysqld --character-set-server=utf8mb4 --collation-server=utf8mb4_general_ci
    healthcheck:
      test: mysql --user=root --password=password --execute "SHOW DATABASES;"
      interval: 2s
      timeout: 10s
      retries: 60
  es:
    image: ghcr.io/traptitech/es-with-sudachi:8.8.1-3.1.0
    environment:
      discovery.type: single-node
      xpack.security.enabled: "false"
      ES_JAVA_OPTS: -Xms512m -Xmx512m
    healthcheck:
      test: curl -s http://localhost:9200 >/dev/null || exit 1
      interval: 2s
      timeout: 10s
      retries: 90
```

- 管理者は `traq` / `traq` (`--dev` の初期 user)。テストは毎回新しいチャンネル・user 名を作り、他のテストや前回の実行と干渉しない。
- CI の job `integration`: `docker compose -p tmbit -f testenv/compose.yaml up -d`、`go test -tags integration -race -count=1 ./pkg/traq/...`、最後に `docker compose -p tmbit -f testenv/compose.yaml down -v` (`if: always()`)。

- [ ] **Step 1: テストを書く** (`pkg/traq/integration_test.go`)。次の名前と確かめる内容:
  - `TestIntegrationOAuth`: `authorize` で得た token の `ExpiresAt` が 360 日より先。`Me` が `traq` を返す。`Revoke` の後の `Me` が 401。`ParseRedirect` に別の `state` を渡すと `ErrStateMismatch`。
  - `TestIntegrationMessageLifecycle`: `PostMessage` → `Message` で本文が一致 → `EditMessage` → `Message` の本文と `UpdatedAt` が変わる → `DeleteMessage` → `Message` が 404。
  - `TestIntegrationEmbedOnPost`: 別 user を作り、`PostMessage(ch, "@<name> hi")` の本文が `!{"type":"user","raw":"@<name>","id":"<id>"} hi` になる (`embed: true` の確認)。
  - `TestIntegrationMessagesPaging`: 5 件投稿し、`Messages(Limit: 2, Order: "desc")` が新しい 2 件と `more == true`、`Since` / `Until` / `Inclusive` が境界の 1 件を含む・含まない。
  - `TestIntegrationStamps`: `createStamp` → `Stamps` に名前が出る → `AddStamp` → `Message.Stamps` に (user, stamp) がある → `RemoveStamp` → 無くなる。
  - `TestIntegrationUnreadAndRead`: 別 user の session で、自分が level 1 で購読したチャンネルに投稿 → `Unread` にそのチャンネルが出る → `ReadChannel` → 出なくなる。DM でも同じ (購読無しで出る)。
  - `TestIntegrationChannelsAndSubscriptions`: `Channels` の `Public` に新しいチャンネル、`DM` に `dmChannel` で作った DM (相手の `UserID`) が出る。`subscribe(…, 2)` の後の `Subscriptions` に level 2 で出る。
  - `TestIntegrationFiles`: `uploadFile` → `FileMeta` の名前・size・mime が一致 → `DownloadFile` の中身が一致。
  - `TestIntegrationSearch`: 一意の語を投稿し、`Search(Word: 語, In: ch)` が 30 秒以内 (1 秒ごとに再試行) に 1 件返す (Elasticsearch の index 待ち)。
  - `TestIntegrationPublicIcon`: `PublicIcon("traq")` が画像の mime と空でない data を返す。
- [ ] **Step 2: testenv を立てて実行する** — Run: `docker compose -p tmbit -f testenv/compose.yaml up -d && go test -tags integration -race -count=1 ./pkg/traq/...` → Expected: PASS (Task 3 の client の不具合が出たら、ここで直す)
- [ ] **Step 3: CI に job を足す**、`go vet -tags integration ./...` も通す
- [ ] **Step 4: 片付ける** — `docker compose -p tmbit -f testenv/compose.yaml down -v`
- [ ] **Step 5: commit** — `git commit -m "test(traq): run the REST and OAuth clients against a real traQ"`

### Task 5: traQ の WebSocket client (`pkg/traq/ws.go`)

**Files:**
- Create: `pkg/traq/ws.go`、`pkg/traq/events.go`
- Test: `pkg/traq/ws_test.go` (`httptest` + `coder/websocket` の server 側)、`pkg/traq/ws_integration_test.go` (`//go:build integration`)

**Interfaces:**
- Consumes: `github.com/coder/websocket`、Task 3 の `HTTPError`
- Produces:

```go
package traq

type WSConn struct{ /* unexported */ }

// DialWS connects to <serverURL>/api/v3/ws with "Authorization: Bearer <accessToken>".
// A 401 handshake response is returned as *HTTPError{Status: 401}.
func DialWS(ctx context.Context, serverURL, accessToken string, httpClient *http.Client) (*WSConn, error)
func (c *WSConn) SetTimelineStreaming(ctx context.Context, on bool) error // "timeline_streaming:on" / "timeline_streaming:off"
func (c *WSConn) SetViewState(ctx context.Context, channelID string) error // "viewstate:<id>:monitoring"; channelID == "" → "viewstate:null"
func (c *WSConn) ReadEvent(ctx context.Context) (Event, error)            // blocks; returns the next decoded event
func (c *WSConn) Ping(ctx context.Context) error                           // WebSocket ping; waits for the pong
func (c *WSConn) Close() error

// events.go
type Event struct {
	Type string          `json:"type"`
	Body json.RawMessage `json:"body"`
}
type IDEvent struct{ ID string `json:"id"` } // MESSAGE_CREATED/UPDATED/DELETED, MESSAGE_READ (channel ID), USER_UPDATED, USER_ICON_UPDATED, STAMP_*, CHANNEL_UPDATED/DELETED
type MessageCreatedEvent struct {
	ID       string `json:"id"`
	IsCiting bool   `json:"is_citing"`
}
type ChannelCreatedEvent struct {
	ID       string `json:"id"`
	DMUserID string `json:"dm_user_id"` // empty unless DM
}
type MessageStampedEvent struct {
	MessageID string    `json:"message_id"`
	UserID    string    `json:"user_id"`
	StampID   string    `json:"stamp_id"`
	Count     int       `json:"count"`
	CreatedAt time.Time `json:"created_at"`
}
type MessageUnstampedEvent struct {
	MessageID string `json:"message_id"`
	UserID    string `json:"user_id"`
	StampID   string `json:"stamp_id"`
}
func (e Event) Decode(v any) error // json.Unmarshal(e.Body, v)
```

決まり:

- URL は `server_url` の scheme が `https` なら `wss`、`http` なら `ws`。
- `ReadEvent` は text frame の JSON を `Event` に decode する。JSON でない frame は読み飛ばす。読み込みの上限は 1 MiB (`SetReadLimit`)。
- `WSConn` は goroutine safe (送信は mutex で直列化)。keepalive の間隔と再接続は呼ぶ側 (connector) が持つ。
- token を error message に含めない。

- [ ] **Step 1: 失敗する単体テストを書く** (`pkg/traq/ws_test.go`)。httptest の server で `websocket.Accept` し、次を確かめる:
  - `TestDialSendsBearerToken`: handshake の `Authorization` が `Bearer tok`、path が `/api/v3/ws`。
  - `TestDial401`: server が 401 を返すと `IsStatus(err, 401)` が true で、err の文字列に token が無い。
  - `TestCommands`: `SetTimelineStreaming(true)`、`SetViewState("c1")`、`SetViewState("")` で server が `timeline_streaming:on`、`viewstate:c1:monitoring`、`viewstate:null` を順に受け取る。
  - `TestReadEventSkipsGarbage`: server が `not json` → `{"type":"MESSAGE_STAMPED","body":{"message_id":"m","user_id":"u","stamp_id":"s","count":2,"created_at":"2026-01-02T03:04:05Z"}}` を送ると、`ReadEvent` は 2 つ目を返し、`Decode` で全 field が埋まる。
  - `TestReadEventContextCancel`: `ctx` を cancel すると `ReadEvent` が `ctx.Err()` を包んだ error で戻る。
- [ ] **Step 2: 失敗を確かめる** — `go test ./pkg/traq/ -run 'Dial|Commands|ReadEvent'` → FAIL
- [ ] **Step 3: 実装する**
- [ ] **Step 4: 単体テストを通す** — `go test -race ./pkg/traq/` → PASS
- [ ] **Step 5: 結合テストを書く** (`ws_integration_test.go`、Task 4 の helper を使う)。spec の「受信」の表 (実測) を固定する:
  - `TestIntegrationWSTimelineStreaming`: `timeline_streaming:on` だけの接続で、別 user が public チャンネルに投稿・編集・削除すると `MESSAGE_CREATED`、`MESSAGE_UPDATED`、`MESSAGE_DELETED` が届く。stamp を付けても `MESSAGE_STAMPED` は 3 秒待っても届かない。
  - `TestIntegrationWSViewState`: `SetViewState(ch)` の後は同じ操作で `MESSAGE_STAMPED` (count と user が一致) と `MESSAGE_UNSTAMPED` が届く。
  - `TestIntegrationWSDM`: 相手が DM に投稿すると `MESSAGE_CREATED` が届き、編集しても viewstate 無しでは `MESSAGE_UPDATED` が 3 秒待っても届かない。
  - `TestIntegrationWSRead`: 自分が `ReadChannel` すると `MESSAGE_READ` (id = channel) が届く。
  - 「届かない」の確認は 3 秒の timeout で、その間に届いた別の種類の event は無視する。
- [ ] **Step 6: 結合テストを通す** — `docker compose -p tmbit -f testenv/compose.yaml up -d && go test -tags integration -race -count=1 ./pkg/traq/...` → PASS。片付ける。
- [ ] **Step 7: commit** — `git commit -m "feat(traq): add a WebSocket client for traQ events"`

### Task 6: traQ の本文 → Matrix (`pkg/msgconv/tomatrix.go`)

**Files:**
- Create: `pkg/msgconv/embed.go` (埋め込みの解析)、`pkg/msgconv/tomatrix.go`、`pkg/msgconv/markdown.go` (goldmark の設定と traQ 用の拡張)
- Test: `pkg/msgconv/tomatrix_test.go`

**Interfaces:**
- Consumes: `github.com/yuin/goldmark` (+ `extension.GFM`、`extension.Linkify`)、`maunium.net/go/mautrix/id`
- Produces:

```go
package msgconv

type Embed struct {
	Type string `json:"type"` // "user", "group", "channel", "file", "message"
	Raw  string `json:"raw"`
	ID   string `json:"id"`
}

// Resolver answers lookups that need bridge state. Implementations must not block on the network.
type Resolver interface {
	UserMXID(traqUserID string) (id.UserID, bool)   // ghost MXID, or the logged-in user's own MXID for themselves
	ChannelLink(channelID string) (string, bool)    // matrix.to URL of the channel's portal room, if it exists
	MessageLink(messageID string) (string, bool)    // matrix.to URL of the bridged event, if bridged
	CanReplyTo(messageID string) bool               // bridged in the same portal as the message being converted
}

type ToMatrixInput struct {
	Content    string
	ServerURL  *url.URL // traQ origin
	SelfUserID string   // logged-in traQ user
	SelfGroups []string // group IDs of the logged-in user
	SelfMXID   id.UserID
}

type ToMatrixResult struct {
	Body     string      // plain text
	HTML     string      // formatted_body (org.matrix.custom.html)
	Mentions []id.UserID // for m.mentions (deduplicated, in order of appearance)
	ReplyTo  string      // traQ message ID to reply to, "" if none
	FileIDs  []string    // traQ file IDs to send as separate parts, in order, deduplicated
}

func ToMatrix(in ToMatrixInput, r Resolver) ToMatrixResult
```

決まり (traQ の web client の描画に合わせる):

- Markdown は goldmark の GFM (table、strikethrough、task list、linkify) と hard wrap (改行 = `<br>`)。raw HTML は描画せず escape する。
- 埋め込み `!{...}` と spoiler `!!...!!` は goldmark の inline 拡張として解析する (code span と code block の中では文字のまま)。JSON として読めない、または `type` が上の 5 つ以外の `!{...}` は文字のまま。
- `user`: `UserMXID` が引ければ HTML は `<a href="https://matrix.to/#/<mxid>">@name</a>` (表示は `raw`)、`Mentions` に足す。引けなければ `raw` の文字。body は常に `raw`。
- `group`: 文字 `raw`。`ID` が `SelfGroups` にあれば `SelfMXID` を `Mentions` に足す (HTML は文字のまま)。
- `channel`: `ChannelLink` が引ければその URL、無ければ `<origin>/channels/<raw から # を除いたもの>` への link (表示は `raw`)。
- `file`: `FileIDs` に足し、本文から消す。本文中の裸の URL `<origin>/files/<uuid>` (linkify された autolink、大文字小文字を区別しない UUID) も同じ。
- `message`: 本文中の裸の URL `<origin>/messages/<uuid>` と埋め込みを順に見て、最初に `CanReplyTo` が true のものを `ReplyTo` にして本文から消す。それ以外は `MessageLink` が引ければその URL への link (表示は元の URL)、無ければ元の URL のまま。
- `[label](url)` の明示的な link は、URL が files / messages でも触らない。
- spoiler: HTML は `<span data-mx-spoiler>…</span>`、body は `!!…!!` のまま。
- 消した結果の空行は詰め、先頭と末尾の空白を除く。本文が空になったら `Body` と `HTML` は空文字列 (呼ぶ側が file の part だけを送る)。
- `Body` は元の Markdown 文字列から、埋め込みを `raw` に、消す URL を消したもの (Markdown の記号は残す)。

- [ ] **Step 1: 失敗するテストを書く** (`pkg/msgconv/tomatrix_test.go`)。fake の `Resolver` (map で引く) を使った table test:

```go
package msgconv

import (
	"net/url"
	"reflect"
	"testing"

	"maunium.net/go/mautrix/id"
)

type fakeResolver struct {
	users    map[string]id.UserID
	channels map[string]string
	links    map[string]string
	replies  map[string]bool
}

func (f fakeResolver) UserMXID(u string) (id.UserID, bool)  { v, ok := f.users[u]; return v, ok }
func (f fakeResolver) ChannelLink(c string) (string, bool) { v, ok := f.channels[c]; return v, ok }
func (f fakeResolver) MessageLink(m string) (string, bool) { v, ok := f.links[m]; return v, ok }
func (f fakeResolver) CanReplyTo(m string) bool            { return f.replies[m] }

const (
	uAlice = "11111111-1111-1111-1111-111111111111"
	uSelf  = "22222222-2222-2222-2222-222222222222"
	gSelf  = "33333333-3333-3333-3333-333333333333"
	gOther = "44444444-4444-4444-4444-444444444444"
	cGen   = "55555555-5555-5555-5555-555555555555"
	cNone  = "66666666-6666-6666-6666-666666666666"
	m1     = "77777777-7777-7777-7777-777777777777"
	m2     = "88888888-8888-8888-8888-888888888888"
	f1     = "99999999-9999-9999-9999-999999999999"
)

func input(content string) ToMatrixInput {
	return ToMatrixInput{
		Content:    content,
		ServerURL:  &url.URL{Scheme: "https", Host: "q.example.jp"},
		SelfUserID: uSelf,
		SelfGroups: []string{gSelf},
		SelfMXID:   "@me:example.org",
	}
}

var resolver = fakeResolver{
	users:    map[string]id.UserID{uAlice: "@traq_alice:example.org", uSelf: "@me:example.org"},
	channels: map[string]string{cGen: "https://matrix.to/#/!gen:example.org"},
	links:    map[string]string{m2: "https://matrix.to/#/!gen:example.org/$ev2"},
	replies:  map[string]bool{m1: true},
}

func TestToMatrix(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want ToMatrixResult
	}{
		{"plain", "hello", ToMatrixResult{Body: "hello", HTML: "hello"}},
		{"hard wrap", "a\nb", ToMatrixResult{Body: "a\nb", HTML: "a<br>b"}},
		{"bold and raw html escaped", "**x** <b>y</b>", ToMatrixResult{Body: "**x** <b>y</b>", HTML: "<strong>x</strong> &lt;b&gt;y&lt;/b&gt;"}},
		{"user mention", `!{"type":"user","raw":"@alice","id":"` + uAlice + `"} hi`,
			ToMatrixResult{Body: "@alice hi", HTML: `<a href="https://matrix.to/#/@traq_alice:example.org">@alice</a> hi`, Mentions: []id.UserID{"@traq_alice:example.org"}}},
		{"self mention", `!{"type":"user","raw":"@me","id":"` + uSelf + `"}`,
			ToMatrixResult{Body: "@me", HTML: `<a href="https://matrix.to/#/@me:example.org">@me</a>`, Mentions: []id.UserID{"@me:example.org"}}},
		{"own group mentions self", `!{"type":"group","raw":"@all","id":"` + gSelf + `"}`,
			ToMatrixResult{Body: "@all", HTML: "@all", Mentions: []id.UserID{"@me:example.org"}}},
		{"other group", `!{"type":"group","raw":"@x","id":"` + gOther + `"}`, ToMatrixResult{Body: "@x", HTML: "@x"}},
		{"bridged channel", `!{"type":"channel","raw":"#gps/general","id":"` + cGen + `"}`,
			ToMatrixResult{Body: "#gps/general", HTML: `<a href="https://matrix.to/#/!gen:example.org">#gps/general</a>`}},
		{"unbridged channel", `!{"type":"channel","raw":"#a/b","id":"` + cNone + `"}`,
			ToMatrixResult{Body: "#a/b", HTML: `<a href="https://q.example.jp/channels/a/b">#a/b</a>`}},
		{"embed in code span stays", "`!{\"type\":\"user\",\"raw\":\"@alice\",\"id\":\"" + uAlice + "\"}`",
			ToMatrixResult{Body: "`!{\"type\":\"user\",\"raw\":\"@alice\",\"id\":\"" + uAlice + "\"}`", HTML: "<code>!{&quot;type&quot;:&quot;user&quot;,&quot;raw&quot;:&quot;@alice&quot;,&quot;id&quot;:&quot;" + uAlice + "&quot;}</code>"}},
		{"broken embed stays", `!{"type":"user",`, ToMatrixResult{Body: `!{"type":"user",`, HTML: `!{&quot;type&quot;:&quot;user&quot;,`}},
		{"spoiler", "a !!secret!! b", ToMatrixResult{Body: "a !!secret!! b", HTML: "a <span data-mx-spoiler>secret</span> b"}},
		{"file url removed", "look\nhttps://q.example.jp/files/" + f1,
			ToMatrixResult{Body: "look", HTML: "look", FileIDs: []string{f1}}},
		{"file embed removed and deduplicated", `!{"type":"file","raw":"x","id":"` + f1 + `"} https://q.example.jp/files/` + f1,
			ToMatrixResult{FileIDs: []string{f1}}},
		{"first reply candidate becomes reply", "https://q.example.jp/messages/" + m1 + "\nyes",
			ToMatrixResult{Body: "yes", HTML: "yes", ReplyTo: m1}},
		{"bridged message elsewhere becomes link", "see https://q.example.jp/messages/" + m2,
			ToMatrixResult{Body: "see https://q.example.jp/messages/" + m2, HTML: `see <a href="https://matrix.to/#/!gen:example.org/$ev2">https://q.example.jp/messages/` + m2 + `</a>`}},
		{"explicit link untouched", "[x](https://q.example.jp/files/" + f1 + ")",
			ToMatrixResult{Body: "[x](https://q.example.jp/files/" + f1 + ")", HTML: `<a href="https://q.example.jp/files/` + f1 + `">x</a>`}},
		{"other origin untouched", "https://other.example/files/" + f1,
			ToMatrixResult{Body: "https://other.example/files/" + f1, HTML: `<a href="https://other.example/files/` + f1 + `">https://other.example/files/` + f1 + `</a>`}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := ToMatrix(input(c.in), resolver)
			if !reflect.DeepEqual(got, c.want) {
				t.Errorf("ToMatrix(%q)\n got  %#v\n want %#v", c.in, got, c.want)
			}
		})
	}
}
```

  HTML の細部 (段落の `<p>` の有無、属性の順) は上の期待値に合わせる: 段落が 1 つだけなら `<p>` で包まない、2 つ以上なら包む。table・list・code block などの block 要素は goldmark の出力のまま。
- [ ] **Step 2: 失敗を確かめる** — `go test ./pkg/msgconv/` → FAIL
- [ ] **Step 3: 実装する**
- [ ] **Step 4: 通す** — `go test -race ./pkg/msgconv/` → PASS。ほかに段落 2 つ・list・code block・table の case を 1 つずつ足し、goldmark の出力どおりであることを固定する。
- [ ] **Step 5: commit** — `git commit -m "feat(msgconv): convert traQ messages to Matrix content"`

### Task 7: Matrix の本文 → traQ (`pkg/msgconv/totraq.go`)

**Files:**
- Create: `pkg/msgconv/totraq.go`
- Test: `pkg/msgconv/totraq_test.go`

**Interfaces:**
- Consumes: `maunium.net/go/mautrix/format` (`HTMLParser` を struct copy して converter を差し替える。§7 of `docs/notes/bridgev2-v0.33.0.md`)、`maunium.net/go/mautrix/id`
- Produces:

```go
package msgconv

const MaxTraQLength = 10000 // in Unicode code points

var ErrTooLong error // wrapped with the actual length

// TraQUserResolver maps a Matrix user (a ghost, or a Matrix user logged into the bridge) to a traQ user.
type TraQUserResolver interface {
	TraQUser(mxid id.UserID) (traqUserID, name string, ok bool)
}

type ToTraQInput struct {
	Body          string
	Format        string // event.FormatHTML or ""
	FormattedBody string
	ReplyTo       string   // traQ message ID of the reply target, "" if none
	ServerURL     *url.URL // traQ origin
}

func ToTraQ(ctx context.Context, in ToTraQInput, r TraQUserResolver) (string, error)
```

決まり:

- `Format == "org.matrix.custom.html"` なら `format.MarkdownHTMLParser` の copy で Markdown に戻す。user の pill は `TraQUser` が引ければ `!{"type":"user","raw":"@<name>","id":"<traqUserID>"}`、引けなければ表示名の文字。room / event の link は URL の文字。spoiler (`data-mx-spoiler`) は `!!…!!`。`<mx-reply>` は捨てる。
- HTML が無ければ `Body` をそのまま使う。`ReplyTo` があり、`Body` の先頭が `> <@` で始まる reply fallback の行なら、最初の空行までを捨てる。
- `ReplyTo` があれば先頭に `<origin>/messages/<ReplyTo>` の行を付ける。
- 長さは code point で数え、`MaxTraQLength` を超えたら `fmt.Errorf("%w: %d characters", ErrTooLong, n)`。

- [ ] **Step 1: 失敗するテストを書く** (`pkg/msgconv/totraq_test.go`):

```go
package msgconv

import (
	"context"
	"errors"
	"net/url"
	"strings"
	"testing"

	"maunium.net/go/mautrix/id"
)

type fakeTraQUsers map[id.UserID][2]string

func (f fakeTraQUsers) TraQUser(mxid id.UserID) (string, string, bool) {
	v, ok := f[mxid]
	return v[0], v[1], ok
}

var traqUsers = fakeTraQUsers{"@traq_alice:example.org": {uAlice, "alice"}}
var origin = &url.URL{Scheme: "https", Host: "q.example.jp"}

func TestToTraQ(t *testing.T) {
	cases := []struct {
		name string
		in   ToTraQInput
		want string
	}{
		{"plain body", ToTraQInput{Body: "hello"}, "hello"},
		{"html bold", ToTraQInput{Body: "x", Format: "org.matrix.custom.html", FormattedBody: "<strong>x</strong>"}, "**x**"},
		{"ghost pill", ToTraQInput{Body: "alice: hi", Format: "org.matrix.custom.html",
			FormattedBody: `<a href="https://matrix.to/#/@traq_alice:example.org">alice</a>: hi`},
			`!{"type":"user","raw":"@alice","id":"` + uAlice + `"}: hi`},
		{"unknown pill", ToTraQInput{Body: "bob", Format: "org.matrix.custom.html",
			FormattedBody: `<a href="https://matrix.to/#/@bob:example.org">bob</a>`}, "bob"},
		{"spoiler", ToTraQInput{Body: "s", Format: "org.matrix.custom.html",
			FormattedBody: `a <span data-mx-spoiler>s</span>`}, "a !!s!!"},
		{"reply prefix", ToTraQInput{Body: "yes", ReplyTo: m1}, "https://q.example.jp/messages/" + m1 + "\nyes"},
		{"reply fallback stripped", ToTraQInput{Body: "> <@a:x> old\n> more\n\nyes", ReplyTo: m1},
			"https://q.example.jp/messages/" + m1 + "\nyes"},
		{"mx-reply stripped", ToTraQInput{Body: "yes", Format: "org.matrix.custom.html",
			FormattedBody: "<mx-reply><blockquote>old</blockquote></mx-reply>yes", ReplyTo: m1},
			"https://q.example.jp/messages/" + m1 + "\nyes"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			c.in.ServerURL = origin
			got, err := ToTraQ(context.Background(), c.in, traqUsers)
			if err != nil || got != c.want {
				t.Errorf("ToTraQ = %q, %v; want %q", got, err, c.want)
			}
		})
	}
}

func TestToTraQLength(t *testing.T) {
	ok := strings.Repeat("あ", MaxTraQLength)
	if _, err := ToTraQ(context.Background(), ToTraQInput{Body: ok, ServerURL: origin}, traqUsers); err != nil {
		t.Errorf("10000 code points rejected: %v", err)
	}
	_, err := ToTraQ(context.Background(), ToTraQInput{Body: ok + "あ", ServerURL: origin}, traqUsers)
	if !errors.Is(err, ErrTooLong) {
		t.Errorf("10001 code points: err = %v, want ErrTooLong", err)
	}
}
```

- [ ] **Step 2: 失敗を確かめる** — `go test ./pkg/msgconv/ -run ToTraQ` → FAIL
- [ ] **Step 3: 実装する**
- [ ] **Step 4: 通す** — `go test -race ./pkg/msgconv/` → PASS
- [ ] **Step 5: commit** — `git commit -m "feat(msgconv): convert Matrix messages to traQ markdown"`

### Task 8: connector の ID、metadata、login (OAuth + PKCE)、logout

**Files:**
- Create: `pkg/connector/ids.go`、`pkg/connector/meta.go`、`pkg/connector/login.go`、`pkg/connector/client.go` (この task では struct と login 状態の method だけ)
- Modify: `pkg/connector/connector.go` (`GetLoginFlows`、`CreateLogin`、`LoadUserLogin`、`GetDBMetaTypes`)
- Test: `pkg/connector/ids_test.go`、`pkg/connector/login_test.go`

**Interfaces:**
- Consumes: Task 3 (`traq.OAuthConfig`、`NewPKCE`、`ParseRedirect`、`Exchange`、`Revoke`、`NewClient`、`Me`)。bridgev2 の login は `docs/notes/bridgev2-v0.33.0.md` §2。
- Produces:

```go
package connector

// ids.go
func PortalKey(channelID string) networkid.PortalKey                           // public channel: {ID: channelID}
func DMPortalKey(channelID string, login networkid.UserLoginID) networkid.PortalKey // DM: {ID: channelID, Receiver: login}
func ChannelID(key networkid.PortalKey) string
func UserID(traqUserID string) networkid.UserID
func TraQUserID(id networkid.UserID) string
func MessageID(traqMessageID string) networkid.MessageID
func TraQMessageID(id networkid.MessageID) string
func EmojiID(stampID string) networkid.EmojiID
func StampID(id networkid.EmojiID) string
func LoginID(traqUserID string) networkid.UserLoginID

// meta.go
type UserLoginMetadata struct {
	AccessToken string    `json:"access_token"`
	ExpiresAt   time.Time `json:"expires_at"`
}
type PortalMetadata struct {
	IsDM     bool   `json:"is_dm,omitempty"`
	DMUserID string `json:"dm_user_id,omitempty"` // the other party of a DM
}
type MessageMetadata struct {
	ContentHash string `json:"content_hash,omitempty"` // hex sha256 of the traQ content as last bridged (first part only)
}
func ContentHash(content string) string

// client.go
type TraQClient struct {
	Main  *TraQConnector
	Login *bridgev2.UserLogin
	API   *traq.Client // nil when the stored token is missing
	// fields for later tasks are added there
}
func (c *TraQClient) IsLoggedIn() bool              // token present and not expired
func (c *TraQClient) LogoutRemote(ctx context.Context) // Revoke (errors logged), then stop the client
func (c *TraQClient) IsThisUser(ctx context.Context, userID networkid.UserID) bool
```

決まり:

- ID は traQ の UUID をそのまま使う (prefix を付けない)。`TraQ*` は逆変換。
- login flow は 1 つ: `{ID: "oauth", Name: "OAuth", Description: "Log in with your traQ account in a browser"}`。
- `Start` は `NewPKCE` と `AuthorizeURL` を作り、`LoginStepTypeUserInput` を返す: `StepID: "io.github.kisepichu.traq.oauth.redirect"`、`Instructions` に「1. 次の URL を開いて承認する。2. 承認後に開いた (読み込めない) ページの URL をそのまま貼る」を英語で、field は 1 つ `{Type: LoginInputFieldTypeURL, ID: "redirect_url", Name: "Redirected URL"}`。
- `SubmitUserInput` は `ParseRedirect` → `Exchange` → `NewClient` → `Me` → `User.NewLogin(ctx, &database.UserLogin{ID: LoginID(me.ID), RemoteName: me.Name, RemoteProfile: {Username: me.Name, Name: me.DisplayName}, Metadata: &UserLoginMetadata{…}}, nil)`。その後、`Login.Client.Connect` を `Login.Bridge.BackgroundCtx` (と login の logger) で goroutine から呼ぶ (bridgev2 は login 後に Connect を呼ばない)。返す step は `LoginStepTypeComplete`、`Instructions: "Successfully logged in as @<name>"`。
- `ErrStateMismatch` / `ErrNoCode` / 認可拒否は `bridgev2.RespError` 相当の user 向け error (「URL が違う、最初からやり直す」) で返す。token と verifier を error に含めない。
- `LoadUserLogin` は IO をせず、metadata の token から `traq.NewClient` を作って `TraQClient` を `Login.Client` に置くだけ (bridgev2 の lock の中で呼ばれる)。
- `Cancel` は何もしない (verifier は memory から消えるだけ)。

- [ ] **Step 1: 失敗するテストを書く**
  - `ids_test.go`: 各 ID の往復。`DMPortalKey` だけ `Receiver` が入る。
  - `login_test.go`: httptest の traQ (`/api/v3/oauth2/token` と `/api/v3/users/me` だけ) に対して、`Start` の step の `Instructions` に authorize URL が入り、その URL の `state` と `code_challenge` が `PKCE` のものであること。`SubmitUserInput` に別の `state` の URL を渡すと error で、token endpoint が呼ばれないこと。`NewLogin` まで進む経路は bridgev2 の `User` が要るので単体テストせず、Task 16 の通しで確かめる。
- [ ] **Step 2: 失敗を確かめる** — `go test ./pkg/connector/` → FAIL
- [ ] **Step 3: 実装する**
- [ ] **Step 4: 通す** — `go test -race ./...` → PASS、`go build -tags goolm ./...`
- [ ] **Step 5: commit** — `git commit -m "feat(connector): add traQ OAuth login with PKCE"`

### Task 9: 接続、購読の同期、ChatInfo / UserInfo

**Files:**
- Modify: `pkg/connector/client.go` (`Connect`、`Disconnect`、WebSocket の loop、bridge state)
- Create: `pkg/connector/sync.go` (チャンネル木、購読、DM、未読 → portal)、`pkg/connector/chatinfo.go` (`GetChatInfo`、`GetUserInfo`)、`pkg/connector/cache.go` (user とチャンネルの cache)
- Test: `pkg/connector/sync_test.go`、`pkg/connector/cache_test.go`

**Interfaces:**
- Consumes: Task 3、5、8。bridgev2 §3 (simplevent)、§4 (ChatInfo)、§12 (bridge state)。
- Produces (後の task が使う):

```go
// cache.go
type channelInfo struct {
	ID       string
	Path     string // "gps/times/kise" (no leading #)
	Topic    string
	DMUserID string // non-empty for DMs
	Level    int    // subscription level; DMs are always 2
}
type channelTree struct{ /* goroutine safe */ }
func buildChannelTree(list *traq.ChannelList) *channelTree // paths from ParentID chains; archived channels included
func (t *channelTree) Get(channelID string) (channelInfo, bool)
func (t *channelTree) ByPath(path string) (channelInfo, bool) // path with or without leading '#'
type userCache struct{ /* goroutine safe; filled from Users() on connect, updated by USER_* events */ }
func (u *userCache) Get(ctx context.Context, api *traq.Client, userID string) (*traq.User, error) // cache, else User()
func (u *userCache) ByName(name string) (*traq.User, bool)

// sync.go
// portalDiff computes which portals to create/update/remove from the old and new subscription snapshot.
type portalChange struct {
	Channel channelInfo
	Kind    int // changeUpsert, changeRemove
}
func portalDiff(old, new map[string]channelInfo) []portalChange // upsert when new or Path/Topic/Level changed; remove when gone or level 0

// client.go (additions)
func (c *TraQClient) Connect(ctx context.Context)
func (c *TraQClient) Disconnect()
func (c *TraQClient) GetChatInfo(ctx context.Context, portal *bridgev2.Portal) (*bridgev2.ChatInfo, error)
func (c *TraQClient) GetUserInfo(ctx context.Context, ghost *bridgev2.Ghost) (*bridgev2.UserInfo, error)
func (c *TraQClient) self() *traq.Me
```

決まり:

- `Connect`:
  1. token が無いか期限切れ → `BAD_CREDENTIALS` (`UserAction: relogin`) を送って終わる。
  2. `StateConnecting` を送る。`Me`、`Users`、`Channels`、`Subscriptions`、`Unread`、`Stamps` を読む。401 → `BAD_CREDENTIALS`。その他の error → `TRANSIENT_DISCONNECT` を送り、backoff (1s から倍、上限 5 分) でやり直す。
  3. portal の対象 = level ≥ 1 の public チャンネル + 全 DM。各々に `simplevent.ChatResync{EventMeta{Type: RemoteEventChatResync, PortalKey, CreatePortal: true}, ChatInfo}` を queue する。Task 12 が `CheckNeedsBackfillFunc` を足す。
  4. 対象から外れた portal (既存の portal で、この login が居るもの: `GetUserLoginsInPortal` / user portal の一覧) には `simplevent.ChatDelete{OnlyForMe: true}` を queue する。
  5. `Unread` に無い portal には `simplevent.Receipt{Type: RemoteEventReadReceipt, Sender: {IsFromMe: true, SenderLogin, Sender}, ReadUpTo: now}` を queue する (portal loop は直列なので resync の後に処理される)。
  6. WebSocket を張り (`DialWS` → `SetTimelineStreaming(true)`)、`StateConnected` を送る。event loop を goroutine で回す (Task 10 が dispatch を足す)。30 秒ごとに `Ping` (10 秒で timeout) し、失敗か read error なら `TRANSIENT_DISCONNECT` を送って backoff で繋ぎ直す。繋ぎ直した後は Task 12 の catch-up を呼ぶ。WebSocket の 401 → `BAD_CREDENTIALS`。
  7. `subscription_sync_interval` ごとに `Channels` と `Subscriptions` を読み直し、`portalDiff` の結果だけを queue する (upsert は `ChatResync` + ChatInfo、remove は `ChatDelete{OnlyForMe: true}`)。
- `Disconnect` は全 goroutine を止めて待つ (context cancel + WaitGroup)、WebSocket を閉じる。何度呼んでもよい。
- `GetChatInfo`:
  - public: `Name` = Path、`Topic`、`Members: {IsFull: false, MemberMap: {自分: {EventSender{IsFromMe: true, SenderLogin, Sender: UserID(me.ID)}, Membership: join}}}`、`CanBackfill: true`、`UserLocal.MutedUntil` = level 1 なら `event.MutedForever`、2 なら `bridgev2.Unmuted`。
  - DM: `Type: DM`、`Name` = 相手の表示名、`Avatar` = 相手の icon (UserInfo と同じ ID)、`Members: {IsFull: true, OtherUserID: 相手, MemberMap: {自分, 相手}}`、`CanBackfill: true`。自分との DM (相手 = 自分) は member が自分だけ。
  - portal の metadata `PortalMetadata` に `IsDM`、`DMUserID` を入れる (`ExtraUpdates` で)。
- `GetUserInfo`: `Name` = `displayName` (bot なら末尾に ` (bot)`)、`Identifiers: ["traq:<name>"]`、`IsBot`、`Avatar: {ID: networkid.AvatarID(iconFileId), Get: PublicIcon(name)}` (icon が変わると `iconFileId` が変わる)。
- 1 つの bridge に複数の login があってもよい: public の portal は共有 (Receiver 無し)、DM は login ごと。

- [ ] **Step 1: 失敗するテストを書く**
  - `cache_test.go`: 親子 3 段のチャンネルと DM を含む `ChannelList` から、Path が `a/b/c`、`ByPath("#a/b")` が引ける、DM は `DMUserID` が入り Path が空。
  - `sync_test.go`: `portalDiff` の table test — 新規、変化なし (出さない)、Topic だけ変化、level 2 → 1、level 1 → 0 (remove)、消えたチャンネル (remove)、DM は level に関わらず残る。
- [ ] **Step 2: 失敗を確かめる** — `go test ./pkg/connector/` → FAIL
- [ ] **Step 3: 実装する**
- [ ] **Step 4: 通す** — `go test -race ./...` → PASS
- [ ] **Step 5: commit** — `git commit -m "feat(connector): connect to traQ and sync subscribed channels to portals"`

### Task 10: traQ の event → Matrix (message、編集、削除、スタンプ、既読、user・チャンネルの変更)

**Files:**
- Create: `pkg/connector/handle_traq.go` (WebSocket event の dispatch)、`pkg/connector/convert.go` (traQ message → `bridgev2.ConvertedMessage`、file の再 upload)
- Modify: `pkg/connector/client.go` (event loop から dispatch を呼ぶ)、`pkg/connector/connector.go` (`TraQConnector.Stamps *stampconv.Table` を足し、`Start` で空の Table を作る。Task 14 が database から読むように変える)
- Test: `pkg/connector/convert_test.go`

**Interfaces:**
- Consumes: Task 2 (`stampconv.Table`、`ReactionKey`)、Task 6 (`msgconv.ToMatrix`)、Task 9。bridgev2 §3、§5 (ConvertedMessage、MatrixAPI の `UploadMedia`)、§11 (BackfillReaction)。
- Produces:

```go
// convert.go
// convertMessage builds the Matrix content for a traQ message: one text part (if the body is not empty)
// followed by one part per file. ReplyTo is set from msgconv's ReplyTo.
func (c *TraQClient) convertMessage(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, msg *traq.Message) (*bridgev2.ConvertedMessage, error)
func (c *TraQClient) convertEdit(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, existing []*database.Message, msg *traq.Message) (*bridgev2.ConvertedEdit, error)
func (c *TraQClient) sender(userID string) bridgev2.EventSender // IsFromMe when userID == me
func (c *TraQClient) portalKeyFor(channelID string) (networkid.PortalKey, bool) // public or DM key, false if the channel is unknown
func (c *TraQClient) stampReactions(msg *traq.Message) []*bridgev2.BackfillReaction // one per (user, stamp)
func (c *TraQClient) reactionKey(stampID string) (string, bool)                    // stampconv; unknown stamp → Stamp() and cache

// part IDs: "" for the text part, "file:<fileID>" for file parts
```

決まり:

- part: 本文の part は `PartID ""`、file の part は `PartID "file:<fileID>"`。本文が空なら file の part だけ。どれも無い (空の message) なら `bridgev2.ErrIgnoringRemoteEvent`。
- file: `FileMeta` → size が homeserver の upload 上限 (bridgev2 の matrix connector が持つ media config。確かめて使う) 以下なら `DownloadFile` して `intent.UploadMedia` (portal が暗号化なら暗号化される)。画像 (`mime` が `image/`) は `m.image`、他は `m.file`、`Info{MimeType, Size}`、body は file 名。上限を超えたら file の part を作らず、本文の末尾に `<origin>/files/<id>` を戻す。
- `msgconv.Resolver` の実装: `UserMXID` は ghost の MXID (`Bridge.Matrix.GhostIntent(UserID(u)).GetMXID()` 相当) か、自分なら login の Matrix user の MXID。`ChannelLink` は portal の room があれば `id.RoomID.URI().MatrixToURL()`。`MessageLink` は `br.DB.Message.GetFirstPartByID` で引いた event の matrix.to URL。`CanReplyTo` は同じ portal に bridge 済み。
- part の metadata: 本文の part の `MessageMetadata.ContentHash = ContentHash(msg.Content)`。
- 送信者: `EventSender{Sender: UserID(msg.UserID), IsFromMe: msg.UserID == me.ID, SenderLogin: 自分なら LoginID}`。`Timestamp` = `CreatedAt`。
- WebSocket event の扱い (`handle_traq.go`):
  - `MESSAGE_CREATED`: `Message(id)` → `portalKeyFor(channelID)` が無ければ無視 (購読していない public チャンネル)。`simplevent.Message[*traq.Message]{Type: RemoteEventMessage, CreatePortal: DM のときだけ true, ID, ConvertMessageFunc}`。最終書き込み時刻 (Task 12 が使う) を更新する。
  - `MESSAGE_UPDATED`: `Message(id)` → `Type: RemoteEventEdit, TargetMessage: ID, ConvertEditFunc` (本文の part を差し替え、`ContentHash` を更新。file の増減は v1 では本文の part の差し替えだけで扱う)。
  - `MESSAGE_DELETED`: database の message から portal を引き (`GetFirstPartByID`、receiver は DM なら login、public なら空)、`simplevent.MessageRemove`。見つからなければ無視。
  - `MESSAGE_STAMPED`: `Type: RemoteEventReaction`、`EmojiID(stampID)`、`Emoji = reactionKey`、`Sender = sender(user_id)`、portal は message から引く。`MESSAGE_UNSTAMPED`: `Type: RemoteEventReactionRemove`。count の増加 (同じ user が同じ stamp を重ねる) は同じ EmojiID なので bridgev2 が重複として捨てる。
  - `MESSAGE_READ` (id = channel): `simplevent.Receipt{Type: RemoteEventReadReceipt, Sender: IsFromMe, ReadUpTo: now}`。
  - `USER_UPDATED` / `USER_ICON_UPDATED`: user cache を `User(id)` で更新し、`ghost.UpdateInfo(ctx, userInfo)` (既存の ghost だけ: `GetExistingGhostByID`)。
  - `CHANNEL_CREATED` (DM、`dm_user_id` あり): channel 木に足す。portal は最初の `MESSAGE_CREATED` で作る。`CHANNEL_UPDATED` / `CHANNEL_DELETED`: `Channels` を読み直して channel 木を作り直し、Path / Topic が変わった portal に `ChatResync` + ChatInfo。
  - `STAMP_CREATED` / `STAMP_UPDATED` / `STAMP_DELETED`: `Stamp(id)` で Table を更新 (Unicode の絵文字の解決は Task 14)。
  - それ以外の event は無視する (debug log)。
  - 1 つの event の処理の失敗は log に残して次へ進む (loop を止めない)。

- [ ] **Step 1: 失敗するテストを書く** (`convert_test.go`)。bridgev2 を立てずに試せる部分だけ: `stampReactions` が (user, stamp) ごとに 1 つ・count を無視・`Timestamp = CreatedAt`、part ID の命名、本文が空のとき file の part だけになる組み立て (file の upload は interface を挟んで fake にする: `type mediaUploader interface{ upload(ctx, fileID) (*event.MessageEventContent, error) }`)。
- [ ] **Step 2: 失敗を確かめる** — `go test ./pkg/connector/` → FAIL
- [ ] **Step 3: 実装する**
- [ ] **Step 4: 通す** — `go test -race ./...` → PASS
- [ ] **Step 5: commit** — `git commit -m "feat(connector): bridge traQ messages, edits, deletions and stamps to Matrix"`

### Task 11: Matrix → traQ (送信、編集、削除、reaction、既読) と viewstate

**Files:**
- Create: `pkg/connector/handle_matrix.go`、`pkg/connector/viewstate.go`
- Modify: `pkg/connector/client.go` (`GetCapabilities`、WebSocket 再接続時に viewstate を送り直す)
- Test: `pkg/connector/viewstate_test.go`

**Interfaces:**
- Consumes: Task 7 (`msgconv.ToTraQ`)、Task 2 (`Table.Resolve`)、Task 5 (`SetViewState`)。bridgev2 §5 (Matrix 側の handler と `MatrixMessageResponse`)、§13 (message status)。
- Produces:

```go
// handle_matrix.go — TraQClient implements these bridgev2 interfaces
func (c *TraQClient) HandleMatrixMessage(ctx context.Context, msg *bridgev2.MatrixMessage) (*bridgev2.MatrixMessageResponse, error)
func (c *TraQClient) HandleMatrixEdit(ctx context.Context, msg *bridgev2.MatrixEdit) error
func (c *TraQClient) HandleMatrixMessageRemove(ctx context.Context, msg *bridgev2.MatrixMessageRemove) error
func (c *TraQClient) PreHandleMatrixReaction(ctx context.Context, msg *bridgev2.MatrixReaction) (bridgev2.MatrixReactionPreResponse, error)
func (c *TraQClient) HandleMatrixReaction(ctx context.Context, msg *bridgev2.MatrixReaction) (*database.Reaction, error)
func (c *TraQClient) HandleMatrixReactionRemove(ctx context.Context, msg *bridgev2.MatrixReactionRemove) error
func (c *TraQClient) HandleMatrixReadReceipt(ctx context.Context, msg *bridgev2.MatrixReadReceipt) error
func (c *TraQClient) GetCapabilities(ctx context.Context, portal *bridgev2.Portal) *event.RoomFeatures

// viewstate.go
type viewState struct{ /* goroutine safe */ }
func newViewState(idle time.Duration, send func(ctx context.Context, channelID string) error, now func() time.Time) *viewState
func (v *viewState) Touch(ctx context.Context, channelID string) // Matrix activity in this channel
func (v *viewState) Current() string                             // channel being viewed, "" if none
func (v *viewState) Tick(ctx context.Context)                    // called every 30s: sends "" after idle
func (v *viewState) Resend(ctx context.Context)                  // after a WebSocket reconnect
```

決まり:

- `HandleMatrixMessage`: msgtype が `m.text` / `m.emote` / `m.notice` 以外 (file、画像など) は `bridgev2.ErrUnsupportedMessageType`。`m.emote` は先頭に `/me ` を付けず、本文の先頭に `*` と表示名を付けない (traQ に emote が無いので本文そのまま)。reply 先は `msg.ReplyTo` の `database.Message.ID`。`ToTraQ` の `ErrTooLong` は `bridgev2.WrapErrorInStatus(err).WithSendNotice(true).WithIsCertain(true).WithErrorAsMessage().WithStatus(event.MessageStatusFail)`。成功したら `PostMessage` の戻りから `MatrixMessageResponse{DB: &database.Message{ID: MessageID(m.ID), SenderID: UserID(me.ID), Timestamp: m.CreatedAt, Metadata: &MessageMetadata{ContentHash: ContentHash(m.Content)}}}` (traQ が埋め込みに直した後の本文の hash)。`viewState.Touch`。
- `HandleMatrixEdit`: 対象が自分の message でなければ `bridgev2.ErrEditsNotSupported` 相当の拒否 (traQ は他人の message を編集できない)。`ToTraQ` (reply 先は `EditTarget.ReplyTo` から引き直して先頭の引用行を保つ) → `EditMessage` → `Message(id)` で本文を読み直し、`EditTarget.Metadata` の `ContentHash` を更新する (poller が自分の編集を traQ 側の編集と誤認しないため)。
- `HandleMatrixMessageRemove`: 自分の message だけ `DeleteMessage`。他人のものは error (traQ の権限が無い)。
- `PreHandleMatrixReaction`: `Table.Resolve(key)` → `MatrixReactionPreResponse{SenderID: UserID(me.ID), EmojiID: EmojiID(stamp.ID), Emoji: ReactionKey(stamp)}`。未知の key、表が未完成で絵文字の key は error (message status で返る)。
- `HandleMatrixReaction`: `AddStamp`。`HandleMatrixReactionRemove`: `RemoveStamp`。
- `HandleMatrixReadReceipt`: `ReadChannel(channel)` と `viewState.Touch`。
- `GetCapabilities`: `MaxTextLength: 10000`、`Reply`・`Edit`・`Delete`・`Reaction`・`ReadReceipts` は対応、`EditMaxCount: -1`、`EditMaxAge` 無し、`ReactionCount: -1` (1 message に何種類でも)、file 系の msgtype は `Rejected`、formatting は bold / italic / strikethrough / inline code / code block / blockquote / list / header / link / user mention / spoiler を対応。`event.RoomFeatures` の field 名は module の source で確かめる。
- viewstate: `Touch(ch)` で ch が現在と違えば `send(ch)`、最終操作時刻を更新。`Tick` で最終操作から `view_idle_timeout` 過ぎたら `send("")` して現在を空にする。`send` の失敗は log に残し、現在を変えない (次の `Touch` でやり直す)。

- [ ] **Step 1: 失敗するテストを書く** (`viewstate_test.go`、時計は差し替え): 初回の Touch で送る、同じチャンネルの Touch では送らない、別チャンネルで送る、idle を過ぎた Tick で `""` を送り `Current() == ""`、idle 前の Tick では送らない、`Resend` は現在のチャンネル (空なら何もしない) を送る、`send` が error のとき `Current` が変わらない。
- [ ] **Step 2: 失敗を確かめる** — `go test ./pkg/connector/ -run ViewState` → FAIL
- [ ] **Step 3: 実装する**
- [ ] **Step 4: 通す** — `go test -race ./...` → PASS
- [ ] **Step 5: commit** — `git commit -m "feat(connector): bridge Matrix messages, edits, redactions, reactions and receipts to traQ"`

### Task 12: poller (編集・削除・スタンプの差分) と再接続時の取り込み

**Files:**
- Create: `pkg/connector/poller.go`、`pkg/connector/pollerdiff.go` (副作用の無い差分計算)、`pkg/connector/catchup.go`
- Modify: `pkg/connector/client.go` (poller の goroutine、最終書き込み時刻の map、再接続時に catch-up)、`pkg/connector/sync.go` (`ChatResync` に `CheckNeedsBackfillFunc`)
- Test: `pkg/connector/pollerdiff_test.go`、`pkg/connector/poller_test.go` (候補選び)

**Interfaces:**
- Consumes: Task 9、10。bridgev2 §3 (`ReactionSync`、`MessageRemove`、edit)、§11 (`ChatResync` の backfill 判定)。
- Produces:

```go
// pollerdiff.go
type knownMessage struct {
	ID          string
	Timestamp   time.Time
	ContentHash string
	Reactions   map[[2]string]bool // (userID, stampID) currently bridged
}
type pollDiff struct {
	Edited          []traq.Message // content hash differs
	Deleted         []string       // known IDs inside the fetched time range but not fetched
	ReactionChanged []traq.Message // (user, stamp) set differs
}
// diffMessages compares fetched (any order, from GET /channels/{id}/messages) with known (bridged).
// Known messages outside [oldest fetched CreatedAt, newest fetched CreatedAt] are never reported as deleted.
// If fetched is empty nothing is reported.
func diffMessages(fetched []traq.Message, known []knownMessage) pollDiff

// poller.go
// pollCandidates picks channels whose last activity is within window, excluding the viewed channel,
// newest first, at most max.
func pollCandidates(lastActivity map[string]time.Time, viewed string, now time.Time, window time.Duration, max int) []string
```

決まり:

- 最終書き込み時刻の map (`channelID → time`): `Connect` のとき各 portal の `br.DB.Message.GetLastNInPortal(ctx, key, 1)` の Timestamp で初期化し、`MESSAGE_CREATED` と poller・catch-up で取り込んだ message で更新する。
- poller は `poll.interval` ごとに `pollCandidates` の各チャンネルについて `Messages(Limit: poll.depth, Order: desc)` を読み、database から `known` を作る: `GetLastNInPortal(key, poll.depth*4)` を message ID でまとめ (file の part があるので)、本文の part の `ContentHash`、`br.DB.Reaction.GetAllToMessage` の (sender, emoji) の組。
- 差分の出し方: `Edited` → `RemoteEventEdit` (Task 10 と同じ `convertEdit`)、`Deleted` → `MessageRemove`、`ReactionChanged` → `simplevent.ReactionSync{Reactions: {Users: 全 user 分, HasAllUsers: true}}` (bridgev2 が足し引きを計算する)。
- 1 回の poll の中で、チャンネルごとの失敗は log に残して次のチャンネルへ。REST の負荷を抑えるため、チャンネルの間で 200ms 空ける。
- 再接続の catch-up (`catchup.go`): 切断時刻を覚えておき、繋がったら `Unread` のチャンネル、`ActivityTimeline(perChannel: true)` のチャンネル、全 DM のうち portal のあるものに `ChatResync{CheckNeedsBackfillFunc}` を queue する。`CheckNeedsBackfillFunc` は `Messages(Limit: 1, Order: desc)` の最新が database の最新 (`latestMessage`) より新しいかを返す (true なら bridgev2 が `FetchMessages` の forward で `max_catchup_messages` まで取り込む。Task 13)。
- 初回の `Connect` (bridge が止まっていた間の分): 新しく queue を作らず、Task 9 の手順 3 で queue する `ChatResync` のうち、上の候補 (Unread・activity timeline・DM) に当たるものに同じ `CheckNeedsBackfillFunc` を付ける。

- [ ] **Step 1: 失敗するテストを書く**
  - `pollerdiff_test.go` (table test): 変化なし → 空。本文の hash が違う → Edited。範囲内で消えた → Deleted。範囲より古い known → Deleted にしない。範囲より新しい known (取った後に来た) → Deleted にしない。stamp の組が増えた / 減った → ReactionChanged。count だけ増えた → 変化なし。fetched が空 → 空。known に無い fetched (poller の前に未 bridge) → 何も出さない (新着は WebSocket と catch-up の担当)。
  - `poller_test.go`: window 外を除く、viewed を除く、新しい順、max で切る、同時刻の順は channel ID の順 (安定)。
- [ ] **Step 2: 失敗を確かめる** — `go test ./pkg/connector/ -run 'Diff|Candidates'` → FAIL
- [ ] **Step 3: 実装する**
- [ ] **Step 4: 通す** — `go test -race ./...` → PASS
- [ ] **Step 5: commit** — `git commit -m "feat(connector): poll recent messages for edits, deletions and stamps"`

### Task 13: 過去ログ (`FetchMessages`)

**Files:**
- Create: `pkg/connector/backfill.go`
- Test: `pkg/connector/backfill_test.go`

**Interfaces:**
- Consumes: Task 10 (`convertMessage`、`stampReactions`、`sender`)。bridgev2 §11。
- Produces: `func (c *TraQClient) FetchMessages(ctx context.Context, params bridgev2.FetchMessagesParams) (*bridgev2.FetchMessagesResponse, error)` と、副作用の無い `func fetchQuery(params bridgev2.FetchMessagesParams) traq.MessagesQuery`、`func encodeCursor(t time.Time) networkid.PaginationCursor` / `decodeCursor`。

決まり:

- forward (`params.Forward`): `Since` = anchor の Timestamp (anchor が無ければ指定なし)、`Inclusive: false`、`Order: asc`、`Limit: min(params.Count, 200)`。
- backward: `Until` = cursor (前回の最古の `CreatedAt`) か anchor の Timestamp、`Order: desc`、`Limit: min(params.Count, 200)`。返す前に古い順に並べ替える。`HasMore` は `X-TRAQ-MORE`、`Cursor` は最古の `CreatedAt` (RFC 3339 nano)。
- 各 message は `BackfillMessage{ConvertedMessage: convertMessage(...), Sender, ID, Timestamp: CreatedAt, Reactions: stampReactions}`。intent は `params.Portal.GetIntentFor(ctx, sender, c.Login, bridgev2.RemoteEventMessage)`。変換に失敗した message は log に残して飛ばす。
- forward で返した message の分だけ、最終書き込み時刻 (Task 12) を更新する。
- `MarkRead`: forward の取り込みで、そのチャンネルが `Unread` に無ければ true。

- [ ] **Step 1: 失敗するテストを書く** (`backfill_test.go`): `fetchQuery` が forward / backward / cursor あり / Count > 200 で上の決まりどおり。`encodeCursor` と `decodeCursor` の往復 (nano 秒を保つ)。
- [ ] **Step 2: 失敗を確かめる** → FAIL
- [ ] **Step 3: 実装する**
- [ ] **Step 4: 通す** — `go test -race ./...` → PASS
- [ ] **Step 5: commit** — `git commit -m "feat(connector): backfill traQ history"`

### Task 14: Unicode スタンプと絵文字の対応表 (connector の database table)

**Files:**
- Create: `pkg/connector/stampdb/upgrades/00-latest.sql`、`pkg/connector/stampdb/stampdb.go`
- Create: `pkg/connector/stamps.go` (表の作成・更新の job)
- Modify: `pkg/connector/connector.go` (`Start` で child DB の upgrade と Table の読み込み)、`pkg/connector/handle_traq.go` (`STAMP_*` で表を更新)
- Test: `pkg/connector/stampdb/stampdb_test.go` (SQLite in-memory)

**Interfaces:**
- Consumes: Task 2 (`stampconv.Stamp`、`EmojiFromFileName`、`Table`)、`go.mau.fi/util/dbutil`。bridgev2 §6 (child DB)。
- Produces:

```go
package stampdb

const VersionTableName = "traq_version"
var UpgradeTable dbutil.UpgradeTable

type Store struct{ db *dbutil.Database }
func New(db *dbutil.Database) *Store                                  // db is the child database (br.DB.Database.Child(...))
func (s *Store) All(ctx context.Context) ([]stampconv.Stamp, error)
func (s *Store) Put(ctx context.Context, st stampconv.Stamp, fileID string) error
func (s *Store) Delete(ctx context.Context, id string) error
func (s *Store) PendingUnicode(ctx context.Context) ([]Pending, error) // isUnicode stamps whose emoji is not resolved yet
type Pending struct{ ID, FileID string }
func (s *Store) Complete(ctx context.Context) (bool, error)           // no pending unicode stamps and at least one stamp
```

- table は `traq_stamp (id TEXT PRIMARY KEY, name TEXT NOT NULL, is_unicode BOOLEAN NOT NULL, emoji TEXT NOT NULL DEFAULT '', file_id TEXT NOT NULL)`。`00-latest.sql` の先頭は `-- v0 -> v1: Latest revision`。SQLite と Postgres の両方で動く SQL にする。
- `Start`: child DB を upgrade (失敗は `bridgev2.DBUpgradeError{Section: "traq", Err}`) → `All` で `Table` を作る (`complete` は `Complete()`)。
- job (`stamps.go`): 最初に `Connected` になった login が 1 つだけ走らせる (connector の mutex)。`Stamps()` の一覧と store を突き合わせ (新規は Put、消えたものは Delete、名前の変更は更新)、`PendingUnicode` の各々について 100ms ごとに `FileMeta(fileID)` → `EmojiFromFileName(name)` → Put → Table.Put。終わったら `Table.SetComplete(true)`。途中で切断したら次の接続で続きから。
- `STAMP_CREATED` / `UPDATED`: `Stamp(id)` → Unicode なら `FileMeta` も読んで Put。`STAMP_DELETED`: Delete。

- [ ] **Step 1: 失敗するテストを書く** (`stampdb_test.go`): `dbutil` で SQLite (`:memory:`) を開き UpgradeTable を当てる → Put / All / Delete / PendingUnicode / Complete の振る舞い (emoji が空の Unicode stamp だけが pending、カスタムは pending にならない、空の表は Complete が false)。
- [ ] **Step 2: 失敗を確かめる** → FAIL
- [ ] **Step 3: 実装する**
- [ ] **Step 4: 通す** — `go test -race ./...` → PASS
- [ ] **Step 5: commit** — `git commit -m "feat(connector): resolve unicode stamps to emoji and persist the table"`

### Task 15: 検索 (bot command `search`)

**Files:**
- Create: `pkg/connector/commands.go`、`pkg/connector/searchquery.go`
- Modify: `pkg/connector/connector.go` (`Init` で command を登録)
- Test: `pkg/connector/searchquery_test.go`

**Interfaces:**
- Consumes: Task 3 (`Search`、`SearchQuery`)、Task 9 (`channelTree.ByPath`、`userCache.ByName`)。bridgev2 §8 (commands)。
- Produces:

```go
type parsedQuery struct {
	Words string   // the rest, joined by single spaces
	In    string   // channel path without '#', "" if none (the last in: wins)
	From  []string // user names without '@', in order, deduplicated
}
func parseSearchQuery(raw string) parsedQuery
func formatSearchHit(hit traq.Message, channelPath, userName string, link string, loc *time.Location) string
```

決まり:

- command 名は `search` (bridgev2 の同名の built-in を置き換える。user 検索は使わないので困らない)。help の section は `commands.HelpSectionChats` 相当、`Args: "<query>"`、説明「Search traQ messages. Supports in:#channel and from:@user.」。
- `in:` / `from:` の値の前の `#` / `@` は省略してよい。`"…"` で囲んだ語は 1 語として扱う (`in:` / `from:` の値も)。
- `in:` の channel が見つからない、`from:` の user が見つからない → その旨を返して検索しない。
- 検索は `Search{Word, In, From, Limit: 20}` (新しい順は traQ の既定)。503 → 「This traQ server has search disabled.」。
- 1 件 1 行: `#path  @name  2026-01-02 15:04  抜粋 (改行を空白に、80 code point で切って … )  link`。link は bridge 済みならその event の matrix.to、無ければ `<origin>/messages/<id>`。時刻の time zone は `Asia/Tokyo` 固定ではなく bridge の process の local time zone。抜粋は `format.EscapeMarkdown` する。0 件なら「No messages found.」。
- login していない user には `bridgev2.ErrNotLoggedIn` 相当の返答。login が複数なら command の portal の login、management room では default login。

- [ ] **Step 1: 失敗するテストを書く** (`searchquery_test.go`): `parseSearchQuery` の table test (`foo bar`、`in:#a/b foo`、`in:a/b`、`from:@x from:y foo`、`from:@x from:@x` の重複、`in:` 2 つ、`"hello world" in:"a/b c"`、空)。`formatSearchHit` で改行の置換、80 code point の切り詰め、Markdown の escape。
- [ ] **Step 2: 失敗を確かめる** → FAIL
- [ ] **Step 3: 実装する**
- [ ] **Step 4: 通す** — `go test -race ./...` → PASS
- [ ] **Step 5: commit** — `git commit -m "feat(connector): add a search command for traQ messages"`

### Task 16: README (運用者向け)、開発用 homeserver、通しの確認

**Files:**
- Modify: `README.md` (日本語)
- Create: `testenv/synapse/` (開発用 homeserver の設定。compose の profile `matrix` で起動)、`docs/development.md` (開発者向けの手順)
- Modify: `testenv/compose.yaml` (profile `matrix` の synapse service。traQ だけの結合テストには影響させない)

決まり:

- README: 概要、できること / できないこと (spec の「目的」「対象外」)、「動かし方」(spec の同名の節: traQ の OAuth client の作り方、appservice 方式 (`-e` → 設定 → `-g` → homeserver に登録 → 起動)、Beeper 方式 (`bbctl config --type bridgev2 -o config.yaml sh-<name>` → `network:` 節を足す → 起動)、double puppeting、encryption、`backfill.enabled: true` にしないと過去ログが入らないこと、購読レベル 1 の mute を後から反映するには `bridge.mute_only_on_create: false`)、login の手順 (bot に `login` → URL を開いて承認 → 開いた URL を貼る)、`search` の使い方、build (`nix build`、`go build`、`-tags goolm`)、license。
- `docs/development.md`: testenv の起動 (`docker compose -p tmb -f testenv/compose.yaml --profile matrix up -d`)、traQ の管理者 (`traq`/`traq`) と OAuth client の作り方 (curl)、開発用 synapse の user 作成と bridge の登録、単体・結合テストの実行、通しの確認の項目表。
- 開発用 synapse: image は実装時点の最新の安定版 tag に固定。server name `localhost`、registration は shared secret、appservice の registration file を mount。暗号化はしない (bridge の `encryption.allow: false`)。

- [ ] **Step 1: 開発用 homeserver を作る** — compose の profile と synapse の設定を書き、`docker compose -p tmb -f testenv/compose.yaml --profile matrix up -d` で traQ と synapse が起動することを確かめる。
- [ ] **Step 2: 通しの確認** — bridge を build して testenv の traQ と synapse に繋ぎ、Matrix の client-server API (curl、または Element web) で次を全部確かめ、結果を PR の本文に表で書く:

| 項目 | 確かめ方 |
|---|---|
| login | bot の DM で `login` → URL を承認 → redirect の URL を貼る → `Successfully logged in` |
| portal | level 2 / level 1 のチャンネルと DM の room ができ、名前がフルパス、level 1 が mute (double puppet ありのとき) |
| 受信 | traQ で別 user が投稿・編集・削除 → Matrix に message・edit・redaction |
| スタンプ | 閲覧中 (Matrix で既読を付けた) チャンネルで即時、他のチャンネルと DM は `poll.interval` 以内に reaction が付く・消える |
| 送信 | Matrix から投稿・reply・編集・削除、`@name` の pill が traQ で mention になる |
| reaction | Matrix の 👍 と `:custom:` が traQ のスタンプになる。未知の key は message status の error |
| 既読 | Matrix の既読で traQ の未読が消える。traQ で読むと Matrix に既読が付く |
| file | traQ の画像と file が Matrix に image / file で届く |
| 過去ログ | 新しい portal に `max_initial_messages` 件入る |
| 再接続 | traQ の container を止めて投稿できない間を挟み、再起動後に bridge を止めていた間の投稿が入る |
| 検索 | `search in:#<path> <語>` が結果を返す |

- [ ] **Step 3: README と `docs/development.md` を書く**
- [ ] **Step 4: 片付ける** — `docker compose -p tmb -f testenv/compose.yaml --profile matrix down -v`
- [ ] **Step 5: commit** — `git commit -m "docs: add the operator guide and development setup"`

## Self-Review のメモ

- spec の各節と task の対応: login → 8、受信 (WebSocket) → 5・9・10、viewstate → 11、poller・再接続 → 12、チャンネルと room → 9、本文 → 6・7、編集・削除・引用 → 6・7・10・11、スタンプ → 2・10・11・14、ファイル → 6・10、既読 → 9・10・11、検索 → 15、過去ログ → 13、失敗時の扱い → 3・9・11、テスト → 各 task と 4・5・16、動かし方 → 16。
- 実測で spec と違った点 (refresh token が無い、`timeline_streaming` で public の編集・削除が届く) は spec と `docs/notes/traq-api.md` に反映済み。
