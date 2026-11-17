package connector

import (
	"context"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"maunium.net/go/mautrix/bridgev2/networkid"

	"github.com/kisepichu/traq-matrix-bridge/pkg/traq"
)

// searchTestClient is a client whose traQ answers every request with status and body. calls counts the
// requests that reached it.
func searchTestClient(t *testing.T, status int, body string, calls *atomic.Int32) *TraQClient {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	api, err := traq.NewClient(srv.URL, srv.Client(), "t", "test")
	if err != nil {
		t.Fatal(err)
	}
	users := newUserCache()
	users.put(&traq.User{ID: "u-bob", Name: "bob"})
	return &TraQClient{
		API:      api,
		users:    users,
		channels: buildChannelTree(&traq.ChannelList{Public: []traq.Channel{{ID: "ch-a", Name: "a"}}}),
	}
}

func TestSearchMessagesReplies(t *testing.T) {
	cases := []struct {
		name      string
		query     string
		status    int
		body      string
		want      string
		wantCalls int32 // requests to traQ; the channel list is read once for an unknown channel, the user list for an unknown user
	}{
		{"search disabled", "foo", http.StatusServiceUnavailable, `{"message":"x"}`, "This traQ server has search disabled.", 1},
		{"no hits", "foo", http.StatusOK, `{"totalHits":0,"hits":[]}`, "No messages found.", 1},
		{"other failure", "foo", http.StatusInternalServerError, `{"message":"boom"}`, "Failed to search traQ: ", 1},
		{"unknown user does not search", "from:@nobody foo", http.StatusOK, `[]`, "User @nobody not found.", 1},
		{"unknown channel does not search", "in:#nope foo", http.StatusOK, `{"public":[],"dm":[]}`, "Channel #nope not found.", 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			c := searchTestClient(t, tc.status, tc.body, &calls)
			got := c.searchMessages(context.Background(), parseSearchQuery(tc.query), time.UTC, noBridged)
			if !strings.HasPrefix(got, tc.want) {
				t.Errorf("reply = %q, want prefix %q", got, tc.want)
			}
			if calls.Load() != tc.wantCalls {
				t.Errorf("requests to traQ = %d, want %d", calls.Load(), tc.wantCalls)
			}
		})
	}
}

func TestSearchMessagesBeforeConnect(t *testing.T) {
	var calls atomic.Int32
	c := searchTestClient(t, http.StatusOK, `{}`, &calls)
	c.channels = nil
	got := c.searchMessages(context.Background(), parseSearchQuery("foo"), time.UTC, noBridged)
	if !strings.Contains(got, "still connecting") || calls.Load() != 0 {
		t.Errorf("reply = %q, requests = %d", got, calls.Load())
	}
}

// noBridged is a bridgedLinkFunc for a bridge without bridged messages.
func noBridged(context.Context, networkid.UserLoginID, string) (string, bool) { return "", false }

// routedTraQ is a traQ that answers by the end of the request path and counts the requests of each.
type routedTraQ struct {
	mu     sync.Mutex
	counts map[string]int
	routes map[string]string // path suffix ("/users") -> JSON body
	query  map[string]string // path suffix -> raw query of the last request
}

func (r *routedTraQ) count(route string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.counts[route]
}

func routedTestClient(t *testing.T, routes map[string]string) (*TraQClient, *routedTraQ) {
	t.Helper()
	rt := &routedTraQ{counts: map[string]int{}, routes: routes, query: map[string]string{}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		rt.mu.Lock()
		defer rt.mu.Unlock()
		for suffix, body := range rt.routes {
			if strings.HasSuffix(req.URL.Path, suffix) {
				rt.counts[suffix]++
				rt.query[suffix] = req.URL.RawQuery
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(body))
				return
			}
		}
		http.NotFound(w, req)
	}))
	t.Cleanup(srv.Close)
	api, err := traq.NewClient(srv.URL, srv.Client(), "t", "test")
	if err != nil {
		t.Fatal(err)
	}
	users := newUserCache()
	users.put(&traq.User{ID: "u-bob", Name: "bob"})
	return &TraQClient{
		API:   api,
		Login: testLogin("login-1", false),
		users: users,
		channels: buildChannelTree(&traq.ChannelList{
			Public: []traq.Channel{{ID: "ch-a", Name: "a"}},
			DM:     []traq.DMChannel{{ID: "ch-dm", UserID: "u-bob"}},
		}),
	}, rt
}

func TestSearchFromRefreshesUsersOncePerCommand(t *testing.T) {
	routes := map[string]string{
		"/users":    `[{"id":"u-carol","name":"carol"}]`,
		"/messages": `{"totalHits":0,"hits":[]}`,
	}
	cases := []struct {
		name       string
		query      string
		want       string
		wantUsers  int
		wantSearch int
		wantFrom   string // from= of the search request
	}{
		{"cached user does not refresh", "from:@bob foo", "No messages found.", 0, 1, "u-bob"},
		{"user created after connect is found", "from:@carol foo", "No messages found.", 1, 1, "u-carol"},
		{"unknown user refreshes once and does not search", "from:@nobody foo", "User @nobody not found.", 1, 0, ""},
		{"two misses refresh once", "from:@x from:@y foo", "User @x not found.", 1, 0, ""},
		{"hit then miss refreshes once", "from:@bob from:@dave", "User @dave not found.", 1, 0, ""},
		{"miss that the refresh fills, then another miss", "from:@carol from:@dave", "User @dave not found.", 1, 0, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, rt := routedTestClient(t, routes)
			got := c.searchMessages(context.Background(), parseSearchQuery(tc.query), time.UTC, noBridged)
			if got != tc.want {
				t.Errorf("reply = %q, want %q", got, tc.want)
			}
			if n := rt.count("/users"); n != tc.wantUsers {
				t.Errorf("GET /users = %d, want %d", n, tc.wantUsers)
			}
			if n := rt.count("/messages"); n != tc.wantSearch {
				t.Errorf("GET /messages = %d, want %d", n, tc.wantSearch)
			}
			if tc.wantFrom != "" && !strings.Contains(rt.query["/messages"], "from="+tc.wantFrom) {
				t.Errorf("search query = %q, want from=%s", rt.query["/messages"], tc.wantFrom)
			}
		})
	}
}

func TestSearchMessagesHits(t *testing.T) {
	at := "2026-01-02T06:04:05Z" // 15:04 in JST
	routes := map[string]string{
		"/messages": `{"totalHits":3,"hits":[
			{"id":"m-bridged","userId":"u-bob","channelId":"ch-a","createdAt":"` + at + `","content":"hi !{\"type\":\"user\",\"raw\":\"@alice\",\"id\":\"u-alice\"}\nsecond_line"},
			{"id":"m-plain","userId":"u-bob","channelId":"ch-a","createdAt":"` + at + `","content":"not bridged"},
			{"id":"m-dm","userId":"u-bob","channelId":"ch-dm","createdAt":"` + at + `","content":"dm hit"}]}`,
	}
	for _, split := range []bool{false, true} {
		c, rt := routedTestClient(t, routes)
		c.Login.Bridge.Config.SplitPortals = split
		var receivers []string
		bridged := func(_ context.Context, receiver networkid.UserLoginID, id string) (string, bool) {
			receivers = append(receivers, id+"@"+string(receiver))
			if id == "m-bridged" || id == "m-dm" {
				return "https://matrix.to/#/!room:example.org/$ev_" + id, true
			}
			return "", false
		}
		got := c.searchMessages(context.Background(), parseSearchQuery("hi"), time.FixedZone("JST", 9*3600), bridged)
		origin := c.origin()
		want := strings.Join([]string{
			`#a  @bob  2026-01-02 15:04  hi @alice second\_line  https://matrix.to/#/!room:example.org/$ev\_m-bridged`,
			`#a  @bob  2026-01-02 15:04  not bridged  ` + origin + `/messages/m-plain`,
			`DM  @bob  2026-01-02 15:04  dm hit  https://matrix.to/#/!room:example.org/$ev\_m-dm`,
		}, "\n")
		if got != want {
			t.Errorf("split_portals=%v: reply =\n%s\nwant\n%s", split, got, want)
		}
		// A message is looked up with the receiver of its portal: none for a shared public channel, the login
		// for a DM and, with split portals, for a public channel too.
		wantReceivers := []string{"m-bridged@", "m-plain@", "m-dm@login-1"}
		if split {
			wantReceivers = []string{"m-bridged@login-1", "m-plain@login-1", "m-dm@login-1"}
		}
		if !reflect.DeepEqual(receivers, wantReceivers) {
			t.Errorf("split_portals=%v: lookups = %v, want %v", split, receivers, wantReceivers)
		}
		if rt.count("/channels") != 0 {
			t.Errorf("GET /channels = %d, want 0: the tree knows every channel", rt.count("/channels"))
		}
	}
}

func TestSearchReadsTheChannelListOncePerCommand(t *testing.T) {
	routes := map[string]string{
		"/channels": `{"public":[{"id":"ch-a","name":"a"},{"id":"ch-new","name":"new"}],"dm":[]}`,
		"/messages": `{"totalHits":2,"hits":[
			{"id":"m1","userId":"u-bob","channelId":"ch-new","createdAt":"2026-01-02T06:04:05Z","content":"in the new channel"},
			{"id":"m2","userId":"u-bob","channelId":"ch-gone","createdAt":"2026-01-02T06:04:05Z","content":"in a channel the list lacks"}]}`,
	}
	c, rt := routedTestClient(t, routes)
	got := c.searchMessages(context.Background(), parseSearchQuery("in:#new foo"), time.UTC, noBridged)
	lines := strings.Split(got, "\n")
	if len(lines) != 2 || !strings.HasPrefix(lines[0], "#new  @bob") || !strings.HasPrefix(lines[1], "#ch-gone  @bob") {
		t.Errorf("reply = %q, want #new for the first hit and the ID for the second", got)
	}
	// The in: lookup found the new channel by reading the list; the hit of ch-gone must not read it again.
	if n := rt.count("/channels"); n != 1 {
		t.Errorf("GET /channels = %d, want 1", n)
	}
	if !strings.Contains(rt.query["/messages"], "in=ch-new") {
		t.Errorf("search query = %q, want in=ch-new", rt.query["/messages"])
	}
}

func TestSearchShowsAHitInAChannelCreatedAfterConnect(t *testing.T) {
	routes := map[string]string{
		"/channels": `{"public":[{"id":"ch-a","name":"a"},{"id":"ch-new","name":"new"}],"dm":[]}`,
		"/messages": `{"totalHits":1,"hits":[{"id":"m1","userId":"u-bob","channelId":"ch-new","createdAt":"2026-01-02T06:04:05Z","content":"x"}]}`,
	}
	c, rt := routedTestClient(t, routes)
	got := c.searchMessages(context.Background(), parseSearchQuery("x"), time.UTC, noBridged)
	if !strings.HasPrefix(got, "#new  @bob") || rt.count("/channels") != 1 {
		t.Errorf("reply = %q, GET /channels = %d, want #new and 1", got, rt.count("/channels"))
	}
	// The stored tree is not changed by a command.
	if _, ok := c.channels.Get("ch-new"); ok {
		t.Error("the stored channel tree was replaced")
	}
}
