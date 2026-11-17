package connector

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/rs/zerolog"
	"go.mau.fi/util/dbutil"
	_ "go.mau.fi/util/dbutil/litestream"
	"maunium.net/go/mautrix/bridgev2"

	"github.com/kisepichu/traq-matrix-bridge/pkg/connector/stampdb"
	"github.com/kisepichu/traq-matrix-bridge/pkg/stampconv"
	"github.com/kisepichu/traq-matrix-bridge/pkg/traq"
)

// newTestStampDB opens an upgraded stamp store on an in-memory SQLite database.
func newTestStampDB(t *testing.T) *stampdb.Store {
	t.Helper()
	db, err := dbutil.NewFromConfig("", dbutil.Config{PoolConfig: dbutil.PoolConfig{
		Type: "sqlite3-fk-wal", URI: ":memory:?_txlock=immediate", MaxOpenConns: 1, MaxIdleConns: 1,
	}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	child := db.Child(stampdb.VersionTableName, stampdb.UpgradeTable, nil)
	if err := child.Upgrade(context.Background()); err != nil {
		t.Fatal(err)
	}
	return stampdb.New(child)
}

// The job adds new stamps, deletes gone ones, renames changed ones keeping their emoji, resolves the pending
// Unicode stamps and then marks the table complete, even when one file name is not an emoji: that stamp is
// stored as unresolvable, so it is not tried again.
func TestSyncStamps(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v3/stamps", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`[
			{"id":"c1","name":"good","fileId":"fc","isUnicode":false},
			{"id":"u1","name":"thumbs_up","fileId":"f1","isUnicode":true},
			{"id":"u2","name":"heart","fileId":"f2","isUnicode":true},
			{"id":"u3","name":"odd","fileId":"f3","isUnicode":true}
		]`))
	})
	var mu sync.Mutex
	var metas []string
	mux.HandleFunc("GET /api/v3/files/{id}/meta", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		mu.Lock()
		metas = append(metas, id)
		mu.Unlock()
		name := map[string]string{"f2": "2764-fe0f.svg", "f3": "odd.png"}[id]
		_, _ = w.Write([]byte(`{"id":"` + id + `","name":"` + name + `"}`))
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	api, err := traq.NewClient(srv.URL, srv.Client(), "token", userAgent)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	store := newTestStampDB(t)
	for _, s := range []stampconv.Stamp{{ID: "u1", Name: "thumbsup", Unicode: true, Emoji: "👍"}, {ID: "gone", Name: "old"}} {
		if err := store.Put(ctx, s, "f-"+s.ID); err != nil {
			t.Fatal(err)
		}
	}
	stored, _ := store.All(ctx)
	c := &TraQConnector{stampDB: store, Stamps: stampconv.NewTable(stored, false)}
	c.stampSync.clients = []*TraQClient{{API: api}}

	done, err := c.syncStamps(ctx)
	if !done || err != nil {
		t.Fatalf("syncStamps = %v, %v, want true, nil", done, err)
	}
	mu.Lock()
	if !slices.Equal(metas, []string{"f2", "f3"}) {
		t.Errorf("FileMeta was called for %v, want [f2 f3] (only the pending Unicode stamps)", metas)
	}
	mu.Unlock()
	all, _ := store.All(ctx)
	slices.SortFunc(all, func(a, b stampconv.Stamp) int { return strings.Compare(a.ID, b.ID) })
	want := []stampconv.Stamp{
		{ID: "c1", Name: "good"},
		{ID: "u1", Name: "thumbs_up", Unicode: true, Emoji: "👍"},
		{ID: "u2", Name: "heart", Unicode: true, Emoji: "❤️"},
		{ID: "u3", Name: "odd", Unicode: true, Unresolvable: true},
	}
	if !slices.Equal(all, want) {
		t.Errorf("store = %+v, want %+v", all, want)
	}
	for key, id := range map[string]string{"👍": "u1", "❤": "u2", ":thumbs_up:": "u1", "odd": "u3"} {
		if s, err := c.Stamps.Resolve(key); err != nil || s.ID != id {
			t.Errorf("Resolve(%q) = %+v, %v, want %s", key, s, err, id)
		}
	}
	if _, err := c.Stamps.Resolve("🦄"); !errors.Is(err, stampconv.ErrUnknownStamp) {
		t.Errorf("Resolve(unknown emoji) err = %v, want ErrUnknownStamp (the table is complete)", err)
	}
	if _, ok := c.Stamps.ByID("gone"); ok {
		t.Error("the deleted stamp is still in the table")
	}
}

// Without a connected login the job stops and lets the next login that connects start it again.
func TestSyncStampsWithoutLogin(t *testing.T) {
	c := &TraQConnector{stampDB: newTestStampDB(t), Stamps: stampconv.NewTable(nil, false)}
	c.stampSync.running = true
	if _, err := c.syncStamps(context.Background()); !errors.Is(err, errNoStampLogin) {
		t.Fatalf("err = %v, want errNoStampLogin", err)
	}
	if c.stampSync.running {
		t.Error("the job is still marked running")
	}
}

// newStampAPI serves mux as traQ.
func newStampAPI(t *testing.T, mux *http.ServeMux) *traq.Client {
	t.Helper()
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	api, err := traq.NewClient(srv.URL, srv.Client(), "token", userAgent)
	if err != nil {
		t.Fatal(err)
	}
	return api
}

// newStampConnector is a connector whose store and table hold stamps, each stored with file "f-<ID>".
func newStampConnector(t *testing.T, stamps ...stampconv.Stamp) *TraQConnector {
	t.Helper()
	store := newTestStampDB(t)
	for _, s := range stamps {
		if err := store.Put(context.Background(), s, "f-"+s.ID); err != nil {
			t.Fatal(err)
		}
	}
	stored, _ := store.All(context.Background())
	return &TraQConnector{stampDB: store, Stamps: stampconv.NewTable(stored, false)}
}

// countingHandler counts the requests of a handler by stamp or file ID.
type countingHandler struct {
	mu    sync.Mutex
	calls map[string]int
}

func (h *countingHandler) add(id string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.calls == nil {
		h.calls = map[string]int{}
	}
	h.calls[id]++
}

func (h *countingHandler) count(id string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.calls[id]
}

// A stamp whose file meta keeps failing without a definite answer (an error status other than 404, a timeout,
// a reset connection) uses up its retry budget and is given up on by the walk: the walk goes on and the table
// is marked complete, but the stamp stays pending, in Stamps and in the store. The job then retries it in the
// background until traQ answers; meanwhile an emoji the table does not have may be its emoji, so it is not
// reported unknown.
func TestSyncStampsRetryBudget(t *testing.T) {
	// The traQ client itself retries a GET that got 502 or 504 3 times; Retry-After: 0 makes that quick.
	proxyError := func(status int) func(w http.ResponseWriter) {
		return func(w http.ResponseWriter) {
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(status)
		}
	}
	cases := []struct {
		name     string
		fail     func(w http.ResponseWriter)
		requests int // HTTP requests per attempt of the job
	}{
		{"500", func(w http.ResponseWriter) { http.Error(w, `{"message":"boom"}`, http.StatusInternalServerError) }, 1},
		{"502", proxyError(http.StatusBadGateway), 4},
		{"503", func(w http.ResponseWriter) { http.Error(w, "unavailable", http.StatusServiceUnavailable) }, 1},
		{"504", proxyError(http.StatusGatewayTimeout), 4},
		{"timeout", func(http.ResponseWriter) { time.Sleep(200 * time.Millisecond) }, 1},
		{"reset", func(w http.ResponseWriter) {
			if conn, _, err := w.(http.Hijacker).Hijack(); err == nil {
				_ = conn.Close() // no response at all
			}
		}, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// walk counts the requests for the failing stamp's file before the stamp after it is resolved,
			// that is the walk's; later the background retries. traQ fails both until recovered is closed.
			var walk, later countingHandler
			recovered := make(chan struct{})
			var c *TraQConnector
			mux := http.NewServeMux()
			mux.HandleFunc("GET /api/v3/stamps", func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write([]byte(`[{"id":"bad","name":"bad","fileId":"f-bad","isUnicode":true},{"id":"ok","name":"thumbsup","fileId":"f-ok","isUnicode":true}]`))
			})
			mux.HandleFunc("GET /api/v3/files/{id}/meta", func(w http.ResponseWriter, r *http.Request) {
				if r.PathValue("id") != "f-bad" {
					_, _ = w.Write([]byte(`{"id":"f-ok","name":"1f44d.svg"}`))
					return
				}
				if s, _ := c.Stamps.ByID("ok"); s.Emoji == "" {
					walk.add("f-bad")
					tc.fail(w)
					return
				}
				later.add("f-bad")
				select {
				case <-recovered:
					_, _ = w.Write([]byte(`{"id":"f-bad","name":"2764-fe0f.svg"}`))
				default:
					tc.fail(w)
				}
			})
			srv := httptest.NewServer(mux)
			t.Cleanup(srv.Close)
			// Without keep-alives, net/http does not resend a request whose reused connection was reset.
			transport := srv.Client().Transport.(*http.Transport).Clone()
			transport.DisableKeepAlives = true
			api, err := traq.NewClient(srv.URL, &http.Client{Transport: transport, Timeout: 50 * time.Millisecond}, "token", userAgent)
			if err != nil {
				t.Fatal(err)
			}
			c = newStampConnector(t)
			c.stampSync.clients = []*TraQClient{{API: api}}
			c.stampSync.retryMin = time.Millisecond

			ctx, cancel := context.WithTimeout(zerolog.Nop().WithContext(context.Background()), 10*time.Second)
			defer cancel()
			type result struct {
				done bool
				err  error
			}
			res := make(chan result, 1)
			go func() {
				done, err := c.syncStamps(ctx)
				res <- result{done, err}
			}()
			for deadline := time.Now().Add(5 * time.Second); later.count("f-bad") < 2; {
				if time.Now().After(deadline) {
					t.Fatalf("the failing stamp was retried %d times in the background, want 2", later.count("f-bad"))
				}
				select {
				case r := <-res:
					t.Fatalf("syncStamps = %v, %v before traQ recovered, want it to retry the failing stamp", r.done, r.err)
				case <-time.After(time.Millisecond):
				}
			}

			if n, want := walk.count("f-bad"), stampResolveAttempts*tc.requests; n != want {
				t.Errorf("the walk requested the failing stamp's file meta %d times, want %d", n, want)
			}
			if s, _ := c.Stamps.ByID("bad"); !s.EmojiPending() {
				t.Errorf("failing stamp = %+v, want it still pending", s)
			}
			if at, ok := c.Stamps.RetryAfter("bad"); !ok || !at.After(time.Now()) {
				t.Errorf("RetryAfter(bad) = %v, %v; want a time to come, so conversions do not ask traQ meanwhile", at, ok)
			}
			if s, _ := c.Stamps.ByID("ok"); s.Emoji != "👍" {
				t.Errorf("stamp after the failing one = %+v, want it resolved", s)
			}
			for _, key := range []string{"❤", "🦄"} {
				if _, err := c.Stamps.Resolve(key); !errors.Is(err, stampconv.ErrEmojiTableIncomplete) {
					t.Errorf("Resolve(%s) while the failing stamp is retried = %v, want ErrEmojiTableIncomplete", key, err)
				}
			}
			if pending, _ := c.stampDB.PendingUnicode(ctx); !slices.Equal(pending, []stampdb.Pending{{ID: "bad", FileID: "f-bad"}}) {
				t.Errorf("stored pending stamps = %+v, want the failing stamp", pending)
			}

			close(recovered)
			if r := <-res; !r.done || r.err != nil {
				t.Fatalf("syncStamps = %v, %v, want true, nil", r.done, r.err)
			}
			if s, err := c.Stamps.Resolve("❤"); err != nil || s.ID != "bad" {
				t.Errorf("Resolve(❤) after traQ recovered = %+v, %v; want bad", s, err)
			}
			if _, err := c.Stamps.Resolve("🦄"); !errors.Is(err, stampconv.ErrUnknownStamp) {
				t.Errorf("Resolve(🦄) = %v, want ErrUnknownStamp (the table is complete)", err)
			}
			if pending, _ := c.stampDB.PendingUnicode(ctx); len(pending) != 0 {
				t.Errorf("stored pending stamps = %+v, want none", pending)
			}
		})
	}
}

// A stamp the job has given up on is not resolved on demand before its retry time, so that a traQ that is
// down is not asked once per conversion. After it, the next conversion asks traQ again, and a failure sets the
// next retry time.
func TestReactionKeyRetriesAGivenUpStampLater(t *testing.T) {
	var metas countingHandler
	var failing atomic.Bool
	failing.Store(true)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v3/files/{id}/meta", func(w http.ResponseWriter, r *http.Request) {
		metas.add(r.PathValue("id"))
		if failing.Load() {
			http.Error(w, `{"message":"boom"}`, http.StatusInternalServerError)
			return
		}
		_, _ = w.Write([]byte(`{"id":"f-u1","name":"1f44d.svg"}`))
	})
	c := &TraQClient{Main: newStampConnector(t, stampconv.Stamp{ID: "u1", Name: "thumbsup", Unicode: true}), API: newStampAPI(t, mux)}
	ctx := zerolog.Nop().WithContext(context.Background())
	key := func(want string, metaCalls int) {
		t.Helper()
		if got, err := c.reactionKey(ctx, "u1"); got != want || err != nil {
			t.Errorf("reactionKey = %q, %v; want %q", got, err, want)
		}
		if n := metas.count("f-u1"); n != metaCalls {
			t.Errorf("FileMeta was called %d times in all, want %d", n, metaCalls)
		}
	}
	// As the job leaves a stamp it gave up on.
	c.Main.Stamps.SetComplete(true)
	c.Main.Stamps.SetRetryAfter("u1", time.Now().Add(time.Hour))
	key(":thumbsup:", 0)

	c.Main.Stamps.SetRetryAfter("u1", time.Now()) // the retry time has come, traQ still fails
	key(":thumbsup:", 1)
	if at, ok := c.Main.Stamps.RetryAfter("u1"); !ok || !at.After(time.Now()) {
		t.Errorf("RetryAfter(u1) after a failure = %v, %v; want a time to come", at, ok)
	}
	key(":thumbsup:", 1)

	failing.Store(false)
	c.Main.Stamps.SetRetryAfter("u1", time.Now())
	key("👍", 2)
	if s, err := c.Main.Stamps.Resolve("👍"); err != nil || s.ID != "u1" {
		t.Errorf("Resolve(👍) = %+v, %v; want u1", s, err)
	}
	if _, err := c.Main.Stamps.Resolve("🦄"); !errors.Is(err, stampconv.ErrUnknownStamp) {
		t.Errorf("Resolve(🦄) = %v, want ErrUnknownStamp (nothing is retried any more)", err)
	}
}

// The completion log counts as resolved only the stamps that got an emoji: not those deleted while their
// file meta was read, nor those that got another file meanwhile (whose meta failed, so they are retried). A
// stamp that stopped being a Unicode stamp meanwhile is not counted, and is not retried.
func TestSyncStampsCompletionCounts(t *testing.T) {
	var metas countingHandler
	releaseDel, releaseMove, releaseFlip := make(chan struct{}), make(chan struct{}), make(chan struct{})
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v3/stamps", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`[
			{"id":"u1","name":"thumbsup","fileId":"f-u1","isUnicode":true},
			{"id":"u2","name":"gone_file","fileId":"f-u2","isUnicode":true},
			{"id":"u3","name":"deleted","fileId":"f-u3","isUnicode":true},
			{"id":"u4","name":"moved","fileId":"f-u4","isUnicode":true},
			{"id":"u5","name":"flipped","fileId":"f-u5","isUnicode":true}
		]`))
	})
	mux.HandleFunc("GET /api/v3/stamps/u4", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"id":"u4","name":"moved","fileId":"f-new","isUnicode":true}`))
	})
	mux.HandleFunc("GET /api/v3/stamps/u5", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"id":"u5","name":"flipped","fileId":"f-custom","isUnicode":false}`))
	})
	mux.HandleFunc("GET /api/v3/files/{id}/meta", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		metas.add(id)
		switch {
		case id == "f-u2":
			http.Error(w, `{"message":"not found"}`, http.StatusNotFound)
		case id == "f-u3":
			<-releaseDel
			_, _ = w.Write([]byte(`{"id":"f-u3","name":"1f600.svg"}`))
		case id == "f-u4":
			<-releaseMove
			_, _ = w.Write([]byte(`{"id":"f-u4","name":"1f601.svg"}`))
		case id == "f-u5":
			<-releaseFlip
			_, _ = w.Write([]byte(`{"id":"f-u5","name":"1f602.svg"}`))
		case id == "f-new" && metas.count(id) == 1: // the STAMP_UPDATED refresh
			http.Error(w, `{"message":"boom"}`, http.StatusInternalServerError)
		case id == "f-new":
			_, _ = w.Write([]byte(`{"id":"f-new","name":"2764-fe0f.svg"}`))
		default:
			_, _ = w.Write([]byte(`{"id":"f-u1","name":"1f44d.svg"}`))
		}
	})
	api := newStampAPI(t, mux)
	c := newStampConnector(t)
	c.stampSync.clients = []*TraQClient{{API: api}}
	c.stampSync.retryMin = time.Millisecond
	var logs syncBuffer
	jobCtx, cancel := context.WithTimeout(zerolog.New(&logs).WithContext(context.Background()), 10*time.Second)
	defer cancel()
	res := make(chan error, 1)
	go func() {
		_, err := c.syncStamps(jobCtx)
		res <- err
	}()
	ctx := zerolog.Nop().WithContext(context.Background())
	awaitRequest(t, &metas, "f-u3", releaseDel)
	within(t, "STAMP_DELETED", func() error { return c.deleteStamp(ctx, "u3") })
	close(releaseDel)
	awaitRequest(t, &metas, "f-u4", releaseMove)
	within(t, "STAMP_UPDATED", func() error { return c.refreshStamp(ctx, api, "u4") })
	close(releaseMove)
	awaitRequest(t, &metas, "f-u5", releaseFlip)
	within(t, "STAMP_UPDATED to a custom stamp", func() error { return c.refreshStamp(ctx, api, "u5") })
	close(releaseFlip)
	if err := <-res; err != nil {
		t.Fatalf("syncStamps: %v", err)
	}

	var got struct{ Resolved, Unresolvable, Deleted, Retrying int }
	for line := range strings.Lines(logs.String()) {
		if strings.Contains(line, `"The stamp table is complete"`) {
			if err := json.Unmarshal([]byte(line), &got); err != nil {
				t.Fatal(err)
			}
		}
	}
	if want := (struct{ Resolved, Unresolvable, Deleted, Retrying int }{1, 1, 1, 1}); got != want {
		t.Errorf("completion log counts = %+v, want %+v (log %s)", got, want, logs.String())
	}
	if s, _ := c.Stamps.ByID("u4"); s.Emoji != "❤️" {
		t.Errorf("moved stamp = %+v, want it resolved with its new file by the retries", s)
	}
}

// A pending Unicode stamp is resolved on demand, once however many conversions need it at the same time, and
// stored, so the job no longer has it to do.
func TestReactionKeyResolvesOnDemand(t *testing.T) {
	var metas countingHandler
	release := make(chan struct{})
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v3/files/{id}/meta", func(w http.ResponseWriter, r *http.Request) {
		metas.add(r.PathValue("id"))
		<-release
		_, _ = w.Write([]byte(`{"id":"f-u1","name":"1f44d.svg"}`))
	})
	main := newStampConnector(t, stampconv.Stamp{ID: "u1", Name: "thumbsup", Unicode: true})
	joins := countJoins(main)
	c := &TraQClient{Main: main, API: newStampAPI(t, mux)}

	const n = 10
	keys := make([]string, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() {
			var err error
			if keys[i], err = c.reactionKey(context.Background(), "u1"); err != nil {
				t.Errorf("reactionKey: %v", err)
			}
		})
	}
	// FileMeta is held until every conversion has joined the resolution.
	awaitCount(t, joins, "u1", n, release)
	close(release)
	wg.Wait()
	for i, key := range keys {
		if key != "👍" {
			t.Errorf("reactionKey #%d = %q, want 👍", i, key)
		}
	}
	if got := metas.count("f-u1"); got != 1 {
		t.Errorf("FileMeta was called %d times, want 1", got)
	}
	if s, err := main.Stamps.Resolve("👍"); err != nil || s.ID != "u1" {
		t.Errorf("Resolve(👍) = %+v, %v; want u1", s, err)
	}
	if pending, _ := main.stampDB.PendingUnicode(context.Background()); len(pending) != 0 {
		t.Errorf("stored pending stamps = %+v, want none", pending)
	}
}

// A stamp the table does not have yet is read from traQ and its emoji resolved with the stamp's file.
func TestReactionKeyResolvesANewStamp(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v3/stamps/{id}", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"id":"u1","name":"heart","fileId":"f-heart","isUnicode":true}`))
	})
	mux.HandleFunc("GET /api/v3/files/f-heart/meta", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"id":"f-heart","name":"2764-fe0f.svg"}`))
	})
	c := &TraQClient{Main: newStampConnector(t), API: newStampAPI(t, mux)}
	if key, err := c.reactionKey(context.Background(), "u1"); key != "❤️" || err != nil {
		t.Errorf("reactionKey = %q, %v; want ❤️", key, err)
	}
}

// When the emoji cannot be resolved, the reaction keeps ":name:". A file that does not exist makes the stamp
// unresolvable, so later conversions do not ask again; another failure is tried again next time.
func TestReactionKeyOnDemandFailure(t *testing.T) {
	var metas countingHandler
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v3/files/{id}/meta", func(w http.ResponseWriter, r *http.Request) {
		metas.add(r.PathValue("id"))
		if r.PathValue("id") == "f-gone" {
			http.Error(w, `{"message":"not found"}`, http.StatusNotFound)
			return
		}
		http.Error(w, `{"message":"boom"}`, http.StatusInternalServerError)
	})
	c := &TraQClient{
		Main: newStampConnector(t, stampconv.Stamp{ID: "gone", Name: "gone", Unicode: true}, stampconv.Stamp{ID: "flaky", Name: "flaky", Unicode: true}),
		API:  newStampAPI(t, mux),
	}
	ctx := zerolog.Nop().WithContext(context.Background())
	for range 2 {
		for id, want := range map[string]string{"gone": ":gone:", "flaky": ":flaky:"} {
			if key, err := c.reactionKey(ctx, id); key != want || err != nil {
				t.Errorf("reactionKey(%s) = %q, %v; want %q", id, key, err, want)
			}
		}
	}
	if metas.count("f-gone") != 1 || metas.count("f-flaky") != 2 {
		t.Errorf("FileMeta calls = %v, want f-gone once (then unresolvable) and f-flaky twice", metas.calls)
	}
	if s, _ := c.Main.Stamps.ByID("gone"); !s.Unresolvable {
		t.Errorf("stamp with a missing file = %+v, want it unresolvable", s)
	}
	// Only the definite answer is stored.
	if pending, _ := c.Main.stampDB.PendingUnicode(ctx); !slices.Equal(pending, []stampdb.Pending{{ID: "flaky", FileID: "f-flaky"}}) {
		t.Errorf("stored pending stamps = %+v, want only flaky", pending)
	}
}

// awaitRequest waits until h has seen a request for id. If none comes, it closes release, so that the blocked
// handlers return, and fails the test.
func awaitRequest(t *testing.T, h *countingHandler, id string, release chan struct{}) {
	t.Helper()
	awaitCount(t, h, id, 1, release)
}

// awaitCount waits until h has counted id n times. If it does not within 2 s, it closes release, so that the
// blocked handlers return, and fails the test.
func awaitCount(t *testing.T, h *countingHandler, id string, n int, release chan struct{}) {
	t.Helper()
	for deadline := time.Now().Add(2 * time.Second); h.count(id) < n; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			close(release)
			t.Fatalf("%s was counted %d times, want %d", id, h.count(id), n)
		}
	}
}

// countJoins counts, by stamp ID, the callers of c.resolveEmoji that have joined a resolution.
func countJoins(c *TraQConnector) *countingHandler {
	h := &countingHandler{}
	c.stampSync.resolveJoined = h.add
	return h
}

// within runs fn and fails the test if it does not return within 2 s, as when it waits for a lock that a
// blocked FileMeta call holds.
func within(t *testing.T, what string, fn func() error) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- fn() }()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("%s: %v", what, err)
		}
	case <-time.After(2 * time.Second):
		t.Errorf("%s waited for the FileMeta call", what)
	}
}

// Concurrent resolutions of one stamp share one FileMeta call also when it fails, and every caller gets the
// failure. The call is made without the write lock, so the STAMP_* handling is not held up by it.
func TestResolveEmojiSharedFailure(t *testing.T) {
	var metas countingHandler
	release := make(chan struct{})
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v3/files/{id}/meta", func(w http.ResponseWriter, r *http.Request) {
		metas.add(r.PathValue("id"))
		<-release
		http.Error(w, `{"message":"boom"}`, http.StatusInternalServerError)
	})
	c := newStampConnector(t, stampconv.Stamp{ID: "u1", Name: "thumbsup", Unicode: true}, stampconv.Stamp{ID: "other", Name: "other"})
	joins := countJoins(c)
	api := newStampAPI(t, mux)
	ctx := zerolog.Nop().WithContext(context.Background())

	const n = 10
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() { _, errs[i] = c.resolveEmoji(ctx, api, "u1") })
	}
	awaitRequest(t, &metas, "f-u1", release)
	awaitCount(t, joins, "u1", n, release) // FileMeta is held until every caller has joined the resolution
	within(t, "STAMP_DELETED of another stamp", func() error { return c.deleteStamp(ctx, "other") })
	close(release)
	wg.Wait()
	for i, err := range errs {
		if !traq.IsStatus(err, http.StatusInternalServerError) {
			t.Errorf("resolveEmoji #%d: err = %v, want the 500", i, err)
		}
	}
	if got := metas.count("f-u1"); got != 1 {
		t.Errorf("FileMeta was called %d times, want 1", got)
	}
	if s, _ := c.Stamps.ByID("u1"); !s.EmojiPending() {
		t.Errorf("stamp = %+v, want it still pending", s)
	}
}

// A resolution is not stored when the stamp got another image meanwhile (STAMP_UPDATED): it is older.
func TestResolveEmojiKeepsANewerFile(t *testing.T) {
	var metas countingHandler
	release := make(chan struct{})
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v3/stamps/u1", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"id":"u1","name":"thumbsup","fileId":"f-new","isUnicode":true}`))
	})
	mux.HandleFunc("GET /api/v3/files/{id}/meta", func(w http.ResponseWriter, r *http.Request) {
		metas.add(r.PathValue("id"))
		if r.PathValue("id") == "f-new" {
			http.Error(w, `{"message":"boom"}`, http.StatusInternalServerError)
			return
		}
		<-release
		_, _ = w.Write([]byte(`{"id":"f-u1","name":"1f44d.svg"}`))
	})
	c := newStampConnector(t, stampconv.Stamp{ID: "u1", Name: "thumbsup", Unicode: true})
	api := newStampAPI(t, mux)
	ctx := zerolog.Nop().WithContext(context.Background())

	res := make(chan stampconv.Stamp, 1)
	go func() {
		s, _ := c.resolveEmoji(ctx, api, "u1")
		res <- s
	}()
	awaitRequest(t, &metas, "f-u1", release)
	// The new image's meta fails, so the refresh stores the stamp pending with the new file.
	within(t, "STAMP_UPDATED", func() error { return c.refreshStamp(ctx, api, "u1") })
	close(release)
	if s := <-res; !s.EmojiPending() {
		t.Errorf("resolveEmoji = %+v, want the stamp as the refresh left it, pending", s)
	}
	if pending, _ := c.stampDB.PendingUnicode(ctx); !slices.Equal(pending, []stampdb.Pending{{ID: "u1", FileID: "f-new"}}) {
		t.Errorf("stored pending stamps = %+v, want u1 with f-new", pending)
	}
}

// A stamp deleted while its emoji is being resolved keeps the key it had.
func TestReactionKeyStampDeletedMeanwhile(t *testing.T) {
	var metas countingHandler
	release := make(chan struct{})
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v3/files/{id}/meta", func(w http.ResponseWriter, r *http.Request) {
		metas.add(r.PathValue("id"))
		<-release
		_, _ = w.Write([]byte(`{"id":"f-u1","name":"1f44d.svg"}`))
	})
	c := &TraQClient{Main: newStampConnector(t, stampconv.Stamp{ID: "u1", Name: "thumbsup", Unicode: true}), API: newStampAPI(t, mux)}
	ctx := zerolog.Nop().WithContext(context.Background())

	keys := make(chan string, 1)
	go func() {
		key, _ := c.reactionKey(ctx, "u1")
		keys <- key
	}()
	awaitRequest(t, &metas, "f-u1", release)
	within(t, "STAMP_DELETED", func() error { return c.Main.deleteStamp(ctx, "u1") })
	close(release)
	if key := <-keys; key != ":thumbsup:" {
		t.Errorf("reactionKey = %q, want :thumbsup:", key)
	}
}

// A STAMP_* refresh whose file meta fails still stores the stamp, pending, so it is resolved later. Like a
// stamp the job has given up on, it gets a retry time: the conversions do not ask traQ for it before then,
// and meanwhile an emoji the table does not have may be its emoji, so it is not reported unknown.
func TestRefreshStampFileMetaFailure(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v3/stamps/{id}", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"id":"u1","name":"heart","fileId":"f-heart","isUnicode":true}`))
	})
	mux.HandleFunc("GET /api/v3/files/{id}/meta", func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, `{"message":"boom"}`, http.StatusInternalServerError)
	})
	c := newStampConnector(t)
	var logs bytes.Buffer
	ctx := zerolog.New(&logs).WithContext(context.Background())
	if err := c.refreshStamp(ctx, newStampAPI(t, mux), "u1"); err != nil {
		t.Fatalf("refreshStamp: %v", err)
	}
	if s, ok := c.Stamps.ByID("u1"); !ok || s.Name != "heart" || !s.EmojiPending() {
		t.Errorf("table stamp = %+v, %v; want heart, pending", s, ok)
	}
	if pending, _ := c.stampDB.PendingUnicode(ctx); !slices.Equal(pending, []stampdb.Pending{{ID: "u1", FileID: "f-heart"}}) {
		t.Errorf("stored pending stamps = %+v, want u1", pending)
	}
	if !strings.Contains(logs.String(), "boom") {
		t.Errorf("log = %q, want the FileMeta failure", logs.String())
	}
	if at, ok := c.Stamps.RetryAfter("u1"); !ok || !at.After(time.Now()) {
		t.Errorf("RetryAfter(u1) = %v, %v; want a time to come", at, ok)
	}
	c.Stamps.SetComplete(true)
	if _, err := c.Stamps.Resolve("❤"); !errors.Is(err, stampconv.ErrEmojiTableIncomplete) {
		t.Errorf("Resolve(❤) = %v, want ErrEmojiTableIncomplete", err)
	}
}

// resolveEmoji's resolution runs on singleflight's own goroutine, where a panic would stop the bridge. A panic
// there is returned as an error, with the stack, and leaves the stamp pending.
func TestResolveEmojiPanic(t *testing.T) {
	c := newStampConnector(t, stampconv.Stamp{ID: "u1", Name: "thumbsup", Unicode: true})
	// A nil client panics on its first request.
	s, err := c.resolveEmoji(zerolog.Nop().WithContext(context.Background()), nil, "u1")
	if err == nil || !strings.Contains(err.Error(), "panic") || !strings.Contains(err.Error(), "resolveEmoji") {
		t.Errorf("err = %v, want the panic with its stack", err)
	}
	if s.ID != "u1" || !s.EmojiPending() {
		t.Errorf("stamp = %+v, want u1 still pending", s)
	}
}

// While traQ fails, each background retry round makes one request: it stops at its first failure instead of
// asking for every stamp given up on. The next round starts after the stamp that failed, so one stamp that
// keeps failing cannot keep the others from being retried.
func TestSyncStampsRetryRoundStopsAtFirstFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const outage = 4 // background requests that fail
		var mu sync.Mutex
		walk := map[string]int{}
		var later []string
		mux := http.NewServeMux()
		mux.HandleFunc("GET /api/v3/stamps", func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`[{"id":"a","name":"a","fileId":"f-a","isUnicode":true},
				{"id":"b","name":"b","fileId":"f-b","isUnicode":true},
				{"id":"c","name":"c","fileId":"f-c","isUnicode":true}]`))
		})
		mux.HandleFunc("GET /api/v3/files/{id}/meta", func(w http.ResponseWriter, r *http.Request) {
			id := r.PathValue("id")
			mu.Lock()
			defer mu.Unlock()
			if walk[id] < stampResolveAttempts {
				walk[id]++
				http.Error(w, `{"message":"boom"}`, http.StatusInternalServerError)
				return
			}
			later = append(later, id)
			if len(later) <= outage {
				http.Error(w, `{"message":"boom"}`, http.StatusInternalServerError)
				return
			}
			_, _ = w.Write([]byte(`{"id":"` + id + `","name":"1f44d.svg"}`))
		})
		c := newStampConnector(t)
		c.stampSync.clients = []*TraQClient{{API: newMemAPI(t, mux)}}
		var logs syncBuffer
		done, err := c.syncStamps(zerolog.New(&logs).WithContext(t.Context()))
		if !done || err != nil {
			t.Fatalf("syncStamps = %v, %v; want true, nil", done, err)
		}
		if want := []string{"f-a", "f-b", "f-c", "f-a"}; !slices.Equal(later[:outage], want) {
			t.Errorf("background requests during the outage = %v, want %v (one per round, each round starting after the stamp that failed)", later[:outage], want)
		}
		if n := strings.Count(logs.String(), "Failed to resolve the emoji of Unicode stamps again"); n != outage {
			t.Errorf("failed rounds = %d, want %d (one per failed request)", n, outage)
		}
		for _, id := range []string{"a", "b", "c"} {
			if s, _ := c.Stamps.ByID(id); s.Emoji != "👍" {
				t.Errorf("stamp %s = %+v, want it resolved after the outage", id, s)
			}
		}
	})
}

// Once the bridge is stopping (BackgroundCtx is done), a login that connects does not start the job: Stop may
// already be waiting for it.
func TestStampJobNotStartedWhenStopping(t *testing.T) {
	// A job that ran logs, through BackgroundCtx's logger, that it pauses for lack of an API client.
	var logs bytes.Buffer
	ctx, cancel := context.WithCancel(zerolog.New(&logs).WithContext(context.Background()))
	cancel()
	c := newStampConnector(t)
	c.Bridge = &bridgev2.Bridge{BackgroundCtx: ctx}
	c.stampClientConnected(&TraQClient{})
	c.Stop()
	if logs.Len() != 0 {
		t.Errorf("the job ran after BackgroundCtx ended: %s", logs.String())
	}
}

// The stamp list is read without holding the write lock, so the STAMP_* events and the conversions are not
// held up by it, and a change those events make meanwhile is not undone by the older list.
func TestReconcileStampsDoesNotHoldTheLockWhileFetching(t *testing.T) {
	fetching, release := make(chan struct{}), make(chan struct{})
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v3/stamps", func(w http.ResponseWriter, _ *http.Request) {
		close(fetching)
		<-release
		_, _ = w.Write([]byte(`[{"id":"a","name":"old_a","fileId":"f-a","isUnicode":false},{"id":"b","name":"b","fileId":"f-b","isUnicode":false}]`))
	})
	mux.HandleFunc("GET /api/v3/stamps/{id}", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		_, _ = w.Write([]byte(`{"id":"` + id + `","name":"new_` + id + `","fileId":"f-` + id + `","isUnicode":false}`))
	})
	api := newStampAPI(t, mux)
	c := newStampConnector(t, stampconv.Stamp{ID: "a", Name: "old_a"}, stampconv.Stamp{ID: "b", Name: "b"}, stampconv.Stamp{ID: "d", Name: "d"})
	ctx := context.Background()
	errs := make(chan error, 1)
	go func() { errs <- c.reconcileStamps(ctx, api) }()
	<-fetching
	// STAMP_UPDATED of a, STAMP_CREATED of c and STAMP_DELETED of b while the list is read.
	done := make(chan error, 1)
	go func() {
		err := c.refreshStamp(ctx, api, "a")
		if err == nil {
			err = c.refreshStamp(ctx, api, "c")
		}
		if err == nil {
			err = c.deleteStamp(ctx, "b")
		}
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("STAMP_* handling: %v", err)
		}
	case <-time.After(2 * time.Second):
		close(release)
		t.Fatal("the STAMP_* handling waited for the stamp list")
	}
	close(release)
	if err := <-errs; err != nil {
		t.Fatalf("reconcileStamps: %v", err)
	}
	stored, _ := c.stampDB.All(ctx)
	slices.SortFunc(stored, func(a, b stampconv.Stamp) int { return strings.Compare(a.ID, b.ID) })
	// a keeps its new name, c stays, b stays deleted; d, missing from the list, is deleted.
	want := []stampconv.Stamp{{ID: "a", Name: "new_a"}, {ID: "c", Name: "new_c"}}
	if !slices.Equal(stored, want) {
		t.Errorf("store = %+v, want %+v", stored, want)
	}
	for _, s := range want {
		if got, _ := c.Stamps.ByID(s.ID); got != s {
			t.Errorf("table stamp %s = %+v, want %+v", s.ID, got, s)
		}
	}
	if _, ok := c.Stamps.ByID("d"); ok {
		t.Error("the stamp missing from the list is still in the table")
	}
}
