package connector

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
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

	ctx := context.Background()
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

// A stamp whose file meta keeps failing uses up its retry budget and is marked unresolvable, so the job still
// completes and the stored table is complete.
func TestSyncStampsRetryBudget(t *testing.T) {
	var metas countingHandler
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v3/stamps", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`[{"id":"bad","name":"bad","fileId":"f-bad","isUnicode":true},{"id":"ok","name":"thumbsup","fileId":"f-ok","isUnicode":true}]`))
	})
	mux.HandleFunc("GET /api/v3/files/{id}/meta", func(w http.ResponseWriter, r *http.Request) {
		metas.add(r.PathValue("id"))
		if r.PathValue("id") == "f-bad" {
			http.Error(w, `{"message":"boom"}`, http.StatusInternalServerError)
			return
		}
		_, _ = w.Write([]byte(`{"id":"f-ok","name":"1f44d.svg"}`))
	})
	c := newStampConnector(t)
	c.stampSync.clients = []*TraQClient{{API: newStampAPI(t, mux)}}
	c.stampSync.retryMin = time.Millisecond

	ctx := context.Background()
	if done, err := c.syncStamps(ctx); !done || err != nil {
		t.Fatalf("syncStamps = %v, %v, want true, nil", done, err)
	}
	if n := metas.count("f-bad"); n != stampResolveAttempts {
		t.Errorf("FileMeta of the failing stamp was called %d times, want %d", n, stampResolveAttempts)
	}
	if s, _ := c.Stamps.ByID("bad"); !s.Unresolvable || s.Emoji != "" {
		t.Errorf("failing stamp = %+v, want it unresolvable", s)
	}
	if s, _ := c.Stamps.ByID("ok"); s.Emoji != "👍" {
		t.Errorf("stamp after the failing one = %+v, want it resolved", s)
	}
	if complete, err := c.stampDB.Complete(ctx); !complete || err != nil {
		t.Errorf("stored Complete = %v, %v, want true", complete, err)
	}
}

// A traQ that cannot be reached says nothing about the stamp, so it uses no retry budget: the job keeps
// retrying instead of marking every stamp unresolvable during an outage.
func TestSyncStampsUnreachableUsesNoBudget(t *testing.T) {
	var metas countingHandler
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v3/stamps", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`[{"id":"u1","name":"thumbsup","fileId":"f-u1","isUnicode":true}]`))
	})
	mux.HandleFunc("GET /api/v3/files/{id}/meta", func(w http.ResponseWriter, r *http.Request) {
		metas.add(r.PathValue("id"))
		conn, _, err := w.(http.Hijacker).Hijack()
		if err == nil {
			_ = conn.Close() // no response at all
		}
	})
	c := newStampConnector(t)
	c.stampSync.clients = []*TraQClient{{API: newStampAPI(t, mux)}}
	c.stampSync.retryMin = time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		for metas.count("f-u1") < 2*stampResolveAttempts {
			time.Sleep(time.Millisecond)
		}
		cancel()
	}()
	if done, err := c.syncStamps(ctx); done || !errors.Is(err, context.Canceled) {
		t.Fatalf("syncStamps = %v, %v, want false, context.Canceled", done, err)
	}
	if s, _ := c.Stamps.ByID("u1"); !s.EmojiPending() {
		t.Errorf("stamp = %+v, want it still pending", s)
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
	for deadline := time.Now().Add(2 * time.Second); metas.count("f-u1") == 0; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			close(release)
			wg.Wait()
			t.Fatalf("no conversion asked traQ for the stamp's file; keys %q", keys)
		}
	}
	time.Sleep(50 * time.Millisecond) // let the other conversions reach the resolution
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
}

// A STAMP_* refresh whose file meta fails still stores the stamp, pending, so it is resolved later.
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
