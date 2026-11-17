package connector

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"

	"go.mau.fi/util/dbutil"
	_ "go.mau.fi/util/dbutil/litestream"

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
// Unicode stamps and then marks the table complete, even when one file name is not an emoji.
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
		{ID: "u3", Name: "odd", Unicode: true},
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
