package stampdb_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"go.mau.fi/util/dbutil"
	_ "go.mau.fi/util/dbutil/litestream"

	"github.com/kisepichu/traq-matrix-bridge/pkg/connector/stampdb"
	"github.com/kisepichu/traq-matrix-bridge/pkg/stampconv"
)

// newStore opens an in-memory SQLite database the way mxmain opens the bridge's (driver sqlite3-fk-wal) and
// upgrades the stamp table on a child of it, as the connector does.
func newStore(t *testing.T) *stampdb.Store {
	t.Helper()
	db, err := dbutil.NewFromConfig("", dbutil.Config{PoolConfig: dbutil.PoolConfig{
		Type:         "sqlite3-fk-wal",
		URI:          ":memory:?_txlock=immediate",
		MaxOpenConns: 1,
		MaxIdleConns: 1,
	}}, nil)
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	child := db.Child(stampdb.VersionTableName, stampdb.UpgradeTable, nil)
	if err := child.Upgrade(context.Background()); err != nil {
		t.Fatalf("upgrade: %v", err)
	}
	return stampdb.New(child)
}

func put(t *testing.T, s *stampdb.Store, st stampconv.Stamp, fileID string) {
	t.Helper()
	if err := s.Put(context.Background(), st, fileID); err != nil {
		t.Fatalf("Put(%+v): %v", st, err)
	}
}

func all(t *testing.T, s *stampdb.Store) []stampconv.Stamp {
	t.Helper()
	stamps, err := s.All(context.Background())
	if err != nil {
		t.Fatalf("All: %v", err)
	}
	slices.SortFunc(stamps, func(a, b stampconv.Stamp) int { return strings.Compare(a.ID, b.ID) })
	return stamps
}

func pending(t *testing.T, s *stampdb.Store) []stampdb.Pending {
	t.Helper()
	p, err := s.PendingUnicode(context.Background())
	if err != nil {
		t.Fatalf("PendingUnicode: %v", err)
	}
	slices.SortFunc(p, func(a, b stampdb.Pending) int { return strings.Compare(a.ID, b.ID) })
	return p
}

func complete(t *testing.T, s *stampdb.Store) bool {
	t.Helper()
	ok, err := s.Complete(context.Background())
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}
	return ok
}

func TestPutReplacesTheStamp(t *testing.T) {
	s := newStore(t)
	put(t, s, stampconv.Stamp{ID: "s1", Name: "good"}, "f1")
	put(t, s, stampconv.Stamp{ID: "s2", Name: "thumbsup", Unicode: true, Emoji: "👍"}, "f2")
	put(t, s, stampconv.Stamp{ID: "s3", Name: "odd", Unicode: true, Unresolvable: true}, "f3")
	put(t, s, stampconv.Stamp{ID: "s1", Name: "great"}, "f1")

	want := []stampconv.Stamp{
		{ID: "s1", Name: "great"},
		{ID: "s2", Name: "thumbsup", Unicode: true, Emoji: "👍"},
		{ID: "s3", Name: "odd", Unicode: true, Unresolvable: true},
	}
	if got := all(t, s); !slices.Equal(got, want) {
		t.Errorf("All = %+v, want %+v", got, want)
	}
}

func TestFileID(t *testing.T) {
	s := newStore(t)
	put(t, s, stampconv.Stamp{ID: "s1", Name: "heart", Unicode: true}, "f1")
	if fileID, ok, err := s.FileID(context.Background(), "s1"); fileID != "f1" || !ok || err != nil {
		t.Errorf("FileID(s1) = %q, %v, %v; want f1, true, nil", fileID, ok, err)
	}
	if fileID, ok, err := s.FileID(context.Background(), "missing"); fileID != "" || ok || err != nil {
		t.Errorf("FileID(missing) = %q, %v, %v; want \"\", false, nil", fileID, ok, err)
	}
}

// The writes of a failed transaction are rolled back together.
func TestDoTxnRollsBack(t *testing.T) {
	s := newStore(t)
	put(t, s, stampconv.Stamp{ID: "s1", Name: "good"}, "f1")
	boom := errors.New("boom")
	err := s.DoTxn(context.Background(), func(ctx context.Context) error {
		if err := s.Put(ctx, stampconv.Stamp{ID: "s2", Name: "new"}, "f2"); err != nil {
			return err
		}
		if err := s.Delete(ctx, "s1"); err != nil {
			return err
		}
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("DoTxn = %v, want boom", err)
	}
	if got, want := all(t, s), []stampconv.Stamp{{ID: "s1", Name: "good"}}; !slices.Equal(got, want) {
		t.Errorf("All after the rollback = %+v, want %+v", got, want)
	}
}

func TestDeleteRemovesOnlyThatStamp(t *testing.T) {
	s := newStore(t)
	put(t, s, stampconv.Stamp{ID: "s1", Name: "good"}, "f1")
	put(t, s, stampconv.Stamp{ID: "s2", Name: "bad"}, "f2")
	if err := s.Delete(context.Background(), "s1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := s.Delete(context.Background(), "missing"); err != nil {
		t.Errorf("Delete of a missing stamp: %v", err)
	}
	if got, want := all(t, s), []stampconv.Stamp{{ID: "s2", Name: "bad"}}; !slices.Equal(got, want) {
		t.Errorf("All = %+v, want %+v", got, want)
	}
}

func TestPendingUnicodeListsOnlyUnresolvedUnicodeStamps(t *testing.T) {
	s := newStore(t)
	put(t, s, stampconv.Stamp{ID: "custom", Name: "good"}, "f1")
	put(t, s, stampconv.Stamp{ID: "resolved", Name: "thumbsup", Unicode: true, Emoji: "👍"}, "f2")
	put(t, s, stampconv.Stamp{ID: "unresolvable", Name: "odd", Unicode: true, Unresolvable: true}, "f5")
	put(t, s, stampconv.Stamp{ID: "u1", Name: "heart", Unicode: true}, "f3")
	put(t, s, stampconv.Stamp{ID: "u2", Name: "smile", Unicode: true}, "f4")

	want := []stampdb.Pending{{ID: "u1", FileID: "f3"}, {ID: "u2", FileID: "f4"}}
	if got := pending(t, s); !slices.Equal(got, want) {
		t.Errorf("PendingUnicode = %+v, want %+v", got, want)
	}

	put(t, s, stampconv.Stamp{ID: "u1", Name: "heart", Unicode: true, Emoji: "❤️"}, "f3")
	want = []stampdb.Pending{{ID: "u2", FileID: "f4"}}
	if got := pending(t, s); !slices.Equal(got, want) {
		t.Errorf("PendingUnicode after resolving u1 = %+v, want %+v", got, want)
	}
}

func TestComplete(t *testing.T) {
	s := newStore(t)
	if complete(t, s) {
		t.Error("Complete of an empty table = true, want false")
	}
	put(t, s, stampconv.Stamp{ID: "custom", Name: "good"}, "f1")
	if !complete(t, s) {
		t.Error("Complete with only custom stamps = false, want true")
	}
	put(t, s, stampconv.Stamp{ID: "u1", Name: "heart", Unicode: true}, "f2")
	if complete(t, s) {
		t.Error("Complete with an unresolved Unicode stamp = true, want false")
	}
	put(t, s, stampconv.Stamp{ID: "u1", Name: "heart", Unicode: true, Emoji: "❤️"}, "f2")
	if !complete(t, s) {
		t.Error("Complete after resolving every Unicode stamp = false, want true")
	}
	put(t, s, stampconv.Stamp{ID: "u2", Name: "odd", Unicode: true, Unresolvable: true}, "f3")
	if !complete(t, s) {
		t.Error("Complete with an unresolvable Unicode stamp = false, want true")
	}
}
