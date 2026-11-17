package connector

import (
	"testing"
	"time"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
)

func TestFetchQuery(t *testing.T) {
	anchorTS := time.Date(2026, 1, 2, 3, 4, 5, 123456000, time.UTC)
	cursorTS := time.Date(2026, 1, 1, 0, 0, 0, 999000000, time.UTC)
	anchor := &database.Message{Timestamp: anchorTS}
	timeEq := func(got *time.Time, want time.Time) bool { return got != nil && got.Equal(want) }

	t.Run("forward with anchor", func(t *testing.T) {
		q := fetchQuery(bridgev2.FetchMessagesParams{Forward: true, AnchorMessage: anchor, Count: 30})
		if q.Order != "asc" || q.Limit != 30 || q.Inclusive || q.Until != nil || !timeEq(q.Since, anchorTS) {
			t.Errorf("query = %+v", q)
		}
	})
	t.Run("forward without anchor takes the newest messages", func(t *testing.T) {
		q := fetchQuery(bridgev2.FetchMessagesParams{Forward: true, Count: 50})
		if q.Order != "desc" || q.Limit != 50 || q.Since != nil || q.Until != nil || q.Inclusive {
			t.Errorf("query = %+v", q)
		}
	})
	t.Run("backward with anchor", func(t *testing.T) {
		q := fetchQuery(bridgev2.FetchMessagesParams{AnchorMessage: anchor, Count: 100})
		if q.Order != "desc" || q.Limit != 100 || q.Inclusive || q.Since != nil || !timeEq(q.Until, anchorTS) {
			t.Errorf("query = %+v", q)
		}
	})
	t.Run("backward cursor wins over anchor", func(t *testing.T) {
		q := fetchQuery(bridgev2.FetchMessagesParams{AnchorMessage: anchor, Cursor: encodeCursor(cursorTS), Count: 100})
		if q.Order != "desc" || !timeEq(q.Until, cursorTS) {
			t.Errorf("query = %+v", q)
		}
	})
	t.Run("backward without anchor or cursor", func(t *testing.T) {
		q := fetchQuery(bridgev2.FetchMessagesParams{Count: 100})
		if q.Order != "desc" || q.Until != nil || q.Since != nil {
			t.Errorf("query = %+v", q)
		}
	})
	t.Run("count is capped at 200", func(t *testing.T) {
		for _, forward := range []bool{true, false} {
			if q := fetchQuery(bridgev2.FetchMessagesParams{Forward: forward, AnchorMessage: anchor, Count: 500}); q.Limit != 200 {
				t.Errorf("forward=%v: limit = %d", forward, q.Limit)
			}
		}
	})
}

func TestCursorRoundTrip(t *testing.T) {
	for _, ts := range []time.Time{
		time.Date(2026, 1, 2, 3, 4, 5, 123456789, time.UTC),
		time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
		time.Date(2026, 1, 2, 12, 4, 5, 1, time.FixedZone("JST", 9*3600)),
	} {
		got, err := decodeCursor(encodeCursor(ts))
		if err != nil || !got.Equal(ts) {
			t.Errorf("decodeCursor(encodeCursor(%v)) = %v, %v", ts, got, err)
		}
	}
	if _, err := decodeCursor(networkid.PaginationCursor("not a time")); err == nil {
		t.Error("an invalid cursor was accepted")
	}
}

func TestResolverCanReplyToEarlierBatchMessage(t *testing.T) {
	r := &resolver{earlier: map[string]struct{}{testUUID: {}}}
	if !r.CanReplyTo(testUUID) {
		t.Error("a message sent earlier in the batch is not a reply target")
	}
}
