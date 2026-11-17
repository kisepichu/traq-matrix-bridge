package connector

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"

	"github.com/kisepichu/traq-matrix-bridge/pkg/traq"
)

func TestFetchQuery(t *testing.T) {
	anchorTS := time.Date(2026, 1, 2, 3, 4, 5, 123456000, time.UTC)
	cursorTS := time.Date(2026, 1, 1, 0, 0, 0, 999000000, time.UTC)
	anchor := &database.Message{ID: "anchor", Timestamp: anchorTS}
	timeEq := func(got *time.Time, want time.Time) bool { return got != nil && got.Equal(want) }

	t.Run("forward with anchor includes the anchor's time", func(t *testing.T) {
		q, err := fetchQuery(bridgev2.FetchMessagesParams{Forward: true, AnchorMessage: anchor, Count: 30})
		if err != nil || q.Order != "desc" || !q.Inclusive || q.Until != nil || !timeEq(q.Since, anchorTS) {
			t.Errorf("query = %+v, %v", q, err)
		}
	})
	t.Run("forward without anchor takes the newest messages", func(t *testing.T) {
		q, err := fetchQuery(bridgev2.FetchMessagesParams{Forward: true, Count: 50})
		if err != nil || q.Order != "desc" || q.Since != nil || q.Until != nil {
			t.Errorf("query = %+v, %v", q, err)
		}
	})
	t.Run("backward with anchor", func(t *testing.T) {
		q, err := fetchQuery(bridgev2.FetchMessagesParams{AnchorMessage: anchor, Count: 100})
		if err != nil || q.Order != "desc" || q.Inclusive || q.Since != nil || !timeEq(q.Until, anchorTS) {
			t.Errorf("query = %+v, %v", q, err)
		}
	})
	t.Run("backward cursor wins over anchor", func(t *testing.T) {
		q, err := fetchQuery(bridgev2.FetchMessagesParams{AnchorMessage: anchor, Cursor: encodeCursor(cursorTS), Count: 100})
		if err != nil || q.Order != "desc" || q.Inclusive || !timeEq(q.Until, cursorTS) {
			t.Errorf("query = %+v, %v", q, err)
		}
	})
	t.Run("backward without anchor or cursor", func(t *testing.T) {
		q, err := fetchQuery(bridgev2.FetchMessagesParams{Count: 100})
		if err != nil || q.Order != "desc" || q.Until != nil || q.Since != nil {
			t.Errorf("query = %+v, %v", q, err)
		}
	})
	t.Run("invalid cursor", func(t *testing.T) {
		if _, err := fetchQuery(bridgev2.FetchMessagesParams{Cursor: "not a time", Count: 100}); err == nil {
			t.Error("an invalid cursor was accepted")
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

var backfillBase = time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)

// fakeChannel answers GET /channels/{id}/messages like traQ over msgs: Since and Until bound CreatedAt,
// both inclusive or both exclusive by Inclusive, newest first, X-TRAQ-MORE when the range has more than
// Limit messages. Messages with equal CreatedAt keep their order in msgs.
type fakeChannel struct {
	msgs    []traq.Message // oldest first
	queries []traq.MessagesQuery
}

func (f *fakeChannel) fetch(_ context.Context, q traq.MessagesQuery) ([]traq.Message, bool, error) {
	f.queries = append(f.queries, q)
	if q.Order != "desc" || q.Limit < 1 || q.Limit > maxMessagesPerFetch {
		return nil, false, fmt.Errorf("bad query %+v", q)
	}
	var in []traq.Message
	for _, m := range slices.Backward(f.msgs) {
		if q.Since != nil && (m.CreatedAt.Before(*q.Since) || !q.Inclusive && m.CreatedAt.Equal(*q.Since)) {
			continue
		}
		if q.Until != nil && (m.CreatedAt.After(*q.Until) || !q.Inclusive && m.CreatedAt.Equal(*q.Until)) {
			continue
		}
		in = append(in, m)
	}
	if len(in) > q.Limit {
		return in[:q.Limit], true, nil
	}
	return in, false, nil
}

// channelOf has n messages "m0" (oldest) .. "m<n-1>", a millisecond apart unless ts says otherwise.
func channelOf(n int, ts func(i int) time.Time) *fakeChannel {
	if ts == nil {
		ts = func(i int) time.Time { return backfillBase.Add(time.Duration(i) * time.Millisecond) }
	}
	f := &fakeChannel{}
	for i := range n {
		f.msgs = append(f.msgs, traq.Message{ID: fmt.Sprintf("m%d", i), CreatedAt: ts(i)})
	}
	return f
}

func messageIDs(msgs []traq.Message) []string {
	ids := make([]string, len(msgs))
	for i, m := range msgs {
		ids[i] = m.ID
	}
	return ids
}

// idRange is "m<from>" .. "m<to-1>".
func idRange(from, to int) []string {
	var ids []string
	for i := from; i < to; i++ {
		ids = append(ids, fmt.Sprintf("m%d", i))
	}
	return ids
}

func TestNewestMessages(t *testing.T) {
	ctx := context.Background()
	desc := traq.MessagesQuery{Order: "desc"}

	t.Run("pages back over 200 and returns oldest first", func(t *testing.T) {
		ch := channelOf(250, nil)
		msgs, more, err := newestMessages(ctx, ch.fetch, desc, 250, "")
		if err != nil || more || !slices.Equal(messageIDs(msgs), idRange(0, 250)) {
			t.Errorf("got %v, more %v, err %v", messageIDs(msgs), more, err)
		}
		if len(ch.queries) != 2 {
			t.Errorf("%d requests, want 2: %+v", len(ch.queries), ch.queries)
		}
	})
	t.Run("stops at count with the newest", func(t *testing.T) {
		ch := channelOf(12, nil)
		msgs, more, err := newestMessages(ctx, ch.fetch, desc, 5, "")
		if err != nil || !more || !slices.Equal(messageIDs(msgs), idRange(7, 12)) {
			t.Errorf("got %v, more %v, err %v", messageIDs(msgs), more, err)
		}
		if len(ch.queries) != 1 || ch.queries[0].Limit != 5 {
			t.Errorf("queries = %+v", ch.queries)
		}
	})
	t.Run("stops at count across pages", func(t *testing.T) {
		ch := channelOf(500, nil)
		msgs, more, err := newestMessages(ctx, ch.fetch, desc, 300, "")
		if err != nil || !more || !slices.Equal(messageIDs(msgs), idRange(200, 500)) {
			t.Errorf("got %d messages, more %v, err %v", len(msgs), more, err)
		}
	})
	t.Run("stops when traQ has no more", func(t *testing.T) {
		ch := channelOf(3, nil)
		msgs, more, err := newestMessages(ctx, ch.fetch, desc, 50, "")
		if err != nil || more || !slices.Equal(messageIDs(msgs), idRange(0, 3)) {
			t.Errorf("got %v, more %v, err %v", messageIDs(msgs), more, err)
		}
		if len(ch.queries) != 1 {
			t.Errorf("queries = %+v", ch.queries)
		}
	})
	t.Run("empty channel", func(t *testing.T) {
		msgs, more, err := newestMessages(ctx, channelOf(0, nil).fetch, desc, 50, "")
		if err != nil || more || len(msgs) != 0 {
			t.Errorf("got %v, more %v, err %v", messageIDs(msgs), more, err)
		}
	})
	t.Run("anchor is left out and not counted, its time-mates are kept", func(t *testing.T) {
		// m3 is the anchor; m4 has the same time; m0..m2 are older.
		ch := channelOf(10, func(i int) time.Time {
			if i == 4 {
				i = 3
			}
			return backfillBase.Add(time.Duration(i) * time.Millisecond)
		})
		since := ch.msgs[3].CreatedAt
		if !ch.msgs[4].CreatedAt.Equal(since) || !ch.msgs[2].CreatedAt.Before(since) {
			t.Fatalf("bad fixture: %v", ch.msgs)
		}
		q := traq.MessagesQuery{Since: &since, Inclusive: true, Order: "desc"}
		msgs, more, err := newestMessages(ctx, ch.fetch, q, 6, "m3")
		if err != nil || more || !slices.Equal(messageIDs(msgs), idRange(4, 10)) {
			t.Errorf("got %v, more %v, err %v", messageIDs(msgs), more, err)
		}
		msgs, more, err = newestMessages(ctx, ch.fetch, q, 3, "m3")
		if err != nil || !more || !slices.Equal(messageIDs(msgs), idRange(7, 10)) {
			t.Errorf("count 3: got %v, more %v, err %v", messageIDs(msgs), more, err)
		}
	})
	t.Run("equal times at a page boundary are neither repeated nor skipped", func(t *testing.T) {
		// m45..m54 share one time; the first page (the newest 200: m50..m249) ends inside that group.
		ch := channelOf(250, func(i int) time.Time {
			if i >= 45 && i < 55 {
				i = 45
			}
			return backfillBase.Add(time.Duration(i) * time.Millisecond)
		})
		msgs, more, err := newestMessages(ctx, ch.fetch, desc, 250, "")
		if err != nil || more || !slices.Equal(messageIDs(msgs), idRange(0, 250)) {
			t.Errorf("got %v, more %v, err %v", messageIDs(msgs), more, err)
		}
		if len(ch.queries) != 2 {
			t.Errorf("%d requests, want 2", len(ch.queries))
		}
	})
	t.Run("more equal times than a page ends instead of looping", func(t *testing.T) {
		ch := channelOf(300, func(int) time.Time { return backfillBase })
		msgs, more, err := newestMessages(ctx, ch.fetch, desc, 300, "")
		if err != nil || !more || len(msgs) != maxMessagesPerFetch {
			t.Errorf("got %d messages, more %v, err %v", len(msgs), more, err)
		}
		if len(ch.queries) > 2 {
			t.Errorf("%d requests", len(ch.queries))
		}
	})
	t.Run("a fetch error is returned", func(t *testing.T) {
		boom := errors.New("boom")
		fetch := func(context.Context, traq.MessagesQuery) ([]traq.Message, bool, error) { return nil, false, boom }
		if _, _, err := newestMessages(ctx, fetch, desc, 5, ""); !errors.Is(err, boom) {
			t.Errorf("err = %v", err)
		}
	})
}

// testAssembler converts every message except "ignored" (ErrIgnoringRemoteEvent) and "broken" (an error),
// and records the batch's earlier messages each conversion saw.
func testAssembler(unread []traq.UnreadChannel, unreadErr error, unreadCalls *int) (*backfillAssembler, map[string][]string) {
	seen := map[string][]string{}
	return &backfillAssembler{
		convert: func(_ context.Context, msg *traq.Message, earlier map[string]struct{}) (*bridgev2.BackfillMessage, error) {
			ids := make([]string, 0, len(earlier))
			for id := range earlier {
				ids = append(ids, id)
			}
			slices.Sort(ids)
			seen[msg.ID] = ids
			switch msg.ID {
			case "ignored":
				return nil, bridgev2.ErrIgnoringRemoteEvent
			case "broken":
				return nil, errors.New("conversion failed")
			}
			return &bridgev2.BackfillMessage{ID: MessageID(msg.ID), Timestamp: msg.CreatedAt}, nil
		},
		unread: func(context.Context) ([]traq.UnreadChannel, error) {
			*unreadCalls++
			return unread, unreadErr
		},
	}, seen
}

func TestAssembleBackfill(t *testing.T) {
	ctx := context.Background()
	at := func(i int) time.Time { return backfillBase.Add(time.Duration(i) * time.Second) }
	batch := []traq.Message{
		{ID: "ignored", CreatedAt: at(0)},
		{ID: "a", CreatedAt: at(1)},
		{ID: "broken", CreatedAt: at(2)},
		{ID: "b", CreatedAt: at(3)},
		{ID: "c", CreatedAt: at(4)},
	}

	t.Run("keeps order, skips what is not bridged, cursor from the oldest fetched", func(t *testing.T) {
		var calls int
		a, seen := testAssembler(nil, nil, &calls)
		resp, err := a.assemble(ctx, "ch", false, batch, true)
		if err != nil {
			t.Fatal(err)
		}
		var ids []string
		for _, m := range resp.Messages {
			ids = append(ids, string(m.ID))
		}
		if !slices.Equal(ids, []string{"a", "b", "c"}) {
			t.Errorf("messages = %v", ids)
		}
		if !resp.HasMore || resp.Forward || resp.MarkRead || resp.Cursor != encodeCursor(at(0)) {
			t.Errorf("resp = %+v", resp)
		}
		if !slices.Equal(seen["c"], []string{"a", "b"}) || !slices.Equal(seen["b"], []string{"a"}) {
			t.Errorf("earlier seen = %v", seen)
		}
		if calls != 0 {
			t.Errorf("backward fetch read the unread list %d times", calls)
		}
	})
	t.Run("stops when the context ends", func(t *testing.T) {
		var calls int
		a, seen := testAssembler(nil, nil, &calls)
		cctx, cancel := context.WithCancel(ctx)
		cancel()
		if _, err := a.assemble(cctx, "ch", true, batch, false); !errors.Is(err, context.Canceled) {
			t.Errorf("err = %v", err)
		}
		if len(seen) != 0 {
			t.Errorf("converted %v after the context ended", seen)
		}
	})

	for _, tc := range []struct {
		name      string
		msgs      []traq.Message
		unread    []traq.UnreadChannel
		unreadErr error
		want      bool
		wantCalls int
	}{
		{name: "read channel", msgs: batch, unread: []traq.UnreadChannel{{ChannelID: "other"}}, want: true, wantCalls: 1},
		{name: "unread channel", msgs: batch, unread: []traq.UnreadChannel{{ChannelID: "ch"}}, wantCalls: 1},
		{name: "unread list error", msgs: batch, unreadErr: errors.New("down"), wantCalls: 1},
		{name: "empty batch", msgs: nil, wantCalls: 0},
		{name: "nothing bridged", msgs: []traq.Message{{ID: "ignored", CreatedAt: at(0)}}, wantCalls: 0},
	} {
		t.Run("forward MarkRead: "+tc.name, func(t *testing.T) {
			var calls int
			a, _ := testAssembler(tc.unread, tc.unreadErr, &calls)
			resp, err := a.assemble(ctx, "ch", true, tc.msgs, false)
			if err != nil || resp.MarkRead != tc.want || !resp.Forward || calls != tc.wantCalls {
				t.Errorf("MarkRead = %v, Forward = %v, unread calls = %d, err = %v", resp.MarkRead, resp.Forward, calls, err)
			}
		})
	}
}
