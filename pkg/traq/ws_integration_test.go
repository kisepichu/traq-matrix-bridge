//go:build integration

package traq_test

import (
	"context"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kisepichu/traq-matrix-bridge/pkg/traq"
)

const (
	wsEventTimeout = 15 * time.Second
	// wsQuietWindow is how long a "traQ does not deliver this" assertion waits.
	wsQuietWindow = 3 * time.Second
)

// wsListener reads a WSConn in the background. The goroutine also keeps pongs flowing for Ping.
type wsListener struct {
	conn   *traq.WSConn
	events chan traq.Event

	exited  chan struct{} // closed when the reader goroutine has returned
	mu      sync.Mutex
	readErr error        // why the reader returned; set before exited is closed
	dropped atomic.Int64 // events discarded because the events buffer was full
}

// listen authorizes cfg for s, dials the WebSocket and starts reading. Every received event is logged (-v).
func listen(t *testing.T, s *session, cfg traq.OAuthConfig) *wsListener {
	t.Helper()
	tok := authorize(t, s, cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := traq.DialWS(ctx, testServerURL(), tok.AccessToken, nil)
	if err != nil {
		t.Fatalf("DialWS: %v", err)
	}
	l := &wsListener{conn: conn, events: make(chan traq.Event, 256), exited: make(chan struct{})}
	readCtx, stop := context.WithCancel(context.Background())
	go func() {
		defer close(l.exited)
		for {
			ev, err := conn.ReadEvent(readCtx)
			if err != nil {
				l.mu.Lock()
				l.readErr = err
				l.mu.Unlock()
				return
			}
			select {
			case l.events <- ev:
			default:
				l.dropped.Add(1)
			}
		}
	}()
	t.Cleanup(func() {
		stop()
		<-l.exited
		conn.Close()
	})
	return l
}

// alive fails the test if the reader goroutine has exited (the connection died) or an event was dropped. A
// negative assertion is only meaningful while both hold.
func (l *wsListener) alive(t *testing.T) {
	t.Helper()
	select {
	case <-l.exited:
		l.mu.Lock()
		err := l.readErr
		l.mu.Unlock()
		t.Fatalf("the WebSocket reader has exited: %v", err)
	default:
	}
	if n := l.dropped.Load(); n > 0 {
		t.Fatalf("%d events were dropped because the listener's buffer was full", n)
	}
}

// sync sends a ping and waits for the pong. traQ handles a connection's frames in order, so the commands sent
// before the ping have been applied once it returns. It also proves the connection is alive and the reader is
// running, and every event traQ sent before the pong is in l.events when it returns.
func (l *wsListener) sync(t *testing.T) {
	t.Helper()
	l.alive(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := l.conn.Ping(ctx); err != nil {
		l.alive(t) // prefer the reader's error, which is the root cause
		t.Fatalf("Ping: %v", err)
	}
	l.alive(t)
}

// idOf returns the "id" of an IDEvent body, or "" if the body is something else.
func idOf(ev traq.Event) string {
	var b traq.IDEvent
	if ev.Decode(&b) != nil {
		return ""
	}
	return b.ID
}

// expect waits for an event for which match returns true and returns it. Other events are ignored (and logged).
func (l *wsListener) expect(t *testing.T, what, typ string, match func(traq.Event) bool) traq.Event {
	t.Helper()
	timer := time.NewTimer(wsEventTimeout)
	defer timer.Stop()
	for {
		select {
		case ev := <-l.events:
			t.Logf("event %s %s", ev.Type, ev.Body)
			if ev.Type == typ && match(ev) {
				return ev
			}
		case <-l.exited:
			l.alive(t)
		case <-timer.C:
			l.alive(t)
			t.Fatalf("%s: no %s event within %s", what, typ, wsEventTimeout)
		}
	}
}

// expectNone fails if a matching event arrives within wsQuietWindow. Other events are ignored (and logged).
// Silence only counts if the connection is still alive afterwards: a final sync proves the reader is still
// running and that no event was dropped, and the events that arrived before its pong are checked too.
func (l *wsListener) expectNone(t *testing.T, what, typ string, match func(traq.Event) bool) {
	t.Helper()
	check := func(ev traq.Event) {
		t.Helper()
		t.Logf("event %s %s", ev.Type, ev.Body)
		if ev.Type == typ && match(ev) {
			t.Fatalf("%s: got %s %s, which traQ is expected not to deliver", what, ev.Type, ev.Body)
		}
	}
	timer := time.NewTimer(wsQuietWindow)
	defer timer.Stop()
wait:
	for {
		select {
		case ev := <-l.events:
			check(ev)
		case <-l.exited:
			l.alive(t)
		case <-timer.C:
			break wait
		}
	}
	l.sync(t)
	for {
		select {
		case ev := <-l.events:
			check(ev)
		default:
			return
		}
	}
}

func messageIDIs(id string) func(traq.Event) bool {
	return func(ev traq.Event) bool {
		var b struct {
			MessageID string `json:"message_id"`
		}
		return ev.Decode(&b) == nil && b.MessageID == id
	}
}

func idIs(id string) func(traq.Event) bool {
	return func(ev traq.Event) bool { return idOf(ev) == id }
}

func TestIntegrationWSTimelineStreaming(t *testing.T) {
	admin, cfg, _, _ := adminAPI(t)
	_, _, otherSession := newUser(t, admin)
	other, _ := apiFor(t, otherSession, cfg)
	ch := newChannel(t, admin)
	stampID := createStamp(t, admin, "it-"+randHex(t, 6))
	ctx := testContext(t)

	l := listen(t, admin, cfg)
	if err := l.conn.SetTimelineStreaming(ctx, true); err != nil {
		t.Fatal(err)
	}
	l.sync(t)

	msg, err := other.PostMessage(ctx, ch, "streamed")
	if err != nil {
		t.Fatalf("PostMessage: %v", err)
	}
	l.expect(t, "post by another user", "MESSAGE_CREATED", idIs(msg.ID))

	if err := other.EditMessage(ctx, msg.ID, "streamed, edited"); err != nil {
		t.Fatalf("EditMessage: %v", err)
	}
	l.expect(t, "edit by another user", "MESSAGE_UPDATED", idIs(msg.ID))

	if err := other.AddStamp(ctx, msg.ID, stampID); err != nil {
		t.Fatalf("AddStamp: %v", err)
	}
	l.expectNone(t, "stamp without viewstate", "MESSAGE_STAMPED", messageIDIs(msg.ID))

	if err := other.DeleteMessage(ctx, msg.ID); err != nil {
		t.Fatalf("DeleteMessage: %v", err)
	}
	l.expect(t, "delete by another user", "MESSAGE_DELETED", idIs(msg.ID))
}

func TestIntegrationWSViewState(t *testing.T) {
	admin, cfg, _, _ := adminAPI(t)
	otherName, otherID, otherSession := newUser(t, admin)
	other, _ := apiFor(t, otherSession, cfg)
	ch := newChannel(t, admin)
	stampID := createStamp(t, admin, "it-"+randHex(t, 6))
	ctx := testContext(t)

	l := listen(t, admin, cfg)
	if err := l.conn.SetTimelineStreaming(ctx, true); err != nil {
		t.Fatal(err)
	}
	if err := l.conn.SetViewState(ctx, ch); err != nil {
		t.Fatal(err)
	}
	l.sync(t)

	msg, err := other.PostMessage(ctx, ch, "stamp me")
	if err != nil {
		t.Fatalf("PostMessage: %v", err)
	}
	l.expect(t, "post", "MESSAGE_CREATED", idIs(msg.ID))

	if err := other.AddStamp(ctx, msg.ID, stampID); err != nil {
		t.Fatalf("AddStamp: %v", err)
	}
	ev := l.expect(t, "stamp with viewstate", "MESSAGE_STAMPED", messageIDIs(msg.ID))
	var stamped traq.MessageStampedEvent
	if err := ev.Decode(&stamped); err != nil {
		t.Fatalf("decode MESSAGE_STAMPED: %v", err)
	}
	if stamped.UserID != otherID || stamped.StampID != stampID || stamped.Count != 1 || stamped.CreatedAt.IsZero() {
		t.Errorf("MESSAGE_STAMPED = %+v, want user %s (%s), stamp %s, count 1, created_at set", stamped, otherID, otherName, stampID)
	}

	if err := other.RemoveStamp(ctx, msg.ID, stampID); err != nil {
		t.Fatalf("RemoveStamp: %v", err)
	}
	ev = l.expect(t, "unstamp with viewstate", "MESSAGE_UNSTAMPED", messageIDIs(msg.ID))
	var unstamped traq.MessageUnstampedEvent
	if err := ev.Decode(&unstamped); err != nil {
		t.Fatalf("decode MESSAGE_UNSTAMPED: %v", err)
	}
	if unstamped.UserID != otherID || unstamped.StampID != stampID {
		t.Errorf("MESSAGE_UNSTAMPED = %+v, want user %s, stamp %s", unstamped, otherID, stampID)
	}
}

func TestIntegrationWSDM(t *testing.T) {
	admin, cfg, _, me := adminAPI(t)
	_, _, otherSession := newUser(t, admin)
	other, _ := apiFor(t, otherSession, cfg)
	dm := dmChannel(t, otherSession, me.ID)
	ctx := testContext(t)

	l := listen(t, admin, cfg)
	if err := l.conn.SetTimelineStreaming(ctx, true); err != nil {
		t.Fatal(err)
	}
	l.sync(t)

	msg, err := other.PostMessage(ctx, dm, "hello in a DM")
	if err != nil {
		t.Fatalf("PostMessage: %v", err)
	}
	l.expect(t, "DM post", "MESSAGE_CREATED", idIs(msg.ID))

	if err := other.EditMessage(ctx, msg.ID, "hello in a DM, edited"); err != nil {
		t.Fatalf("EditMessage: %v", err)
	}
	l.expectNone(t, "DM edit without viewstate", "MESSAGE_UPDATED", idIs(msg.ID))
}

func TestIntegrationWSRead(t *testing.T) {
	admin, cfg, api, me := adminAPI(t)
	_, _, otherSession := newUser(t, admin)
	other, _ := apiFor(t, otherSession, cfg)
	dm := dmChannel(t, otherSession, me.ID)
	ctx := testContext(t)

	l := listen(t, admin, cfg)
	if err := l.conn.SetTimelineStreaming(ctx, true); err != nil {
		t.Fatal(err)
	}
	l.sync(t)

	if _, err := other.PostMessage(ctx, dm, "read me"); err != nil {
		t.Fatalf("PostMessage: %v", err)
	}
	eventually(t, 15*time.Second, "the DM to be unread", func() bool {
		list, err := api.Unread(ctx)
		if err != nil {
			t.Fatalf("Unread: %v", err)
		}
		return slices.ContainsFunc(list, func(u traq.UnreadChannel) bool { return u.ChannelID == dm })
	})

	if err := api.ReadChannel(ctx, dm); err != nil {
		t.Fatalf("ReadChannel: %v", err)
	}
	l.expect(t, "ReadChannel", "MESSAGE_READ", idIs(dm))
}
