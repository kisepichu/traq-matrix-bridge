//go:build integration

package traq_test

import (
	"context"
	"slices"
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
	l := &wsListener{conn: conn, events: make(chan traq.Event, 256)}
	readCtx, stop := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			ev, err := conn.ReadEvent(readCtx)
			if err != nil {
				return
			}
			select {
			case l.events <- ev:
			default: // a full buffer would only happen if the test is already failing
			}
		}
	}()
	t.Cleanup(func() {
		stop()
		<-done
		conn.Close()
	})
	return l
}

// sync sends a ping and waits for the pong. traQ handles a connection's frames in order, so the commands sent
// before the ping have been applied once it returns.
func (l *wsListener) sync(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := l.conn.Ping(ctx); err != nil {
		t.Fatalf("Ping: %v", err)
	}
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
		case <-timer.C:
			t.Fatalf("%s: no %s event within %s", what, typ, wsEventTimeout)
		}
	}
}

// expectNone fails if a matching event arrives within wsQuietWindow. Other events are ignored (and logged).
func (l *wsListener) expectNone(t *testing.T, what, typ string, match func(traq.Event) bool) {
	t.Helper()
	timer := time.NewTimer(wsQuietWindow)
	defer timer.Stop()
	for {
		select {
		case ev := <-l.events:
			t.Logf("event %s %s", ev.Type, ev.Body)
			if ev.Type == typ && match(ev) {
				t.Fatalf("%s: got %s %s, which traQ is expected not to deliver", what, ev.Type, ev.Body)
			}
		case <-timer.C:
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
