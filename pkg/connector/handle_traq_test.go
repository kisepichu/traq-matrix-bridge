package connector

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"

	"github.com/kisepichu/traq-matrix-bridge/pkg/stampconv"
	"github.com/kisepichu/traq-matrix-bridge/pkg/traq"
)

func TestRecentMessages(t *testing.T) {
	at := func(portal string) messageTarget {
		return messageTarget{portal: networkid.PortalKey{ID: networkid.PortalID(portal)}, userID: "u-" + portal}
	}
	r := newRecentMessages(2)
	check := func(id string, want messageTarget, wantOK bool) {
		t.Helper()
		if got, ok := r.get(id); ok != wantOK || got != want {
			t.Errorf("get(%s) = %+v, %v; want %+v, %v", id, got, ok, want, wantOK)
		}
	}
	r.add("m1", at("a"))
	r.add("m2", at("b"))
	check("m1", at("a"), true)
	check("m2", at("b"), true)
	check("m3", messageTarget{}, false)

	r.add("m3", at("c")) // evicts the oldest, m1
	check("m1", messageTarget{}, false)
	check("m2", at("b"), true)
	check("m3", at("c"), true)

	// Adding an ID again updates it in place: it takes no second slot, so m3 is still there after m4.
	r.add("m2", at("d"))
	check("m2", at("d"), true)
	r.add("m4", at("e")) // evicts m2
	check("m2", messageTarget{}, false)
	check("m3", at("c"), true)
	check("m4", at("e"), true)
}

// The failure log must stay valid JSON whatever the event body is, and must not hold a huge body.
func TestFailedEventLogBody(t *testing.T) {
	var logs bytes.Buffer
	c := &TraQClient{Login: &bridgev2.UserLogin{UserLogin: &database.UserLogin{}, Log: zerolog.New(&logs)}}
	w := &workerState{recent: newRecentMessages(1)}
	long := `{"id":1,"x":"` + strings.Repeat("a", 5000) + `"}`
	for _, body := range []string{`{"id":`, long} {
		logs.Reset()
		c.handleEvent(context.Background(), w, traq.Event{Type: "MESSAGE_DELETED", Body: json.RawMessage(body)})
		var line struct {
			Message   string `json:"message"`
			EventBody string `json:"event_body"`
		}
		if err := json.Unmarshal(logs.Bytes(), &line); err != nil {
			t.Errorf("log line %q is not a JSON object with a string event_body: %v", logs.String(), err)
			continue
		}
		if line.Message != "Failed to handle traQ event" {
			t.Errorf("message = %q", line.Message)
		}
		if got := strings.TrimSuffix(line.EventBody, "…"); !strings.HasPrefix(body, got) || len(got) > maxLoggedEventBody {
			t.Errorf("event_body = %q (%d bytes), want a prefix of the body of at most %d bytes", line.EventBody, len(line.EventBody), maxLoggedEventBody)
		}
	}
}

// A burst of channel events must not block the worker on a sync, and adds up to at most one pending sync.
func TestChannelEventsCoalesceSyncs(t *testing.T) {
	var calls atomic.Int32
	release := make(chan struct{})
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v3/channels", func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		_, _ = w.Write([]byte(`{"public":[],"dm":[]}`))
	})
	mux.HandleFunc("GET /api/v3/users/me/subscriptions", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`[]`))
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	api, err := traq.NewClient(srv.URL, srv.Client(), "token", userAgent)
	if err != nil {
		t.Fatal(err)
	}
	c := &TraQClient{
		Main: &TraQConnector{
			Config: NetworkConfig{SubscriptionSyncInterval: time.Hour},
			Stamps: stampconv.NewTable([]stampconv.Stamp{{ID: "s1", Name: "good"}}, false),
		},
		Login: &bridgev2.UserLogin{UserLogin: &database.UserLogin{}, Log: zerolog.Nop()},
		API:   api,
	}
	ctx, stop := context.WithCancelCause(context.Background())
	queue := make(chan traq.Event, eventQueueSize)
	requests := make(chan struct{}, 1)
	var wg sync.WaitGroup
	wg.Go(func() { c.eventWorker(ctx, queue, requests) })
	wg.Go(func() { c.subscriptionSyncLoop(ctx, stop, requests) })
	t.Cleanup(func() {
		stop(nil)
		wg.Wait()
	})
	wait := func(what string, cond func() bool) {
		t.Helper()
		for deadline := time.Now().Add(2 * time.Second); !cond(); time.Sleep(5 * time.Millisecond) {
			if time.Now().After(deadline) {
				t.Fatalf("timed out waiting for %s", what)
			}
		}
	}
	channelEvent := func(typ string) traq.Event {
		return traq.Event{Type: typ, Body: json.RawMessage(`{"id":"0193a1b2-0000-7000-8000-0000000000c1"}`)}
	}

	queue <- channelEvent("CHANNEL_UPDATED")
	wait("the first sync", func() bool { return calls.Load() == 1 })
	for i := range 10 {
		queue <- channelEvent([]string{"CHANNEL_UPDATED", "CHANNEL_DELETED"}[i%2])
	}
	queue <- traq.Event{Type: "STAMP_DELETED", Body: json.RawMessage(`{"id":"s1"}`)}
	wait("the worker to handle the events after the burst", func() bool {
		_, ok := c.Main.Stamps.ByID("s1")
		return !ok
	})
	close(release)
	wait("the pending sync", func() bool { return calls.Load() == 2 })
	time.Sleep(100 * time.Millisecond)
	if got := calls.Load(); got != 2 {
		t.Errorf("Channels() was called %d times, want 2 (the running sync and one pending)", got)
	}
}
