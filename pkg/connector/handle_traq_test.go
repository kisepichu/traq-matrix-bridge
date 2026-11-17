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
	"testing"
	"testing/synctest"
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

// A burst of channel events must not block the worker on a sync, and adds up to at most one pending sync. A
// sync that channel events trigger starts at least eventSyncSpacing after the previous one, so a server whose
// channels change all the time does not make the bridge read the whole channel list again and again.
func TestChannelEventSyncs(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var mu sync.Mutex
		var syncs []time.Time
		release := make(chan struct{})
		mux := http.NewServeMux()
		mux.HandleFunc("GET /api/v3/channels", func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			syncs = append(syncs, time.Now())
			mu.Unlock()
			<-release
			_, _ = w.Write([]byte(`{"public":[],"dm":[]}`))
		})
		mux.HandleFunc("GET /api/v3/users/me/subscriptions", func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`[]`))
		})
		c := &TraQClient{
			Main: &TraQConnector{
				Config:  NetworkConfig{SubscriptionSyncInterval: time.Hour},
				Stamps:  stampconv.NewTable([]stampconv.Stamp{{ID: "s1", Name: "good"}}, false),
				stampDB: newTestStampDB(t),
			},
			Login: &bridgev2.UserLogin{UserLogin: &database.UserLogin{}, Log: zerolog.Nop()},
			API:   newMemAPI(t, mux),
		}
		ctx, stop := context.WithCancelCause(t.Context())
		queue := make(chan traq.Event, eventQueueSize)
		requests := make(chan struct{}, 1)
		var wg sync.WaitGroup
		wg.Go(func() { c.eventWorker(ctx, queue, requests) })
		wg.Go(func() { c.subscriptionSyncLoop(ctx, stop, requests) })
		defer func() {
			stop(nil)
			wg.Wait()
		}()
		syncTimes := func() []time.Duration {
			mu.Lock()
			defer mu.Unlock()
			var d []time.Duration
			for _, s := range syncs {
				d = append(d, s.Sub(syncs[0]))
			}
			return d
		}
		channelEvent := func(typ string) traq.Event {
			return traq.Event{Type: typ, Body: json.RawMessage(`{"id":"0193a1b2-0000-7000-8000-0000000000c1"}`)}
		}

		queue <- channelEvent("CHANNEL_UPDATED")
		synctest.Wait()
		for i := range 10 {
			queue <- channelEvent([]string{"CHANNEL_UPDATED", "CHANNEL_DELETED"}[i%2])
		}
		queue <- traq.Event{Type: "STAMP_DELETED", Body: json.RawMessage(`{"id":"s1"}`)}
		synctest.Wait()
		if _, ok := c.Main.Stamps.ByID("s1"); ok {
			t.Fatal("the worker did not handle the event after the burst while a sync was running")
		}
		close(release)
		time.Sleep(10 * time.Minute)
		synctest.Wait()
		if got, want := syncTimes(), []time.Duration{0, eventSyncSpacing}; !slices.Equal(got, want) {
			t.Errorf("syncs at %v, want %v (the running sync, and the burst's one sync after the spacing)", got, want)
		}

		// Long enough after the last sync, an event is synced at once.
		queue <- channelEvent("CHANNEL_DELETED")
		synctest.Wait()
		if got, want := syncTimes(), []time.Duration{0, eventSyncSpacing, 10 * time.Minute}; !slices.Equal(got, want) {
			t.Errorf("syncs at %v, want %v", got, want)
		}
	})
}

// The worker reads the message of MESSAGE_CREATED and MESSAGE_UPDATED again a few times, a moment apart, when
// the read fails in a way that may pass (a 5xx or a connection error), before it drops the event. A 4xx is
// final, a 502 or 504 has already been retried by the traQ client (1 + 3 requests), and a client timeout is not
// retried, so a hung traQ does not stall the worker for several timeouts.
func TestMessageEventRetriesTransientFailures(t *testing.T) {
	ok := func(w http.ResponseWriter) {
		_, _ = w.Write([]byte(`{"id":"0193a1b2-0000-7000-8000-000000000001","channelId":"ch-none","content":"hi"}`))
	}
	status := func(code int) func(http.ResponseWriter) {
		return func(w http.ResponseWriter) { http.Error(w, `{"message":"boom"}`, code) }
	}
	errNet := errors.New("connection reset by peer")
	cases := []struct {
		name      string
		responses []func(http.ResponseWriter) // nil: an error, err
		err       error
		calls     int
		dropped   bool
	}{
		{"500 then ok", []func(http.ResponseWriter){status(500), status(500), ok}, nil, 3, false},
		{"network error then ok", []func(http.ResponseWriter){nil, ok}, errNet, 2, false},
		{"timeout", []func(http.ResponseWriter){nil, ok}, timeoutError{}, 1, true},
		{"503 every time", []func(http.ResponseWriter){status(503), status(503), status(503), status(503)}, nil, messageFetchAttempts, true},
		{"404", []func(http.ResponseWriter){status(404), ok}, nil, 1, true},
		{"502 every time", []func(http.ResponseWriter){status(502)}, nil, 4, true},
	}
	for _, tc := range cases {
		for _, typ := range []string{"MESSAGE_CREATED", "MESSAGE_UPDATED"} {
			t.Run(tc.name+"/"+typ, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					var calls int
					client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
						respond := tc.responses[min(calls, len(tc.responses)-1)]
						calls++
						if respond == nil {
							return nil, tc.err
						}
						rec := httptest.NewRecorder()
						respond(rec)
						return rec.Result(), nil
					})}
					api, err := traq.NewClient("http://traq.test", client, "token", userAgent)
					if err != nil {
						t.Fatal(err)
					}
					var logs bytes.Buffer
					c := &TraQClient{Login: &bridgev2.UserLogin{UserLogin: &database.UserLogin{}, Log: zerolog.New(&logs)}, API: api}
					w := &workerState{recent: newRecentMessages(1)}
					c.handleEvent(t.Context(), w, traq.Event{Type: typ, Body: json.RawMessage(`{"id":"0193a1b2-0000-7000-8000-000000000001"}`)})
					if calls != tc.calls {
						t.Errorf("GET /messages/{id} was called %d times, want %d", calls, tc.calls)
					}
					if dropped := strings.Contains(logs.String(), "Failed to handle traQ event"); dropped != tc.dropped {
						t.Errorf("dropped = %v, want %v (log %s)", dropped, tc.dropped, logs.String())
					}
				})
			})
		}
	}
}

// timeoutError is the error of a request that timed out, as net/http reports it (url.Error.Timeout).
type timeoutError struct{}

func (timeoutError) Error() string { return "i/o timeout" }
func (timeoutError) Timeout() bool { return true }
