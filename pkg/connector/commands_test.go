package connector

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kisepichu/traq-matrix-bridge/pkg/traq"
)

// searchTestClient is a client whose traQ answers every request with status and body. calls counts the
// requests that reached it.
func searchTestClient(t *testing.T, status int, body string, calls *atomic.Int32) *TraQClient {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	api, err := traq.NewClient(srv.URL, srv.Client(), "t", "test")
	if err != nil {
		t.Fatal(err)
	}
	users := newUserCache()
	users.put(&traq.User{ID: "u-bob", Name: "bob"})
	return &TraQClient{
		API:      api,
		users:    users,
		channels: buildChannelTree(&traq.ChannelList{Public: []traq.Channel{{ID: "ch-a", Name: "a"}}}),
	}
}

func TestSearchMessagesReplies(t *testing.T) {
	cases := []struct {
		name      string
		query     string
		status    int
		body      string
		want      string
		wantCalls int32 // requests to traQ; the channel list is read once for an unknown channel
	}{
		{"search disabled", "foo", http.StatusServiceUnavailable, `{"message":"x"}`, "This traQ server has search disabled.", 1},
		{"no hits", "foo", http.StatusOK, `{"totalHits":0,"hits":[]}`, "No messages found.", 1},
		{"other failure", "foo", http.StatusInternalServerError, `{"message":"boom"}`, "Failed to search traQ: ", 1},
		{"unknown user does not search", "from:@nobody foo", http.StatusOK, `{"hits":[]}`, "User @nobody not found.", 0},
		{"unknown channel does not search", "in:#nope foo", http.StatusOK, `{"public":[],"dm":[]}`, "Channel #nope not found.", 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			c := searchTestClient(t, tc.status, tc.body, &calls)
			got := c.searchMessages(context.Background(), parseSearchQuery(tc.query), time.UTC)
			if !strings.HasPrefix(got, tc.want) {
				t.Errorf("reply = %q, want prefix %q", got, tc.want)
			}
			if calls.Load() != tc.wantCalls {
				t.Errorf("requests to traQ = %d, want %d", calls.Load(), tc.wantCalls)
			}
		})
	}
}

func TestSearchMessagesBeforeConnect(t *testing.T) {
	var calls atomic.Int32
	c := searchTestClient(t, http.StatusOK, `{}`, &calls)
	c.channels = nil
	got := c.searchMessages(context.Background(), parseSearchQuery("foo"), time.UTC)
	if !strings.Contains(got, "still connecting") || calls.Load() != 0 {
		t.Errorf("reply = %q, requests = %d", got, calls.Load())
	}
}
