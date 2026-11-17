package connector

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/kisepichu/traq-matrix-bridge/pkg/traq"
)

// roundTripFunc is an http.RoundTripper.
type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// newMemAPI returns a traQ client whose requests are served by h in the calling goroutine, without a network
// connection or a timer. It works inside a testing/synctest bubble, whose fake clock only advances while every
// goroutine of the bubble is blocked on something other than the network.
func newMemAPI(t *testing.T, h http.Handler) *traq.Client {
	t.Helper()
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, r)
		return rec.Result(), nil
	})}
	api, err := traq.NewClient("http://traq.test", client, "token", userAgent)
	if err != nil {
		t.Fatal(err)
	}
	return api
}

// syncBuffer is a bytes.Buffer that is safe for concurrent use, for a log written by one goroutine and read
// by the test.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}
