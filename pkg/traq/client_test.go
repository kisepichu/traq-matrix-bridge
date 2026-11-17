package traq

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestUnknownFieldsAreIgnored(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v3/messages/m1" {
			t.Errorf("path = %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"m1","userId":"u1","channelId":"c1","content":"hi","createdAt":"2026-01-02T03:04:05Z","updatedAt":"2026-01-02T03:04:05Z","stamps":[],"threadId":null,"aBrandNewField":{"x":1}}`))
	}))
	defer srv.Close()
	c, err := NewClient(srv.URL, srv.Client(), "t", "test")
	if err != nil {
		t.Fatal(err)
	}
	m, err := c.Message(context.Background(), "m1")
	if err != nil {
		t.Fatalf("Message: %v", err)
	}
	if m.ID != "m1" || m.Content != "hi" || !m.CreatedAt.Equal(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)) {
		t.Errorf("decoded %+v", m)
	}
}

func TestBearerTokenAnd401(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Header.Get("Authorization") != "Bearer good" {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"message":"invalid token"}`))
			return
		}
		_, _ = w.Write([]byte(`{"id":"me","name":"kise","displayName":"Kise","groups":["g1"],"permissions":["get_me"]}`))
	}))
	defer srv.Close()
	good, _ := NewClient(srv.URL, srv.Client(), "good", "test")
	me, err := good.Me(context.Background())
	if err != nil || me.Name != "kise" || me.ID != "me" || len(me.Groups) != 1 {
		t.Fatalf("Me = %+v, %v", me, err)
	}
	bad, _ := NewClient(srv.URL, srv.Client(), "bad", "test")
	calls.Store(0)
	_, err = bad.Me(context.Background())
	var he *HTTPError
	if !IsStatus(err, http.StatusUnauthorized) || !errors.As(err, &he) || he.Message != "invalid token" {
		t.Fatalf("err = %v, want 401 HTTPError with the server message", err)
	}
	if calls.Load() != 1 {
		t.Errorf("401 was retried: %d calls", calls.Load())
	}
	if strings.Contains(err.Error(), "bad") {
		t.Error("error message leaks the token")
	}
}

func TestRetriesRateLimit(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		_, _ = w.Write([]byte(`[]`))
	}))
	defer srv.Close()
	c, _ := NewClient(srv.URL, srv.Client(), "t", "test")
	if _, err := c.Stamps(context.Background()); err != nil {
		t.Fatalf("Stamps: %v", err)
	}
	if calls.Load() != 2 {
		t.Errorf("calls = %d, want 2", calls.Load())
	}
}

func TestServiceUnavailableIsNotRetried(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()
	c, _ := NewClient(srv.URL, srv.Client(), "t", "test")
	_, err := c.Search(context.Background(), SearchQuery{Word: "x", Limit: 20})
	if !IsStatus(err, http.StatusServiceUnavailable) || calls.Load() != 1 {
		t.Errorf("err = %v, calls = %d", err, calls.Load())
	}
}

func TestMessagesQueryAndMoreHeader(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if r.URL.Path != "/api/v3/channels/c1/messages" || q.Get("limit") != "30" || q.Get("order") != "asc" ||
			q.Get("since") != "2026-01-02T03:04:05Z" || q.Get("until") != "" {
			t.Errorf("request %s?%s", r.URL.Path, r.URL.RawQuery)
		}
		w.Header().Set("X-TRAQ-MORE", "true")
		_, _ = w.Write([]byte(`[]`))
	}))
	defer srv.Close()
	c, _ := NewClient(srv.URL, srv.Client(), "t", "test")
	since := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	_, more, err := c.Messages(context.Background(), "c1", MessagesQuery{Limit: 30, Since: &since, Order: "asc"})
	if err != nil || !more {
		t.Errorf("more = %v, err = %v", more, err)
	}
}

func TestPostMessageSendsEmbed(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		if r.Method != http.MethodPost || body["content"] != "hello" || body["embed"] != true {
			t.Errorf("%s body = %v", r.Method, body)
		}
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"id":"m2","userId":"u1","channelId":"c1","content":"hello","createdAt":"2026-01-02T03:04:05Z","updatedAt":"2026-01-02T03:04:05Z","stamps":[]}`))
	}))
	defer srv.Close()
	c, _ := NewClient(srv.URL, srv.Client(), "t", "test")
	m, err := c.PostMessage(context.Background(), "c1", "hello")
	if err != nil || m.ID != "m2" {
		t.Errorf("PostMessage = %+v, %v", m, err)
	}
}

func TestTrailingSlashServerURL(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v3/users/me" {
			t.Errorf("path = %s", r.URL.Path)
		}
		_, _ = w.Write([]byte(`{"id":"me"}`))
	}))
	defer srv.Close()
	c, err := NewClient(srv.URL+"/", srv.Client(), "t", "test")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Me(context.Background()); err != nil {
		t.Fatalf("Me: %v", err)
	}
	if got := c.ServerURL().String(); got != srv.URL {
		t.Errorf("ServerURL = %q, want %q", got, srv.URL)
	}
}

func TestNewClientRejectsBadURL(t *testing.T) {
	for _, u := range []string{"", "q.trap.jp", "/relative"} {
		if _, err := NewClient(u, nil, "t", "test"); err == nil {
			t.Errorf("NewClient(%q) succeeded", u)
		}
	}
}

func TestSearchQueryEncoding(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if r.URL.Path != "/api/v3/messages" || q.Get("word") != `"a b" +日本語&x=1` || q.Get("in") != "c1" ||
			strings.Join(q["from"], ",") != "u1,u2" || q.Get("limit") != "20" || q.Get("offset") != "40" {
			t.Errorf("request %s?%s", r.URL.Path, r.URL.RawQuery)
		}
		_, _ = w.Write([]byte(`{"totalHits":1,"hits":[{"id":"m1"}]}`))
	}))
	defer srv.Close()
	c, _ := NewClient(srv.URL, srv.Client(), "t", "test")
	res, err := c.Search(context.Background(), SearchQuery{Word: `"a b" +日本語&x=1`, In: "c1", From: []string{"u1", "u2"}, Limit: 20, Offset: 40})
	if err != nil || res.TotalHits != 1 || len(res.Hits) != 1 {
		t.Errorf("Search = %+v, %v", res, err)
	}
}

func TestRetriesAreBounded(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Retry-After", "0")
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer srv.Close()
	c, _ := NewClient(srv.URL, srv.Client(), "t", "test")
	_, err := c.Stamps(context.Background())
	if !IsStatus(err, http.StatusBadGateway) || calls.Load() != 4 {
		t.Errorf("err = %v, calls = %d, want 502 after 4 calls (1 + 3 retries)", err, calls.Load())
	}
}

func TestRetryWaitStopsOnContextCancel(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Retry-After", "3600")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()
	c, _ := NewClient(srv.URL, srv.Client(), "t", "test")
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := c.Stamps(ctx)
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(start) > 5*time.Second || calls.Load() != 1 {
		t.Errorf("err = %v, elapsed = %v, calls = %d", err, time.Since(start), calls.Load())
	}
}

func TestHTTPErrorFallsBackToTruncatedBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(strings.Repeat("x", 1000)))
	}))
	defer srv.Close()
	c, _ := NewClient(srv.URL, srv.Client(), "t", "test")
	_, err := c.Channel(context.Background(), "c1")
	var he *HTTPError
	if !errors.As(err, &he) || he.Status != http.StatusNotFound || len(he.Message) != 200 {
		t.Errorf("err = %v", err)
	}
}

func TestDownloadFileStreamsBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v3/files/f1" {
			t.Errorf("path = %s", r.URL.Path)
		}
		_, _ = w.Write([]byte("filedata"))
	}))
	defer srv.Close()
	c, _ := NewClient(srv.URL, srv.Client(), "t", "test")
	rc, err := c.DownloadFile(context.Background(), "f1")
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	buf := make([]byte, 16)
	n, _ := rc.Read(buf)
	if string(buf[:n]) != "filedata" {
		t.Errorf("body = %q", buf[:n])
	}
}
