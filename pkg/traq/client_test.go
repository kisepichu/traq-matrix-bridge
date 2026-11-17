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

func TestMessagesMoreHeaderFailsClosed(t *testing.T) {
	for _, tc := range []struct {
		name, header string
		want         bool
	}{
		{"true", "true", true},
		{"false", "false", false},
		{"missing", "", true},
		{"unparseable", "no", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tc.header != "" {
					w.Header().Set("X-TRAQ-MORE", tc.header)
				}
				_, _ = w.Write([]byte(`[]`))
			}))
			defer srv.Close()
			c, _ := NewClient(srv.URL, srv.Client(), "t", "test")
			_, more, err := c.Messages(context.Background(), "c1", MessagesQuery{Limit: 30})
			if err != nil || more != tc.want {
				t.Errorf("more = %v, want %v, err = %v", more, tc.want, err)
			}
		})
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
	_, err := c.User(context.Background(), "u1")
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

func TestRetryPolicyByMethod(t *testing.T) {
	const okMessage = `{"id":"m2","userId":"u1","channelId":"c1","content":"hi","createdAt":"2026-01-02T03:04:05Z","updatedAt":"2026-01-02T03:04:05Z","stamps":[]}`
	tests := []struct {
		name      string
		first     int // status of the first response; later responses are success
		call      func(c *Client) error
		wantCalls int32
		wantErr   int // expected HTTPError status, 0 for success
	}{
		{"POST 502 is not retried", http.StatusBadGateway, func(c *Client) error {
			_, err := c.PostMessage(context.Background(), "c1", "hi")
			return err
		}, 1, http.StatusBadGateway},
		{"POST 504 is not retried", http.StatusGatewayTimeout, func(c *Client) error {
			return c.AddStamp(context.Background(), "m1", "s1")
		}, 1, http.StatusGatewayTimeout},
		{"GET 502 is retried", http.StatusBadGateway, func(c *Client) error {
			_, err := c.Stamps(context.Background())
			return err
		}, 2, 0},
		{"DELETE 504 is retried", http.StatusGatewayTimeout, func(c *Client) error {
			return c.DeleteMessage(context.Background(), "m1")
		}, 2, 0},
		{"POST 429 is retried", http.StatusTooManyRequests, func(c *Client) error {
			_, err := c.PostMessage(context.Background(), "c1", "hi")
			return err
		}, 2, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var calls atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if calls.Add(1) == 1 {
					w.Header().Set("Retry-After", "0")
					w.WriteHeader(tt.first)
					return
				}
				if r.Method == http.MethodPost {
					w.WriteHeader(http.StatusCreated)
				}
				if r.URL.Path == "/api/v3/stamps" {
					_, _ = w.Write([]byte(`[]`))
					return
				}
				_, _ = w.Write([]byte(okMessage))
			}))
			defer srv.Close()
			c, _ := NewClient(srv.URL, srv.Client(), "t", "test")
			err := tt.call(c)
			if calls.Load() != tt.wantCalls {
				t.Errorf("calls = %d, want %d", calls.Load(), tt.wantCalls)
			}
			if tt.wantErr == 0 && err != nil {
				t.Errorf("err = %v", err)
			}
			var he *HTTPError
			if tt.wantErr != 0 && (!errors.As(err, &he) || he.Status != tt.wantErr) {
				t.Errorf("err = %v, want HTTPError %d", err, tt.wantErr)
			}
		})
	}
}

func TestHugeRetryAfterIsClamped(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Retry-After", "99999999999")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()
	c, _ := NewClient(srv.URL, srv.Client(), "t", "test")
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := c.Stamps(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("err = %v, want context deadline exceeded", err)
	}
	if calls.Load() != 1 {
		t.Errorf("calls = %d, want 1 (a huge Retry-After must wait, not retry at once)", calls.Load())
	}
	if time.Since(start) > 5*time.Second {
		t.Errorf("call blocked for %v", time.Since(start))
	}
}

func TestRetryDelayClamp(t *testing.T) {
	for _, tt := range []struct {
		header string
		want   time.Duration
	}{
		{"5", 5 * time.Second},
		{"60", 60 * time.Second},
		{"61", 60 * time.Second},
		{"99999999999", 60 * time.Second},
		{"99999999999999999999999999", 60 * time.Second},
		{"-1", 2 * time.Second},
		{"soon", 2 * time.Second},
		{"", 2 * time.Second},
	} {
		resp := &http.Response{Header: http.Header{}}
		if tt.header != "" {
			resp.Header.Set("Retry-After", tt.header)
		}
		if got := retryDelay(resp, 1); got != tt.want {
			t.Errorf("Retry-After %q: delay = %v, want %v", tt.header, got, tt.want)
		}
	}
}

func TestDownloadFileDoesNotRequestJSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Accept"); strings.Contains(got, "application/json") {
			t.Errorf("Accept = %q on a binary endpoint", got)
		}
		_, _ = w.Write([]byte("bytes"))
	}))
	defer srv.Close()
	c, _ := NewClient(srv.URL, srv.Client(), "t", "test")
	body, err := c.DownloadFile(context.Background(), "f1")
	if err != nil {
		t.Fatal(err)
	}
	body.Close()
	if _, _, err := c.PublicIcon(context.Background(), "kise"); err != nil {
		t.Fatal(err)
	}
}
