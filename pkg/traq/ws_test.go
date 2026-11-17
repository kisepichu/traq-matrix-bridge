package traq_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/kisepichu/traq-matrix-bridge/pkg/traq"
)

// wsServer starts an httptest server whose /api/v3/ws endpoint upgrades the request and passes the connection
// to handle. The handshake request is sent on the returned channel.
func wsServer(t *testing.T, handle func(ctx context.Context, c *websocket.Conn)) (*httptest.Server, <-chan *http.Request) {
	t.Helper()
	reqs := make(chan *http.Request, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqs <- r
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.CloseNow()
		handle(r.Context(), c)
	}))
	t.Cleanup(srv.Close)
	return srv, reqs
}

func dialTest(t *testing.T, srv *httptest.Server) *traq.WSConn {
	t.Helper()
	c, err := traq.DialWS(t.Context(), srv.URL, "tok", nil)
	if err != nil {
		t.Fatalf("DialWS: %v", err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func TestDialSendsBearerToken(t *testing.T) {
	srv, reqs := wsServer(t, func(ctx context.Context, c *websocket.Conn) {
		c.Read(ctx) // hold the connection open until the client closes it
	})
	dialTest(t, srv)
	r := <-reqs
	if got := r.Header.Get("Authorization"); got != "Bearer tok" {
		t.Errorf("Authorization = %q, want %q", got, "Bearer tok")
	}
	if r.URL.Path != "/api/v3/ws" {
		t.Errorf("path = %q, want /api/v3/ws", r.URL.Path)
	}
}

func TestDial401(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"message":"unauthorized"}`, http.StatusUnauthorized)
	}))
	t.Cleanup(srv.Close)
	_, err := traq.DialWS(t.Context(), srv.URL, "secret-token", nil)
	if !traq.IsStatus(err, http.StatusUnauthorized) {
		t.Fatalf("err = %v, want a 401 *HTTPError", err)
	}
	if strings.Contains(err.Error(), "secret-token") {
		t.Errorf("error leaks the token: %v", err)
	}
}

func TestDialRejectsBadURL(t *testing.T) {
	if _, err := traq.DialWS(t.Context(), "ftp://example.com", "tok", nil); err == nil {
		t.Error("DialWS accepted an ftp:// URL")
	}
}

func TestCommands(t *testing.T) {
	got := make(chan string, 3)
	srv, _ := wsServer(t, func(ctx context.Context, c *websocket.Conn) {
		for range 3 {
			_, data, err := c.Read(ctx)
			if err != nil {
				return
			}
			got <- string(data)
		}
	})
	c := dialTest(t, srv)
	ctx := t.Context()
	if err := c.SetTimelineStreaming(ctx, true); err != nil {
		t.Fatal(err)
	}
	if err := c.SetViewState(ctx, "c1"); err != nil {
		t.Fatal(err)
	}
	if err := c.SetViewState(ctx, ""); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"timeline_streaming:on", "viewstate:c1:monitoring", "viewstate:null"} {
		select {
		case s := <-got:
			if s != want {
				t.Errorf("server received %q, want %q", s, want)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("server did not receive %q", want)
		}
	}
}

func TestSetTimelineStreamingOff(t *testing.T) {
	got := make(chan string, 1)
	srv, _ := wsServer(t, func(ctx context.Context, c *websocket.Conn) {
		_, data, err := c.Read(ctx)
		if err == nil {
			got <- string(data)
		}
	})
	c := dialTest(t, srv)
	if err := c.SetTimelineStreaming(t.Context(), false); err != nil {
		t.Fatal(err)
	}
	select {
	case s := <-got:
		if s != "timeline_streaming:off" {
			t.Errorf("server received %q", s)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server received nothing")
	}
}

func TestReadEventSkipsGarbage(t *testing.T) {
	srv, _ := wsServer(t, func(ctx context.Context, c *websocket.Conn) {
		c.Write(ctx, websocket.MessageText, []byte("not json"))
		c.Write(ctx, websocket.MessageBinary, []byte(`{"type":"BINARY","body":{}}`))
		c.Write(ctx, websocket.MessageText, []byte(`{"type":"MESSAGE_STAMPED","body":{"message_id":"m","user_id":"u","stamp_id":"s","count":2,"created_at":"2026-01-02T03:04:05Z"}}`))
		c.Read(ctx)
	})
	c := dialTest(t, srv)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	ev, err := c.ReadEvent(ctx)
	if err != nil {
		t.Fatalf("ReadEvent: %v", err)
	}
	if ev.Type != "MESSAGE_STAMPED" {
		t.Fatalf("Type = %q, want MESSAGE_STAMPED", ev.Type)
	}
	var body traq.MessageStampedEvent
	if err := ev.Decode(&body); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	want := traq.MessageStampedEvent{MessageID: "m", UserID: "u", StampID: "s", Count: 2, CreatedAt: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)}
	if !body.CreatedAt.Equal(want.CreatedAt) {
		t.Errorf("CreatedAt = %v, want %v", body.CreatedAt, want.CreatedAt)
	}
	body.CreatedAt = want.CreatedAt
	if body != want {
		t.Errorf("body = %+v, want %+v", body, want)
	}
}

func TestReadEventContextCancel(t *testing.T) {
	srv, _ := wsServer(t, func(ctx context.Context, c *websocket.Conn) {
		c.Read(ctx)
	})
	c := dialTest(t, srv)
	ctx, cancel := context.WithCancel(t.Context())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	_, err := c.ReadEvent(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want it to wrap context.Canceled", err)
	}
}

func TestPing(t *testing.T) {
	srv, _ := wsServer(t, func(ctx context.Context, c *websocket.Conn) {
		c.Read(ctx) // the read loop answers pings
	})
	c := dialTest(t, srv)
	// Ping waits for the pong, which is only processed while a reader is active.
	go c.ReadEvent(t.Context())
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := c.Ping(ctx); err != nil {
		t.Fatalf("Ping: %v", err)
	}
}
