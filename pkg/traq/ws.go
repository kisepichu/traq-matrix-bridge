package traq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"

	"github.com/coder/websocket"
)

const (
	wsPath      = apiPath + "/ws"
	wsReadLimit = 1 << 20
)

// WSConn is a connection to traQ's event WebSocket. Its methods are safe for concurrent use. Keepalive and
// reconnection are the caller's job.
type WSConn struct {
	conn *websocket.Conn
	mu   sync.Mutex // serializes writes
}

// DialWS connects to <serverURL>/api/v3/ws with "Authorization: Bearer <accessToken>". An https server URL
// is dialed as wss and http as ws. A refused handshake is returned as *HTTPError, so a rejected token shows
// up as IsStatus(err, 401). httpClient may be nil; its Timeout must be 0 because it would also cap the
// lifetime of the connection.
func DialWS(ctx context.Context, serverURL, accessToken string, httpClient *http.Client) (*WSConn, error) {
	origin, err := parseOrigin(serverURL)
	if err != nil {
		return nil, err
	}
	if origin.Scheme == "https" {
		origin.Scheme = "wss"
	} else {
		origin.Scheme = "ws"
	}
	header := http.Header{"Authorization": {"Bearer " + accessToken}}
	conn, resp, err := websocket.Dial(ctx, origin.String()+wsPath, &websocket.DialOptions{
		HTTPClient: httpClient,
		HTTPHeader: header,
	})
	if err != nil {
		if resp != nil && resp.StatusCode != http.StatusSwitchingProtocols {
			return nil, newHTTPError(resp)
		}
		return nil, fmt.Errorf("connect to the traQ WebSocket: %w", err)
	}
	conn.SetReadLimit(wsReadLimit)
	return &WSConn{conn: conn}, nil
}

func (c *WSConn) send(ctx context.Context, command string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.conn.Write(ctx, websocket.MessageText, []byte(command)); err != nil {
		return fmt.Errorf("send %q to the traQ WebSocket: %w", command, err)
	}
	return nil
}

// SetTimelineStreaming sends "timeline_streaming:on" or "timeline_streaming:off". With streaming on, traQ
// delivers events of public channels without a viewstate.
func (c *WSConn) SetTimelineStreaming(ctx context.Context, on bool) error {
	if on {
		return c.send(ctx, "timeline_streaming:on")
	}
	return c.send(ctx, "timeline_streaming:off")
}

// SetViewState sends "viewstate:<channelID>:monitoring", which makes traQ deliver that channel's stamp
// events. An empty channelID sends "viewstate:null" and clears the view state.
func (c *WSConn) SetViewState(ctx context.Context, channelID string) error {
	if channelID == "" {
		return c.send(ctx, "viewstate:null")
	}
	return c.send(ctx, "viewstate:"+channelID+":monitoring")
}

// ReadEvent blocks until the next event arrives. Frames that are not text or not a JSON object are skipped.
// When ctx is done the connection is closed and the error wraps ctx.Err(). Only one goroutine may call
// ReadEvent at a time.
func (c *WSConn) ReadEvent(ctx context.Context) (Event, error) {
	for {
		typ, data, err := c.conn.Read(ctx)
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				err = errors.Join(ctxErr, err)
			}
			return Event{}, fmt.Errorf("read from the traQ WebSocket: %w", err)
		}
		if typ != websocket.MessageText {
			continue
		}
		var ev Event
		if json.Unmarshal(data, &ev) != nil || ev.Type == "" {
			continue
		}
		return ev, nil
	}
}

// Ping sends a WebSocket ping and waits for the pong. The pong is only processed while another goroutine is
// inside ReadEvent.
func (c *WSConn) Ping(ctx context.Context) error {
	if err := c.conn.Ping(ctx); err != nil {
		return fmt.Errorf("ping the traQ WebSocket: %w", err)
	}
	return nil
}

// Close closes the connection. It does not wait for the server's close frame.
func (c *WSConn) Close() error {
	return c.conn.CloseNow()
}
