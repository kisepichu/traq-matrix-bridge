package connector

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/status"
	"maunium.net/go/mautrix/event"

	"github.com/kisepichu/traq-matrix-bridge/pkg/traq"
)

const userAgent = "traq-matrix-bridge"

// httpTimeout bounds every REST and OAuth request, so that a hung traQ cannot block a login or logout.
const httpTimeout = 30 * time.Second

var httpClient = &http.Client{Timeout: httpTimeout}

// Bridge state error codes.
const (
	errCodeNoToken        status.BridgeStateErrorCode = "traq-no-token"
	errCodeTokenExpired   status.BridgeStateErrorCode = "traq-token-expired"
	errCodeTokenInvalid   status.BridgeStateErrorCode = "traq-token-invalid"
	errCodeConnectFail    status.BridgeStateErrorCode = "traq-connect-failed"
	errCodeWSDisconnected status.BridgeStateErrorCode = "traq-websocket-disconnected"
)

// Timing of the connection.
const (
	pingInterval = 30 * time.Second
	pingTimeout  = 10 * time.Second
	minBackoff   = time.Second
	maxBackoff   = 5 * time.Minute
)

var errNotImplemented = errors.New("traq: not implemented yet")

// errTokenRejected is the cancel cause of a session whose token traQ rejected (HTTP 401).
var errTokenRejected = errors.New("traq: access token rejected")

// TraQClient is the per-login client stored in bridgev2.UserLogin.Client.
type TraQClient struct {
	Main  *TraQConnector
	Login *bridgev2.UserLogin
	API   *traq.Client // nil when the stored token is missing
	Files *traq.Client // file downloads, without httpTimeout (see fileHTTPClient); nil with API
	users *userCache

	// syncMu serializes the changes of channels and targets that are based on a fresh read from traQ, so an
	// older read cannot overwrite a newer one.
	syncMu sync.Mutex

	stateMu       sync.RWMutex
	me            *traq.Me               // nil until Connect has loaded it
	channels      *channelTree           // nil until Connect has loaded it
	targets       map[string]channelInfo // channels that have a portal, by ID: the last subscription snapshot
	lastMessageAt time.Time              // CreatedAt of the newest message seen on the WebSocket

	runMu   sync.Mutex
	running *session // the last session started by Connect; nil before the first Connect
}

// session is one run of Connect, stopped by Disconnect.
type session struct {
	cancel context.CancelFunc
	done   chan struct{} // closed when every goroutine of the session has returned
}

var _ bridgev2.NetworkAPI = (*TraQClient)(nil)

func (c *TraQConnector) oauthConfig() traq.OAuthConfig {
	return traq.OAuthConfig{
		ServerURL:   c.Config.ServerURL,
		ClientID:    c.Config.OAuth.ClientID,
		RedirectURI: c.Config.OAuth.RedirectURI,
		HTTPClient:  httpClient,
	}
}

func (c *TraQClient) metadata() *UserLoginMetadata {
	meta, _ := c.Login.Metadata.(*UserLoginMetadata)
	if meta == nil {
		return &UserLoginMetadata{}
	}
	return meta
}

// IsLoggedIn reports whether a token is stored and has not expired. It does no IO, so a token that traQ
// has revoked is still reported as logged in until Connect sees a 401.
func (c *TraQClient) IsLoggedIn() bool {
	if c.API == nil {
		return false
	}
	exp := c.metadata().ExpiresAt
	return exp.IsZero() || time.Now().Before(exp)
}

// Connect checks the stored token and starts a session in the background: it loads the user, the channels
// and the subscriptions, syncs the portals, then keeps the WebSocket open and re-syncs the subscriptions
// periodically until Disconnect. bridgev2 calls Connect for each login in turn at startup, so it must not block.
func (c *TraQClient) Connect(ctx context.Context) {
	if c.API == nil {
		c.sendBadCredentials(errCodeNoToken, "No traQ access token is stored, please log in again")
		return
	}
	if !c.IsLoggedIn() {
		c.sendBadCredentials(errCodeTokenExpired, "The traQ access token has expired, please log in again")
		return
	}
	c.runMu.Lock()
	defer c.runMu.Unlock()
	if s := c.running; s != nil {
		select {
		case <-s.done:
		default:
			c.Login.Log.Warn().Msg("Ignoring Connect: already connected")
			return
		}
	}
	// The session outlives the caller's context (a login request, for example); Disconnect stops it.
	runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	s := &session{cancel: cancel, done: make(chan struct{})}
	c.running = s
	c.Login.BridgeState.Send(status.BridgeState{StateEvent: status.StateConnecting})
	go func() {
		defer close(s.done)
		defer cancel()
		c.run(runCtx)
	}()
}

// run is the body of a session. It returns when ctx ends or the token is rejected, after every goroutine it
// started has returned. BAD_CREDENTIALS for a rejected token is sent only here, after all loops have stopped,
// so no TRANSIENT_DISCONNECT of the same session can follow it.
func (c *TraQClient) run(ctx context.Context) {
	ctx, stop := context.WithCancelCause(ctx)
	c.runLoops(ctx, stop)
	stop(nil)
	if errors.Is(context.Cause(ctx), errTokenRejected) {
		c.sendBadCredentials(errCodeTokenInvalid, "traQ rejected the access token, please log in again")
	}
}

// runLoops loads, syncs and runs the subscription sync and WebSocket loops until ctx ends. A loop that sees
// the token rejected cancels ctx through stop with errTokenRejected.
func (c *TraQClient) runLoops(ctx context.Context, stop context.CancelCauseFunc) {
	var wg sync.WaitGroup
	defer wg.Wait()
	defer stop(nil)

	me, targets, unread, ok := c.load(ctx, stop)
	if !ok {
		return
	}
	c.initialSync(ctx, me, targets, unread)
	// One queue and one worker per session, so events are handled in the order traQ sent them, across
	// reconnects too.
	queue := make(chan traq.Event, eventQueueSize)
	// Size 1: the channel events of a burst add up to at most one pending sync.
	syncRequests := make(chan struct{}, 1)
	wg.Go(func() { c.eventWorker(ctx, queue, syncRequests) })
	wg.Go(func() { c.subscriptionSyncLoop(ctx, stop, syncRequests) })
	c.websocketLoop(ctx, stop, queue)
}

// load reads what the bridge needs from traQ, retrying with backoff. The bool is false when ctx ended or the
// token was rejected.
func (c *TraQClient) load(ctx context.Context, stop context.CancelCauseFunc) (*traq.Me, map[string]channelInfo, []traq.UnreadChannel, bool) {
	var retry backoff
	for {
		me, targets, unread, err := c.loadOnce(ctx)
		if err == nil {
			return me, targets, unread, true
		}
		if !c.handleConnectError(ctx, stop, err, errCodeConnectFail, "Failed to load data from traQ") || !retry.wait(ctx, c.Login.Log) {
			return nil, nil, nil, false
		}
	}
}

func (c *TraQClient) loadOnce(ctx context.Context) (*traq.Me, map[string]channelInfo, []traq.UnreadChannel, error) {
	me, err := c.API.Me(ctx)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("get me: %w", err)
	}
	users, err := c.API.Users(ctx)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("get users: %w", err)
	}
	list, err := c.API.Channels(ctx)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("get channels: %w", err)
	}
	subs, err := c.API.Subscriptions(ctx)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("get subscriptions: %w", err)
	}
	unread, err := c.API.Unread(ctx)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("get unread channels: %w", err)
	}
	if err := c.Main.loadStamps(ctx, c.API); err != nil {
		return nil, nil, nil, fmt.Errorf("get stamps: %w", err)
	}
	tree := buildChannelTree(list)
	targets := subscriptionSnapshot(tree, subs)
	c.users.setAll(users)
	c.stateMu.Lock()
	c.me, c.channels, c.targets = me, tree, targets
	c.stateMu.Unlock()
	return me, targets, unread, nil
}

// handleConnectError reports a failed attempt to reach traQ and tells whether to retry: not when ctx ended
// (Disconnect), and not when traQ rejected the token, which stops the session with errTokenRejected (run then
// sends BAD_CREDENTIALS); otherwise it sends TRANSIENT_DISCONNECT.
func (c *TraQClient) handleConnectError(ctx context.Context, stop context.CancelCauseFunc, err error, code status.BridgeStateErrorCode, message string) bool {
	if ctx.Err() != nil {
		return false
	}
	if traq.IsStatus(err, http.StatusUnauthorized) {
		stop(errTokenRejected)
		return false
	}
	c.Login.Log.Warn().Err(err).Str("error_code", string(code)).Msg(message)
	c.Login.BridgeState.Send(status.BridgeState{
		StateEvent: status.StateTransientDisconnect,
		Error:      code,
		Message:    message,
	})
	return true
}

// backoff doubles the delay between attempts from minBackoff up to maxBackoff.
type backoff struct{ next time.Duration }

// wait sleeps for the next delay. It returns false if ctx ended first.
func (b *backoff) wait(ctx context.Context, log zerolog.Logger) bool {
	d := max(b.next, minBackoff)
	b.next = min(2*d, maxBackoff)
	log.Info().Stringer("delay", d).Msg("Retrying the connection to traQ")
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func (b *backoff) reset() { b.next = 0 }

// websocketLoop keeps the WebSocket open, reconnecting with backoff, until ctx ends or the token is rejected.
// Events go to queue.
func (c *TraQClient) websocketLoop(ctx context.Context, stop context.CancelCauseFunc, queue chan<- traq.Event) {
	var retry backoff
	for {
		ws, err := c.dialWebSocket(ctx)
		if err == nil {
			c.Login.Log.Info().Msg("Connected to the traQ WebSocket")
			c.Login.BridgeState.Send(status.BridgeState{StateEvent: status.StateConnected})
			// A catch-up of what was missed while disconnected belongs here, for every connection but the first
			// (initialSync covers the time before Connect).
			start := time.Now()
			err = c.readWebSocket(ctx, ws, queue)
			_ = ws.Close()
			// Only a connection that stayed up resets the backoff, so a server that drops every connection
			// right away is not hammered.
			if time.Since(start) > pingInterval {
				retry.reset()
			}
		}
		if !c.handleConnectError(ctx, stop, err, errCodeWSDisconnected, "Lost the connection to traQ, reconnecting") || !retry.wait(ctx, c.Login.Log) {
			return
		}
	}
}

// dialWebSocket opens the WebSocket and turns on timeline streaming. ctx must live as long as the connection:
// its end closes the connection (see traq.WSConn).
func (c *TraQClient) dialWebSocket(ctx context.Context) (*traq.WSConn, error) {
	ws, err := traq.DialWS(ctx, c.Main.Config.ServerURL, c.metadata().AccessToken, httpClient)
	if err != nil {
		return nil, fmt.Errorf("dial WebSocket: %w", err)
	}
	if err := ws.SetTimelineStreaming(ctx, true); err != nil {
		_ = ws.Close()
		return nil, fmt.Errorf("turn on timeline streaming: %w", err)
	}
	return ws, nil
}

// readWebSocket reads events and hands them to queue until the connection fails, a ping fails or ctx ends.
func (c *TraQClient) readWebSocket(ctx context.Context, ws *traq.WSConn, queue chan<- traq.Event) error {
	// One context for the whole connection: its end closes the connection and unblocks ReadEvent.
	connCtx, cancel := context.WithCancelCause(ctx)
	var wg sync.WaitGroup
	defer wg.Wait()
	defer cancel(nil)
	wg.Go(func() { keepAlive(connCtx, ws, cancel) })
	for {
		evt, err := ws.ReadEvent(connCtx)
		if err != nil {
			if cause := context.Cause(connCtx); cause != nil {
				return cause
			}
			return fmt.Errorf("read WebSocket: %w", err)
		}
		c.enqueueEvent(connCtx, queue, evt)
	}
}

// keepAlive pings every pingInterval. A failed ping cancels ctx with the error as the cause, which closes the
// connection and ends the reader. The pong is only processed while the reader is inside ReadEvent.
func keepAlive(ctx context.Context, ws *traq.WSConn, fail context.CancelCauseFunc) {
	t := time.NewTicker(pingInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		pingCtx, cancel := context.WithTimeout(ctx, pingTimeout)
		err := ws.Ping(pingCtx)
		cancel()
		if err != nil {
			fail(fmt.Errorf("ping traQ WebSocket: %w", err))
			return
		}
	}
}

// subscriptionSyncLoop re-syncs the subscriptions every subscription_sync_interval and when the event worker
// asks for it through requests (a channel was changed or deleted). A rejected token stops the session with
// errTokenRejected.
func (c *TraQClient) subscriptionSyncLoop(ctx context.Context, stop context.CancelCauseFunc, requests <-chan struct{}) {
	t := time.NewTicker(c.Main.Config.SubscriptionSyncInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		case <-requests:
		}
		err := c.syncSubscriptions(ctx)
		switch {
		case err == nil || ctx.Err() != nil:
		case traq.IsStatus(err, http.StatusUnauthorized):
			stop(errTokenRejected)
			return
		default:
			c.Login.Log.Warn().Err(err).Msg("Failed to sync subscriptions")
		}
	}
}

func (c *TraQClient) sendBadCredentials(code status.BridgeStateErrorCode, message string) {
	c.Login.Log.Warn().Str("error_code", string(code)).Msg(message)
	c.Login.BridgeState.Send(status.BridgeState{
		StateEvent: status.StateBadCredentials,
		Error:      code,
		Message:    message,
		UserAction: status.UserActionRelogin,
	})
}

// Disconnect stops the session started by Connect and waits until all of its goroutines have returned, which
// also closes the WebSocket. It may be called any number of times, before Connect, and while Connect is
// still retrying.
func (c *TraQClient) Disconnect() {
	c.runMu.Lock()
	s := c.running
	c.runMu.Unlock()
	if s == nil {
		return
	}
	s.cancel()
	<-s.done
}

// LogoutRemote revokes the access token. A failure is logged and otherwise ignored: the login is being
// deleted either way, and traQ expires the token on its own.
func (c *TraQClient) LogoutRemote(ctx context.Context) {
	if token := c.metadata().AccessToken; token != "" {
		if err := c.Main.oauthConfig().Revoke(ctx, token); err != nil {
			c.Login.Log.Warn().Err(err).Msg("Failed to revoke the traQ access token")
		}
	}
	c.Disconnect()
}

func (c *TraQClient) IsThisUser(_ context.Context, userID networkid.UserID) bool {
	return TraQUserID(userID) == string(c.Login.ID)
}

func (c *TraQClient) GetCapabilities(context.Context, *bridgev2.Portal) *event.RoomFeatures {
	return &event.RoomFeatures{}
}

func (c *TraQClient) HandleMatrixMessage(context.Context, *bridgev2.MatrixMessage) (*bridgev2.MatrixMessageResponse, error) {
	return nil, fmt.Errorf("HandleMatrixMessage: %w", errNotImplemented)
}
