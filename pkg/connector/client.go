package connector

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/status"

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

// errWSNotConnected: there is no WebSocket connection to send a command on.
var errWSNotConnected = errors.New("the traQ WebSocket is not connected")

// viewStateSendWait bounds how long a caller waits for a viewstate write (see sendViewState).
const viewStateSendWait = 5 * time.Second

// eventSyncSpacing is the least time between the start of a subscription sync and that of a sync triggered by
// channel events (see subscriptionSyncLoop).
const eventSyncSpacing = 30 * time.Second

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
	// heldRemovals is the channels whose removal the last subscription sync held back (see holdRemovals). It is
	// written by syncSubscriptions under syncMu, and reset by loadOnce before the session's loops start.
	heldRemovals []string

	stateMu  sync.RWMutex
	me       *traq.Me               // nil until Connect has loaded it
	channels *channelTree           // nil until Connect has loaded it
	targets  map[string]channelInfo // channels that have a portal, by ID: the last subscription snapshot
	// lastActivity is the CreatedAt of the newest message known in each channel that has a portal, by
	// channel ID: the poller re-reads the recently active ones. Connect fills it from the database.
	lastActivity map[string]time.Time

	runMu   sync.Mutex
	running *session // the last session started by Connect; nil before the first Connect

	view *viewState // the channel traQ is told the user is viewing
	// resyncs paces the portal resyncs of the running session; nil between sessions.
	resyncs atomic.Pointer[resyncPacer]

	wsMu  sync.Mutex
	ws    *traq.WSConn    // the open WebSocket connection, nil while disconnected
	wsCtx context.Context // the context of ws's connection; nil with ws
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
		defer c.Main.stampClientGone(c)
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

// runLoops loads, syncs and runs the event worker and the subscription sync, viewstate, poll and WebSocket
// loops until ctx ends. A loop that sees the token rejected cancels ctx through stop with errTokenRejected.
func (c *TraQClient) runLoops(ctx context.Context, stop context.CancelCauseFunc) {
	pacer := newResyncPacer(ctx, c.runResync)
	c.resyncs.Store(pacer)
	defer c.resyncs.CompareAndSwap(pacer, nil)
	var wg sync.WaitGroup
	defer wg.Wait()
	defer stop(nil)

	me, targets, unread, ok := c.load(ctx, stop)
	if !ok {
		return
	}
	c.loadLastActivity(ctx, targets)
	c.initialSync(ctx, me, targets, unread)
	// After initialSync, which reads targets (c.targets) without the lock, and which a failed resync changes.
	wg.Go(pacer.loop)
	// One queue and one worker per session, so events are handled in the order traQ sent them, across
	// reconnects too.
	queue := make(chan traq.Event, eventQueueSize)
	// Size 1: the channel events of a burst add up to at most one pending sync.
	syncRequests := make(chan struct{}, 1)
	wg.Go(func() { c.eventWorker(ctx, queue, syncRequests) })
	wg.Go(func() { c.subscriptionSyncLoop(ctx, stop, syncRequests) })
	wg.Go(func() { c.viewStateLoop(ctx) })
	wg.Go(func() { c.pollLoop(ctx, stop) })
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
		if !c.handleConnectError(ctx, stop, err, errCodeConnectFail, "Failed to load data from traQ") || !retry.wait(ctx, c.Login.Log, "Retrying the connection to traQ") {
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
	tree := buildChannelTree(list)
	targets := subscriptionSnapshot(tree, subs)
	c.users.setAll(users)
	c.stateMu.Lock()
	c.me, c.channels, c.targets, c.heldRemovals = me, tree, targets, nil
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

// backoff doubles the delay between attempts from min (minBackoff if zero) up to maxBackoff.
type backoff struct{ next, min time.Duration }

// wait logs msg, which says what is retried, and sleeps for the next delay. It returns false if ctx ended first.
func (b *backoff) wait(ctx context.Context, log zerolog.Logger, msg string) bool {
	d := max(b.next, cmp.Or(b.min, minBackoff))
	b.next = min(2*d, maxBackoff)
	log.Info().Stringer("delay", d).Msg(msg)
	return sleepCtx(ctx, d)
}

// sleepCtx waits for d. It returns false if ctx ended first.
func sleepCtx(ctx context.Context, d time.Duration) bool {
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
// Events go to queue. After a reconnect, the messages missed while disconnected are caught up on before the
// new connection is read (initialSync covers the time before Connect).
func (c *TraQClient) websocketLoop(ctx context.Context, stop context.CancelCauseFunc, queue chan<- traq.Event) {
	var retry backoff
	var lostAt time.Time // when the last connection ended; zero before the first one
	for {
		ws, err := c.dialWebSocket(ctx)
		if err == nil {
			c.Login.Log.Info().Msg("Connected to the traQ WebSocket")
			c.Login.BridgeState.Send(status.BridgeState{StateEvent: status.StateConnected})
			c.Main.stampClientConnected(c)
			if !lostAt.IsZero() {
				c.catchUp(ctx, lostAt.Add(-catchUpMargin))
			}
			// The viewstate is sent again in readWebSocket.
			start := time.Now()
			err = c.readWebSocket(ctx, ws, queue)
			_ = ws.Close()
			lostAt = time.Now()
			// Only a connection that stayed up resets the backoff, so a server that drops every connection
			// right away is not hammered.
			if time.Since(start) > pingInterval {
				retry.reset()
			}
		}
		if !c.handleConnectError(ctx, stop, err, errCodeWSDisconnected, "Lost the connection to traQ, reconnecting") || !retry.wait(ctx, c.Login.Log, "Retrying the connection to traQ") {
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
// While it runs, ws is the connection that viewstate commands are sent on.
func (c *TraQClient) readWebSocket(ctx context.Context, ws *traq.WSConn, queue chan<- traq.Event) error {
	// One context for the whole connection: its end closes the connection and unblocks ReadEvent.
	connCtx, cancel := context.WithCancelCause(ctx)
	var wg sync.WaitGroup
	defer wg.Wait()
	defer cancel(nil)
	wg.Go(func() { keepAlive(connCtx, ws, cancel) })
	c.setWebSocket(ws, connCtx)
	defer c.setWebSocket(nil, nil)
	// A new connection starts without a viewstate.
	c.view.Resend(connCtx)
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

func (c *TraQClient) setWebSocket(ws *traq.WSConn, connCtx context.Context) {
	c.wsMu.Lock()
	defer c.wsMu.Unlock()
	c.ws, c.wsCtx = ws, connCtx
}

// sendViewState is viewState's send function. It fails at once while the WebSocket is down.
//
// The write uses the connection's context, never a per-call timeout, because a context that ends during a
// write closes the connection (see traq.WSConn). A write can block only on a connection that is dead but
// not detected yet; keepAlive notices that within pingInterval + pingTimeout and ends the connection's
// context, which ends the write. The caller, often a Matrix handler in a portal's event loop, waits for at
// most viewStateSendWait per send (or until ctx ends) and gets an error; the write itself goes on in the
// background and may still succeed. viewState holds its lock during a send, so concurrent handlers on such
// a dead connection wait in turn, each up to viewStateSendWait; overall they are bounded by keepAlive's
// detection, after which the connection is unregistered and sends fail at once.
func (c *TraQClient) sendViewState(ctx context.Context, channelID string) error {
	c.wsMu.Lock()
	ws, connCtx := c.ws, c.wsCtx
	c.wsMu.Unlock()
	if ws == nil {
		return errWSNotConnected
	}
	done := make(chan error, 1)
	go func() { done <- ws.SetViewState(connCtx, channelID) }()
	t := time.NewTimer(viewStateSendWait)
	defer t.Stop()
	select {
	case err := <-done:
		return err
	case <-t.C:
		return fmt.Errorf("the traQ WebSocket did not take the viewstate within %s", viewStateSendWait)
	case <-ctx.Done():
		return ctx.Err()
	}
}

// viewStateLoop clears the viewstate after view_idle_timeout without Matrix activity, until ctx ends.
func (c *TraQClient) viewStateLoop(ctx context.Context) {
	t := time.NewTicker(viewStateTickInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			c.view.Tick(ctx)
		}
	}
}

// subscriptionSyncLoop re-syncs the subscriptions every subscription_sync_interval and when the event worker
// asks for it through requests (a channel was changed or deleted). A rejected token stops the session with
// errTokenRejected.
//
// traQ sends the channel events of every public channel, and each sync reads the whole channel list, so a sync
// for requests starts no sooner than eventSyncSpacing after the previous sync started; the requests made
// meanwhile add up to that one sync. A periodic sync due meanwhile runs at once and covers them.
func (c *TraQClient) subscriptionSyncLoop(ctx context.Context, stop context.CancelCauseFunc, requests <-chan struct{}) {
	t := time.NewTicker(c.Main.Config.SubscriptionSyncInterval)
	defer t.Stop()
	var last time.Time // when the last sync started
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		case <-requests:
			if wait := time.Until(last.Add(eventSyncSpacing)); !last.IsZero() && wait > 0 {
				spacing := time.NewTimer(wait)
				select {
				case <-ctx.Done():
					spacing.Stop()
					return
				case <-t.C:
				case <-spacing.C:
				}
				spacing.Stop()
			}
		}
		// The sync reads traQ after every change requested so far, so it covers a request still pending.
		select {
		case <-requests:
		default:
		}
		last = time.Now()
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
