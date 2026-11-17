package connector

import (
	"context"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

// viewStateTickInterval is how often the idle viewstate is checked.
const viewStateTickInterval = 30 * time.Second

// viewState tracks the traQ channel that the user is reading on Matrix and tells traQ through send, so
// that traQ delivers that channel's stamp events on the WebSocket. traQ has one viewstate per connection,
// so only the channel of the latest Matrix activity is viewed; after idle without activity the viewstate
// is cleared. It is safe for concurrent use.
//
// The lock is held while send runs, so the channel sent last is always the one Current reports; send must
// therefore return in bounded time.
type viewState struct {
	idle time.Duration
	send func(ctx context.Context, channelID string) error
	now  func() time.Time

	mu           sync.Mutex
	current      string    // the channel traQ was last told about, "" if none
	lastActivity time.Time // time of the last Touch
}

func newViewState(idle time.Duration, send func(ctx context.Context, channelID string) error, now func() time.Time) *viewState {
	return &viewState{idle: idle, send: send, now: now}
}

// Touch records Matrix activity in channelID and makes it the viewed channel if it is not already. A failed
// send is logged and leaves the viewed channel unchanged, so the next Touch tries again.
func (v *viewState) Touch(ctx context.Context, channelID string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.lastActivity = v.now()
	if channelID != v.current {
		v.sendLocked(ctx, channelID)
	}
}

// Current returns the viewed channel, "" if none.
func (v *viewState) Current() string {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.current
}

// Tick clears the viewstate once there has been no Touch for the idle timeout. It is called every
// viewStateTickInterval.
func (v *viewState) Tick(ctx context.Context) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.current != "" && v.now().Sub(v.lastActivity) >= v.idle {
		v.sendLocked(ctx, "")
	}
}

// Resend sends the viewed channel again, for a new WebSocket connection, which starts without a viewstate.
func (v *viewState) Resend(ctx context.Context) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.current != "" && v.sendLocked(ctx, v.current) {
		zerolog.Ctx(ctx).Info().Str("traq_channel_id", v.current).Msg("Sent the traQ viewstate again on the new connection")
	}
}

// sendLocked sends channelID and makes it current. A failure is logged and leaves current unchanged.
func (v *viewState) sendLocked(ctx context.Context, channelID string) bool {
	if err := v.send(ctx, channelID); err != nil {
		zerolog.Ctx(ctx).Warn().Err(err).Str("traq_channel_id", channelID).Msg("Failed to send the traQ viewstate")
		return false
	}
	zerolog.Ctx(ctx).Debug().Str("traq_channel_id", channelID).Msg("Sent the traQ viewstate")
	v.current = channelID
	return true
}
