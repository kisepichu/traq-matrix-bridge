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
	wanted       string    // the channel of the last Touch, "" once cleared for idle; current may lag behind
	lastActivity time.Time // time of the last Touch
}

func newViewState(idle time.Duration, send func(ctx context.Context, channelID string) error, now func() time.Time) *viewState {
	return &viewState{idle: idle, send: send, now: now}
}

// Touch records Matrix activity in channelID and makes it the viewed channel if it is not already. A failed
// send is logged and leaves the viewed channel unchanged; the next Touch or Resend tries again.
func (v *viewState) Touch(ctx context.Context, channelID string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.lastActivity = v.now()
	v.wanted = channelID
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
// viewStateTickInterval. The clear is also sent when current is empty but a channel is still wanted: a send
// that failed (for example timed out) may have reached traQ. wanted is reset only once the clear succeeded,
// so a failed clear is tried again on the next Tick.
func (v *viewState) Tick(ctx context.Context) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.activeLocked() {
		return
	}
	if (v.current != "" || v.wanted != "") && v.sendLocked(ctx, "") {
		v.wanted = ""
	}
}

// activeLocked reports whether the last Touch is more recent than the idle timeout.
func (v *viewState) activeLocked() bool {
	return v.now().Sub(v.lastActivity) < v.idle
}

// Resend is called on a new WebSocket connection, which starts without a viewstate. It sends the channel of
// the last Touch, also when its send failed while the connection was down. If the idle timeout has passed
// since the last Touch, nothing is sent and the channel is no longer wanted. If the send fails, nothing is
// current, so the next Touch sends again.
func (v *viewState) Resend(ctx context.Context) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.current = ""
	if v.wanted == "" {
		return
	}
	if !v.activeLocked() {
		v.wanted = ""
		return
	}
	if v.sendLocked(ctx, v.wanted) {
		zerolog.Ctx(ctx).Info().Str("traq_channel_id", v.wanted).Msg("Sent the traQ viewstate again on the new connection")
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
