package connector

import (
	"context"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/simplevent"

	"github.com/kisepichu/traq-matrix-bridge/pkg/traq"
)

// ownReceiptEchoWindow is how long after a read receipt bridged from traQ is sent to Matrix a Matrix receipt
// on its target (or an older message) is taken for its echo. The homeserver sends the echo within seconds.
const ownReceiptEchoWindow = 2 * time.Minute

// ownReceipt returns the read receipt of the logged-in user, bridged from traQ, up to readUpTo in the portal
// of key.
//
// bridgev2 posts it through the double puppet with a marker in the receipt's extra content, and drops the
// receipt that the homeserver echoes back if the marker is still there. Synapse drops it, so the echo would
// reach HandleMatrixReadReceipt as user activity. Therefore the target is resolved here, in the portal's event
// loop right before bridgev2 handles the receipt, with the query bridgev2 would use (the newest bridged message
// at or before readUpTo, so messages bridged before the receipt are included), and remembered in c.ownReceipts.
func (c *TraQClient) ownReceipt(key networkid.PortalKey, me *traq.Me, logContext func(zerolog.Context) zerolog.Context, readUpTo time.Time) *simplevent.Receipt {
	evt := &simplevent.Receipt{
		EventMeta: simplevent.EventMeta{
			Type:       bridgev2.RemoteEventReadReceipt,
			LogContext: logContext,
			PortalKey:  key,
			Sender:     c.selfSender(me),
		},
		ReadUpTo: readUpTo,
	}
	evt.PreHandleFunc = func(ctx context.Context, portal *bridgev2.Portal) {
		target, err := portal.Bridge.DB.Message.GetLastNonFakePartAtOrBeforeTime(ctx, portal.PortalKey, readUpTo)
		if err != nil {
			// bridgev2 resolves the target itself; only the echo goes unrecognized.
			zerolog.Ctx(ctx).Err(err).Msg("Failed to get the target message of the own read receipt")
			return
		}
		if target == nil {
			return // bridgev2 finds no target either and ignores the receipt
		}
		evt.LastTarget = target.ID
		c.ownReceipts.sent(portal.PortalKey, target)
	}
	return evt
}

// ownReceipts remembers, per portal, the target of the last read receipt bridged from traQ, to recognize its
// echo (see ownReceipt). It holds at most one entry per portal. The zero value is ready to use; it is safe for
// concurrent use.
type ownReceipts struct {
	now func() time.Time // nil: time.Now

	mu       sync.Mutex
	byPortal map[networkid.PortalKey]ownReceipt
}

type ownReceipt struct {
	target time.Time // the timestamp of the target message
	sentAt time.Time
}

func (r *ownReceipts) clock() time.Time {
	if r.now == nil {
		return time.Now()
	}
	return r.now()
}

// sent records that a read receipt on target is being sent to Matrix in the portal of key.
func (r *ownReceipts) sent(key networkid.PortalKey, target *database.Message) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.byPortal == nil {
		r.byPortal = make(map[networkid.PortalKey]ownReceipt)
	}
	r.byPortal[key] = ownReceipt{target: target.Timestamp, sentAt: r.clock()}
}

// isEcho reports whether a Matrix read receipt of the logged-in user on exact (nil if the event is no bridged
// message) in the portal of key is the echo of the last receipt bridged from traQ there: exact is its target or
// older, and the receipt was sent within ownReceiptEchoWindow. A receipt on a newer message is real activity.
// An expired entry is removed.
func (r *ownReceipts) isEcho(key networkid.PortalKey, exact *database.Message) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	own, ok := r.byPortal[key]
	if !ok {
		return false
	}
	if r.clock().Sub(own.sentAt) >= ownReceiptEchoWindow {
		delete(r.byPortal, key)
		return false
	}
	return exact != nil && !exact.Timestamp.After(own.target)
}
