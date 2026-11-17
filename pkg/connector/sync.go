package connector

import (
	"cmp"
	"context"
	"slices"
	"time"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/simplevent"

	"github.com/kisepichu/traq-matrix-bridge/pkg/traq"
)

// portalChange kinds.
const (
	changeUpsert = iota + 1 // create the portal or update its info
	changeRemove            // leave the portal
)

// portalChange is one portal to create/update or remove.
type portalChange struct {
	Channel channelInfo
	Kind    int // changeUpsert, changeRemove
}

// portalDiff computes which portals to create/update/remove from the old and new subscription snapshot.
// A channel is upserted when it is new or its Path, Topic or Level changed, and removed when it is gone or
// its level dropped to 0. DMs keep their portal whatever their level. The result is sorted by channel ID.
func portalDiff(old, new map[string]channelInfo) []portalChange {
	var changes []portalChange
	for id, ch := range new {
		prev, had := old[id]
		switch {
		case ch.isTarget():
			if !had || prev != ch {
				changes = append(changes, portalChange{Channel: ch, Kind: changeUpsert})
			}
		case had && prev.isTarget():
			changes = append(changes, portalChange{Channel: ch, Kind: changeRemove})
		}
	}
	for id, prev := range old {
		if _, ok := new[id]; !ok && prev.isTarget() {
			changes = append(changes, portalChange{Channel: prev, Kind: changeRemove})
		}
	}
	slices.SortFunc(changes, func(a, b portalChange) int { return cmp.Compare(a.Channel.ID, b.Channel.ID) })
	return changes
}

// revertFailedUpserts undoes, in the stored snapshot, the upserts that could not be queued, so that the next
// portalDiff emits them again: a channel that was in old gets its old entry back, a new one is dropped.
func revertFailedUpserts(stored, old map[string]channelInfo, failed []channelInfo) {
	for _, ch := range failed {
		if prev, ok := old[ch.ID]; ok {
			stored[ch.ID] = prev
		} else {
			delete(stored, ch.ID)
		}
	}
}

// revertTargets applies revertFailedUpserts to c.targets.
func (c *TraQClient) revertTargets(old map[string]channelInfo, failed []channelInfo) {
	if len(failed) == 0 {
		return
	}
	c.stateMu.Lock()
	revertFailedUpserts(c.targets, old, failed)
	c.stateMu.Unlock()
}

// subscriptionSnapshot returns the channels that get a portal, by ID: every DM, and every public channel
// subscribed at level >= 1 with that level. Subscriptions of channels missing from the tree are skipped.
func subscriptionSnapshot(tree *channelTree, subs []traq.Subscription) map[string]channelInfo {
	snap := make(map[string]channelInfo, len(subs))
	for _, ch := range tree.byID {
		if ch.isDM() {
			snap[ch.ID] = ch
		}
	}
	for _, sub := range subs {
		ch, ok := tree.Get(sub.ChannelID)
		if !ok || ch.isDM() || sub.Level < 1 {
			continue
		}
		ch.Level = sub.Level
		snap[ch.ID] = ch
	}
	return snap
}

// portalKey returns the portal key of a channel: public channels are shared by all logins, DMs are per login.
func (c *TraQClient) portalKey(ch channelInfo) networkid.PortalKey {
	if ch.isDM() {
		return DMPortalKey(ch.ID, c.Login.ID)
	}
	return PortalKey(ch.ID)
}

// selfSender is the EventSender for events of the logged-in user.
func (c *TraQClient) selfSender(me *traq.Me) bridgev2.EventSender {
	return bridgev2.EventSender{IsFromMe: true, SenderLogin: c.Login.ID, Sender: UserID(me.ID)}
}

func channelLogContext(ch channelInfo) func(zerolog.Context) zerolog.Context {
	return func(c zerolog.Context) zerolog.Context { return c.Str("traq_channel_id", ch.ID) }
}

// queue queues evt and reports whether bridgev2 accepted it.
func (c *TraQClient) queue(evt bridgev2.RemoteEvent) bool {
	res := c.Login.QueueRemoteEvent(evt)
	if res.Error != nil {
		c.Login.Log.Err(res.Error).Stringer("event_type", evt.GetType()).Msg("Failed to queue remote event")
		return false
	}
	return true
}

// queueResync queues the ChatResync that creates the portal of ch or updates its info, and reports whether
// it was queued. Every portal resync is built here. A non-nil needsBackfill makes bridgev2 check, on an
// existing room, whether to fetch the messages after the newest bridged one (see newerMessages).
func (c *TraQClient) queueResync(ctx context.Context, ch channelInfo, needsBackfill backfillCheck) bool {
	info, err := c.chatInfo(ctx, ch)
	if err != nil {
		c.Login.Log.Err(err).Str("traq_channel_id", ch.ID).Msg("Failed to build chat info")
		return false
	}
	return c.queue(&simplevent.ChatResync{
		EventMeta: simplevent.EventMeta{
			Type:         bridgev2.RemoteEventChatResync,
			LogContext:   channelLogContext(ch),
			PortalKey:    c.portalKey(ch),
			CreatePortal: true,
		},
		ChatInfo:               info,
		CheckNeedsBackfillFunc: needsBackfill,
	})
}

// queueLeave queues a ChatDelete that removes this login from the portal. bridgev2 deletes the room when
// no other login is in it.
func (c *TraQClient) queueLeave(key networkid.PortalKey) {
	c.queue(&simplevent.ChatDelete{
		EventMeta: simplevent.EventMeta{
			Type:      bridgev2.RemoteEventChatDelete,
			PortalKey: key,
		},
		OnlyForMe: true,
	})
}

// initialSync brings the portals in line with the snapshot loaded by Connect: it creates or updates every
// target portal, leaves the portals of this login that are no longer targets, and marks the targets that
// have no unread messages as read. Each portal's event loop is serial, so the receipt is handled after the
// resync. Targets whose resync could not be queued are dropped from c.targets, so the next subscription sync
// retries them.
//
// The resyncs of the channels that may have messages posted while the bridge was not running (see
// catchUpChannels) carry newerMessages, so bridgev2 fetches those messages into existing rooms. They are
// queued before the WebSocket is read, so each portal handles its catch-up before any live message.
func (c *TraQClient) initialSync(ctx context.Context, me *traq.Me, targets map[string]channelInfo, unread []traq.UnreadChannel) {
	log := c.Login.Log
	ids := make([]string, 0, len(targets))
	for id := range targets {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	catchUp := c.catchUpCandidates(ctx, unread, time.Time{})
	keys := make(map[networkid.PortalKey]struct{}, len(ids))
	var failed []channelInfo
	for _, id := range ids {
		ch := targets[id]
		keys[c.portalKey(ch)] = struct{}{}
		var needsBackfill backfillCheck
		if _, ok := catchUp[id]; ok {
			needsBackfill = c.newerMessages(id)
		}
		if !c.queueResync(ctx, ch, needsBackfill) {
			failed = append(failed, ch)
		}
	}

	userPortals, err := c.Login.Bridge.DB.UserPortal.GetAllForLogin(ctx, c.Login.UserLogin)
	if err != nil {
		log.Err(err).Msg("Failed to list the portals of this login")
	}
	for _, up := range userPortals {
		if _, ok := keys[up.Portal]; ok {
			continue
		}
		portal, err := c.Login.Bridge.GetExistingPortalByKey(ctx, up.Portal)
		if err != nil {
			log.Err(err).Stringer("portal_key", up.Portal).Msg("Failed to get portal")
			continue
		}
		if portal == nil || portal.MXID == "" {
			continue // nothing to leave
		}
		log.Debug().Stringer("portal_key", up.Portal).Msg("Leaving portal that is no longer subscribed")
		c.queueLeave(up.Portal)
	}

	hasUnread := make(map[string]struct{}, len(unread))
	for _, u := range unread {
		hasUnread[u.ChannelID] = struct{}{}
	}
	now := time.Now()
	for _, id := range ids {
		if _, ok := hasUnread[id]; ok {
			continue
		}
		ch := targets[id]
		c.queue(&simplevent.Receipt{
			EventMeta: simplevent.EventMeta{
				Type:       bridgev2.RemoteEventReadReceipt,
				LogContext: channelLogContext(ch),
				PortalKey:  c.portalKey(ch),
				Sender:     c.selfSender(me),
			},
			ReadUpTo: now,
		})
	}
	// Last, because targets is c.targets and the loops above read it.
	c.revertTargets(nil, failed)
}

// syncSubscriptions re-reads the channels and subscriptions and queues only what changed. Upserts that could
// not be queued are reverted in c.targets, so the next sync emits them again. It runs on the subscription
// sync loop (periodically, and when the event worker sees a channel change); syncMu serializes it with the
// worker's CHANNEL_CREATED handling.
func (c *TraQClient) syncSubscriptions(ctx context.Context) error {
	c.syncMu.Lock()
	defer c.syncMu.Unlock()
	list, err := c.API.Channels(ctx)
	if err != nil {
		return err
	}
	subs, err := c.API.Subscriptions(ctx)
	if err != nil {
		return err
	}
	tree := buildChannelTree(list)
	snap := subscriptionSnapshot(tree, subs)
	c.stateMu.Lock()
	old := c.targets
	c.channels, c.targets = tree, snap
	c.stateMu.Unlock()

	var failed []channelInfo
	for _, change := range portalDiff(old, snap) {
		switch change.Kind {
		case changeUpsert:
			if !c.queueResync(ctx, change.Channel, nil) {
				failed = append(failed, change.Channel)
			}
		case changeRemove:
			c.queueLeave(c.portalKey(change.Channel))
		}
	}
	c.revertTargets(old, failed)
	return nil
}
