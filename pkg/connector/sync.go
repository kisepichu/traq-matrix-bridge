package connector

import (
	"cmp"
	"context"
	"maps"
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

// massRemovalMin is the least number of public portals whose removal by one snapshot can be held back (see
// holdRemovals).
const massRemovalMin = 5

// holdRemovals guards against a bad answer from traQ, which would make the next sync leave, and so delete,
// the rooms of many channels: a snapshot is suspect when it removes more than half of the public portals in
// old and at least massRemovalMin of them, or when it has no subscription at all (noSubscriptions) and removes
// public portals. The removals of a suspect snapshot are held back: apply is changes without them, and
// nowHeld their channel IDs. They are applied when the next snapshot removes the same portals (held, the IDs
// the previous sync held back), so a real change waits for one more sync. DMs do not count, but are held back
// with the rest.
func holdRemovals(old map[string]channelInfo, changes []portalChange, noSubscriptions bool, held []string) (apply []portalChange, nowHeld []string) {
	public, removedPublic := 0, 0
	for _, ch := range old {
		if ch.isTarget() && !ch.isDM() {
			public++
		}
	}
	for _, change := range changes {
		if change.Kind == changeRemove && !change.Channel.isDM() {
			removedPublic++
		}
	}
	removed := removedIDs(changes)
	suspect := removedPublic > 0 && (noSubscriptions || removedPublic*2 > public && removedPublic >= massRemovalMin)
	if !suspect || slices.Equal(removed, held) {
		return changes, nil
	}
	for _, change := range changes {
		if change.Kind != changeRemove {
			apply = append(apply, change)
		}
	}
	return apply, removed
}

// removedIDs returns the channel IDs of the removals in changes, in order.
func removedIDs(changes []portalChange) []string {
	var ids []string
	for _, change := range changes {
		if change.Kind == changeRemove {
			ids = append(ids, change.Channel.ID)
		}
	}
	return ids
}

// startupRemovals tells whether initialSync may leave the portals of gone, by the rule of holdRemovals. kept is
// the channels of this login's portals with a room that are still targets, gone those of the ones that are
// not; together they stand for the snapshot of the previous session. held is nil when the portals may be left;
// otherwise it is the IDs of gone in the order portalDiff removes them, so that the next subscription sync can
// agree.
func startupRemovals(kept, gone []channelInfo, noSubscriptions bool) (held []string) {
	old := make(map[string]channelInfo, len(kept)+len(gone))
	for _, ch := range kept {
		old[ch.ID] = ch
	}
	changes := make([]portalChange, 0, len(gone))
	for _, ch := range gone {
		old[ch.ID] = ch
		changes = append(changes, portalChange{Channel: ch, Kind: changeRemove})
	}
	slices.SortFunc(changes, func(a, b portalChange) int { return cmp.Compare(a.Channel.ID, b.Channel.ID) })
	_, held = holdRemovals(old, changes, noSubscriptions, nil)
	return held
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

// portalKey returns the portal key of a channel. Every portal key is built here. A DM's portal belongs to
// this login. A public channel's portal is shared by all logins (no receiver), unless bridgev2 splits
// portals (bridge.split_portals, which bbctl's bridgev2 config sets): bridgev2 then rejects every key
// without a receiver, and each login has its own portal of the channel.
func (c *TraQClient) portalKey(ch channelInfo) networkid.PortalKey {
	if ch.isDM() || c.Login.Bridge.Config.SplitPortals {
		return PortalKey(ch.ID, c.Login.ID)
	}
	return PortalKey(ch.ID, "")
}

// selfSender is the EventSender for events of the logged-in user.
func (c *TraQClient) selfSender(me *traq.Me) bridgev2.EventSender {
	return bridgev2.EventSender{IsFromMe: true, SenderLogin: c.Login.ID, Sender: UserID(me.ID)}
}

func channelLogContext(ch channelInfo) func(zerolog.Context) zerolog.Context {
	return func(c zerolog.Context) zerolog.Context { return c.Str("traq_channel_id", ch.ID) }
}

// queue queues evt and reports whether bridgev2 accepted it. A resync of evt's portal that the session's
// resyncPacer still holds is queued first, so that the portal handles it before evt.
func (c *TraQClient) queue(evt bridgev2.RemoteEvent) bool {
	if p := c.resyncs.Load(); p != nil {
		p.flush(ChannelID(evt.GetPortalKey()))
	}
	return c.queueNow(evt)
}

// queueNow queues evt without waiting for its portal's resync: for the resync itself, and what is queued in
// its place or right after it.
func (c *TraQClient) queueNow(evt bridgev2.RemoteEvent) bool {
	res := c.Login.QueueRemoteEvent(evt)
	if res.Error != nil {
		c.Login.Log.Err(res.Error).Stringer("event_type", evt.GetType()).Msg("Failed to queue remote event")
		return false
	}
	return true
}

// queueResync queues the ChatResync that creates the portal of ch or updates its info, and reports whether
// it was queued. Every portal resync is built here; the syncs go through addResync to be paced. A non-nil
// needsBackfill makes bridgev2 check, on an existing room, whether to fetch the messages after the newest
// bridged one (see newerMessages).
func (c *TraQClient) queueResync(ctx context.Context, ch channelInfo, needsBackfill backfillCheck) bool {
	info, err := c.chatInfo(ctx, ch)
	if err != nil {
		c.Login.Log.Err(err).Str("traq_channel_id", ch.ID).Msg("Failed to build chat info")
		return false
	}
	return c.queueNow(&simplevent.ChatResync{
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

// addResync hands a resync to the session's resyncPacer. Outside a session it is queued at once.
func (c *TraQClient) addResync(ctx context.Context, r pendingResync) {
	if p := c.resyncs.Load(); p != nil {
		p.add(r)
		return
	}
	c.runResync(ctx, &r)
}

// runResync is the resyncPacer's run function: it queues the resync, then its after events. If the resync
// cannot be queued, neither are they, and the resync is reverted.
func (c *TraQClient) runResync(ctx context.Context, r *pendingResync) {
	if !c.queueResync(ctx, r.ch, r.needsBackfill) {
		if r.revert != nil {
			r.revert()
		}
		return
	}
	c.Login.Log.Debug().Str("traq_channel_id", r.ch.ID).Bool("check_backfill", r.needsBackfill != nil).
		Msg("Queued a portal resync")
	for _, evt := range r.after {
		c.queueNow(evt)
	}
}

// queueLeave queues a ChatDelete that removes this login from the portal, instead of a resync still waiting
// for it, and after one being queued. bridgev2 deletes the room when no other login is in it.
func (c *TraQClient) queueLeave(key networkid.PortalKey) {
	if p := c.resyncs.Load(); p != nil {
		p.drop(ChannelID(key))
	}
	c.queueNow(&simplevent.ChatDelete{
		EventMeta: simplevent.EventMeta{
			Type:      bridgev2.RemoteEventChatDelete,
			PortalKey: key,
		},
		OnlyForMe: true,
	})
}

// hasRoom reports whether a portal has a Matrix room. A failed lookup is logged and counts as a room, so that
// nothing is skipped because of it.
func (c *TraQClient) hasRoom(ctx context.Context, key networkid.PortalKey) bool {
	portal, err := c.Login.Bridge.GetExistingPortalByKey(ctx, key)
	if err != nil {
		if ctx.Err() == nil {
			c.Login.Log.Err(err).Stringer("portal_key", key).Msg("Failed to get portal")
		}
		return true
	}
	return portal != nil && portal.MXID != ""
}

// hasBridgedMessages reports whether a target channel had a bridged message when the session started (see
// loadLastActivity) or got one since.
func (c *TraQClient) hasBridgedMessages(channelID string) bool {
	c.stateMu.RLock()
	defer c.stateMu.RUnlock()
	_, ok := c.lastActivity[channelID]
	return ok
}

// initialSync brings the portals in line with the snapshot loaded by Connect: it resyncs the target portals,
// leaves the portals of this login that are no longer targets, and marks the targets that have no unread
// messages as read. The resyncs are paced (see resyncPacer); the receipt of a portal is queued right after
// its resync, which its serial event loop then handles first. Targets whose resync could not be queued are
// dropped from c.targets, so the next subscription sync retries them. The receipt is left out for portals
// without a bridged message: there is nothing to mark, and the backfill of a new room marks its own read state.
//
// A DM gets its room lazily: traQ creates a DM channel as soon as someone opens the DM page, so most of them
// are empty. A DM without a room is resynced, which creates the room with its history, only if it has unread
// messages (traQ tracks the unread messages of every DM); the others stay targets, and their first message
// creates the room (queueMessage).
//
// The resyncs of the channels that may have messages posted while the bridge was not running (see
// catchUpChannels; every target when the activity timeline cannot be read) carry newerMessages, so bridgev2
// fetches those messages into existing rooms. They are added before the WebSocket is read, and every other
// event of a portal waits for its resync (see queue), so each portal handles its catch-up before any live
// message.
//
// The portals are not left when that looks like a bad answer from traQ (see startupRemovals): they are kept
// in c.targets and in c.heldRemovals, as if the previous subscription sync had held their removals back, so
// the first subscription sync leaves them if it agrees, and otherwise keeps them and catches them up (see
// syncSubscriptions); they get no resync here. Their last activity is loaded, so the poller sees them meanwhile.
func (c *TraQClient) initialSync(ctx context.Context, l loaded) {
	log := c.Login.Log
	targets := l.targets
	ids := make([]string, 0, len(targets))
	for id := range targets {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	catchUp, err := c.catchUpCandidates(ctx, l.unread, time.Time{})
	checkAll := err != nil
	if checkAll && ctx.Err() == nil {
		log.Warn().Err(err).Msg("Failed to get the activity timeline, checking every channel for messages missed while stopped")
	}
	hasUnread := make(map[string]struct{}, len(l.unread))
	for _, u := range l.unread {
		hasUnread[u.ChannelID] = struct{}{}
	}
	now := time.Now()
	keys := make(map[networkid.PortalKey]channelInfo, len(ids))
	resyncs, lazyDMs := 0, 0
	for _, id := range ids {
		ch := targets[id]
		key := c.portalKey(ch)
		keys[key] = ch
		_, isUnread := hasUnread[id]
		if ch.isDM() && !isUnread && !c.hasRoom(ctx, key) {
			lazyDMs++
			continue
		}
		r := pendingResync{ch: ch, revert: func() { c.revertTargets(nil, []channelInfo{ch}) }}
		if _, ok := catchUp[id]; ok || checkAll {
			r.needsBackfill = c.newerMessages(id)
		}
		if !isUnread && c.hasBridgedMessages(id) {
			r.after = append(r.after, &simplevent.Receipt{
				EventMeta: simplevent.EventMeta{
					Type:       bridgev2.RemoteEventReadReceipt,
					LogContext: channelLogContext(ch),
					PortalKey:  key,
					Sender:     c.selfSender(l.me),
				},
				ReadUpTo: now,
			})
		}
		c.addResync(ctx, r)
		resyncs++
	}
	log.Info().Int("resyncs", resyncs).Int("dms_without_room", lazyDMs).Stringer("gap", resyncGap).
		Msg("Syncing the portals, paced")

	userPortals, err := c.Login.Bridge.DB.UserPortal.GetAllForLogin(ctx, c.Login.UserLogin)
	if err != nil {
		log.Err(err).Msg("Failed to list the portals of this login")
	}
	var kept, gone []channelInfo
	var goneKeys []networkid.PortalKey
	for _, up := range userPortals {
		portal, err := c.Login.Bridge.GetExistingPortalByKey(ctx, up.Portal)
		if err != nil {
			log.Err(err).Stringer("portal_key", up.Portal).Msg("Failed to get portal")
			continue
		}
		if portal == nil || portal.MXID == "" {
			continue // nothing to leave
		}
		if ch, ok := keys[up.Portal]; ok {
			kept = append(kept, ch)
			continue
		}
		gone = append(gone, c.formerTarget(portal))
		goneKeys = append(goneKeys, up.Portal)
	}
	if held := startupRemovals(kept, gone, l.subscriptions == 0); held != nil {
		activity := c.newestBridged(ctx, slices.Values(gone))
		c.stateMu.Lock()
		for _, ch := range gone {
			c.targets[ch.ID] = ch
		}
		if c.lastActivity == nil {
			c.lastActivity = make(map[string]time.Time, len(activity))
		}
		maps.Copy(c.lastActivity, activity)
		c.heldRemovals = held
		c.stateMu.Unlock()
		log.Warn().Int("portals", len(held)).Int("subscriptions", l.subscriptions).
			Msg("Not leaving the portals of many channels at once until the next subscription sync agrees")
		return
	}
	for _, key := range goneKeys {
		log.Debug().Stringer("portal_key", key).Msg("Leaving portal that is no longer subscribed")
		c.queueLeave(key)
	}
}

// formerTarget returns the channel of a portal that is no longer a target, as the target it was, for
// startupRemovals and c.targets. A channel missing from the channel list is known only by its portal.
func (c *TraQClient) formerTarget(portal *bridgev2.Portal) channelInfo {
	id := ChannelID(portal.PortalKey)
	ch, ok := c.lookupChannel(id)
	if !ok {
		ch = channelInfo{ID: id}
		if meta, _ := portal.Metadata.(*PortalMetadata); meta != nil && meta.IsDM {
			ch.DMUserID = meta.DMUserID
		}
	}
	if ch.isDM() {
		ch.Level = dmLevel
	} else {
		ch.Level = max(ch.Level, 1)
	}
	return ch
}

// syncSubscriptions re-reads the channels and subscriptions and queues only what changed. The upserts are
// paced (see resyncPacer); one that cannot be queued is reverted in c.targets, so the next sync emits it again.
// A new DM gets no room until its first message (see initialSync). Removals that look like a bad answer from
// traQ are held back (see holdRemovals): those channels stay in c.targets, so the next sync emits them again.
// The held channels that the sync keeps get a resync with newerMessages, as at startup: a channel held back at
// startup got no catch-up then (see initialSync).
// It runs on the subscription sync loop (periodically, and when the event worker sees a channel change);
// syncMu serializes it with the worker's CHANNEL_CREATED handling.
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
	old, prevHeld := c.targets, c.heldRemovals
	var kept []channelInfo
	for _, id := range prevHeld {
		if ch, ok := snap[id]; ok {
			kept = append(kept, ch)
		}
	}
	changes, held := holdRemovals(old, portalDiff(old, snap), len(subs) == 0, prevHeld)
	for _, id := range held {
		snap[id] = old[id]
	}
	c.channels, c.targets, c.heldRemovals = tree, snap, held
	c.stateMu.Unlock()
	switch {
	case len(held) > 0:
		c.Login.Log.Warn().Int("portals", len(held)).Int("subscriptions", len(subs)).
			Msg("Not leaving the portals of many channels at once until the next subscription sync agrees")
	case len(prevHeld) > 0 && slices.Equal(removedIDs(changes), prevHeld):
		c.Login.Log.Info().Int("portals", len(prevHeld)).Msg("Leaving the portals held back by the previous subscription sync")
	case len(kept) > 0:
		c.Login.Log.Info().Int("portals", len(kept)).Msg("Catching up the portals kept after their removal was held back")
	}

	for _, change := range changes {
		ch := change.Channel
		switch change.Kind {
		case changeUpsert:
			if ch.isDM() && !c.hasRoom(ctx, c.portalKey(ch)) {
				continue
			}
			prev := map[string]channelInfo{}
			if p, ok := old[ch.ID]; ok {
				prev[ch.ID] = p
			}
			c.addResync(ctx, pendingResync{ch: ch, revert: func() { c.revertTargets(prev, []channelInfo{ch}) }})
		case changeRemove:
			c.queueLeave(c.portalKey(ch))
		}
	}
	// Added after the upserts, so a merged resync keeps the upsert's revert.
	for _, ch := range kept {
		if ch.isDM() && !c.hasRoom(ctx, c.portalKey(ch)) {
			continue
		}
		c.addResync(ctx, pendingResync{ch: ch, needsBackfill: c.newerMessages(ch.ID)})
	}
	return nil
}
