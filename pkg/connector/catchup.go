package connector

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"time"

	"maunium.net/go/mautrix/bridgev2/database"

	"github.com/kisepichu/traq-matrix-bridge/pkg/traq"
)

// catchUpMargin moves the start of a reconnect catch-up back from the time the connection was found lost: a
// dead connection is noticed only after up to pingInterval + pingTimeout, and the traQ server's clock may be
// behind the bridge's.
const catchUpMargin = pingInterval + pingTimeout + time.Minute

// backfillCheck is ChatResync.CheckNeedsBackfillFunc.
type backfillCheck func(ctx context.Context, latestMessage *database.Message) (bool, error)

// catchUpChannels returns the targets that may have messages the bridge missed, sorted by ID: the channels
// with unread messages, the channels of the activity timeline with a message at or after since (a zero since
// takes them all), and every DM (the timeline has no DMs, and the user's own DM messages are not unread).
func catchUpChannels(targets map[string]channelInfo, unread []traq.UnreadChannel, timeline []traq.ActivityMessage, since time.Time) []string {
	set := make(map[string]struct{})
	for _, u := range unread {
		set[u.ChannelID] = struct{}{}
	}
	for _, a := range timeline {
		if !a.CreatedAt.Before(since) {
			set[a.ChannelID] = struct{}{}
		}
	}
	for id, ch := range targets {
		if ch.isDM() {
			set[id] = struct{}{}
		}
	}
	ids := make([]string, 0, len(set))
	for id := range set {
		if _, ok := targets[id]; ok {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	return ids
}

// catchUpCandidates reads the activity timeline and returns catchUpChannels of the current targets. If the
// read fails, the error comes with the candidates without the timeline's channels.
func (c *TraQClient) catchUpCandidates(ctx context.Context, unread []traq.UnreadChannel, since time.Time) (map[string]struct{}, error) {
	timeline, err := c.API.ActivityTimeline(ctx)
	if err != nil {
		err = fmt.Errorf("get the activity timeline: %w", err)
	}
	c.stateMu.RLock()
	ids := catchUpChannels(c.targets, unread, timeline, since)
	c.stateMu.RUnlock()
	set := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		set[id] = struct{}{}
	}
	return set, err
}

// catchUp adds, after a reconnect, a resync with newerMessages for each candidate channel that has a Matrix
// room, so that bridgev2 fetches what was posted while the WebSocket was down. A DM without a room that has
// unread messages gets a resync that creates its room with its history, like at connect (see initialSync).
// since is the time the previous connection was lost, less catchUpMargin.
//
// The resyncs are paced (see resyncPacer). They are added before the new connection is read, and every other
// event of a portal waits for its resync (see queue), so each portal handles its catch-up before the live
// messages that follow it: a live message bridged first would become the anchor of the forward fetch, and the
// missed messages before it would be cut off.
func (c *TraQClient) catchUp(ctx context.Context, since time.Time) {
	log := c.Login.Log
	unread, err := c.API.Unread(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		log.Warn().Err(err).Msg("Failed to get the unread channels for the catch-up")
	}
	candidates, err := c.catchUpCandidates(ctx, unread, since)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		log.Warn().Err(err).Msg("Failed to get the activity timeline, catching up on unread channels and DMs only")
	}
	hasUnread := make(map[string]struct{}, len(unread))
	for _, u := range unread {
		hasUnread[u.ChannelID] = struct{}{}
	}
	added := 0
	for _, id := range slices.Sorted(maps.Keys(candidates)) {
		ch, ok := c.lookupChannel(id)
		if !ok {
			continue
		}
		key := c.portalKey(ch)
		portal, err := c.Login.Bridge.GetExistingPortalByKey(ctx, key)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Err(err).Stringer("portal_key", key).Msg("Failed to get portal")
			continue
		}
		r := pendingResync{ch: ch}
		if portal != nil && portal.MXID != "" {
			r.needsBackfill = c.newerMessages(id)
		} else if _, ok := hasUnread[id]; !ok || !ch.isDM() {
			continue // the first message creates the room and brings the history with it
		}
		c.addResync(ctx, r)
		added++
	}
	log.Info().Time("since", since).Int("channels", added).Stringer("gap", resyncGap).
		Msg("Catching up on messages missed while disconnected, paced")
}

// newerMessages is the backfill check of a catch-up: whether the channel's newest message in traQ is newer
// than the newest bridged one (latestMessage, nil in an empty room). If so, bridgev2 fetches forward from
// there (FetchMessages, up to max_catchup_messages).
func (c *TraQClient) newerMessages(channelID string) backfillCheck {
	return func(ctx context.Context, latest *database.Message) (bool, error) {
		msgs, _, err := c.API.Messages(ctx, channelID, traq.MessagesQuery{Limit: 1, Order: "desc"})
		if err != nil {
			return false, fmt.Errorf("get the newest message of channel %s: %w", channelID, err)
		}
		return hasNewerMessage(msgs, latest), nil
	}
}

// hasNewerMessage reports whether newest (the channel's newest message, or none) is newer than latest, the
// newest bridged message (nil in an empty room). A different message at the same time as latest is newer:
// it was posted at the same instant.
func hasNewerMessage(newest []traq.Message, latest *database.Message) bool {
	if len(newest) == 0 {
		return false
	}
	if latest == nil {
		return true
	}
	return TraQMessageID(latest.ID) != newest[0].ID && !newest[0].CreatedAt.Before(latest.Timestamp)
}
