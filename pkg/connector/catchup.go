package connector

import (
	"context"
	"fmt"
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

// catchUpCandidates reads the activity timeline and returns catchUpChannels of the current targets. A failed
// read is logged and leaves the timeline's channels out.
func (c *TraQClient) catchUpCandidates(ctx context.Context, unread []traq.UnreadChannel, since time.Time) map[string]struct{} {
	timeline, err := c.API.ActivityTimeline(ctx, true)
	if err != nil && ctx.Err() == nil {
		c.Login.Log.Warn().Err(err).Msg("Failed to get the activity timeline, catching up on unread channels and DMs only")
	}
	c.stateMu.RLock()
	ids := catchUpChannels(c.targets, unread, timeline, since)
	c.stateMu.RUnlock()
	set := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		set[id] = struct{}{}
	}
	return set
}

// catchUp queues, after a reconnect, a ChatResync with newerMessages for each candidate channel that has a
// Matrix room, so that bridgev2 fetches what was posted while the WebSocket was down. since is the time the
// previous connection was lost, less catchUpMargin.
//
// It runs before the new connection is read, so each portal handles its catch-up before the live messages
// that follow it: a live message bridged first would become the anchor of the forward fetch, and the missed
// messages before it would be cut off.
func (c *TraQClient) catchUp(ctx context.Context, since time.Time) {
	log := c.Login.Log
	unread, err := c.API.Unread(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		log.Warn().Err(err).Msg("Failed to get the unread channels for the catch-up")
	}
	candidates := c.catchUpCandidates(ctx, unread, since)
	queued := 0
	for id := range candidates {
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
		if portal == nil || portal.MXID == "" {
			continue // the first message creates the room and brings the history with it
		}
		if c.queueResync(ctx, ch, c.newerMessages(id)) {
			queued++
		}
	}
	log.Info().Time("since", since).Int("channels", queued).Msg("Catching up on messages missed while disconnected")
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
		if len(msgs) == 0 {
			return false, nil
		}
		if latest == nil {
			return true, nil
		}
		// Not After: a message posted at the same time as the newest bridged one is new too.
		return TraQMessageID(latest.ID) != msgs[0].ID && !msgs[0].CreatedAt.Before(latest.Timestamp), nil
	}
}
