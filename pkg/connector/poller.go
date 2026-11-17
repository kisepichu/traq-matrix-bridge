package connector

import (
	"cmp"
	"context"
	"fmt"
	"net/http"
	"slices"
	"time"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/simplevent"

	"github.com/kisepichu/traq-matrix-bridge/pkg/traq"
)

// pollChannelGap is the pause between the channels of one poll, to spread the REST requests.
const pollChannelGap = 200 * time.Millisecond

// knownRowsPerMessage is how many database rows per polled message are read to find the bridged ones: a
// message has one row per part (its text and each file).
const knownRowsPerMessage = 4

// pollCandidates picks channels whose last activity is within window, excluding the viewed channel,
// newest first, at most max.
func pollCandidates(lastActivity map[string]time.Time, viewed string, now time.Time, window time.Duration, max int) []string {
	since := now.Add(-window)
	var ids []string
	for id, t := range lastActivity {
		if id != viewed && !t.Before(since) {
			ids = append(ids, id)
		}
	}
	slices.SortFunc(ids, func(a, b string) int {
		if c := lastActivity[b].Compare(lastActivity[a]); c != 0 {
			return c
		}
		return cmp.Compare(a, b)
	})
	if len(ids) > max {
		ids = ids[:max]
	}
	return ids
}

// noteActivity records t as the last activity of channelID if it is newer.
func (c *TraQClient) noteActivity(channelID string, t time.Time) {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	if c.lastActivity == nil {
		c.lastActivity = make(map[string]time.Time)
	}
	if t.After(c.lastActivity[channelID]) {
		c.lastActivity[channelID] = t
	}
}

// loadLastActivity starts the session's last activity map from the newest bridged message of each target.
func (c *TraQClient) loadLastActivity(ctx context.Context, targets map[string]channelInfo) {
	activity := make(map[string]time.Time, len(targets))
	for id, ch := range targets {
		rows, err := c.Login.Bridge.DB.Message.GetLastNInPortal(ctx, c.portalKey(ch), 1)
		if err != nil {
			if ctx.Err() == nil {
				c.Login.Log.Err(err).Str("traq_channel_id", id).Msg("Failed to get the newest bridged message")
			}
			continue
		}
		if len(rows) > 0 {
			activity[id] = rows[0].Timestamp
		}
	}
	c.stateMu.Lock()
	c.lastActivity = activity
	c.stateMu.Unlock()
}

// pollLoop re-reads the recently active channels every poll.interval until ctx ends, to bridge the edits,
// deletions and stamp changes that the WebSocket does not deliver (it sends stamps only for the viewed
// channel, and nothing but new messages for DMs), and the new messages it missed while its connection was
// dead but not noticed yet. It also polls, once, each channel the viewstate switches to (see
// requestViewPoll). A rejected token stops the session with errTokenRejected.
func (c *TraQClient) pollLoop(ctx context.Context, stop context.CancelCauseFunc) {
	t := time.NewTicker(c.Main.Config.Poll.Interval)
	ctx = c.Login.Log.With().Str("action", "poll").Logger().WithContext(ctx)
	defer t.Stop()
	for {
		var rejected bool
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			rejected = c.pollOnce(ctx)
		case id := <-c.viewPolls:
			// Paced like the channels of a poll: the switch follows Matrix activity, which may have just sent
			// requests to traQ.
			if !sleepCtx(ctx, pollChannelGap) {
				return
			}
			zerolog.Ctx(ctx).Debug().Str("traq_channel_id", id).Msg("Polling the channel the viewstate switched to")
			rejected = c.pollLogged(ctx, id)
		}
		if rejected {
			stop(errTokenRejected)
			return
		}
	}
}

// requestViewPoll asks the poll loop to poll a channel once, without waiting: the viewstate switched to it,
// and traQ never sent the stamp events of the time before (see viewState). A request still waiting is
// replaced, as only the viewed channel matters. It is viewState's switched function, called under its lock, so
// there is one caller at a time.
func (c *TraQClient) requestViewPoll(channelID string) {
	select {
	case <-c.viewPolls:
	default:
	}
	select {
	case c.viewPolls <- channelID:
	default:
	}
}

// pollOnce polls each candidate channel in turn, pollChannelGap apart. A failed channel is logged and the
// next one is polled. It reports whether traQ rejected the token.
func (c *TraQClient) pollOnce(ctx context.Context) (rejected bool) {
	cfg := c.Main.Config.Poll
	now := time.Now()
	c.stateMu.RLock()
	activity := make(map[string]time.Time, len(c.lastActivity))
	for id, t := range c.lastActivity {
		if _, ok := c.targets[id]; ok {
			activity[id] = t
		}
	}
	c.stateMu.RUnlock()
	viewed := c.view.Current()
	if t, ok := activity[viewed]; ok && !t.Before(now.Add(-cfg.Window)) {
		zerolog.Ctx(ctx).Debug().Str("traq_channel_id", viewed).Msg("Not polling the viewed channel, the WebSocket delivers its changes")
	}
	for i, id := range pollCandidates(activity, viewed, now, cfg.Window, cfg.MaxChannels) {
		if i > 0 && !sleepCtx(ctx, pollChannelGap) {
			return false
		}
		if c.pollLogged(ctx, id) {
			return true
		}
		if ctx.Err() != nil {
			return false
		}
	}
	return false
}

// pollLogged polls a channel and logs a failure. It reports whether traQ rejected the token.
func (c *TraQClient) pollLogged(ctx context.Context, channelID string) (rejected bool) {
	err := c.pollChannel(ctx, channelID)
	switch {
	case err == nil || ctx.Err() != nil:
	case traq.IsStatus(err, http.StatusUnauthorized):
		return true
	default:
		zerolog.Ctx(ctx).Warn().Err(err).Str("traq_channel_id", channelID).Msg("Failed to poll channel")
	}
	return false
}

// pollChannel reads the newest poll.depth messages of a channel, compares them with what is bridged and
// queues a message, an edit, a removal or a reaction sync for each difference.
func (c *TraQClient) pollChannel(ctx context.Context, channelID string) error {
	ch, ok := c.target(channelID)
	if !ok {
		return nil
	}
	key := c.portalKey(ch)
	depth := c.Main.Config.Poll.Depth
	// Read before the fetch, so each of these messages was in traQ when it was fetched (see diffMessages),
	// and a reaction bridged from Matrix after the fetch is not taken for a change in traQ.
	parts, err := c.Login.Bridge.DB.Message.GetLastNInPortal(ctx, key, depth*knownRowsPerMessage)
	if err != nil {
		return fmt.Errorf("get bridged messages: %w", err)
	}
	known, senders := groupKnown(parts)
	if err := c.readReactions(ctx, key, known, depth); err != nil {
		return err
	}
	fetched, more, err := c.API.Messages(ctx, channelID, traq.MessagesQuery{Limit: depth, Order: "desc"})
	if err != nil {
		return fmt.Errorf("get messages: %w", err)
	}
	for _, m := range fetched {
		c.noteActivity(channelID, m.CreatedAt)
	}
	d := diffMessages(fetched, known, more)
	if more && len(d.Deleted) > 0 {
		// The rows read may have left out the earliest part of a message to delete; check all its parts.
		if err := c.completeTimestamps(ctx, key, known, d.Deleted); err != nil {
			return err
		}
		d = diffMessages(fetched, known, more)
	}
	for i := range d.New {
		c.queueMessage(key, ch.isDM(), &d.New[i])
	}
	for i := range d.Edited {
		c.queueEdit(key, &d.Edited[i])
	}
	for _, id := range d.Deleted {
		c.queueRemove(key, id, senders[id])
	}
	for i := range d.ReactionChanged {
		c.queueReactionSync(ctx, key, &d.ReactionChanged[i])
	}
	if len(d.New)+len(d.Edited)+len(d.Deleted)+len(d.ReactionChanged) > 0 {
		zerolog.Ctx(ctx).Debug().
			Str("traq_channel_id", channelID).
			Int("new", len(d.New)).
			Int("edited", len(d.Edited)).
			Int("deleted", len(d.Deleted)).
			Int("reactions_changed", len(d.ReactionChanged)).
			Msg("Polling found changes")
	}
	return nil
}

// groupKnown groups the rows bridged to a portal by message, without reactions. A message's Timestamp is
// that of its earliest part. senders maps each message ID to its traQ author.
func groupKnown(parts []*database.Message) (known []knownMessage, senders map[string]string) {
	index := make(map[string]int)
	senders = make(map[string]string)
	for _, part := range parts {
		id := TraQMessageID(part.ID)
		i, ok := index[id]
		if !ok {
			i = len(known)
			index[id] = i
			known = append(known, knownMessage{ID: id, Timestamp: part.Timestamp})
			senders[id] = TraQUserID(part.SenderID)
		} else if part.Timestamp.Before(known[i].Timestamp) {
			known[i].Timestamp = part.Timestamp
		}
		if meta, ok := part.Metadata.(*MessageMetadata); ok && part.PartID == textPartID {
			known[i].ContentHash = meta.ContentHash
		}
	}
	return known, senders
}

// readReactions reads the bridged reactions of the newest n known messages: a fetch of the newest n messages
// returns no older known message unless newer ones were deleted, and the others keep nil Reactions (not
// compared) until those deletions are bridged.
func (c *TraQClient) readReactions(ctx context.Context, key networkid.PortalKey, known []knownMessage, n int) error {
	newest := make([]int, len(known))
	for i := range newest {
		newest[i] = i
	}
	slices.SortFunc(newest, func(a, b int) int { return known[b].Timestamp.Compare(known[a].Timestamp) })
	for _, i := range newest[:min(n, len(newest))] {
		reactions, err := c.Login.Bridge.DB.Reaction.GetAllToMessage(ctx, key.Receiver, MessageID(known[i].ID))
		if err != nil {
			return fmt.Errorf("get reactions of message %s: %w", known[i].ID, err)
		}
		known[i].Reactions = make(map[[2]string]bool, len(reactions))
		for _, r := range reactions {
			known[i].Reactions[[2]string{TraQUserID(r.SenderID), StampID(r.EmojiID)}] = true
		}
	}
	return nil
}

// completeTimestamps sets the Timestamp of each known message in ids from all of its parts.
func (c *TraQClient) completeTimestamps(ctx context.Context, key networkid.PortalKey, known []knownMessage, ids []string) error {
	for i := range known {
		if !slices.Contains(ids, known[i].ID) {
			continue
		}
		parts, err := c.Login.Bridge.DB.Message.GetAllPartsByID(ctx, key.Receiver, MessageID(known[i].ID))
		if err != nil {
			return fmt.Errorf("get the parts of message %s: %w", known[i].ID, err)
		}
		for _, p := range parts {
			if p.Timestamp.Before(known[i].Timestamp) {
				known[i].Timestamp = p.Timestamp
			}
		}
	}
	return nil
}

// queueReactionSync queues the full set of msg's stamps as its reactions. Every user and each user's every
// stamp is listed, so bridgev2 adds the missing reactions and removes the others on this message.
func (c *TraQClient) queueReactionSync(ctx context.Context, key networkid.PortalKey, msg *traq.Message) {
	users := make(map[networkid.UserID]*bridgev2.ReactionSyncUser)
	for _, r := range c.stampReactions(ctx, msg) {
		u := users[r.Sender.Sender]
		if u == nil {
			u = &bridgev2.ReactionSyncUser{HasAllReactions: true}
			users[r.Sender.Sender] = u
		}
		u.Reactions = append(u.Reactions, r)
	}
	c.queue(&simplevent.ReactionSync{
		EventMeta: simplevent.EventMeta{
			Type:       bridgev2.RemoteEventReactionSync,
			LogContext: messageLogContext(msg),
			PortalKey:  key,
		},
		TargetMessage: MessageID(msg.ID),
		Reactions:     &bridgev2.ReactionSyncData{Users: users, HasAllUsers: true},
	})
}
