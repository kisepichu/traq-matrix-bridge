package connector

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"

	"github.com/kisepichu/traq-matrix-bridge/pkg/traq"
)

// maxMessagesPerFetch is the most messages traQ returns for one request.
const maxMessagesPerFetch = 200

var _ bridgev2.BackfillingNetworkAPI = (*TraQClient)(nil)

// encodeCursor is the pagination cursor for a backward fetch: the CreatedAt of the oldest message returned,
// in RFC 3339 with nanoseconds.
func encodeCursor(t time.Time) networkid.PaginationCursor {
	return networkid.PaginationCursor(t.UTC().Format(time.RFC3339Nano))
}

func decodeCursor(c networkid.PaginationCursor) (time.Time, error) {
	t, err := time.Parse(time.RFC3339Nano, string(c))
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid pagination cursor %q: %w", c, err)
	}
	return t, nil
}

// fetchQuery is the traQ query for params, newest first; newestMessages sets Limit and pages back.
//
//   - Forward: the newest messages, from the anchor's time on (inclusive, so a message at the anchor's
//     time is not lost; bridgev2's cutoffMessages and newestMessages' skipID drop the anchor itself).
//     Without an anchor the room is empty and there is no lower bound.
//   - Backward: the messages before the cursor, else before the anchor.
func fetchQuery(params bridgev2.FetchMessagesParams) (traq.MessagesQuery, error) {
	q := traq.MessagesQuery{Order: "desc"}
	switch {
	case params.Forward:
		if params.AnchorMessage != nil {
			since := params.AnchorMessage.Timestamp
			q.Since, q.Inclusive = &since, true
		}
	case params.Cursor != "":
		until, err := decodeCursor(params.Cursor)
		if err != nil {
			return traq.MessagesQuery{}, err
		}
		q.Until = &until
	case params.AnchorMessage != nil:
		until := params.AnchorMessage.Timestamp
		q.Until = &until
	}
	return q, nil
}

// messagePage is one GET /channels/{channelId}/messages of a channel: the messages and X-TRAQ-MORE.
type messagePage func(ctx context.Context, q traq.MessagesQuery) ([]traq.Message, bool, error)

// newestMessages returns the newest count messages of q's range, oldest first, and whether the range has
// more. It pages back from the newest: each next page ends at the oldest CreatedAt fetched so far,
// inclusive, so messages sharing that time are not skipped, and the ones already fetched are dropped by ID.
// Because the following pages are inclusive, q.Since, if set, must be inclusive too. The message skipID is
// left out and not counted. A page of nothing but already fetched messages (over 200 at one time) ends
// the paging instead of looping.
func newestMessages(ctx context.Context, fetch messagePage, q traq.MessagesQuery, count int, skipID string) ([]traq.Message, bool, error) {
	q.Order = "desc"
	var got []traq.Message // newest first
	seen := make(map[string]struct{})
	skipPending := skipID != ""
	if skipPending {
		seen[skipID] = struct{}{}
	}
	repeats := 0 // fetched messages the next page returns again, at its boundary time
	more := false
	for len(got) < count {
		q.Limit = min(count-len(got)+repeats, maxMessagesPerFetch)
		if skipPending {
			q.Limit = min(q.Limit+1, maxMessagesPerFetch)
		}
		page, pageMore, err := fetch(ctx, q)
		if err != nil {
			return nil, false, err
		}
		more = pageMore
		added := 0
		for _, m := range page {
			if m.ID == skipID {
				skipPending = false
			}
			if _, dup := seen[m.ID]; dup {
				continue
			}
			if len(got) == count {
				more = true
				break
			}
			seen[m.ID] = struct{}{}
			got = append(got, m)
			added++
		}
		if !more || added == 0 || len(got) == count {
			break
		}
		boundary := page[len(page)-1].CreatedAt
		q.Until, q.Inclusive = &boundary, true
		repeats = 0
		for _, m := range slices.Backward(got) {
			if !m.CreatedAt.Equal(boundary) {
				break
			}
			repeats++
		}
	}
	slices.Reverse(got)
	return got, more, nil
}

// backfillAssembler turns fetched messages into a FetchMessagesResponse. convert makes one message's
// BackfillMessage, given the IDs converted earlier in the batch; unread is GET /users/me/unread.
type backfillAssembler struct {
	convert func(ctx context.Context, msg *traq.Message, earlier map[string]struct{}) (*bridgev2.BackfillMessage, error)
	unread  func(ctx context.Context) ([]traq.UnreadChannel, error)
}

// assemble converts msgs (oldest first) of channelID. A message that is not bridged (ErrIgnoringRemoteEvent)
// or fails to convert is skipped and is no reply target for the later ones. The cursor is the oldest
// fetched message's time, whether or not it was bridged.
func (a *backfillAssembler) assemble(ctx context.Context, channelID string, forward bool, msgs []traq.Message, more bool) (*bridgev2.FetchMessagesResponse, error) {
	log := zerolog.Ctx(ctx)
	resp := &bridgev2.FetchMessagesResponse{
		Messages: make([]*bridgev2.BackfillMessage, 0, len(msgs)),
		HasMore:  more,
		Forward:  forward,
	}
	// Messages are sent in order, so each one's quote target is in the database by then if it is in the batch.
	earlier := make(map[string]struct{}, len(msgs))
	for i := range msgs {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		msg := &msgs[i]
		converted, err := a.convert(ctx, msg, earlier)
		if errors.Is(err, bridgev2.ErrIgnoringRemoteEvent) {
			continue
		} else if err != nil {
			log.Err(err).Str("traq_message_id", msg.ID).Msg("Skipping a message that failed to convert")
			continue
		}
		earlier[msg.ID] = struct{}{}
		resp.Messages = append(resp.Messages, converted)
	}
	if len(msgs) > 0 {
		resp.Cursor = encodeCursor(msgs[0].CreatedAt)
	}
	if forward {
		resp.MarkRead = len(resp.Messages) > 0 && a.channelRead(ctx, channelID)
	}
	return resp, nil
}

// FetchMessages returns a batch of history of the portal's channel, oldest first, already converted. A
// forward fetch brings the newest params.Count messages: those of a new room (max_initial_messages), or
// those since the newest bridged message (max_catchup_messages), contiguous with the live ones, since
// bridgev2 asks forward only once and does not follow HasMore. A backward fetch pages towards older
// messages; bridgev2 runs the backward queue only on homeservers with batch sending, which Synapse does not
// have.
func (c *TraQClient) FetchMessages(ctx context.Context, params bridgev2.FetchMessagesParams) (*bridgev2.FetchMessagesResponse, error) {
	if c.API == nil {
		return nil, errNotConnected
	}
	q, err := fetchQuery(params)
	if err != nil {
		return nil, err
	}
	var skipID string
	if params.Forward && params.AnchorMessage != nil {
		skipID = TraQMessageID(params.AnchorMessage.ID)
	}
	channelID := ChannelID(params.Portal.PortalKey)
	page := func(ctx context.Context, q traq.MessagesQuery) ([]traq.Message, bool, error) {
		return c.API.Messages(ctx, channelID, q)
	}
	msgs, more, err := newestMessages(ctx, page, q, params.Count, skipID)
	if err != nil {
		return nil, fmt.Errorf("get messages of channel %s: %w", channelID, err)
	}
	if params.Forward && len(msgs) > 0 {
		// A forward fetch brings the channel's newest messages: the poller re-reads it while recently active.
		c.noteActivity(channelID, msgs[len(msgs)-1].CreatedAt)
	}
	a := &backfillAssembler{
		convert: func(ctx context.Context, msg *traq.Message, earlier map[string]struct{}) (*bridgev2.BackfillMessage, error) {
			sender := c.sender(msg.UserID)
			intent, ok := params.Portal.GetIntentFor(ctx, sender, c.Login, bridgev2.RemoteEventMessage)
			if !ok {
				return nil, errors.New("the sender has no intent")
			}
			converted, err := c.convertMessageAfter(ctx, params.Portal, intent, msg, earlier)
			if err != nil {
				return nil, err
			}
			return &bridgev2.BackfillMessage{
				ConvertedMessage: converted,
				Sender:           sender,
				ID:               MessageID(msg.ID),
				Timestamp:        msg.CreatedAt,
				Reactions:        c.stampReactions(ctx, msg),
			}, nil
		},
		unread: c.API.Unread,
	}
	return a.assemble(ctx, channelID, params.Forward, msgs, more)
}

// channelRead reports whether the channel has no unread messages for this login's user. The unread list is
// read from traQ at each call, not from the snapshot Connect took: the snapshot is stale after a reconnect,
// when this runs for the catch-up, and one request per forward fetch (once per new portal, and per catch-up)
// is cheap. When the list cannot be read the channel counts as unread, so nothing is wrongly marked read.
func (a *backfillAssembler) channelRead(ctx context.Context, channelID string) bool {
	unread, err := a.unread(ctx)
	if err != nil {
		zerolog.Ctx(ctx).Warn().Err(err).Msg("Failed to get the unread channels, not marking the backfilled channel as read")
		return false
	}
	return !slices.ContainsFunc(unread, func(u traq.UnreadChannel) bool { return u.ChannelID == channelID })
}
