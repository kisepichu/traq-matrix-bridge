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

// fetchQuery is the traQ query for params.
//
//   - Forward with an anchor: the messages after it, oldest first. Without an anchor the room is empty and
//     the newest messages are wanted, which is what the descending order returns.
//   - Backward: the messages before the cursor, else before the anchor, newest first.
//
// A result in descending order is turned around by FetchMessages.
func fetchQuery(params bridgev2.FetchMessagesParams) traq.MessagesQuery {
	q := traq.MessagesQuery{Limit: min(params.Count, maxMessagesPerFetch), Order: "desc"}
	if params.Forward {
		if params.AnchorMessage != nil {
			since := params.AnchorMessage.Timestamp
			q.Since, q.Order = &since, "asc"
		}
		return q
	}
	if params.Cursor != "" {
		if until, err := decodeCursor(params.Cursor); err == nil {
			q.Until = &until
			return q
		}
	}
	if params.AnchorMessage != nil {
		until := params.AnchorMessage.Timestamp
		q.Until = &until
	}
	return q
}

// FetchMessages returns a batch of history of the portal's channel, oldest first, already converted. A
// forward fetch fills a new room with its newest messages (max_initial_messages) or catches up from the
// newest bridged message; a backward fetch pages towards older messages. bridgev2 runs the backward queue
// only on homeservers with batch sending, which Synapse does not have.
func (c *TraQClient) FetchMessages(ctx context.Context, params bridgev2.FetchMessagesParams) (*bridgev2.FetchMessagesResponse, error) {
	if c.API == nil {
		return nil, errNotConnected
	}
	if params.Cursor != "" {
		if _, err := decodeCursor(params.Cursor); err != nil {
			return nil, err
		}
	}
	channelID := ChannelID(params.Portal.PortalKey)
	q := fetchQuery(params)
	msgs, more, err := c.API.Messages(ctx, channelID, q)
	if err != nil {
		return nil, fmt.Errorf("get messages of channel %s: %w", channelID, err)
	}
	// traQ returns the messages in the requested order; bridgev2 wants oldest first.
	if q.Order == "desc" {
		slices.Reverse(msgs)
	}

	log := zerolog.Ctx(ctx)
	resp := &bridgev2.FetchMessagesResponse{
		Messages: make([]*bridgev2.BackfillMessage, 0, len(msgs)),
		HasMore:  more,
		Forward:  params.Forward,
	}
	// Messages are sent in order, so each one's quote target is in the database by then if it is in the batch.
	earlier := make(map[string]struct{}, len(msgs))
	for i := range msgs {
		msg := &msgs[i]
		sender := c.sender(msg.UserID)
		intent, ok := params.Portal.GetIntentFor(ctx, sender, c.Login, bridgev2.RemoteEventMessage)
		if !ok {
			log.Warn().Str("traq_message_id", msg.ID).Msg("Skipping a message whose sender has no intent")
			continue
		}
		converted, err := c.convertMessageAfter(ctx, params.Portal, intent, msg, earlier)
		if errors.Is(err, bridgev2.ErrIgnoringRemoteEvent) {
			continue
		} else if err != nil {
			log.Err(err).Str("traq_message_id", msg.ID).Msg("Skipping a message that failed to convert")
			continue
		}
		earlier[msg.ID] = struct{}{}
		resp.Messages = append(resp.Messages, &bridgev2.BackfillMessage{
			ConvertedMessage: converted,
			Sender:           sender,
			ID:               MessageID(msg.ID),
			Timestamp:        msg.CreatedAt,
			Reactions:        c.stampReactions(ctx, msg),
		})
	}
	if len(msgs) > 0 {
		resp.Cursor = encodeCursor(msgs[0].CreatedAt)
	}
	if params.Forward {
		// Task 12: record the newest of resp.Messages as the channel's last activity here, so that the
		// catch-up after the next reconnect starts after what this fetch brought in.
		resp.MarkRead = len(resp.Messages) > 0 && c.channelRead(ctx, channelID)
	}
	return resp, nil
}

// channelRead reports whether the channel has no unread messages for this login's user. The unread list is
// read from traQ at each call, not from the snapshot Connect took: the snapshot is stale after a reconnect,
// when this runs for the catch-up, and one request per forward fetch (once per new portal, and per catch-up)
// is cheap. When the list cannot be read the channel counts as unread, so nothing is wrongly marked read.
func (c *TraQClient) channelRead(ctx context.Context, channelID string) bool {
	unread, err := c.API.Unread(ctx)
	if err != nil {
		zerolog.Ctx(ctx).Warn().Err(err).Msg("Failed to get the unread channels, not marking the backfilled channel as read")
		return false
	}
	return !slices.ContainsFunc(unread, func(u traq.UnreadChannel) bool { return u.ChannelID == channelID })
}
