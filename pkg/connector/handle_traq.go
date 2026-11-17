package connector

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/simplevent"

	"github.com/kisepichu/traq-matrix-bridge/pkg/traq"
)

// eventQueueSize is the number of WebSocket events waiting for the worker before the reader blocks.
const eventQueueSize = 256

// enqueueEvent hands evt to the session's event worker. It runs on the WebSocket reader goroutine, which
// must keep reading for keepAlive's pongs to be processed, so the handling itself happens on the worker.
//
// When the queue is full, the reader blocks until there is room or ctx (the connection) ends. Nothing is
// dropped while the connection is up; if the worker stays stuck for longer than pingTimeout, the ping
// fails, the connection is recycled and this event is dropped with a warning. Messages missed that way, or
// while disconnected, are left to the catch-up after reconnecting.
func (c *TraQClient) enqueueEvent(ctx context.Context, queue chan<- traq.Event, evt traq.Event) {
	select {
	case queue <- evt:
		return
	default:
	}
	c.Login.Log.Warn().Str("event_type", evt.Type).Msg("The traQ event queue is full, waiting for the event worker")
	select {
	case queue <- evt:
	case <-ctx.Done():
		c.Login.Log.Warn().Str("event_type", evt.Type).Msg("Dropped a traQ event: the connection closed while the event queue was full")
	}
}

// eventWorker handles the queued events one at a time, in order, until ctx (the session) ends. Events still
// queued then are dropped.
func (c *TraQClient) eventWorker(ctx context.Context, queue <-chan traq.Event) {
	for {
		select {
		case <-ctx.Done():
			return
		case evt := <-queue:
			c.handleEvent(ctx, evt)
		}
	}
}

// handleEvent does the REST fetches an event needs and queues the resulting remote events. A failure is
// logged and does not stop the worker. Events without a handler are ignored.
func (c *TraQClient) handleEvent(ctx context.Context, evt traq.Event) {
	log := c.Login.Log.With().Str("event_type", evt.Type).Logger()
	ctx = log.WithContext(ctx)
	var err error
	switch evt.Type {
	case "MESSAGE_CREATED":
		err = c.handleMessageCreated(ctx, evt)
	case "MESSAGE_UPDATED":
		err = c.handleMessageUpdated(ctx, evt)
	case "MESSAGE_DELETED":
		err = c.handleMessageDeleted(ctx, evt)
	case "MESSAGE_STAMPED":
		err = c.handleMessageStamped(ctx, evt)
	case "MESSAGE_UNSTAMPED":
		err = c.handleMessageUnstamped(ctx, evt)
	case "MESSAGE_READ":
		err = c.handleMessageRead(ctx, evt)
	case "USER_UPDATED", "USER_ICON_UPDATED":
		err = c.handleUserUpdated(ctx, evt)
	case "CHANNEL_CREATED":
		err = c.handleChannelCreated(ctx, evt)
	case "CHANNEL_UPDATED", "CHANNEL_DELETED":
		err = c.syncSubscriptions(ctx)
	case "STAMP_CREATED", "STAMP_UPDATED":
		err = c.handleStampUpdated(ctx, evt)
	case "STAMP_DELETED":
		err = c.handleStampDeleted(evt)
	default:
		log.Debug().Msg("Ignoring traQ event")
		return
	}
	if err != nil && ctx.Err() == nil {
		log.Err(err).RawJSON("event_body", evt.Body).Msg("Failed to handle traQ event")
	}
}

func decodeEvent[T any](evt traq.Event) (T, error) {
	var body T
	if err := evt.Decode(&body); err != nil {
		return body, fmt.Errorf("decode %s: %w", evt.Type, err)
	}
	return body, nil
}

func messageLogContext(msg *traq.Message) func(zerolog.Context) zerolog.Context {
	return func(c zerolog.Context) zerolog.Context {
		return c.Str("traq_message_id", msg.ID).Str("traq_channel_id", msg.ChannelID)
	}
}

func (c *TraQClient) handleMessageCreated(ctx context.Context, evt traq.Event) error {
	body, err := decodeEvent[traq.MessageCreatedEvent](evt)
	if err != nil {
		return err
	}
	msg, err := c.API.Message(ctx, body.ID)
	if err != nil {
		return fmt.Errorf("get message %s: %w", body.ID, err)
	}
	c.noteMessageAt(msg.CreatedAt)
	key, ok := c.portalKeyFor(msg.ChannelID)
	if !ok {
		zerolog.Ctx(ctx).Debug().Str("traq_channel_id", msg.ChannelID).Msg("Ignoring message in a channel without a portal")
		return nil
	}
	c.queue(&simplevent.Message[*traq.Message]{
		EventMeta: simplevent.EventMeta{
			Type:       bridgev2.RemoteEventMessage,
			LogContext: messageLogContext(msg),
			PortalKey:  key,
			Sender:     c.sender(msg.UserID),
			// A public channel's portal is created by its resync; a DM's portal by its first message.
			CreatePortal: key.Receiver != "",
			Timestamp:    msg.CreatedAt,
		},
		Data:               msg,
		ID:                 MessageID(msg.ID),
		ConvertMessageFunc: c.convertMessage,
	})
	return nil
}

// noteMessageAt records the newest message time seen, where a catch-up after a reconnect can start.
func (c *TraQClient) noteMessageAt(t time.Time) {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	if t.After(c.lastMessageAt) {
		c.lastMessageAt = t
	}
}

func (c *TraQClient) handleMessageUpdated(ctx context.Context, evt traq.Event) error {
	body, err := decodeEvent[traq.IDEvent](evt)
	if err != nil {
		return err
	}
	msg, err := c.API.Message(ctx, body.ID)
	if err != nil {
		return fmt.Errorf("get message %s: %w", body.ID, err)
	}
	key, ok := c.portalKeyFor(msg.ChannelID)
	if !ok {
		return nil
	}
	c.queue(&simplevent.Message[*traq.Message]{
		EventMeta: simplevent.EventMeta{
			Type:       bridgev2.RemoteEventEdit,
			LogContext: messageLogContext(msg),
			PortalKey:  key,
			Sender:     c.sender(msg.UserID),
			Timestamp:  msg.UpdatedAt,
		},
		Data:            msg,
		TargetMessage:   MessageID(msg.ID),
		ConvertEditFunc: c.convertEdit,
	})
	return nil
}

// bridgedMessage returns the first part of a bridged message in a portal of this login (a public channel or
// this login's DM), or nil if it was not bridged.
func (c *TraQClient) bridgedMessage(ctx context.Context, messageID string) (*database.Message, error) {
	msg, err := c.Login.Bridge.DB.Message.GetFirstPartByID(ctx, c.Login.ID, MessageID(messageID))
	if err != nil {
		return nil, fmt.Errorf("look up message %s: %w", messageID, err)
	}
	if msg == nil {
		zerolog.Ctx(ctx).Debug().Str("traq_message_id", messageID).Msg("Ignoring event for a message that was not bridged")
	}
	return msg, nil
}

func (c *TraQClient) handleMessageDeleted(ctx context.Context, evt traq.Event) error {
	body, err := decodeEvent[traq.IDEvent](evt)
	if err != nil {
		return err
	}
	target, err := c.bridgedMessage(ctx, body.ID)
	if err != nil || target == nil {
		return err
	}
	c.queue(&simplevent.MessageRemove{
		EventMeta: simplevent.EventMeta{
			Type:       bridgev2.RemoteEventMessageRemove,
			LogContext: func(lc zerolog.Context) zerolog.Context { return lc.Str("traq_message_id", body.ID) },
			PortalKey:  target.Room,
			Sender:     c.sender(TraQUserID(target.SenderID)),
		},
		TargetMessage: target.ID,
	})
	return nil
}

func (c *TraQClient) handleMessageStamped(ctx context.Context, evt traq.Event) error {
	body, err := decodeEvent[traq.MessageStampedEvent](evt)
	if err != nil {
		return err
	}
	target, err := c.bridgedMessage(ctx, body.MessageID)
	if err != nil || target == nil {
		return err
	}
	emoji, ok := c.reactionKey(ctx, body.StampID)
	if !ok {
		return fmt.Errorf("unknown stamp %s", body.StampID)
	}
	// A count increase (the same user stamping again) has the same EmojiID, and bridgev2 drops it as a
	// duplicate.
	c.queue(&simplevent.Reaction{
		EventMeta: simplevent.EventMeta{
			Type:       bridgev2.RemoteEventReaction,
			LogContext: stampLogContext(body.MessageID, body.StampID),
			PortalKey:  target.Room,
			Sender:     c.sender(body.UserID),
			Timestamp:  body.CreatedAt,
		},
		TargetMessage: target.ID,
		EmojiID:       EmojiID(body.StampID),
		Emoji:         emoji,
	})
	return nil
}

func (c *TraQClient) handleMessageUnstamped(ctx context.Context, evt traq.Event) error {
	body, err := decodeEvent[traq.MessageUnstampedEvent](evt)
	if err != nil {
		return err
	}
	target, err := c.bridgedMessage(ctx, body.MessageID)
	if err != nil || target == nil {
		return err
	}
	c.queue(&simplevent.Reaction{
		EventMeta: simplevent.EventMeta{
			Type:       bridgev2.RemoteEventReactionRemove,
			LogContext: stampLogContext(body.MessageID, body.StampID),
			PortalKey:  target.Room,
			Sender:     c.sender(body.UserID),
		},
		TargetMessage: target.ID,
		EmojiID:       EmojiID(body.StampID),
	})
	return nil
}

func stampLogContext(messageID, stampID string) func(zerolog.Context) zerolog.Context {
	return func(c zerolog.Context) zerolog.Context {
		return c.Str("traq_message_id", messageID).Str("traq_stamp_id", stampID)
	}
}

// handleMessageRead moves the read receipt of this login's user: traQ sends MESSAGE_READ (with the channel
// ID) when the user has read a channel.
func (c *TraQClient) handleMessageRead(ctx context.Context, evt traq.Event) error {
	body, err := decodeEvent[traq.IDEvent](evt)
	if err != nil {
		return err
	}
	key, ok := c.portalKeyFor(body.ID)
	me := c.self()
	if !ok || me == nil {
		return nil
	}
	c.queue(&simplevent.Receipt{
		EventMeta: simplevent.EventMeta{
			Type:       bridgev2.RemoteEventReadReceipt,
			LogContext: func(lc zerolog.Context) zerolog.Context { return lc.Str("traq_channel_id", body.ID) },
			PortalKey:  key,
			Sender:     c.selfSender(me),
		},
		ReadUpTo: time.Now(),
	})
	return nil
}

// handleUserUpdated refreshes the cached user and the profile of its ghost, if the ghost exists.
func (c *TraQClient) handleUserUpdated(ctx context.Context, evt traq.Event) error {
	body, err := decodeEvent[traq.IDEvent](evt)
	if err != nil {
		return err
	}
	user, err := c.API.User(ctx, body.ID)
	if err != nil {
		return fmt.Errorf("get user %s: %w", body.ID, err)
	}
	c.users.put(user)
	ghost, err := c.Login.Bridge.GetExistingGhostByID(ctx, UserID(user.ID))
	if err != nil {
		return fmt.Errorf("get ghost %s: %w", user.ID, err)
	}
	if ghost == nil {
		return nil
	}
	info, err := c.GetUserInfo(ctx, ghost)
	if err != nil {
		return err
	}
	ghost.UpdateInfo(ctx, info)
	return nil
}

// handleChannelCreated adds a new DM to the channel tree and the targets, so that its first message creates
// the portal. A new public channel gets a portal only once it is subscribed, which the subscription sync sees.
func (c *TraQClient) handleChannelCreated(ctx context.Context, evt traq.Event) error {
	body, err := decodeEvent[traq.ChannelCreatedEvent](evt)
	if err != nil {
		return err
	}
	if body.DMUserID == "" {
		zerolog.Ctx(ctx).Debug().Str("traq_channel_id", body.ID).Msg("Ignoring new public channel")
		return nil
	}
	ch := channelInfo{ID: body.ID, DMUserID: body.DMUserID, Level: dmLevel}
	c.syncMu.Lock()
	defer c.syncMu.Unlock()
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	if c.channels == nil {
		return nil
	}
	if _, ok := c.targets[ch.ID]; ok {
		return nil
	}
	c.channels = c.channels.withDM(ch)
	c.targets[ch.ID] = ch
	return nil
}

func (c *TraQClient) handleStampUpdated(ctx context.Context, evt traq.Event) error {
	body, err := decodeEvent[traq.IDEvent](evt)
	if err != nil {
		return err
	}
	st, err := c.API.Stamp(ctx, body.ID)
	if traq.IsStatus(err, http.StatusNotFound) {
		c.Main.Stamps.Delete(body.ID)
		return nil
	}
	if err != nil {
		return fmt.Errorf("get stamp %s: %w", body.ID, err)
	}
	c.Main.Stamps.Put(stampFromTraQ(st))
	return nil
}

func (c *TraQClient) handleStampDeleted(evt traq.Event) error {
	body, err := decodeEvent[traq.IDEvent](evt)
	if err != nil {
		return err
	}
	c.Main.Stamps.Delete(body.ID)
	return nil
}
