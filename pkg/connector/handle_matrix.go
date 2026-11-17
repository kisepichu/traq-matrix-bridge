package connector

import (
	"context"
	"errors"
	"fmt"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"github.com/kisepichu/traq-matrix-bridge/pkg/msgconv"
	"github.com/kisepichu/traq-matrix-bridge/pkg/stampconv"
	"github.com/kisepichu/traq-matrix-bridge/pkg/traq"
)

var (
	_ bridgev2.EditHandlingNetworkAPI        = (*TraQClient)(nil)
	_ bridgev2.RedactionHandlingNetworkAPI   = (*TraQClient)(nil)
	_ bridgev2.ReactionHandlingNetworkAPI    = (*TraQClient)(nil)
	_ bridgev2.ReadReceiptHandlingNetworkAPI = (*TraQClient)(nil)
)

var (
	errEditNotOwn = bridgev2.WrapErrorInStatus(errors.New("traQ only allows editing your own messages")).
			WithIsCertain(true).WithErrorAsMessage().WithSendNotice(true).WithErrorReason(event.MessageStatusUnsupported)
	errDeleteNotOwn = bridgev2.WrapErrorInStatus(errors.New("traQ only allows deleting your own messages")).
			WithIsCertain(true).WithErrorAsMessage().WithSendNotice(true).WithErrorReason(event.MessageStatusNoPermission)
	errUnstampNotOwn = bridgev2.WrapErrorInStatus(errors.New("traQ only allows removing your own stamps")).
				WithIsCertain(true).WithErrorAsMessage().WithSendNotice(true).WithErrorReason(event.MessageStatusNoPermission)
)

// roomFeatures are the capabilities of every traQ room. traQ has no file messages from Matrix in v1:
// file msgtypes are missing from File, so bridgev2 rejects them with ErrUnsupportedMessageType.
var roomFeatures = &event.RoomFeatures{
	Formatting: event.FormattingFeatureMap{
		event.FmtBold:          event.CapLevelFullySupported,
		event.FmtItalic:        event.CapLevelFullySupported,
		event.FmtStrikethrough: event.CapLevelFullySupported,
		event.FmtInlineCode:    event.CapLevelFullySupported,
		event.FmtCodeBlock:     event.CapLevelFullySupported,
		event.FmtBlockquote:    event.CapLevelFullySupported,
		event.FmtUnorderedList: event.CapLevelFullySupported,
		event.FmtOrderedList:   event.CapLevelFullySupported,
		event.FmtHeaders:       event.CapLevelFullySupported,
		event.FmtInlineLink:    event.CapLevelFullySupported,
		event.FmtUserLink:      event.CapLevelFullySupported,
		event.FmtSpoiler:       event.CapLevelFullySupported,
	},
	MaxTextLength: msgconv.MaxTraQLength,
	Reply:         event.CapLevelFullySupported,
	Edit:          event.CapLevelFullySupported,
	EditMaxCount:  -1,
	Delete:        event.CapLevelFullySupported,
	Reaction:      event.CapLevelFullySupported,
	ReactionCount: -1,
	ReadReceipts:  true,
}

func (c *TraQClient) GetCapabilities(context.Context, *bridgev2.Portal) *event.RoomFeatures {
	return roomFeatures
}

// requireMe returns the logged-in traQ user, which every handler needs.
func (c *TraQClient) requireMe() (*traq.Me, error) {
	if c.API == nil {
		return nil, bridgev2.ErrNotLoggedIn
	}
	me := c.self()
	if me == nil {
		return nil, errNotConnected
	}
	return me, nil
}

func isTextMsgType(t event.MessageType) bool {
	return t == event.MsgText || t == event.MsgEmote || t == event.MsgNotice
}

// toTraQ converts Matrix content to traQ Markdown. replyTo is the traQ message ID of the reply target, ""
// if none. traQ has no emote, so an m.emote is posted as its plain body.
func (c *TraQClient) toTraQ(ctx context.Context, content *event.MessageEventContent, replyTo string) (string, error) {
	text, err := msgconv.ToTraQ(ctx, msgconv.ToTraQInput{
		Body:          content.Body,
		Format:        string(content.Format),
		FormattedBody: content.FormattedBody,
		ReplyTo:       replyTo,
		ServerURL:     c.API.ServerURL(),
	}, &traqUserResolver{ctx: ctx, c: c})
	if errors.Is(err, msgconv.ErrTooLong) {
		return "", bridgev2.WrapErrorInStatus(err).WithSendNotice(true).WithIsCertain(true).WithErrorAsMessage().WithStatus(event.MessageStatusFail)
	}
	return text, err
}

// HandleMatrixMessage posts a text message. The echo of the post (MESSAGE_CREATED) is handled by the
// portal's event loop after the returned row is saved, and dropped there as a duplicate of its ID.
func (c *TraQClient) HandleMatrixMessage(ctx context.Context, msg *bridgev2.MatrixMessage) (*bridgev2.MatrixMessageResponse, error) {
	if !isTextMsgType(msg.Content.MsgType) {
		return nil, bridgev2.ErrUnsupportedMessageType
	}
	me, err := c.requireMe()
	if err != nil {
		return nil, err
	}
	var replyTo string
	if msg.ReplyTo != nil {
		replyTo = TraQMessageID(msg.ReplyTo.ID)
	}
	content, err := c.toTraQ(ctx, msg.Content, replyTo)
	if err != nil {
		return nil, err
	}
	channelID := ChannelID(msg.Portal.PortalKey)
	c.view.Touch(ctx, channelID)
	posted, err := c.API.PostMessage(ctx, channelID, content)
	if err != nil {
		return nil, fmt.Errorf("post message: %w", err)
	}
	return &bridgev2.MatrixMessageResponse{DB: &database.Message{
		ID:        MessageID(posted.ID),
		SenderID:  UserID(me.ID),
		Timestamp: posted.CreatedAt,
		// The hash of the content as traQ stored it (with embeds), which MESSAGE_UPDATED compares against.
		Metadata: &MessageMetadata{ContentHash: ContentHash(posted.Content)},
	}}, nil
}

// HandleMatrixEdit edits one of the user's messages. The reply quote line is rebuilt from the stored reply
// target. The content hash is updated from the edited message as traQ stored it, so the echo
// (MESSAGE_UPDATED) is recognized as already bridged; bridgev2 saves EditTarget after this returns.
func (c *TraQClient) HandleMatrixEdit(ctx context.Context, msg *bridgev2.MatrixEdit) error {
	me, err := c.requireMe()
	if err != nil {
		return err
	}
	target := msg.EditTarget
	if TraQUserID(target.SenderID) != me.ID {
		return errEditNotOwn
	}
	if !isTextMsgType(msg.Content.MsgType) {
		return bridgev2.ErrUnsupportedMessageType
	}
	content, err := c.toTraQ(ctx, msg.Content, TraQMessageID(target.ReplyTo.MessageID))
	if err != nil {
		return err
	}
	messageID := TraQMessageID(target.ID)
	if err := c.API.EditMessage(ctx, messageID, content); err != nil {
		return fmt.Errorf("edit message: %w", err)
	}
	stored := content
	if edited, err := c.API.Message(ctx, messageID); err != nil {
		// The edit went through; without the stored content the echo may be bridged as one more edit.
		zerolog.Ctx(ctx).Warn().Err(err).Str("traq_message_id", messageID).Msg("Failed to read back the edited traQ message")
	} else {
		stored = edited.Content
	}
	meta, _ := target.Metadata.(*MessageMetadata)
	if meta == nil {
		meta = &MessageMetadata{}
		target.Metadata = meta
	}
	meta.ContentHash = ContentHash(stored)
	return nil
}

// HandleMatrixMessageRemove deletes one of the user's messages; traQ does not let users delete others'.
func (c *TraQClient) HandleMatrixMessageRemove(ctx context.Context, msg *bridgev2.MatrixMessageRemove) error {
	me, err := c.requireMe()
	if err != nil {
		return err
	}
	if TraQUserID(msg.TargetMessage.SenderID) != me.ID {
		return errDeleteNotOwn
	}
	if err := c.API.DeleteMessage(ctx, TraQMessageID(msg.TargetMessage.ID)); err != nil {
		return fmt.Errorf("delete message: %w", err)
	}
	return nil
}

// PreHandleMatrixReaction finds the stamp of the reaction key. The stamp ID is the EmojiID, so a user can
// put any number of different stamps on a message.
func (c *TraQClient) PreHandleMatrixReaction(_ context.Context, msg *bridgev2.MatrixReaction) (bridgev2.MatrixReactionPreResponse, error) {
	me, err := c.requireMe()
	if err != nil {
		return bridgev2.MatrixReactionPreResponse{}, err
	}
	key := msg.Content.RelatesTo.Key
	stamp, err := c.Main.Stamps.Resolve(key)
	if err != nil {
		return bridgev2.MatrixReactionPreResponse{}, bridgev2.WrapErrorInStatus(fmt.Errorf("%w: %s", err, key)).
			WithIsCertain(true).WithErrorAsMessage().WithSendNotice(true).
			WithStatus(event.MessageStatusFail).WithErrorReason(event.MessageStatusUnsupported)
	}
	return bridgev2.MatrixReactionPreResponse{
		SenderID: UserID(me.ID),
		EmojiID:  EmojiID(stamp.ID),
		Emoji:    stampconv.ReactionKey(stamp),
	}, nil
}

// HandleMatrixReaction adds the stamp. traQ stamps a whole message, and stamps from traQ go to the first part
// of a message, so the reaction is saved on the first part too: the echo (MESSAGE_STAMPED) then finds it and
// is dropped as a duplicate even when the reaction was on a file part.
func (c *TraQClient) HandleMatrixReaction(ctx context.Context, msg *bridgev2.MatrixReaction) (*database.Reaction, error) {
	target := msg.TargetMessage
	if err := c.API.AddStamp(ctx, TraQMessageID(target.ID), StampID(msg.PreHandleResp.EmojiID)); err != nil {
		return nil, fmt.Errorf("add stamp: %w", err)
	}
	reaction := &database.Reaction{Emoji: msg.PreHandleResp.Emoji}
	first, err := c.Login.Bridge.DB.Message.GetFirstPartByID(ctx, msg.Portal.Receiver, target.ID)
	if err != nil {
		zerolog.Ctx(ctx).Warn().Err(err).Msg("Failed to look up the first part of the stamped message")
	} else if first != nil && first.PartID != target.PartID {
		reaction.MessageID, reaction.MessagePartID = first.ID, first.PartID
	}
	return reaction, nil
}

// HandleMatrixReactionRemove removes the stamp, which traQ allows only for the user's own stamps.
func (c *TraQClient) HandleMatrixReactionRemove(ctx context.Context, msg *bridgev2.MatrixReactionRemove) error {
	me, err := c.requireMe()
	if err != nil {
		return err
	}
	r := msg.TargetReaction
	if TraQUserID(r.SenderID) != me.ID {
		return errUnstampNotOwn
	}
	if err := c.API.RemoveStamp(ctx, TraQMessageID(r.MessageID), StampID(r.EmojiID)); err != nil {
		return fmt.Errorf("remove stamp: %w", err)
	}
	return nil
}

// HandleMatrixReadReceipt marks the whole channel read (traQ has no per-message read state) and makes it
// the viewed channel.
func (c *TraQClient) HandleMatrixReadReceipt(ctx context.Context, msg *bridgev2.MatrixReadReceipt) error {
	if c.API == nil {
		return bridgev2.ErrNotLoggedIn
	}
	channelID := ChannelID(msg.Portal.PortalKey)
	c.view.Touch(ctx, channelID)
	if err := c.API.ReadChannel(ctx, channelID); err != nil {
		return fmt.Errorf("mark channel %s read: %w", channelID, err)
	}
	return nil
}

// traqUserResolver maps the Matrix users in pills to traQ users: ghosts by their ID, and Matrix users of
// this bridge by their login. The ctx of the conversion is captured because the interface has none.
type traqUserResolver struct {
	ctx context.Context
	c   *TraQClient
}

func (r *traqUserResolver) TraQUserName(mxid id.UserID) (string, bool) {
	userID, ok := r.traqUserID(mxid)
	if !ok {
		return "", false
	}
	u, err := r.c.users.Get(r.ctx, r.c.API, userID)
	if err != nil {
		zerolog.Ctx(r.ctx).Warn().Err(err).Str("traq_user_id", userID).Msg("Failed to get the traQ user of a mention")
		return "", false
	}
	return u.Name, true
}

func (r *traqUserResolver) traqUserID(mxid id.UserID) (string, bool) {
	if mxid == "" {
		return "", false
	}
	br := r.c.Login.Bridge
	if ghostID, ok := br.Matrix.ParseGhostMXID(mxid); ok {
		return TraQUserID(ghostID), true
	}
	if mxid == r.c.Login.UserMXID {
		return string(r.c.Login.ID), true
	}
	user, err := br.GetExistingUserByMXID(r.ctx, mxid)
	if err != nil {
		zerolog.Ctx(r.ctx).Warn().Err(err).Stringer("user_id", mxid).Msg("Failed to look up a mentioned Matrix user")
		return "", false
	}
	if user == nil {
		return "", false
	}
	if login := user.GetDefaultLogin(); login != nil {
		return string(login.ID), true
	}
	return "", false
}
