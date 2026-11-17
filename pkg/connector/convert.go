package connector

import (
	"context"
	"errors"
	"fmt"
	"html"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"github.com/kisepichu/traq-matrix-bridge/pkg/msgconv"
	"github.com/kisepichu/traq-matrix-bridge/pkg/stampconv"
	"github.com/kisepichu/traq-matrix-bridge/pkg/traq"
)

// fileDownloadTimeout bounds one file download and re-upload.
const fileDownloadTimeout = 5 * time.Minute

// fileHTTPClient downloads files. It has no total timeout, unlike httpClient, because a large file on a
// slow link can take longer than httpTimeout; each download has a fileDownloadTimeout deadline instead.
var fileHTTPClient = &http.Client{}

// errFileTooLarge: the file is larger than the homeserver accepts, so it is linked instead.
var errFileTooLarge = errors.New("the file is larger than the homeserver's upload limit")

// textPartID is the part ID of the text part; file parts are "file:<fileID>".
const textPartID networkid.PartID = ""

func filePartID(fileID string) networkid.PartID { return networkid.PartID("file:" + fileID) }

func fileURL(origin, fileID string) string { return origin + "/files/" + fileID }

// mediaUploader re-uploads one traQ file to Matrix and returns the content of its part.
type mediaUploader interface {
	upload(ctx context.Context, fileID string) (*event.MessageEventContent, error)
}

// convertMessage builds the Matrix content for a traQ message: one text part (if the body is not empty)
// followed by one part per file. ReplyTo is set from msgconv's ReplyTo.
func (c *TraQClient) convertMessage(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, msg *traq.Message) (*bridgev2.ConvertedMessage, error) {
	res := msgconv.ToMatrix(c.toMatrixInput(msg.Content), &resolver{ctx: ctx, c: c, portal: portal})
	up := &portalUploader{c: c, portal: portal, intent: intent}
	return buildMessage(ctx, res, msg.Content, c.origin(), up)
}

// convertEdit replaces the text part of an edited message. v1 neither adds nor removes file parts: a file new
// to the message is linked in the text. The reply target cannot change, so only a quote of the original
// reply target is turned into the reply; other quotes stay links.
func (c *TraQClient) convertEdit(ctx context.Context, portal *bridgev2.Portal, _ bridgev2.MatrixAPI, existing []*database.Message, msg *traq.Message) (*bridgev2.ConvertedEdit, error) {
	r := &resolver{ctx: ctx, c: c, portal: portal, edit: true}
	for _, part := range existing {
		if part.ReplyTo.MessageID != "" {
			r.editReplyTo = part.ReplyTo.MessageID
			break
		}
	}
	res := msgconv.ToMatrix(c.toMatrixInput(msg.Content), r)
	return buildEdit(existing, res, msg.Content, c.origin())
}

func (c *TraQClient) toMatrixInput(content string) msgconv.ToMatrixInput {
	in := msgconv.ToMatrixInput{Content: content, ServerURL: c.API.ServerURL(), SelfMXID: c.Login.UserMXID}
	if me := c.self(); me != nil {
		in.SelfGroups = me.Groups
	}
	return in
}

// origin is the traQ origin without a trailing slash.
func (c *TraQClient) origin() string { return c.API.ServerURL().String() }

// buildMessage assembles the parts of a new message. A file that cannot be uploaded (too large, or the
// download or upload failed) keeps its traQ URL at the end of the text instead.
func buildMessage(ctx context.Context, res msgconv.ToMatrixResult, content, origin string, up mediaUploader) (*bridgev2.ConvertedMessage, error) {
	files := make([]*bridgev2.ConvertedMessagePart, 0, len(res.FileIDs))
	var links []string
	for _, fileID := range res.FileIDs {
		fc, err := up.upload(ctx, fileID)
		if err != nil {
			if !errors.Is(err, errFileTooLarge) {
				zerolog.Ctx(ctx).Warn().Err(err).Str("traq_file_id", fileID).Msg("Failed to bridge a traQ file, linking it instead")
			}
			links = append(links, fileURL(origin, fileID))
			continue
		}
		files = append(files, &bridgev2.ConvertedMessagePart{ID: filePartID(fileID), Type: event.EventMessage, Content: fc})
	}
	cm := &bridgev2.ConvertedMessage{Parts: make([]*bridgev2.ConvertedMessagePart, 0, len(files)+1)}
	if text := textContent(res, links); text != nil {
		cm.Parts = append(cm.Parts, textPart(text, content))
	}
	cm.Parts = append(cm.Parts, files...)
	if len(cm.Parts) == 0 {
		return nil, bridgev2.ErrIgnoringRemoteEvent
	}
	if res.ReplyTo != "" {
		cm.ReplyTo = &networkid.MessageOptionalPartID{MessageID: MessageID(res.ReplyTo)}
	}
	return cm, nil
}

// buildEdit computes the edit of the text part. Content the text part was last bridged from is ignored, so
// the same edit seen by several logins is bridged once. Files that have no part are linked in the text.
func buildEdit(existing []*database.Message, res msgconv.ToMatrixResult, content, origin string) (*bridgev2.ConvertedEdit, error) {
	var text *database.Message
	hasPart := make(map[networkid.PartID]bool, len(existing))
	for _, part := range existing {
		hasPart[part.PartID] = true
		if part.PartID == textPartID {
			text = part
		}
	}
	if text != nil {
		if meta, ok := text.Metadata.(*MessageMetadata); ok && meta.ContentHash == ContentHash(content) {
			return nil, bridgev2.ErrIgnoringRemoteEvent
		}
	}
	var links []string
	for _, fileID := range res.FileIDs {
		if !hasPart[filePartID(fileID)] {
			links = append(links, fileURL(origin, fileID))
		}
	}
	newText := textContent(res, links)
	switch {
	case newText == nil && text == nil:
		return nil, bridgev2.ErrIgnoringRemoteEvent
	case newText == nil:
		return &bridgev2.ConvertedEdit{DeletedParts: []*database.Message{text}}, nil
	case text == nil:
		return &bridgev2.ConvertedEdit{AddedParts: &bridgev2.ConvertedMessage{Parts: []*bridgev2.ConvertedMessagePart{textPart(newText, content)}}}, nil
	default:
		// ToEditPart stores the new ContentHash in the existing part's metadata.
		return &bridgev2.ConvertedEdit{ModifiedParts: []*bridgev2.ConvertedEditPart{textPart(newText, content).ToEditPart(text)}}, nil
	}
}

// textContent is the content of the text part: the converted body followed by links, one per line. It is
// nil when both are empty.
func textContent(res msgconv.ToMatrixResult, links []string) *event.MessageEventContent {
	body, formatted := res.Body, res.HTML
	for _, link := range links {
		if body != "" {
			body += "\n"
			formatted += "<br>"
		}
		escaped := html.EscapeString(link)
		body += link
		formatted += `<a href="` + escaped + `">` + escaped + `</a>`
	}
	if body == "" {
		return nil
	}
	return &event.MessageEventContent{
		MsgType:       event.MsgText,
		Body:          body,
		Format:        event.FormatHTML,
		FormattedBody: formatted,
		Mentions:      &event.Mentions{UserIDs: res.Mentions},
	}
}

func textPart(content *event.MessageEventContent, traqContent string) *bridgev2.ConvertedMessagePart {
	return &bridgev2.ConvertedMessagePart{
		ID:         textPartID,
		Type:       event.EventMessage,
		Content:    content,
		DBMetadata: &MessageMetadata{ContentHash: ContentHash(traqContent)},
	}
}

// portalUploader downloads traQ files and uploads them to the portal's room (encrypted if the room is).
type portalUploader struct {
	c      *TraQClient
	portal *bridgev2.Portal
	intent bridgev2.MatrixAPI
}

func (u *portalUploader) upload(ctx context.Context, fileID string) (*event.MessageEventContent, error) {
	meta, err := u.c.API.FileMeta(ctx, fileID)
	if err != nil {
		return nil, fmt.Errorf("get file meta: %w", err)
	}
	limit := u.c.Main.maxFileSize()
	if meta.Size > limit {
		return nil, errFileTooLarge
	}
	ctx, cancel := context.WithTimeout(ctx, fileDownloadTimeout)
	defer cancel()
	body, err := u.c.Files.DownloadFile(ctx, fileID)
	if err != nil {
		return nil, fmt.Errorf("download file: %w", err)
	}
	defer body.Close()
	mime := meta.Mime
	if mime == "" {
		mime = "application/octet-stream"
	}
	url, file, err := u.intent.UploadMediaStream(ctx, u.portal.MXID, meta.Size, false, func(w io.Writer) (*bridgev2.FileStreamResult, error) {
		// The limit stops a body longer than announced from filling memory; the upload then rejects it.
		if _, err := io.Copy(w, io.LimitReader(body, limit+1)); err != nil {
			return nil, err
		}
		return &bridgev2.FileStreamResult{FileName: meta.Name, MimeType: mime}, nil
	})
	if err != nil {
		return nil, fmt.Errorf("upload file: %w", err)
	}
	content := &event.MessageEventContent{
		MsgType: event.MsgFile,
		Body:    meta.Name,
		Info:    &event.FileInfo{MimeType: mime, Size: int(meta.Size)},
	}
	if strings.HasPrefix(mime, "image/") {
		content.MsgType = event.MsgImage
	}
	if file != nil {
		file.URL = url
		content.File = file
	} else {
		content.URL = url
	}
	return content, nil
}

// resolver implements msgconv.Resolver for one conversion. It reads only the bridge's caches and database.
type resolver struct {
	ctx    context.Context
	c      *TraQClient
	portal *bridgev2.Portal
	// edit restricts CanReplyTo to editReplyTo, the reply target of the message being edited.
	edit        bool
	editReplyTo networkid.MessageID
}

var _ msgconv.Resolver = (*resolver)(nil)

func (r *resolver) UserMXID(traqUserID string) (id.UserID, bool) {
	if me := r.c.self(); me != nil && me.ID == traqUserID {
		return r.c.Login.UserMXID, true
	}
	return r.c.Login.Bridge.Matrix.FormatGhostMXID(UserID(traqUserID)), true
}

func (r *resolver) ChannelLink(channelID string) (string, bool) {
	ch, ok := r.c.lookupChannel(channelID)
	if !ok {
		return "", false
	}
	portal, err := r.c.Login.Bridge.GetExistingPortalByKey(r.ctx, r.c.portalKey(ch))
	if err != nil || portal == nil || portal.MXID == "" {
		return "", false
	}
	return portal.MXID.URI(r.c.Login.Bridge.Matrix.ServerName()).MatrixToURL(), true
}

func (r *resolver) MessageLink(messageID string) (string, bool) {
	msg := r.bridged(messageID, r.c.Login.ID)
	if msg == nil {
		return "", false
	}
	roomID := r.portal.MXID
	if msg.Room != r.portal.PortalKey {
		portal, err := r.c.Login.Bridge.GetExistingPortalByKey(r.ctx, msg.Room)
		if err != nil || portal == nil {
			return "", false
		}
		roomID = portal.MXID
	}
	if roomID == "" {
		return "", false
	}
	return roomID.EventURI(msg.MXID, r.c.Login.Bridge.Matrix.ServerName()).MatrixToURL(), true
}

func (r *resolver) CanReplyTo(messageID string) bool {
	if r.edit {
		return r.editReplyTo != "" && MessageID(messageID) == r.editReplyTo
	}
	msg := r.bridged(messageID, r.portal.Receiver)
	return msg != nil && msg.Room == r.portal.PortalKey
}

// bridged returns the first part of a bridged message visible to receiver, or nil.
func (r *resolver) bridged(messageID string, receiver networkid.UserLoginID) *database.Message {
	msg, err := r.c.Login.Bridge.DB.Message.GetFirstPartByID(r.ctx, receiver, MessageID(messageID))
	if err != nil {
		zerolog.Ctx(r.ctx).Warn().Err(err).Str("traq_message_id", messageID).Msg("Failed to look up a bridged message")
		return nil
	}
	return msg
}

// sender is the EventSender of a traQ user. IsFromMe is set for the logged-in user. SenderLogin is always
// the login with that user's ID: bridgev2 uses it, in shared portals, for the double puppet of another
// login of this bridge and falls back to the ghost when there is no such login.
func (c *TraQClient) sender(userID string) bridgev2.EventSender {
	me := c.self()
	return bridgev2.EventSender{
		IsFromMe:    me != nil && me.ID == userID,
		SenderLogin: LoginID(userID),
		Sender:      UserID(userID),
	}
}

// portalKeyFor returns the portal key of a channel that has a portal for this login (a DM, or a public
// channel subscribed at level >= 1), and false for any other channel.
func (c *TraQClient) portalKeyFor(channelID string) (networkid.PortalKey, bool) {
	c.stateMu.RLock()
	ch, ok := c.targets[channelID]
	c.stateMu.RUnlock()
	if !ok {
		return networkid.PortalKey{}, false
	}
	return c.portalKey(ch), true
}

// stampReactions returns one reaction per (user, stamp) of a message, ignoring the count. Stamps whose
// reaction key cannot be found are skipped.
func (c *TraQClient) stampReactions(ctx context.Context, msg *traq.Message) []*bridgev2.BackfillReaction {
	type userStamp struct{ user, stamp string }
	seen := make(map[userStamp]bool, len(msg.Stamps))
	reactions := make([]*bridgev2.BackfillReaction, 0, len(msg.Stamps))
	for _, s := range msg.Stamps {
		key := userStamp{s.UserID, s.StampID}
		if seen[key] {
			continue
		}
		seen[key] = true
		emoji, ok := c.reactionKey(ctx, s.StampID)
		if !ok {
			continue
		}
		reactions = append(reactions, &bridgev2.BackfillReaction{
			Timestamp: s.CreatedAt,
			Sender:    c.sender(s.UserID),
			EmojiID:   EmojiID(s.StampID),
			Emoji:     emoji,
		})
	}
	return reactions
}

// reactionKey returns the Matrix reaction key of a stamp. A stamp missing from the table is fetched with
// Stamp() and added to it.
func (c *TraQClient) reactionKey(ctx context.Context, stampID string) (string, bool) {
	if s, ok := c.Main.Stamps.ByID(stampID); ok {
		return stampconv.ReactionKey(s), true
	}
	st, err := c.API.Stamp(ctx, stampID)
	if err != nil {
		zerolog.Ctx(ctx).Warn().Err(err).Str("traq_stamp_id", stampID).Msg("Failed to get traQ stamp")
		return "", false
	}
	s := stampFromTraQ(st)
	c.Main.Stamps.Put(s)
	return stampconv.ReactionKey(s), true
}

// stampFromTraQ converts a traQ stamp for the table. Emoji stays empty: Unicode stamps are resolved to their
// emoji elsewhere, and until then their reaction key is ":name:".
func stampFromTraQ(s *traq.Stamp) stampconv.Stamp {
	return stampconv.Stamp{ID: s.ID, Name: s.Name, Unicode: s.IsUnicode}
}
