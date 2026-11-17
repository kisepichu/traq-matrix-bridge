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

// messageFilesTimeout bounds the download and re-upload of all files of one message together, so one
// message cannot hold the portal's event loop for longer. Files not done by then are linked.
const messageFilesTimeout = 5 * time.Minute

// fileHTTPClient downloads files. It has no total timeout, unlike httpClient, because a large file on a
// slow link can take longer than httpTimeout; messageFilesTimeout bounds the downloads instead.
var fileHTTPClient = &http.Client{}

// errFileTooLarge: the file is larger than the homeserver accepts, so it is linked instead.
var errFileTooLarge = errors.New("the file is larger than the homeserver's upload limit")

// textPartID is the part ID of the text part; file parts are filePartPrefix + the file ID.
const (
	textPartID     networkid.PartID = ""
	filePartPrefix                  = "file:"
)

func filePartID(fileID string) networkid.PartID { return networkid.PartID(filePartPrefix + fileID) }

// fileIDOfPart returns the file ID of a file part.
func fileIDOfPart(partID networkid.PartID) (string, bool) {
	return strings.CutPrefix(string(partID), filePartPrefix)
}

func fileURL(origin, fileID string) string { return origin + "/files/" + fileID }

// mediaUploader re-uploads one traQ file to Matrix and returns the content of its part.
type mediaUploader interface {
	upload(ctx context.Context, fileID string) (*event.MessageEventContent, error)
}

// convertMessage builds the Matrix content for a traQ message: one text part (if the body is not empty)
// followed by one part per file. ReplyTo is set from msgconv's ReplyTo.
func (c *TraQClient) convertMessage(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, msg *traq.Message) (*bridgev2.ConvertedMessage, error) {
	return c.convertMessageAfter(ctx, portal, intent, msg, nil)
}

// convertMessageAfter is convertMessage for a backfill batch: earlier are the IDs of the messages of the
// batch that are sent before this one, so a quote of one of them becomes a reply although the database
// does not have it yet.
func (c *TraQClient) convertMessageAfter(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, msg *traq.Message, earlier map[string]struct{}) (*bridgev2.ConvertedMessage, error) {
	res := msgconv.ToMatrix(c.toMatrixInput(msg.Content), &resolver{ctx: ctx, c: c, portal: portal, earlier: earlier})
	up := &portalUploader{api: c.API, files: c.Files, limit: c.Main.maxFileSize(), roomID: portal.MXID, intent: intent}
	return buildMessage(ctx, res, messageMetadata(msg), c.origin(), up, messageFilesTimeout)
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
	return buildEdit(existing, res, messageMetadata(msg), c.origin())
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

// buildMessage assembles the parts of a new message; meta is the text part's metadata. A file that cannot be
// uploaded (too large, or the download or upload failed) keeps its traQ URL at the end of the text instead,
// and so do the files not uploaded within filesTimeout, which bounds all uploads of the message together.
func buildMessage(ctx context.Context, res msgconv.ToMatrixResult, meta *MessageMetadata, origin string, up mediaUploader, filesTimeout time.Duration) (*bridgev2.ConvertedMessage, error) {
	files := make([]*bridgev2.ConvertedMessagePart, 0, len(res.FileIDs))
	var links []string
	if len(res.FileIDs) > 0 {
		filesCtx, cancel := context.WithTimeout(ctx, filesTimeout)
		defer cancel()
		for _, fileID := range res.FileIDs {
			if filesCtx.Err() != nil {
				links = append(links, fileURL(origin, fileID))
				continue
			}
			fc, err := up.upload(filesCtx, fileID)
			if err == nil {
				files = append(files, &bridgev2.ConvertedMessagePart{ID: filePartID(fileID), Type: event.EventMessage, Content: fc})
				continue
			}
			// A timeout is logged once below, for all files it cut off.
			if filesCtx.Err() == nil && !errors.Is(err, errFileTooLarge) {
				zerolog.Ctx(ctx).Warn().Err(err).Str("traq_file_id", fileID).Msg("Failed to bridge a traQ file, linking it instead")
			}
			links = append(links, fileURL(origin, fileID))
		}
		if ctx.Err() == nil && filesCtx.Err() != nil {
			zerolog.Ctx(ctx).Warn().Dur("timeout", filesTimeout).Msg("Bridging the files of a traQ message took too long, linking the rest instead")
		}
	}
	cm := &bridgev2.ConvertedMessage{Parts: make([]*bridgev2.ConvertedMessagePart, 0, len(files)+1)}
	if text := textContent(res, links); text != nil {
		cm.Parts = append(cm.Parts, textPart(text, meta))
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

// buildEdit computes the edit of the text part to the version meta describes. A version not newer than the
// one last bridged (by traQ's updatedAt, when the text part has one stored) is ignored, so an older read (a
// stale poll) does not undo a newer edit. So is content the text part was last bridged from, so the same
// edit seen by several logins is bridged once. Files that have no part are linked in the text.
func buildEdit(existing []*database.Message, res msgconv.ToMatrixResult, meta *MessageMetadata, origin string) (*bridgev2.ConvertedEdit, error) {
	var text *database.Message
	hasPart := make(map[networkid.PartID]bool, len(existing))
	for _, part := range existing {
		hasPart[part.PartID] = true
		if part.PartID == textPartID {
			text = part
		}
	}
	if text != nil {
		if stored, ok := text.Metadata.(*MessageMetadata); ok {
			if (!stored.UpdatedAt.IsZero() && !meta.UpdatedAt.After(stored.UpdatedAt)) || stored.ContentHash == meta.ContentHash {
				return nil, bridgev2.ErrIgnoringRemoteEvent
			}
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
		return &bridgev2.ConvertedEdit{AddedParts: &bridgev2.ConvertedMessage{Parts: []*bridgev2.ConvertedMessagePart{textPart(newText, meta)}}}, nil
	default:
		// ToEditPart stores meta in the existing part.
		return &bridgev2.ConvertedEdit{ModifiedParts: []*bridgev2.ConvertedEditPart{textPart(newText, meta).ToEditPart(text)}}, nil
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

func textPart(content *event.MessageEventContent, meta *MessageMetadata) *bridgev2.ConvertedMessagePart {
	return &bridgev2.ConvertedMessagePart{
		ID:         textPartID,
		Type:       event.EventMessage,
		Content:    content,
		DBMetadata: meta,
	}
}

// mediaStreamUploader is the part of bridgev2.MatrixAPI that portalUploader uses.
type mediaStreamUploader interface {
	UploadMediaStream(ctx context.Context, roomID id.RoomID, size int64, requireFile bool, cb bridgev2.FileStreamCallback) (id.ContentURIString, *event.EncryptedFileInfo, error)
}

// portalUploader downloads traQ files and uploads them to the portal's room (encrypted if the room is).
type portalUploader struct {
	api    *traq.Client // file metadata
	files  *traq.Client // downloads, without httpTimeout
	limit  int64        // the homeserver's upload size limit
	roomID id.RoomID
	intent mediaStreamUploader
}

func (u *portalUploader) upload(ctx context.Context, fileID string) (*event.MessageEventContent, error) {
	meta, err := u.api.FileMeta(ctx, fileID)
	if err != nil {
		return nil, fmt.Errorf("get file meta: %w", err)
	}
	if meta.Size > u.limit {
		return nil, errFileTooLarge
	}
	body, err := u.files.DownloadFile(ctx, fileID)
	if err != nil {
		return nil, fmt.Errorf("download file: %w", err)
	}
	defer body.Close()
	mime := meta.Mime
	if mime == "" {
		mime = "application/octet-stream"
	}
	url, file, err := u.intent.UploadMediaStream(ctx, u.roomID, meta.Size, false, func(w io.Writer) (*bridgev2.FileStreamResult, error) {
		// Copy at most limit+1 bytes: a body over the homeserver's upload limit, whatever size was announced,
		// stops there instead of filling the buffer or temp file, and the upload's own size check rejects it.
		if _, err := io.Copy(w, io.LimitReader(body, u.limit+1)); err != nil {
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
	// In an encrypted room, the mxc URI is in file.URL and url is empty.
	if file != nil {
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
	// earlier is the IDs of the backfill batch's messages that are sent before the one being converted.
	earlier map[string]struct{}
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
	if _, ok := r.earlier[messageID]; ok {
		return true
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

// target returns a channel that has a portal for this login (a DM, or a public channel subscribed at level
// >= 1), and false for any other channel.
func (c *TraQClient) target(channelID string) (channelInfo, bool) {
	c.stateMu.RLock()
	defer c.stateMu.RUnlock()
	ch, ok := c.targets[channelID]
	return ch, ok
}

// portalKeyFor returns the portal key of a target channel (see target), and false for any other channel.
func (c *TraQClient) portalKeyFor(channelID string) (networkid.PortalKey, bool) {
	ch, ok := c.target(channelID)
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
		emoji, err := c.reactionKey(ctx, s.StampID)
		if err != nil {
			zerolog.Ctx(ctx).Warn().Err(err).Str("traq_stamp_id", s.StampID).Msg("Skipping a stamp whose reaction key is unknown")
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
// Stamp() and added to it. A failure is returned, not logged.
func (c *TraQClient) reactionKey(ctx context.Context, stampID string) (string, error) {
	if s, ok := c.Main.Stamps.ByID(stampID); ok {
		return stampconv.ReactionKey(s), nil
	}
	st, err := c.API.Stamp(ctx, stampID)
	if err != nil {
		return "", fmt.Errorf("get stamp %s: %w", stampID, err)
	}
	return stampconv.ReactionKey(c.Main.tableStamp(stampFromTraQ(st))), nil
}

// stampFromTraQ converts a traQ stamp for the table. Emoji stays empty: the stamp sync job (stamps.go) resolves
// the emoji of Unicode stamps, and until then their reaction key is ":name:".
func stampFromTraQ(s *traq.Stamp) stampconv.Stamp {
	return stampconv.Stamp{ID: s.ID, Name: s.Name, Unicode: s.IsUnicode}
}
