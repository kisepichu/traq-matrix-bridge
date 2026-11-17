package connector

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"github.com/kisepichu/traq-matrix-bridge/pkg/msgconv"
	"github.com/kisepichu/traq-matrix-bridge/pkg/stampconv"
	"github.com/kisepichu/traq-matrix-bridge/pkg/traq"
)

const (
	testOrigin = "https://q.example.jp"
	fileA      = "0193a1b2-0000-7000-8000-00000000000a"
	fileB      = "0193a1b2-0000-7000-8000-00000000000b"
)

// fakeUploader returns an m.file content named after the file ID, or the error set for that ID.
type fakeUploader struct {
	errs map[string]error
}

func (f *fakeUploader) upload(_ context.Context, fileID string) (*event.MessageEventContent, error) {
	if err := f.errs[fileID]; err != nil {
		return nil, err
	}
	return &event.MessageEventContent{MsgType: event.MsgFile, Body: fileID, URL: id.ContentURIString("mxc://example.org/" + fileID)}, nil
}

func partIDs(parts []*bridgev2.ConvertedMessagePart) []networkid.PartID {
	ids := make([]networkid.PartID, len(parts))
	for i, p := range parts {
		ids[i] = p.ID
	}
	return ids
}

func TestBuildMessageParts(t *testing.T) {
	res := msgconv.ToMatrixResult{Body: "hi", HTML: "<b>hi</b>", FileIDs: []string{fileA, fileB}, ReplyTo: testUUID}
	up := &fakeUploader{}
	cm, err := buildMessage(context.Background(), res, "**hi** files", testOrigin, up)
	if err != nil {
		t.Fatalf("buildMessage: %v", err)
	}
	if got, want := partIDs(cm.Parts), []networkid.PartID{"", "file:" + fileA, "file:" + fileB}; !slices.Equal(got, want) {
		t.Fatalf("part IDs = %q, want %q", got, want)
	}
	text := cm.Parts[0]
	if text.Type != event.EventMessage || text.Content.MsgType != event.MsgText || text.Content.Body != "hi" || text.Content.FormattedBody != "<b>hi</b>" {
		t.Errorf("text part = %+v", text.Content)
	}
	if meta, _ := text.DBMetadata.(*MessageMetadata); meta == nil || meta.ContentHash != ContentHash("**hi** files") {
		t.Errorf("text part metadata = %+v, want the hash of the traQ content", text.DBMetadata)
	}
	if cm.Parts[1].Content.Body != fileA || cm.Parts[2].Content.Body != fileB {
		t.Errorf("file parts out of order: %q, %q", cm.Parts[1].Content.Body, cm.Parts[2].Content.Body)
	}
	if cm.ReplyTo == nil || cm.ReplyTo.MessageID != MessageID(testUUID) || cm.ReplyTo.PartID != nil {
		t.Errorf("ReplyTo = %+v, want the first part of %s", cm.ReplyTo, testUUID)
	}
}

func TestBuildMessageWithoutBody(t *testing.T) {
	res := msgconv.ToMatrixResult{FileIDs: []string{fileA}}
	cm, err := buildMessage(context.Background(), res, testOrigin+"/files/"+fileA, testOrigin, &fakeUploader{})
	if err != nil {
		t.Fatalf("buildMessage: %v", err)
	}
	if got, want := partIDs(cm.Parts), []networkid.PartID{"file:" + fileA}; !slices.Equal(got, want) {
		t.Fatalf("part IDs = %q, want %q", got, want)
	}
	if cm.ReplyTo != nil {
		t.Errorf("ReplyTo = %+v, want nil", cm.ReplyTo)
	}
}

func TestBuildMessageEmpty(t *testing.T) {
	_, err := buildMessage(context.Background(), msgconv.ToMatrixResult{}, "", testOrigin, &fakeUploader{})
	if !errors.Is(err, bridgev2.ErrIgnoringRemoteEvent) {
		t.Fatalf("err = %v, want ErrIgnoringRemoteEvent", err)
	}
}

// A file that cannot be re-uploaded keeps its traQ URL in the text, and the text part is created for it.
func TestBuildMessageFileNotUploaded(t *testing.T) {
	up := &fakeUploader{errs: map[string]error{fileA: errFileTooLarge}}
	cm, err := buildMessage(context.Background(), msgconv.ToMatrixResult{FileIDs: []string{fileA, fileB}}, "x", testOrigin, up)
	if err != nil {
		t.Fatalf("buildMessage: %v", err)
	}
	if got, want := partIDs(cm.Parts), []networkid.PartID{"", "file:" + fileB}; !slices.Equal(got, want) {
		t.Fatalf("part IDs = %q, want %q", got, want)
	}
	url := testOrigin + "/files/" + fileA
	if got := cm.Parts[0].Content.Body; got != url {
		t.Errorf("text body = %q, want %q", got, url)
	}

	cm, err = buildMessage(context.Background(), msgconv.ToMatrixResult{Body: "look", HTML: "look", FileIDs: []string{fileA}}, "x", testOrigin, up)
	if err != nil {
		t.Fatalf("buildMessage: %v", err)
	}
	if got, want := cm.Parts[0].Content.Body, "look\n"+url; got != want {
		t.Errorf("text body = %q, want %q", got, want)
	}
	if got, want := cm.Parts[0].Content.FormattedBody, `look<br><a href="`+url+`">`+url+`</a>`; got != want {
		t.Errorf("text HTML = %q, want %q", got, want)
	}
}

func textPartRow(content string) *database.Message {
	return &database.Message{ID: "m", PartID: "", MXID: "$text", Metadata: &MessageMetadata{ContentHash: ContentHash(content)}}
}

func TestBuildEditReplacesTextPart(t *testing.T) {
	existing := []*database.Message{textPartRow("old"), {ID: "m", PartID: "file:" + fileA, MXID: "$file"}}
	res := msgconv.ToMatrixResult{Body: "new", HTML: "new", FileIDs: []string{fileA}}
	edit, err := buildEdit(existing, res, "new", testOrigin)
	if err != nil {
		t.Fatalf("buildEdit: %v", err)
	}
	if len(edit.ModifiedParts) != 1 || edit.ModifiedParts[0].Part != existing[0] {
		t.Fatalf("ModifiedParts = %+v, want only the text part", edit.ModifiedParts)
	}
	if got := edit.ModifiedParts[0].Content.Body; got != "new" {
		t.Errorf("edited body = %q, want %q (a file that already has a part is not linked)", got, "new")
	}
	if meta := existing[0].Metadata.(*MessageMetadata); meta.ContentHash != ContentHash("new") {
		t.Errorf("ContentHash not updated")
	}
	if edit.AddedParts != nil || len(edit.DeletedParts) != 0 {
		t.Errorf("unexpected added/deleted parts: %+v / %+v", edit.AddedParts, edit.DeletedParts)
	}
}

func TestBuildEditUnchangedContent(t *testing.T) {
	_, err := buildEdit([]*database.Message{textPartRow("same")}, msgconv.ToMatrixResult{Body: "same"}, "same", testOrigin)
	if !errors.Is(err, bridgev2.ErrIgnoringRemoteEvent) {
		t.Fatalf("err = %v, want ErrIgnoringRemoteEvent", err)
	}
}

// v1 does not add or remove file parts in an edit: a file new to the message is linked in the text.
func TestBuildEditNewFileLinked(t *testing.T) {
	existing := []*database.Message{textPartRow("old")}
	edit, err := buildEdit(existing, msgconv.ToMatrixResult{Body: "new", HTML: "new", FileIDs: []string{fileB}}, "new2", testOrigin)
	if err != nil {
		t.Fatalf("buildEdit: %v", err)
	}
	if got, want := edit.ModifiedParts[0].Content.Body, "new\n"+testOrigin+"/files/"+fileB; got != want {
		t.Errorf("edited body = %q, want %q", got, want)
	}
}

// A message that had only files gets its text as an added part.
func TestBuildEditAddsTextPart(t *testing.T) {
	existing := []*database.Message{{ID: "m", PartID: "file:" + fileA, MXID: "$file"}}
	edit, err := buildEdit(existing, msgconv.ToMatrixResult{Body: "caption", HTML: "caption", FileIDs: []string{fileA}}, "caption", testOrigin)
	if err != nil {
		t.Fatalf("buildEdit: %v", err)
	}
	if len(edit.ModifiedParts) != 0 || edit.AddedParts == nil || len(edit.AddedParts.Parts) != 1 || edit.AddedParts.Parts[0].ID != "" {
		t.Fatalf("edit = %+v, want one added text part", edit)
	}
}

// Removing the whole text of a message with files deletes the text part.
func TestBuildEditRemovesTextPart(t *testing.T) {
	existing := []*database.Message{textPartRow("old"), {ID: "m", PartID: "file:" + fileA, MXID: "$file"}}
	edit, err := buildEdit(existing, msgconv.ToMatrixResult{FileIDs: []string{fileA}}, testOrigin+"/files/"+fileA, testOrigin)
	if err != nil {
		t.Fatalf("buildEdit: %v", err)
	}
	if len(edit.DeletedParts) != 1 || edit.DeletedParts[0] != existing[0] || len(edit.ModifiedParts) != 0 {
		t.Fatalf("edit = %+v, want the text part deleted", edit)
	}
}

func TestStampReactions(t *testing.T) {
	const other = "0193a1b2-0000-7000-8000-0000000000ff"
	c := &TraQClient{
		Main:  &TraQConnector{Stamps: stampconv.NewTable([]stampconv.Stamp{{ID: "s1", Name: "good"}, {ID: "s2", Name: "thumbsup", Unicode: true, Emoji: "👍"}}, false)},
		Login: &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: LoginID(testUUID)}},
		me:    &traq.Me{User: traq.User{ID: testUUID}},
	}
	t1 := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	t2 := t1.Add(time.Minute)
	msg := &traq.Message{ID: "m", Stamps: []traq.MessageStamp{
		{UserID: testUUID, StampID: "s1", Count: 5, CreatedAt: t1, UpdatedAt: t2},
		{UserID: testUUID, StampID: "s2", Count: 1, CreatedAt: t2, UpdatedAt: t2},
		{UserID: other, StampID: "s1", Count: 2, CreatedAt: t2, UpdatedAt: t2},
	}}
	got := c.stampReactions(context.Background(), msg)
	if len(got) != 3 {
		t.Fatalf("got %d reactions, want 3 (one per user and stamp, count ignored)", len(got))
	}
	want := []struct {
		sender    networkid.UserID
		fromMe    bool
		emojiID   networkid.EmojiID
		emoji     string
		createdAt time.Time
	}{
		{UserID(testUUID), true, "s1", ":good:", t1},
		{UserID(testUUID), true, "s2", "👍", t2},
		{UserID(other), false, "s1", ":good:", t2},
	}
	for i, w := range want {
		r := got[i]
		if r.Sender.Sender != w.sender || r.Sender.IsFromMe != w.fromMe || r.EmojiID != w.emojiID || r.Emoji != w.emoji || !r.Timestamp.Equal(w.createdAt) {
			t.Errorf("reaction %d = {%+v %q %q %v}, want %+v", i, r.Sender, r.EmojiID, r.Emoji, r.Timestamp, w)
		}
		if r.TargetPart != nil {
			t.Errorf("reaction %d targets part %q, want the first part", i, *r.TargetPart)
		}
	}
	if got[0].Sender.SenderLogin != LoginID(testUUID) {
		t.Errorf("own reaction SenderLogin = %q, want this login", got[0].Sender.SenderLogin)
	}
}
