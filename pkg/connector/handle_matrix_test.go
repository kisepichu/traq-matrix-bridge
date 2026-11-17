package connector

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"go.mau.fi/util/dbutil"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"github.com/kisepichu/traq-matrix-bridge/pkg/msgconv"
	"github.com/kisepichu/traq-matrix-bridge/pkg/traq"
)

// newTestBridgeDB opens bridgev2's database, upgraded, on an in-memory SQLite database.
func newTestBridgeDB(t *testing.T) *database.Database {
	t.Helper()
	db := database.New("test", (&TraQConnector{}).GetDBMetaTypes(), newTestRawDB(t))
	if err := db.Upgrade(context.Background()); err != nil {
		t.Fatal(err)
	}
	return db
}

func newTestRawDB(t *testing.T) *dbutil.Database {
	t.Helper()
	raw, err := dbutil.NewFromConfig("", dbutil.Config{PoolConfig: dbutil.PoolConfig{
		Type: "sqlite3-fk-wal", URI: ":memory:?_txlock=immediate", MaxOpenConns: 1, MaxIdleConns: 1,
	}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = raw.Close() })
	return raw
}

// Reacting and removing a reaction are Matrix activity in the channel, like sending a message, so the channel
// becomes the viewed one.
func TestMatrixReactionTouchesView(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }))
	t.Cleanup(srv.Close)
	api, err := traq.NewClient(srv.URL, srv.Client(), "token", userAgent)
	if err != nil {
		t.Fatal(err)
	}
	view, sender, _ := newTestViewState()
	c := &TraQClient{API: api, Login: testLogin("u-me", false), me: &traq.Me{User: traq.User{ID: "u-me"}}, view: view}
	c.Login.Bridge.DB = newTestBridgeDB(t)
	portal := func(channelID string) *bridgev2.Portal {
		return &bridgev2.Portal{Portal: &database.Portal{PortalKey: PortalKey(channelID, "")}}
	}
	ctx := context.Background()

	_, err = c.HandleMatrixReaction(ctx, &bridgev2.MatrixReaction{
		MatrixEventBase: bridgev2.MatrixEventBase[*event.ReactionEventContent]{Portal: portal("ch-a")},
		TargetMessage:   &database.Message{ID: "m-1"},
		PreHandleResp:   &bridgev2.MatrixReactionPreResponse{EmojiID: "s-1", Emoji: ":good:"},
	})
	if err != nil {
		t.Fatalf("HandleMatrixReaction: %v", err)
	}
	expectSent(t, sender, "ch-a")

	err = c.HandleMatrixReactionRemove(ctx, &bridgev2.MatrixReactionRemove{
		MatrixEventBase: bridgev2.MatrixEventBase[*event.RedactionEventContent]{Portal: portal("ch-b")},
		TargetReaction:  &database.Reaction{MessageID: "m-2", EmojiID: "s-1", SenderID: "u-me"},
	})
	if err != nil {
		t.Fatalf("HandleMatrixReactionRemove: %v", err)
	}
	expectSent(t, sender, "ch-b")
}

func TestWithFileURLs(t *testing.T) {
	// Out of RowID order, as GetAllPartsByID may return them.
	parts := []*database.Message{
		{RowID: 3, ID: "m", PartID: filePartID(fileA)},
		{RowID: 1, ID: "m", PartID: textPartID},
		{RowID: 2, ID: "m", PartID: filePartID(fileB)},
	}
	got, err := withFileURLs("new text", parts, testOrigin)
	if want := "new text\n" + testOrigin + "/files/" + fileB + "\n" + testOrigin + "/files/" + fileA; err != nil || got != want {
		t.Errorf("withFileURLs = %q, %v; want %q", got, err, want)
	}

	got, err = withFileURLs("new text", []*database.Message{{RowID: 1, ID: "m", PartID: textPartID}}, testOrigin)
	if err != nil || got != "new text" {
		t.Errorf("withFileURLs without file parts = %q, %v; want the text unchanged", got, err)
	}
}

func TestWithFileURLsLength(t *testing.T) {
	parts := []*database.Message{{ID: "m", PartID: textPartID}, {ID: "m", PartID: filePartID(fileA)}}
	url := testOrigin + "/files/" + fileA
	fits := strings.Repeat("あ", msgconv.MaxTraQLength-len(url)-1)
	if _, err := withFileURLs(fits, parts, testOrigin); err != nil {
		t.Errorf("content of exactly %d code points with the file URL rejected: %v", msgconv.MaxTraQLength, err)
	}
	_, err := withFileURLs(fits+"あ", parts, testOrigin)
	if !errors.Is(err, msgconv.ErrTooLong) || !strings.Contains(err.Error(), "file links kept from the original message") {
		t.Errorf("one code point over only because of the file URL: err = %v, want ErrTooLong naming the file links", err)
	}
	_, err = withFileURLs(strings.Repeat("あ", msgconv.MaxTraQLength+1), parts, testOrigin)
	if !errors.Is(err, msgconv.ErrTooLong) || strings.Contains(err.Error(), "file links") {
		t.Errorf("text alone over the limit: err = %v, want ErrTooLong without the file links note", err)
	}
}

func TestTraQResolverUserName(t *testing.T) {
	names := map[string]string{"u-ghost": "ghost", "u-me": "me", "u-carol": "carol"}
	var lookedUp []string
	r := &traqResolver{
		ctx: context.Background(),
		ghostUserID: func(mxid id.UserID) (networkid.UserID, bool) {
			userID, ok := strings.CutPrefix(string(mxid), "@traq_")
			userID, _, _ = strings.Cut(userID, ":")
			return UserID(userID), ok
		},
		selfMXID:   "@me:example.org",
		selfUserID: "u-me",
		loginUserID: func(_ context.Context, mxid id.UserID) (string, bool, error) {
			switch mxid {
			case "@carol:example.org":
				return "u-carol", true, nil
			case "@broken:example.org":
				return "", false, errors.New("database is down")
			}
			return "", false, nil
		},
		userName: func(_ context.Context, userID string) (string, error) {
			lookedUp = append(lookedUp, userID)
			if name, ok := names[userID]; ok {
				return name, nil
			}
			return "", errors.New("no such traQ user")
		},
	}
	cases := []struct {
		mxid     id.UserID
		want     string
		wantOK   bool
		lookedUp []string
	}{
		{"@traq_u-ghost:example.org", "ghost", true, []string{"u-ghost"}},
		{"@me:example.org", "me", true, []string{"u-me"}},
		{"@carol:example.org", "carol", true, []string{"u-carol"}},
		{"@dave:example.org", "", false, nil},
		{"@broken:example.org", "", false, nil},
		{"@traq_u-gone:example.org", "", false, []string{"u-gone"}},
		{"", "", false, nil},
	}
	for _, c := range cases {
		lookedUp = nil
		got, ok := r.TraQUserName(c.mxid)
		if got != c.want || ok != c.wantOK || !slices.Equal(lookedUp, c.lookedUp) {
			t.Errorf("TraQUserName(%q) = %q, %v, looked up %q; want %q, %v, %q", c.mxid, got, ok, lookedUp, c.want, c.wantOK, c.lookedUp)
		}
	}
}

// A pill of a public channel's portal becomes the channel; DM portals and rooms that are no portal stay links.
func TestTraQResolverChannelPath(t *testing.T) {
	channels := map[string]channelInfo{
		"ch-gen": {ID: "ch-gen", Path: "gps/general", Level: 1},
		"ch-dm":  {ID: "ch-dm", DMUserID: "u-bob", Level: dmLevel},
	}
	r := &traqResolver{
		ctx: context.Background(),
		roomChannelID: func(_ context.Context, roomID id.RoomID) (string, bool, error) {
			switch roomID {
			case "!broken:example.org":
				return "", false, errors.New("database is down")
			case "!gen:example.org":
				return "ch-gen", true, nil
			case "!dm:example.org":
				return "ch-dm", true, nil
			case "!gone:example.org":
				return "ch-gone", true, nil
			}
			return "", false, nil
		},
		channel: func(channelID string) (channelInfo, bool) { ch, ok := channels[channelID]; return ch, ok },
	}
	cases := []struct {
		room   id.RoomID
		want   string
		wantOK bool
	}{
		{"!gen:example.org", "gps/general", true},
		{"!dm:example.org", "", false},
		{"!gone:example.org", "", false},
		{"!other:example.org", "", false},
		{"!broken:example.org", "", false},
	}
	for _, c := range cases {
		if got, ok := r.ChannelPath(c.room); got != c.want || ok != c.wantOK {
			t.Errorf("ChannelPath(%q) = %q, %v; want %q, %v", c.room, got, ok, c.want, c.wantOK)
		}
	}
}

func TestTraQResolverMessageID(t *testing.T) {
	r := &traqResolver{
		ctx: context.Background(),
		eventMessageID: func(_ context.Context, eventID id.EventID) (string, bool, error) {
			switch eventID {
			case "$bridged":
				return "m-1", true, nil
			case "$broken":
				return "", false, errors.New("database is down")
			}
			return "", false, nil
		},
	}
	for eventID, want := range map[id.EventID]string{"$bridged": "m-1", "$other": "", "$broken": ""} {
		if got, ok := r.MessageID(eventID); got != want || ok != (want != "") {
			t.Errorf("MessageID(%q) = %q, %v; want %q", eventID, got, ok, want)
		}
	}
}
