//go:build integration

package traq_test

import (
	"context"
	"errors"
	"io"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/kisepichu/traq-matrix-bridge/pkg/traq"
)

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)
	return ctx
}

// eventually retries cond every second until it returns true or timeout passes. traQ updates unread state and
// the search index asynchronously.
func eventually(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for %s", timeout, what)
		}
		time.Sleep(time.Second)
	}
}

// adminAPI returns an API client for the admin plus a fresh OAuth client config.
func adminAPI(t *testing.T) (*session, traq.OAuthConfig, *traq.Client, *traq.Me) {
	t.Helper()
	admin := adminSession(t)
	cfg := newOAuthClient(t, admin)
	api, me := apiFor(t, admin, cfg)
	return admin, cfg, api, me
}

func TestIntegrationOAuth(t *testing.T) {
	admin := adminSession(t)
	cfg := newOAuthClient(t, admin)
	ctx := testContext(t)

	tok := authorize(t, admin, cfg)
	if min := time.Now().Add(360 * 24 * time.Hour); !tok.ExpiresAt.After(min) {
		t.Errorf("ExpiresAt = %v, want after %v", tok.ExpiresAt, min)
	}
	api, err := traq.NewClient(testServerURL(), nil, tok.AccessToken, "traq-matrix-bridge-integration-test")
	if err != nil {
		t.Fatal(err)
	}
	me, err := api.Me(ctx)
	if err != nil {
		t.Fatalf("Me: %v", err)
	}
	if me.Name != adminName {
		t.Errorf("Me.Name = %q, want %q", me.Name, adminName)
	}

	if err := cfg.Revoke(ctx, tok.AccessToken); err != nil {
		t.Fatalf("Revoke: %v", err)
	}
	if _, err := api.Me(ctx); !traq.IsStatus(err, 401) {
		t.Errorf("Me after Revoke: err = %v, want HTTP 401", err)
	}

	if _, err := traq.ParseRedirect(cfg.RedirectURI+"?code=abc&state=sent", "expected"); !errors.Is(err, traq.ErrStateMismatch) {
		t.Errorf("ParseRedirect with another state: err = %v, want ErrStateMismatch", err)
	}
}

func TestIntegrationMessageLifecycle(t *testing.T) {
	admin, _, api, _ := adminAPI(t)
	ch := newChannel(t, admin)
	ctx := testContext(t)

	posted, err := api.PostMessage(ctx, ch, "hello "+randHex(t, 4))
	if err != nil {
		t.Fatalf("PostMessage: %v", err)
	}
	got, err := api.Message(ctx, posted.ID)
	if err != nil {
		t.Fatalf("Message: %v", err)
	}
	if got.Content != posted.Content || got.ChannelID != ch {
		t.Fatalf("Message = %+v, want content %q in channel %s", got, posted.Content, ch)
	}

	if err := api.EditMessage(ctx, posted.ID, "edited"); err != nil {
		t.Fatalf("EditMessage: %v", err)
	}
	edited, err := api.Message(ctx, posted.ID)
	if err != nil {
		t.Fatalf("Message after edit: %v", err)
	}
	if edited.Content != "edited" {
		t.Errorf("Content after edit = %q, want %q", edited.Content, "edited")
	}
	if !edited.UpdatedAt.After(got.UpdatedAt) {
		t.Errorf("UpdatedAt after edit = %v, want after %v", edited.UpdatedAt, got.UpdatedAt)
	}

	if err := api.DeleteMessage(ctx, posted.ID); err != nil {
		t.Fatalf("DeleteMessage: %v", err)
	}
	if _, err := api.Message(ctx, posted.ID); !traq.IsStatus(err, 404) {
		t.Errorf("Message after delete: err = %v, want HTTP 404", err)
	}
}

func TestIntegrationEmbedOnPost(t *testing.T) {
	admin, _, api, _ := adminAPI(t)
	other, otherID, _ := newUser(t, admin)
	ch := newChannel(t, admin)

	msg, err := api.PostMessage(testContext(t), ch, "@"+other+" hi")
	if err != nil {
		t.Fatalf("PostMessage: %v", err)
	}
	want := `!{"type":"user","raw":"@` + other + `","id":"` + otherID + `"} hi`
	if msg.Content != want {
		t.Errorf("Content = %q, want %q", msg.Content, want)
	}
}

func TestIntegrationMessagesPaging(t *testing.T) {
	admin, _, api, _ := adminAPI(t)
	ch := newChannel(t, admin)
	ctx := testContext(t)

	var posted []*traq.Message
	for i := range 5 {
		m, err := api.PostMessage(ctx, ch, "m"+string(rune('1'+i)))
		if err != nil {
			t.Fatalf("PostMessage %d: %v", i, err)
		}
		posted = append(posted, m)
	}
	ids := func(msgs []traq.Message) []string {
		var out []string
		for _, m := range msgs {
			out = append(out, m.ID)
		}
		return out
	}
	check := func(what string, q traq.MessagesQuery, wantIdx []int, wantMore bool) {
		t.Helper()
		msgs, more, err := api.Messages(ctx, ch, q)
		if err != nil {
			t.Fatalf("%s: %v", what, err)
		}
		var want []string
		for _, i := range wantIdx {
			want = append(want, posted[i].ID)
		}
		if got := ids(msgs); !slices.Equal(got, want) {
			t.Errorf("%s: messages = %v, want %v", what, got, want)
		}
		if more != wantMore {
			t.Errorf("%s: more = %v, want %v", what, more, wantMore)
		}
	}

	boundary := posted[2].CreatedAt
	check("limit 2 desc", traq.MessagesQuery{Limit: 2, Order: "desc"}, []int{4, 3}, true)
	check("all asc", traq.MessagesQuery{Limit: 5, Order: "asc"}, []int{0, 1, 2, 3, 4}, false)
	check("since exclusive", traq.MessagesQuery{Since: &boundary, Order: "asc"}, []int{3, 4}, false)
	check("since inclusive", traq.MessagesQuery{Since: &boundary, Inclusive: true, Order: "asc"}, []int{2, 3, 4}, false)
	check("until exclusive", traq.MessagesQuery{Until: &boundary, Order: "asc"}, []int{0, 1}, false)
	check("until inclusive", traq.MessagesQuery{Until: &boundary, Inclusive: true, Order: "asc"}, []int{0, 1, 2}, false)
}

func TestIntegrationStamps(t *testing.T) {
	admin, _, api, me := adminAPI(t)
	ch := newChannel(t, admin)
	ctx := testContext(t)

	name := "it-" + randHex(t, 6)
	stampID := createStamp(t, admin, name)
	stamps, err := api.Stamps(ctx)
	if err != nil {
		t.Fatalf("Stamps: %v", err)
	}
	if !slices.ContainsFunc(stamps, func(s traq.Stamp) bool { return s.ID == stampID && s.Name == name }) {
		t.Fatalf("Stamps does not contain %q (%s)", name, stampID)
	}

	msg, err := api.PostMessage(ctx, ch, "stamp me")
	if err != nil {
		t.Fatalf("PostMessage: %v", err)
	}
	hasStamp := func() bool {
		t.Helper()
		m, err := api.Message(ctx, msg.ID)
		if err != nil {
			t.Fatalf("Message: %v", err)
		}
		return slices.ContainsFunc(m.Stamps, func(s traq.MessageStamp) bool { return s.UserID == me.ID && s.StampID == stampID })
	}
	if err := api.AddStamp(ctx, msg.ID, stampID); err != nil {
		t.Fatalf("AddStamp: %v", err)
	}
	if !hasStamp() {
		t.Fatal("Message.Stamps lacks the stamp after AddStamp")
	}
	if err := api.RemoveStamp(ctx, msg.ID, stampID); err != nil {
		t.Fatalf("RemoveStamp: %v", err)
	}
	if hasStamp() {
		t.Error("Message.Stamps still has the stamp after RemoveStamp")
	}
}

func TestIntegrationUnreadAndRead(t *testing.T) {
	admin, cfg, adminAPIClient, _ := adminAPI(t)
	_, readerID, readerSession := newUser(t, admin)
	reader, _ := apiFor(t, readerSession, cfg)
	ctx := testContext(t)

	unreadIn := func(ch string) bool {
		t.Helper()
		list, err := reader.Unread(ctx)
		if err != nil {
			t.Fatalf("Unread: %v", err)
		}
		return slices.ContainsFunc(list, func(u traq.UnreadChannel) bool { return u.ChannelID == ch })
	}
	run := func(what, ch string) {
		t.Helper()
		if _, err := adminAPIClient.PostMessage(ctx, ch, "ping "+what); err != nil {
			t.Fatalf("%s: PostMessage: %v", what, err)
		}
		eventually(t, 15*time.Second, what+" to be unread", func() bool { return unreadIn(ch) })
		if err := reader.ReadChannel(ctx, ch); err != nil {
			t.Fatalf("%s: ReadChannel: %v", what, err)
		}
		if unreadIn(ch) {
			t.Errorf("%s: channel still unread after ReadChannel", what)
		}
	}

	ch := newChannel(t, admin)
	subscribe(t, readerSession, ch, 1)
	run("subscribed channel", ch)

	// A DM is unread without any subscription.
	dm := dmChannel(t, admin, readerID)
	if dm == "" {
		t.Fatal("empty DM channel ID")
	}
	run("DM", dm)
}

func TestIntegrationChannelsAndSubscriptions(t *testing.T) {
	admin, _, api, _ := adminAPI(t)
	_, otherID, _ := newUser(t, admin)
	ctx := testContext(t)

	ch := newChannel(t, admin)
	dm := dmChannel(t, admin, otherID)

	list, err := api.Channels(ctx)
	if err != nil {
		t.Fatalf("Channels: %v", err)
	}
	if !slices.ContainsFunc(list.Public, func(c traq.Channel) bool { return c.ID == ch }) {
		t.Errorf("Channels.Public lacks %s", ch)
	}
	if !slices.ContainsFunc(list.DM, func(c traq.DMChannel) bool { return c.ID == dm && c.UserID == otherID }) {
		t.Errorf("Channels.DM lacks {%s, %s}: %+v", dm, otherID, list.DM)
	}

	subscribe(t, admin, ch, 2)
	subs, err := api.Subscriptions(ctx)
	if err != nil {
		t.Fatalf("Subscriptions: %v", err)
	}
	if !slices.Contains(subs, traq.Subscription{ChannelID: ch, Level: 2}) {
		t.Errorf("Subscriptions lacks {%s, level 2}: %+v", ch, subs)
	}
}

func TestIntegrationFiles(t *testing.T) {
	admin, _, api, _ := adminAPI(t)
	ch := newChannel(t, admin)
	ctx := testContext(t)

	name := "note-" + randHex(t, 4) + ".txt"
	data := []byte("integration file " + randHex(t, 8))
	id := uploadFile(t, admin, ch, name, data)

	meta, err := api.FileMeta(ctx, id)
	if err != nil {
		t.Fatalf("FileMeta: %v", err)
	}
	if meta.Name != name || meta.Size != int64(len(data)) || !strings.HasPrefix(meta.Mime, "text/plain") {
		t.Errorf("FileMeta = %+v, want name %q size %d mime text/plain", meta, name, len(data))
	}

	body, err := api.DownloadFile(ctx, id)
	if err != nil {
		t.Fatalf("DownloadFile: %v", err)
	}
	defer body.Close()
	got, err := io.ReadAll(body)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(data) {
		t.Errorf("downloaded %q, want %q", got, data)
	}
}

func TestIntegrationSearch(t *testing.T) {
	admin, _, api, _ := adminAPI(t)
	ch := newChannel(t, admin)
	ctx := testContext(t)

	// Letters only: the Sudachi analyzer splits tokens at letter/digit boundaries, so a hex suffix would not match.
	word := "uniqueword" + randLetters(t, 8)
	if _, err := api.PostMessage(ctx, ch, "search target "+word); err != nil {
		t.Fatalf("PostMessage: %v", err)
	}
	var res *traq.SearchResult
	// traQ indexes new messages into Elasticsearch in batches roughly every 67s (es_sync), so the brief's 30s is too short.
	eventually(t, 120*time.Second, "the message to be searchable", func() bool {
		r, err := api.Search(ctx, traq.SearchQuery{Word: word, In: ch})
		if err != nil {
			t.Logf("Search: %v", err)
			return false
		}
		res = r
		return len(r.Hits) > 0
	})
	if len(res.Hits) != 1 || res.TotalHits != 1 || !strings.Contains(res.Hits[0].Content, word) {
		t.Errorf("Search = %+v, want exactly one hit containing %q", res, word)
	}
}

func TestIntegrationPublicIcon(t *testing.T) {
	_, _, api, _ := adminAPI(t)
	data, mime, err := api.PublicIcon(testContext(t), adminName)
	if err != nil {
		t.Fatalf("PublicIcon: %v", err)
	}
	if !strings.HasPrefix(mime, "image/") || len(data) == 0 {
		t.Errorf("PublicIcon = %d bytes, mime %q; want a non-empty image", len(data), mime)
	}
}
