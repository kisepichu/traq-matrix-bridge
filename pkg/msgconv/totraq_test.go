package msgconv

import (
	"context"
	"errors"
	"net/url"
	"strings"
	"testing"

	"maunium.net/go/mautrix/id"
)

type fakeTraQUsers map[id.UserID]string

func (f fakeTraQUsers) TraQUserName(mxid id.UserID) (string, bool) {
	name, ok := f[mxid]
	return name, ok
}

var traqUsers = fakeTraQUsers{
	"@traq_alice:example.org": "alice",
}
var origin = &url.URL{Scheme: "https", Host: "q.example.jp"}

func TestToTraQ(t *testing.T) {
	cases := []struct {
		name string
		in   ToTraQInput
		want string
	}{
		{"plain body", ToTraQInput{Body: "hello"}, "hello"},
		{"html bold", ToTraQInput{Body: "x", Format: "org.matrix.custom.html", FormattedBody: "<strong>x</strong>"}, "**x**"},
		{"ghost pill", ToTraQInput{Body: "alice: hi", Format: "org.matrix.custom.html",
			FormattedBody: `<a href="https://matrix.to/#/@traq_alice:example.org">Alice A.</a>: hi`},
			"@alice: hi"},
		{"unknown pill", ToTraQInput{Body: "bob", Format: "org.matrix.custom.html",
			FormattedBody: `<a href="https://matrix.to/#/@bob:example.org">bob</a>`}, "bob"},
		{"room pill becomes its URL", ToTraQInput{Body: "r", Format: "org.matrix.custom.html",
			FormattedBody: `<a href="https://matrix.to/#/!room:example.org">some room</a>`}, "https://matrix.to/#/!room:example.org"},
		{"event pill becomes its URL", ToTraQInput{Body: "e", Format: "org.matrix.custom.html",
			FormattedBody: `<a href="https://matrix.to/#/!room:example.org/$ev">msg</a>`}, "https://matrix.to/#/!room:example.org/$ev"},
		{"spoiler", ToTraQInput{Body: "s", Format: "org.matrix.custom.html",
			FormattedBody: `a <span data-mx-spoiler>s</span>`}, "a !!s!!"},
		{"reply prefix", ToTraQInput{Body: "yes", ReplyTo: m1}, "https://q.example.jp/messages/" + m1 + "\nyes"},
		{"reply fallback stripped", ToTraQInput{Body: "> <@a:x> old\n> more\n\nyes", ReplyTo: m1},
			"https://q.example.jp/messages/" + m1 + "\nyes"},
		{"quote without a reply target is kept", ToTraQInput{Body: "> <@a:x> old\n\nyes"}, "> <@a:x> old\n\nyes"},
		{"mx-reply stripped", ToTraQInput{Body: "yes", Format: "org.matrix.custom.html",
			FormattedBody: "<mx-reply><blockquote>old</blockquote></mx-reply>yes", ReplyTo: m1},
			"https://q.example.jp/messages/" + m1 + "\nyes"},
		{"pill followed by a name character is separated", ToTraQInput{Body: "x", Format: "org.matrix.custom.html",
			FormattedBody: `<a href="https://matrix.to/#/@traq_alice:example.org">Alice</a>san`}, "@alice san"},
		{"pill followed by a hyphen is separated", ToTraQInput{Body: "x", Format: "org.matrix.custom.html",
			FormattedBody: `<a href="https://matrix.to/#/@traq_alice:example.org">Alice</a>-x`}, "@alice -x"},
		{"pill followed by an underscore is separated", ToTraQInput{Body: "x", Format: "org.matrix.custom.html",
			FormattedBody: `<a href="https://matrix.to/#/@traq_alice:example.org">Alice</a>_1`}, "@alice _1"},
		{"pill followed by a non-name character is kept", ToTraQInput{Body: "x", Format: "org.matrix.custom.html",
			FormattedBody: `<a href="https://matrix.to/#/@traq_alice:example.org">Alice</a>さん`}, "@aliceさん"},
		{"pill after a colon is separated", ToTraQInput{Body: "x", Format: "org.matrix.custom.html",
			FormattedBody: `cc:<a href="https://matrix.to/#/@traq_alice:example.org">Alice</a>`}, "cc: @alice"},
		{"pill after a colon and before a name character", ToTraQInput{Body: "x", Format: "org.matrix.custom.html",
			FormattedBody: `cc:<a href="https://matrix.to/#/@traq_alice:example.org">Alice</a>san`}, "cc: @alice san"},
		{"pill after a space is kept", ToTraQInput{Body: "x", Format: "org.matrix.custom.html",
			FormattedBody: `cc: <a href="https://matrix.to/#/@traq_alice:example.org">Alice</a>`}, "cc: @alice"},
		{"unknown pill followed by a name character is kept", ToTraQInput{Body: "x", Format: "org.matrix.custom.html",
			FormattedBody: `<a href="https://matrix.to/#/@bob:example.org">bob</a>by`}, "bobby"},
		{"private-use characters in the text are kept", ToTraQInput{Body: "x", Format: "org.matrix.custom.html",
			FormattedBody: "\ue000&#xe001;<a href=\"https://matrix.to/#/@traq_alice:example.org\">A</a>x\ue000\ue001"},
			"\ue000\ue001@alice x\ue000\ue001"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			c.in.ServerURL = origin
			got, err := ToTraQ(context.Background(), c.in, traqUsers)
			if err != nil || got != c.want {
				t.Errorf("ToTraQ = %q, %v; want %q", got, err, c.want)
			}
		})
	}
}

func TestToTraQLength(t *testing.T) {
	ok := strings.Repeat("あ", MaxTraQLength)
	if _, err := ToTraQ(context.Background(), ToTraQInput{Body: ok, ServerURL: origin}, traqUsers); err != nil {
		t.Errorf("10000 code points rejected: %v", err)
	}
	_, err := ToTraQ(context.Background(), ToTraQInput{Body: ok + "あ", ServerURL: origin}, traqUsers)
	if !errors.Is(err, ErrTooLong) {
		t.Errorf("10001 code points: err = %v, want ErrTooLong", err)
	}
}
