package msgconv

import (
	"context"
	"testing"
)

// A traQ message converted to Matrix and back, as a Matrix edit of it is, keeps its channel embeds and quotes:
// traQ embeds "#path" itself, and a message URL becomes a quote again. appended is the text the edit adds to
// the end of the Matrix message.
func TestRoundTrip(t *testing.T) {
	const quote1, quote2 = "https://q.example.jp/messages/" + m1, "https://q.example.jp/messages/" + m2
	cases := []struct {
		name     string
		content  string
		appended string
		want     string
	}{
		{"channel embed", `see !{"type":"channel","raw":"#gps/general","id":"` + cGen + `"} now`, "", "see #gps/general now"},
		// The first quote becomes the reply, whose quote line goes first; the second stays a quote.
		{"second quote", "a\n" + quote1 + "\n" + quote2, "", quote1 + "\na\n" + quote2},
		// A lone quote stays visible in Matrix as the link to the reply target; it must not come back twice.
		{"quote only", quote1, "", quote1},
		// Nor when the edit adds text on the quote's line.
		{"quote only, text added", quote1, " (edited)", quote1 + " (edited)"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			m := ToMatrix(input(c.content), resolver)
			got, err := ToTraQ(context.Background(), ToTraQInput{
				Body: m.Body + c.appended, Format: "org.matrix.custom.html", FormattedBody: m.HTML + c.appended,
				ReplyTo: m.ReplyTo, ServerURL: origin,
			}, traqUsers)
			if err != nil || got != c.want {
				t.Errorf("ToTraQ(ToMatrix(%q) + %q) = %q, %v; want %q (Matrix HTML %q)", c.content, c.appended, got, err, c.want, m.HTML)
			}
		})
	}
}
