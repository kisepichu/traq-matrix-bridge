package msgconv

import (
	"context"
	"testing"
)

// A traQ message converted to Matrix and back, as a Matrix edit of it is, keeps its channel embeds and quotes:
// traQ embeds "#path" itself, and a message URL becomes a quote again.
func TestRoundTrip(t *testing.T) {
	const quote1, quote2 = "https://q.example.jp/messages/" + m1, "https://q.example.jp/messages/" + m2
	cases := []struct {
		name    string
		content string
		want    string
	}{
		{"channel embed", `see !{"type":"channel","raw":"#gps/general","id":"` + cGen + `"} now`, "see #gps/general now"},
		// The first quote becomes the reply, whose quote line goes first; the second stays a quote.
		{"second quote", "a\n" + quote1 + "\n" + quote2, quote1 + "\na\n" + quote2},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			m := ToMatrix(input(c.content), resolver)
			got, err := ToTraQ(context.Background(), ToTraQInput{
				Body: m.Body, Format: "org.matrix.custom.html", FormattedBody: m.HTML, ReplyTo: m.ReplyTo, ServerURL: origin,
			}, traqUsers)
			if err != nil || got != c.want {
				t.Errorf("ToTraQ(ToMatrix(%q)) = %q, %v; want %q (Matrix HTML %q)", c.content, got, err, c.want, m.HTML)
			}
		})
	}
}
