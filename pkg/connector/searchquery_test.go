package connector

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/kisepichu/traq-matrix-bridge/pkg/traq"
)

func TestParseSearchQuery(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want parsedQuery
	}{
		{"words", "foo bar", parsedQuery{Words: "foo bar"}},
		{"extra spaces", "  foo   bar ", parsedQuery{Words: "foo bar"}},
		{"in with hash", "in:#a/b foo", parsedQuery{Words: "foo", In: "a/b"}},
		{"in without hash", "in:a/b", parsedQuery{In: "a/b"}},
		{"from with and without at", "from:@x from:y foo", parsedQuery{Words: "foo", From: []string{"x", "y"}}},
		{"from deduplicated", "from:@x from:@x", parsedQuery{From: []string{"x"}}},
		{"from deduplicated across spellings", "from:@x foo from:x", parsedQuery{Words: "foo", From: []string{"x"}}},
		{"last in wins", "in:#a in:#b foo", parsedQuery{Words: "foo", In: "b"}},
		{"quotes", `"hello world" in:"a/b c"`, parsedQuery{Words: `"hello world"`, In: "a/b c"}},
		{"quoted from", `from:"@x y" z`, parsedQuery{Words: "z", From: []string{"x y"}}},
		{"quoted hash inside the value", `in:"#a b"`, parsedQuery{In: "a b"}},
		{"filter between words", "foo in:#a bar", parsedQuery{Words: "foo bar", In: "a"}},
		{"empty filter value is a word", "in: from:@ foo", parsedQuery{Words: "in: from:@ foo"}},
		{"unterminated quote", `"foo bar in:a`, parsedQuery{Words: `"foo bar in:a`}},
		{"filter name in a word", "xin:a", parsedQuery{Words: "xin:a"}},
		{"empty", "", parsedQuery{}},
		{"blank", "  \t ", parsedQuery{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := parseSearchQuery(tc.raw)
			if got.Words != tc.want.Words || got.In != tc.want.In || !reflect.DeepEqual(got.From, tc.want.From) {
				t.Errorf("parseSearchQuery(%q) = %+v, want %+v", tc.raw, got, tc.want)
			}
		})
	}
}

func TestFormatSearchHit(t *testing.T) {
	loc := time.FixedZone("JST", 9*3600)
	at := time.Date(2026, 1, 2, 6, 4, 5, 0, time.UTC) // 15:04 in loc
	hit := func(content string) traq.Message { return traq.Message{CreatedAt: at, Content: content} }
	const link = "https://matrix.to/#/!r:example.org/$e"

	cases := []struct {
		name    string
		hit     traq.Message
		channel string
		want    string
	}{
		{"plain", hit("hello"), "gps/times", "#gps/times  @bob  2026-01-02 15:04  hello  " + link},
		{"newlines become spaces", hit("a\nb\r\nc\rd"), "gps", "#gps  @bob  2026-01-02 15:04  a b c d  " + link},
		{"markdown is escaped", hit("*a* _b_ [c](d) `e` <f>"), "gps",
			"#gps  @bob  2026-01-02 15:04  \\*a\\* \\_b\\_ \\[c\\]\\(d\\) \\`e\\` &lt;f&gt;  " + link},
		{"80 code points are kept", hit(strings.Repeat("あ", 80)), "gps",
			"#gps  @bob  2026-01-02 15:04  " + strings.Repeat("あ", 80) + "  " + link},
		{"81 code points are cut", hit(strings.Repeat("あ", 81)), "gps",
			"#gps  @bob  2026-01-02 15:04  " + strings.Repeat("あ", 80) + "…  " + link},
		{"escape is not counted in the limit", hit(strings.Repeat("*", 80)), "gps",
			"#gps  @bob  2026-01-02 15:04  " + strings.Repeat("\\*", 80) + "  " + link},
		{"names are escaped", hit("x"), "a_b/c_d", "#a\\_b/c\\_d  @bob  2026-01-02 15:04  x  " + link},
		{"DM has no path", hit("x"), "", "DM  @bob  2026-01-02 15:04  x  " + link},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := formatSearchHit(tc.hit, tc.channel, "bob", link, loc); got != tc.want {
				t.Errorf("formatSearchHit =\n%q\nwant\n%q", got, tc.want)
			}
		})
	}
}

func TestFormatSearchHitUsesTheGivenZone(t *testing.T) {
	at := time.Date(2026, 1, 2, 23, 30, 0, 0, time.UTC)
	got := formatSearchHit(traq.Message{CreatedAt: at}, "g", "u", "l", time.UTC)
	if !strings.Contains(got, "2026-01-02 23:30") {
		t.Errorf("got %q, want the UTC time", got)
	}
}
