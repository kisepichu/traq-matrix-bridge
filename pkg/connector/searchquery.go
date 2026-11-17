package connector

import (
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"maunium.net/go/mautrix/format"

	"github.com/kisepichu/traq-matrix-bridge/pkg/msgconv"
	"github.com/kisepichu/traq-matrix-bridge/pkg/traq"
)

// parsedQuery is the argument of the search command split into its parts.
type parsedQuery struct {
	Words string   // the rest, joined by single spaces; a quoted phrase keeps its quotes, which traQ reads as a phrase
	In    string   // channel path without '#', "" if none (the last in: wins)
	From  []string // user names without '@', in order, deduplicated
}

// parseSearchQuery splits raw into words, in:<channel> and from:<user>. A "..." keeps its spaces in one token,
// also after in: and from:. A filter without a value (in:, from:@) is an ordinary word.
func parseSearchQuery(raw string) parsedQuery {
	var q parsedQuery
	var words []string
	for _, tok := range splitQuoted(raw) {
		if v, ok := filterValue(tok, "in:", '#'); ok {
			q.In = v
		} else if v, ok := filterValue(tok, "from:", '@'); ok {
			if !slices.Contains(q.From, v) {
				q.From = append(q.From, v)
			}
		} else {
			words = append(words, tok)
		}
	}
	q.Words = strings.Join(words, " ")
	return q
}

// splitQuoted splits s at whitespace outside double quotes. Quotes stay in the tokens. An unterminated
// quote runs to the end of s.
func splitQuoted(s string) []string {
	var toks []string
	start, inQuote := -1, false
	for i, r := range s {
		switch {
		case r == '"':
			inQuote = !inQuote
			if start < 0 {
				start = i
			}
		case !inQuote && (r == ' ' || r == '\t' || r == '\n' || r == '\r'):
			if start >= 0 {
				toks = append(toks, s[start:i])
				start = -1
			}
		default:
			if start < 0 {
				start = i
			}
		}
	}
	if start >= 0 {
		toks = append(toks, s[start:])
	}
	return toks
}

// filterValue returns the value of a prefix:value token with its quotes and sigil removed. The bool is
// false if tok is not such a filter or the value is empty.
func filterValue(tok, prefix string, sigil byte) (string, bool) {
	v, ok := strings.CutPrefix(tok, prefix)
	if !ok {
		return "", false
	}
	v = strings.TrimPrefix(v, string(sigil))
	v = strings.TrimSuffix(strings.TrimPrefix(v, `"`), `"`)
	v = strings.TrimPrefix(v, string(sigil))
	return v, v != ""
}

// excerptRunes is the longest excerpt of a message in a search result, in code points.
const excerptRunes = 80

// formatSearchHit renders one search result as a line of markdown: channel, user, time in loc, an excerpt
// of the message and the link. An empty channelPath is a DM.
func formatSearchHit(hit traq.Message, channelPath, userName string, link string, loc *time.Location) string {
	where := "DM"
	if channelPath != "" {
		where = "#" + format.EscapeMarkdown(channelPath)
	}
	var b strings.Builder
	b.WriteString(where)
	b.WriteString("  @")
	b.WriteString(format.EscapeMarkdown(userName))
	b.WriteString("  ")
	b.WriteString(hit.CreatedAt.In(loc).Format("2006-01-02 15:04"))
	b.WriteString("  ")
	b.WriteString(format.EscapeMarkdown(excerpt(hit.Content)))
	b.WriteString("  ")
	b.WriteString(format.EscapeMarkdown(link))
	return b.String()
}

// excerpt is the first excerptRunes code points of content on one line, with … if it was cut. traQ embeds
// show as their text.
func excerpt(content string) string {
	content = strings.NewReplacer("\r\n", " ", "\n", " ", "\r", " ").Replace(msgconv.PlainEmbeds(content))
	if utf8.RuneCountInString(content) <= excerptRunes {
		return content
	}
	i := 0
	for range excerptRunes {
		_, size := utf8.DecodeRuneInString(content[i:])
		i += size
	}
	return content[:i] + "…"
}
