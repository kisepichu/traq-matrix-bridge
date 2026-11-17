package msgconv

import (
	"context"
	"errors"
	"fmt"
	"html"
	"net/url"
	"strings"
	"unicode/utf8"

	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/format"
	"maunium.net/go/mautrix/id"
)

// MaxTraQLength is the longest message traQ accepts, in Unicode code points.
const MaxTraQLength = 10000

// ErrTooLong is returned (wrapped with the actual length) when the converted
// message exceeds MaxTraQLength.
var ErrTooLong = errors.New("message is too long for traQ")

// TraQResolver maps the Matrix side of the bridge to traQ: a user (a ghost, or a Matrix user logged into the
// bridge) to a traQ user name, a portal room to its channel and a bridged event to its message.
type TraQResolver interface {
	TraQUserName(mxid id.UserID) (name string, ok bool)
	ChannelPath(roomID id.RoomID) (path string, ok bool) // "gps/times" for the portal of a public channel
	MessageID(eventID id.EventID) (messageID string, ok bool)
}

// ToTraQInput is a Matrix message to convert.
type ToTraQInput struct {
	Body          string
	Format        string // event.FormatHTML or ""
	FormattedBody string
	ReplyTo       string   // traQ message ID of the reply target, "" if none
	ServerURL     *url.URL // traQ origin
}

// ToTraQ converts a Matrix message into the traQ Markdown to post with embed
// enabled. traQ turns plain @name and #channel into embeds itself, so a user
// pill becomes "@name" and a portal room pill "#path". It must not be an embed
// already: traQ also converts the "@name" inside an embed's raw text, which
// nests one embed in another.
func ToTraQ(ctx context.Context, in ToTraQInput, r TraQResolver) (string, error) {
	var text string
	if in.Format == string(event.FormatHTML) {
		// A pill's "@name" is preceded by start and followed by end, runes not in the input; separatePills
		// replaces them once the text around the pill is known.
		raw := html.UnescapeString(in.FormattedBody)
		end := unusedRune(raw, 0)
		start := unusedRune(raw, end)
		parser := *format.MarkdownHTMLParser
		parser.PillConverter = func(displayname, mxid, eventID string, _ format.Context) string {
			s, isName := traqPill(r, in.ServerURL, displayname, mxid, eventID)
			if isName {
				s = string(start) + s + string(end)
			}
			return s
		}
		parser.SpoilerConverter = func(text, _ string, _ format.Context) string {
			return "!!" + text + "!!"
		}
		text = separatePills(parser.Parse(event.TrimReplyFallbackHTML(in.FormattedBody), format.NewContext(ctx)), start, end)
	} else {
		text = in.Body
		if in.ReplyTo != "" {
			text = trimReplyFallbackBody(text)
		}
	}
	if in.ReplyTo != "" {
		if in.ServerURL == nil {
			return "", errors.New("reply target without a traQ server URL")
		}
		text = in.ServerURL.JoinPath("messages", in.ReplyTo).String() + "\n" + text
	}
	if n := utf8.RuneCountInString(text); n > MaxTraQLength {
		return "", fmt.Errorf("%w: %d characters", ErrTooLong, n)
	}
	return text, nil
}

// traqPill renders a matrix.to link: a resolvable user becomes "@name" and a
// portal room "#path" (which traQ embeds; isName is true), an unknown user its
// display name, a bridged event the URL of its traQ message (which traQ shows
// as a quote), and any other room or event link its URL.
func traqPill(r TraQResolver, server *url.URL, displayname, mxid, eventID string) (s string, isName bool) {
	if mxid == "" || mxid[0] == '@' {
		if name, ok := r.TraQUserName(id.UserID(mxid)); ok {
			return "@" + name, true
		}
		return displayname, false
	}
	if eventID != "" {
		if messageID, ok := r.MessageID(id.EventID(eventID)); ok && server != nil {
			return server.JoinPath("messages", messageID).String(), false
		}
	} else if mxid[0] == '!' { // not a room alias
		if path, ok := r.ChannelPath(id.RoomID(mxid)); ok {
			return "#" + path, true
		}
	}
	link := "https://matrix.to/#/" + mxid
	if eventID != "" {
		link += "/" + eventID
	}
	return link, false
}

// unusedRune returns a rune that s does not contain, from the private use
// area unless s holds all of it. after is skipped, so that two calls with
// the first result as after give distinct runes; 0 means no exclusion.
func unusedRune(s string, after rune) rune {
	r := rune(0xE000)
	for r == after || strings.ContainsRune(s, r) {
		r++
	}
	return r
}

// separatePills removes the start and end marks around each "@name" or
// "#path" pill. traQ embeds neither after ":", so a space is put before a pill
// that follows one. A space is also put after the pill if a traQ name character
// follows: traQ would otherwise read the characters after the pill as part
// of the name and mention another user or channel.
func separatePills(text string, start, end rune) string {
	var b strings.Builder
	b.Grow(len(text))
	var prev rune
	for i, c := range text {
		switch c {
		case start:
			if prev == ':' {
				b.WriteByte(' ')
				prev = ' '
			}
		case end:
			if next, _ := utf8.DecodeRuneInString(text[i+utf8.RuneLen(end):]); isTraQNameRune(next) {
				b.WriteByte(' ')
				prev = ' '
			}
		default:
			b.WriteRune(c)
			prev = c
		}
	}
	return b.String()
}

// isTraQNameRune reports whether c may appear in a traQ user name.
func isTraQNameRune(c rune) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-'
}

// trimReplyFallbackBody drops the "> <@user> ..." quote and the blank line
// that ends it. A body without a blank line is left alone rather than emptied.
func trimReplyFallbackBody(body string) string {
	if !strings.HasPrefix(body, "> <@") {
		return body
	}
	_, rest, found := strings.Cut(body, "\n\n")
	if !found {
		return body
	}
	return rest
}
