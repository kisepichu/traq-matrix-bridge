package msgconv

import (
	"context"
	"errors"
	"fmt"
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

// TraQUserResolver maps a Matrix user (a ghost, or a Matrix user logged into the bridge) to a traQ user name.
type TraQUserResolver interface {
	TraQUserName(mxid id.UserID) (name string, ok bool)
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
// pill becomes "@name". It must not be an embed already: traQ also converts the
// "@name" inside an embed's raw text, which nests one embed in another.
func ToTraQ(ctx context.Context, in ToTraQInput, r TraQUserResolver) (string, error) {
	var text string
	if in.Format == string(event.FormatHTML) {
		parser := *format.MarkdownHTMLParser
		parser.PillConverter = func(displayname, mxid, eventID string, _ format.Context) string {
			return traqPill(r, displayname, mxid, eventID)
		}
		parser.SpoilerConverter = func(text, _ string, _ format.Context) string {
			return "!!" + text + "!!"
		}
		text = parser.Parse(event.TrimReplyFallbackHTML(in.FormattedBody), format.NewContext(ctx))
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

// traqPill renders a matrix.to link: a resolvable user becomes "@name" (which
// traQ embeds), an unknown user its display name, and a room or event link its URL.
func traqPill(r TraQUserResolver, displayname, mxid, eventID string) string {
	if mxid == "" || mxid[0] == '@' {
		if name, ok := r.TraQUserName(id.UserID(mxid)); ok {
			return "@" + name
		}
		return displayname
	}
	link := "https://matrix.to/#/" + mxid
	if eventID != "" {
		link += "/" + eventID
	}
	return link
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
