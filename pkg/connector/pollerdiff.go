package connector

import (
	"cmp"
	"slices"
	"time"

	"github.com/kisepichu/traq-matrix-bridge/pkg/traq"
)

// knownMessage is what the bridge has bridged of one traQ message.
type knownMessage struct {
	ID string
	// Timestamp is the earliest of the message's parts: a text part added by an edit has the edit's time.
	Timestamp   time.Time
	ContentHash string             // of the content the text part was bridged from; "" when there is no text part
	Reactions   map[[2]string]bool // (userID, stampID) currently bridged; nil when not read, then not compared
}

// pollDiff is what changed in traQ since the messages were bridged.
type pollDiff struct {
	New             []traq.Message // fetched but not bridged (see diffMessages), oldest first
	Edited          []traq.Message // content hash differs
	Deleted         []string       // known IDs that the fetch covers but did not return
	ReactionChanged []traq.Message // (user, stamp) set differs
}

// diffMessages compares fetched (any order, from GET /channels/{id}/messages) with known (bridged). more is
// the fetch's more flag: false when fetched is the channel's whole history.
//
// known must be read before the fetch. Then every known message was in traQ when it was fetched, and the
// fetch has all messages newer than its oldest one, so a known message newer than that but not fetched was
// deleted, also when it is newer than the newest fetched one (the latest message of the channel deleted).
// One at or before the oldest fetched time is not reported: it may be older than the fetch reaches, or
// another message of that time may have been left out by the fetch's limit. When more is false the fetch
// reaches the start of the channel, and every known message not fetched was deleted, also when nothing was
// fetched.
//
// New are the fetched messages that are not known, at or after the oldest fetched message that is known:
// the WebSocket missed them (a dead connection not noticed yet). The fetch holds every message after that
// known one, so bridging these leaves no gap before them. Older ones are history from before the room, or
// beyond the rows read; when no fetched message is known, more messages came than the fetch holds, and the
// catch-up fetches them from the newest bridged one.
//
// A known message without a text part (only files) has no ContentHash and is not checked for edits, since
// nothing tells its content. The stamp count is ignored, like in stampReactions.
func diffMessages(fetched []traq.Message, known []knownMessage, more bool) pollDiff {
	var d pollDiff
	var oldest time.Time
	byID := make(map[string]*traq.Message, len(fetched))
	for i := range fetched {
		m := &fetched[i]
		byID[m.ID] = m
		if i == 0 || m.CreatedAt.Before(oldest) {
			oldest = m.CreatedAt
		}
	}
	isKnown := make(map[string]bool, len(known))
	var anchor *traq.Message // the oldest fetched message that is known
	for _, k := range known {
		isKnown[k.ID] = true
		m, ok := byID[k.ID]
		if !ok {
			if !more || len(fetched) > 0 && k.Timestamp.After(oldest) {
				d.Deleted = append(d.Deleted, k.ID)
			}
			continue
		}
		if anchor == nil || m.CreatedAt.Before(anchor.CreatedAt) {
			anchor = m
		}
		if k.ContentHash != "" && k.ContentHash != ContentHash(m.Content) {
			d.Edited = append(d.Edited, *m)
		}
		if k.Reactions != nil && !sameStamps(m.Stamps, k.Reactions) {
			d.ReactionChanged = append(d.ReactionChanged, *m)
		}
	}
	if anchor != nil {
		for _, m := range fetched {
			if !isKnown[m.ID] && !m.CreatedAt.Before(anchor.CreatedAt) {
				d.New = append(d.New, m)
			}
		}
		slices.SortFunc(d.New, func(a, b traq.Message) int {
			if c := a.CreatedAt.Compare(b.CreatedAt); c != 0 {
				return c
			}
			return cmp.Compare(a.ID, b.ID)
		})
	}
	return d
}

// sameStamps reports whether the (user, stamp) pairs of stamps are exactly those of bridged.
func sameStamps(stamps []traq.MessageStamp, bridged map[[2]string]bool) bool {
	seen := make(map[[2]string]bool, len(stamps))
	for _, s := range stamps {
		key := [2]string{s.UserID, s.StampID}
		if !bridged[key] {
			return false
		}
		seen[key] = true
	}
	for key, ok := range bridged {
		if ok && !seen[key] {
			return false
		}
	}
	return true
}
