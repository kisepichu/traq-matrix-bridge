package connector

import (
	"time"

	"github.com/kisepichu/traq-matrix-bridge/pkg/traq"
)

// knownMessage is what the bridge has bridged of one traQ message.
type knownMessage struct {
	ID          string
	Timestamp   time.Time
	ContentHash string             // of the content the text part was bridged from; "" when there is no text part
	Reactions   map[[2]string]bool // (userID, stampID) currently bridged
}

// pollDiff is what changed in traQ since the messages were bridged.
type pollDiff struct {
	Edited          []traq.Message // content hash differs
	Deleted         []string       // known IDs inside the fetched time range but not fetched
	ReactionChanged []traq.Message // (user, stamp) set differs
}

// diffMessages compares fetched (any order, from GET /channels/{id}/messages) with known (bridged).
// Known messages at or before the oldest fetched CreatedAt are never reported as deleted.
// If fetched is empty nothing is reported.
//
// known must be read before the fetch. Then every known message was in traQ when it was fetched, and the
// fetch has all messages newer than its oldest one, so a known message newer than that but not fetched was
// deleted, also when it is newer than the newest fetched one (the latest message of the channel deleted).
// One at exactly the oldest fetched time is not reported: another message of that time may have been left
// out by the fetch's limit.
//
// A known message without a text part (only files) has no ContentHash and is not checked for edits, since
// nothing tells its content. Fetched messages that are not known are left to the WebSocket and the
// catch-up. The stamp count is ignored, like in stampReactions.
func diffMessages(fetched []traq.Message, known []knownMessage) pollDiff {
	var d pollDiff
	if len(fetched) == 0 {
		return d
	}
	oldest := fetched[0].CreatedAt
	byID := make(map[string]*traq.Message, len(fetched))
	for i := range fetched {
		m := &fetched[i]
		byID[m.ID] = m
		if m.CreatedAt.Before(oldest) {
			oldest = m.CreatedAt
		}
	}
	for _, k := range known {
		m, ok := byID[k.ID]
		if !ok {
			if k.Timestamp.After(oldest) {
				d.Deleted = append(d.Deleted, k.ID)
			}
			continue
		}
		if k.ContentHash != "" && k.ContentHash != ContentHash(m.Content) {
			d.Edited = append(d.Edited, *m)
		}
		if !sameStamps(m.Stamps, k.Reactions) {
			d.ReactionChanged = append(d.ReactionChanged, *m)
		}
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
