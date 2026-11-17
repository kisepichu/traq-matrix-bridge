package connector

import (
	"reflect"
	"testing"
	"time"

	"github.com/kisepichu/traq-matrix-bridge/pkg/traq"
)

func TestDiffMessages(t *testing.T) {
	base := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	at := func(min int) time.Time { return base.Add(time.Duration(min) * time.Minute) }
	msg := func(id string, min int, content string, stamps ...traq.MessageStamp) traq.Message {
		return traq.Message{ID: id, CreatedAt: at(min), Content: content, Stamps: stamps}
	}
	stamp := func(user, id string, count int) traq.MessageStamp {
		return traq.MessageStamp{UserID: user, StampID: id, Count: count}
	}
	known := func(id string, min int, content string, reactions ...[2]string) knownMessage {
		k := knownMessage{ID: id, Timestamp: at(min), ContentHash: ContentHash(content), Reactions: map[[2]string]bool{}}
		for _, r := range reactions {
			k.Reactions[r] = true
		}
		return k
	}
	ids := func(msgs []traq.Message) []string {
		var out []string
		for _, m := range msgs {
			out = append(out, m.ID)
		}
		return out
	}

	for name, tc := range map[string]struct {
		fetched                     []traq.Message
		complete                    bool // the fetch is the channel's whole history (more == false)
		known                       []knownMessage
		new, edited, deleted, stamp []string
	}{
		"no change": {
			fetched: []traq.Message{msg("b", 2, "B", stamp("u", "s", 1)), msg("a", 1, "A")},
			known:   []knownMessage{known("a", 1, "A"), known("b", 2, "B", [2]string{"u", "s"})},
		},
		"content hash differs": {
			fetched: []traq.Message{msg("a", 1, "A edited")},
			known:   []knownMessage{known("a", 1, "A")},
			edited:  []string{"a"},
		},
		"known without a text part is not checked for edits": {
			fetched: []traq.Message{msg("a", 1, "http://traq/files/x")},
			known:   []knownMessage{{ID: "a", Timestamp: at(1)}},
		},
		"gone inside the range": {
			fetched: []traq.Message{msg("c", 3, "C"), msg("a", 1, "A")},
			known:   []knownMessage{known("a", 1, "A"), known("b", 2, "B"), known("c", 3, "C")},
			deleted: []string{"b"},
		},
		"older than the range": {
			fetched: []traq.Message{msg("c", 3, "C"), msg("b", 2, "B")},
			known:   []knownMessage{known("a", 1, "A"), known("b", 2, "B"), known("c", 3, "C")},
		},
		"at the oldest fetched time but not fetched": {
			// Another message with the same time as the oldest fetched one may have been cut off by the limit.
			fetched: []traq.Message{msg("c", 3, "C"), msg("b", 2, "B")},
			known:   []knownMessage{known("a", 2, "A"), known("b", 2, "B"), known("c", 3, "C")},
		},
		"newer than the newest fetched": {
			// known is read before the fetch, so a message missing from it was deleted, also the newest one.
			fetched: []traq.Message{msg("b", 2, "B"), msg("a", 1, "A")},
			known:   []knownMessage{known("a", 1, "A"), known("b", 2, "B"), known("c", 3, "C")},
			deleted: []string{"c"},
		},
		"stamp added": {
			fetched: []traq.Message{msg("a", 1, "A", stamp("u", "s", 1), stamp("v", "s", 1))},
			known:   []knownMessage{known("a", 1, "A", [2]string{"u", "s"})},
			stamp:   []string{"a"},
		},
		"stamp removed": {
			fetched: []traq.Message{msg("a", 1, "A")},
			known:   []knownMessage{known("a", 1, "A", [2]string{"u", "s"})},
			stamp:   []string{"a"},
		},
		"stamp replaced by another": {
			fetched: []traq.Message{msg("a", 1, "A", stamp("u", "t", 1))},
			known:   []knownMessage{known("a", 1, "A", [2]string{"u", "s"})},
			stamp:   []string{"a"},
		},
		"only the count grew": {
			fetched: []traq.Message{msg("a", 1, "A", stamp("u", "s", 5))},
			known:   []knownMessage{known("a", 1, "A", [2]string{"u", "s"})},
		},
		"edited and stamped": {
			fetched: []traq.Message{msg("a", 1, "A2", stamp("u", "s", 1))},
			known:   []knownMessage{known("a", 1, "A")},
			edited:  []string{"a"},
			stamp:   []string{"a"},
		},
		"nothing fetched": {
			known: []knownMessage{known("a", 1, "A")},
		},
		"fetched but not bridged": {
			// Missed by the WebSocket (a dead connection not noticed yet).
			fetched: []traq.Message{msg("b", 2, "B", stamp("u", "s", 1)), msg("a", 1, "A")},
			known:   []knownMessage{known("a", 1, "A")},
			new:     []string{"b"},
		},
		"not bridged, oldest first": {
			fetched: []traq.Message{msg("d", 4, "D"), msg("b", 2, "B"), msg("c", 3, "C"), msg("a", 1, "A")},
			known:   []knownMessage{known("a", 1, "A"), known("c", 3, "C")},
			new:     []string{"b", "d"},
		},
		"not bridged and older than every fetched bridged message": {
			// History from before the room, or beyond the rows read: not posted as new messages.
			fetched: []traq.Message{msg("c", 3, "C"), msg("b", 2, "B"), msg("a", 1, "A")},
			known:   []knownMessage{known("b", 2, "B")},
			new:     []string{"c"},
		},
		"not bridged and no fetched message is bridged": {
			// More messages than the fetch holds came since the newest bridged one: the catch-up's to fetch.
			fetched: []traq.Message{msg("c", 3, "C"), msg("b", 2, "B")},
			known:   []knownMessage{known("a", 1, "A")},
		},
		"reactions not read are not compared": {
			fetched: []traq.Message{msg("a", 1, "A", stamp("u", "s", 1))},
			known:   []knownMessage{{ID: "a", Timestamp: at(1), ContentHash: ContentHash("A")}},
		},
		"whole history: older than the fetched ones": {
			fetched:  []traq.Message{msg("c", 3, "C"), msg("b", 2, "B")},
			complete: true,
			known:    []knownMessage{known("a", 1, "A"), known("b", 2, "B"), known("c", 3, "C")},
			deleted:  []string{"a"},
		},
		"whole history: at the oldest fetched time": {
			fetched:  []traq.Message{msg("b", 2, "B")},
			complete: true,
			known:    []knownMessage{known("a", 2, "A"), known("b", 2, "B")},
			deleted:  []string{"a"},
		},
		"whole history: nothing fetched": {
			// A DM whose only message was deleted.
			complete: true,
			known:    []knownMessage{known("a", 1, "A")},
			deleted:  []string{"a"},
		},
	} {
		t.Run(name, func(t *testing.T) {
			d := diffMessages(tc.fetched, tc.known, !tc.complete)
			if got := ids(d.New); !reflect.DeepEqual(got, tc.new) {
				t.Errorf("New = %q, want %q", got, tc.new)
			}
			if got := ids(d.Edited); !reflect.DeepEqual(got, tc.edited) {
				t.Errorf("Edited = %q, want %q", got, tc.edited)
			}
			if !reflect.DeepEqual(d.Deleted, tc.deleted) {
				t.Errorf("Deleted = %q, want %q", d.Deleted, tc.deleted)
			}
			if got := ids(d.ReactionChanged); !reflect.DeepEqual(got, tc.stamp) {
				t.Errorf("ReactionChanged = %q, want %q", got, tc.stamp)
			}
		})
	}
}
