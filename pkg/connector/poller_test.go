package connector

import (
	"reflect"
	"testing"
	"time"

	"maunium.net/go/mautrix/bridgev2/database"
)

func TestPollCandidates(t *testing.T) {
	now := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	ago := func(d time.Duration) time.Time { return now.Add(-d) }
	window := time.Hour

	for name, tc := range map[string]struct {
		activity map[string]time.Time
		viewed   string
		max      int
		want     []string
	}{
		"outside the window": {
			activity: map[string]time.Time{"a": ago(time.Minute), "old": ago(2 * time.Hour), "edge": ago(window)},
			max:      10,
			want:     []string{"a", "edge"},
		},
		"viewed channel excluded": {
			activity: map[string]time.Time{"a": ago(time.Minute), "v": ago(time.Second)},
			viewed:   "v",
			max:      10,
			want:     []string{"a"},
		},
		"newest first": {
			activity: map[string]time.Time{"a": ago(30 * time.Minute), "b": ago(time.Minute), "c": ago(10 * time.Minute)},
			max:      10,
			want:     []string{"b", "c", "a"},
		},
		"cut at max": {
			activity: map[string]time.Time{"a": ago(30 * time.Minute), "b": ago(time.Minute), "c": ago(10 * time.Minute)},
			max:      2,
			want:     []string{"b", "c"},
		},
		"same time ordered by channel ID": {
			activity: map[string]time.Time{"c": ago(time.Minute), "a": ago(time.Minute), "b": ago(time.Minute)},
			max:      10,
			want:     []string{"a", "b", "c"},
		},
		"nothing active": {
			activity: map[string]time.Time{"old": ago(2 * time.Hour)},
			max:      10,
		},
	} {
		t.Run(name, func(t *testing.T) {
			got := pollCandidates(tc.activity, tc.viewed, now, window, tc.max)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("pollCandidates = %q, want %q", got, tc.want)
			}
		})
	}
}

// A message's time is that of its earliest part: a text part added to a file-only message by an edit has
// the edit's time, and the message would otherwise look newer than it is (and deleted once out of the fetch).
func TestGroupKnown(t *testing.T) {
	base := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	at := func(min int) time.Time { return base.Add(time.Duration(min) * time.Minute) }
	// Newest first, as GetLastNInPortal returns them.
	parts := []*database.Message{
		{ID: MessageID("m"), PartID: textPartID, Timestamp: at(5), SenderID: UserID("u"), Metadata: metaOf("caption")},
		{ID: MessageID("n"), PartID: textPartID, Timestamp: at(3), SenderID: UserID("v"), Metadata: metaOf("N")},
		{ID: MessageID("m"), PartID: filePartID(fileA), Timestamp: at(0), SenderID: UserID("u"), Metadata: &MessageMetadata{}},
	}
	known, senders := groupKnown(parts)
	want := []knownMessage{
		{ID: "m", Timestamp: at(0), ContentHash: ContentHash("caption")},
		{ID: "n", Timestamp: at(3), ContentHash: ContentHash("N")},
	}
	if !reflect.DeepEqual(known, want) {
		t.Errorf("known = %+v, want %+v", known, want)
	}
	if want := map[string]string{"m": "u", "n": "v"}; !reflect.DeepEqual(senders, want) {
		t.Errorf("senders = %v, want %v", senders, want)
	}
}
