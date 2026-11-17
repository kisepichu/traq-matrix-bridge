package connector

import (
	"reflect"
	"testing"
	"time"

	"maunium.net/go/mautrix/bridgev2/database"

	"github.com/kisepichu/traq-matrix-bridge/pkg/traq"
)

func TestCatchUpChannels(t *testing.T) {
	since := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	targets := map[string]channelInfo{
		"pub-before": {ID: "pub-before", Level: 2},
		"pub-at":     {ID: "pub-at", Level: 2},
		"pub-after":  {ID: "pub-after", Level: 2},
		"pub-unread": {ID: "pub-unread", Level: 1},
		"pub-quiet":  {ID: "pub-quiet", Level: 2},
		"dm":         {ID: "dm", DMUserID: "bob"},
	}
	unread := []traq.UnreadChannel{{ChannelID: "pub-unread"}, {ChannelID: "not-a-target"}}
	timeline := []traq.ActivityMessage{
		{ChannelID: "pub-after", CreatedAt: since.Add(time.Second)},
		{ChannelID: "pub-at", CreatedAt: since},
		{ChannelID: "pub-before", CreatedAt: since.Add(-time.Second)},
		{ChannelID: "unsubscribed", CreatedAt: since.Add(time.Minute)},
	}

	for name, tc := range map[string]struct {
		since time.Time
		want  []string
	}{
		"since the disconnect": {since: since, want: []string{"dm", "pub-after", "pub-at", "pub-unread"}},
		"zero since takes the whole timeline": {
			want: []string{"dm", "pub-after", "pub-at", "pub-before", "pub-unread"},
		},
	} {
		t.Run(name, func(t *testing.T) {
			if got := catchUpChannels(targets, unread, timeline, tc.since); !reflect.DeepEqual(got, tc.want) {
				t.Errorf("catchUpChannels = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestHasNewerMessage(t *testing.T) {
	t0 := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	latest := &database.Message{ID: MessageID("a"), Timestamp: t0}
	for name, tc := range map[string]struct {
		newest []traq.Message
		latest *database.Message
		want   bool
	}{
		"no messages in traQ":          {latest: latest},
		"empty room":                   {newest: []traq.Message{{ID: "a", CreatedAt: t0}}, want: true},
		"the newest is bridged":        {newest: []traq.Message{{ID: "a", CreatedAt: t0}}, latest: latest},
		"another one at the same time": {newest: []traq.Message{{ID: "b", CreatedAt: t0}}, latest: latest, want: true},
		"a newer one":                  {newest: []traq.Message{{ID: "b", CreatedAt: t0.Add(time.Millisecond)}}, latest: latest, want: true},
		// The newest bridged one was deleted in traQ; the one before it is older and was bridged.
		"only older ones": {newest: []traq.Message{{ID: "z", CreatedAt: t0.Add(-time.Second)}}, latest: latest},
	} {
		t.Run(name, func(t *testing.T) {
			if got := hasNewerMessage(tc.newest, tc.latest); got != tc.want {
				t.Errorf("hasNewerMessage = %v, want %v", got, tc.want)
			}
		})
	}
}
