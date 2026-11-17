package connector

import (
	"reflect"
	"testing"
	"time"
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
