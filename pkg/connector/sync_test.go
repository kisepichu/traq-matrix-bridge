package connector

import (
	"maps"
	"reflect"
	"testing"
)

func TestPortalDiff(t *testing.T) {
	pub := func(id string, level int, topic string) channelInfo {
		return channelInfo{ID: id, Path: "p/" + id, Topic: topic, Level: level}
	}
	dm := func(id string, level int) channelInfo {
		return channelInfo{ID: id, DMUserID: "user-" + id, Level: level}
	}
	snap := func(chs ...channelInfo) map[string]channelInfo {
		m := make(map[string]channelInfo, len(chs))
		for _, ch := range chs {
			m[ch.ID] = ch
		}
		return m
	}
	upsert := func(ch channelInfo) portalChange { return portalChange{Channel: ch, Kind: changeUpsert} }
	remove := func(ch channelInfo) portalChange { return portalChange{Channel: ch, Kind: changeRemove} }

	for name, tc := range map[string]struct {
		old, new map[string]channelInfo
		want     []portalChange
	}{
		"new channel":           {snap(), snap(pub("a", 2, "")), []portalChange{upsert(pub("a", 2, ""))}},
		"unchanged":             {snap(pub("a", 2, "t"), dm("d", 2)), snap(pub("a", 2, "t"), dm("d", 2)), nil},
		"topic changed":         {snap(pub("a", 2, "old")), snap(pub("a", 2, "new")), []portalChange{upsert(pub("a", 2, "new"))}},
		"path changed":          {snap(pub("a", 2, "")), snap(channelInfo{ID: "a", Path: "moved/a", Level: 2}), []portalChange{upsert(channelInfo{ID: "a", Path: "moved/a", Level: 2})}},
		"level 2 to 1":          {snap(pub("a", 2, "")), snap(pub("a", 1, "")), []portalChange{upsert(pub("a", 1, ""))}},
		"level 1 to 0":          {snap(pub("a", 1, "")), snap(pub("a", 0, "")), []portalChange{remove(pub("a", 0, ""))}},
		"channel gone":          {snap(pub("a", 2, "")), snap(), []portalChange{remove(pub("a", 2, ""))}},
		"level 0 stays level 0": {snap(pub("a", 0, "")), snap(pub("a", 0, "")), nil},
		"new level 0 channel":   {snap(), snap(pub("a", 0, "")), nil},
		"level 0 to 1":          {snap(pub("a", 0, "")), snap(pub("a", 1, "")), []portalChange{upsert(pub("a", 1, ""))}},
		"DM kept at level 0":    {snap(dm("d", 0)), snap(dm("d", 0)), nil},
		"new DM at level 0":     {snap(), snap(dm("d", 0)), []portalChange{upsert(dm("d", 0))}},
		"sorted by channel ID": {
			snap(pub("b", 1, ""), pub("c", 1, "")),
			snap(pub("a", 1, ""), pub("c", 2, "")),
			[]portalChange{upsert(pub("a", 1, "")), remove(pub("b", 1, "")), upsert(pub("c", 2, ""))},
		},
	} {
		t.Run(name, func(t *testing.T) {
			if got := portalDiff(tc.old, tc.new); !reflect.DeepEqual(got, tc.want) {
				t.Errorf("portalDiff =\n%+v\nwant\n%+v", got, tc.want)
			}
		})
	}
}

// An upsert that could not be queued must be emitted again by the next sync of the same traQ state.
func TestRevertFailedUpserts(t *testing.T) {
	pub := func(id string, level int) channelInfo { return channelInfo{ID: id, Path: "p/" + id, Level: level} }
	snap := func(chs ...channelInfo) map[string]channelInfo {
		m := make(map[string]channelInfo, len(chs))
		for _, ch := range chs {
			m[ch.ID] = ch
		}
		return m
	}
	upsert := func(ch channelInfo) portalChange { return portalChange{Channel: ch, Kind: changeUpsert} }

	for name, tc := range map[string]struct {
		old, new map[string]channelInfo
		failed   []channelInfo
		want     []portalChange
	}{
		"periodic sync": {
			old:    snap(pub("changed", 1), pub("same", 2), pub("ok", 1)),
			new:    snap(pub("changed", 2), pub("same", 2), pub("ok", 2), pub("added", 1)),
			failed: []channelInfo{pub("changed", 2), pub("added", 1)},
			want:   []portalChange{upsert(pub("added", 1)), upsert(pub("changed", 2))},
		},
		// initialSync upserts every target whatever the previous snapshot held, so a failure there has no old
		// entry to restore.
		"initial sync": {
			old:    nil,
			new:    snap(pub("a", 1), pub("b", 2)),
			failed: []channelInfo{pub("b", 2)},
			want:   []portalChange{upsert(pub("b", 2))},
		},
		"nothing failed": {
			old:  snap(pub("a", 1)),
			new:  snap(pub("a", 2)),
			want: nil,
		},
	} {
		t.Run(name, func(t *testing.T) {
			stored := maps.Clone(tc.new)
			revertFailedUpserts(stored, tc.old, tc.failed)
			if got := portalDiff(stored, tc.new); !reflect.DeepEqual(got, tc.want) {
				t.Errorf("next sync =\n%+v\nwant\n%+v", got, tc.want)
			}
		})
	}
}
