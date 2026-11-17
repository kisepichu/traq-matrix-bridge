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

// A snapshot that would remove many public portals at once, or that has no subscriptions while public portals
// exist, is more likely a bad answer from traQ than the user's doing: its removals are held back, and applied
// only when the next snapshot removes the same portals.
func TestHoldRemovals(t *testing.T) {
	pub := func(id string) channelInfo { return channelInfo{ID: id, Path: "p/" + id, Level: 1} }
	dm := func(id string) channelInfo { return channelInfo{ID: id, DMUserID: "user-" + id, Level: dmLevel} }
	snap := func(chs ...channelInfo) map[string]channelInfo {
		m := make(map[string]channelInfo, len(chs))
		for _, ch := range chs {
			m[ch.ID] = ch
		}
		return m
	}
	pubs := func(ids ...string) []channelInfo {
		var chs []channelInfo
		for _, id := range ids {
			chs = append(chs, pub(id))
		}
		return chs
	}
	ten := snap(pubs("a", "b", "c", "d", "e", "f", "g", "h", "i", "j")...)
	upsert := func(ch channelInfo) portalChange { return portalChange{Channel: ch, Kind: changeUpsert} }
	removals := func(ids ...string) []portalChange {
		var cs []portalChange
		for _, id := range ids {
			cs = append(cs, portalChange{Channel: pub(id), Kind: changeRemove})
		}
		return cs
	}

	for name, tc := range map[string]struct {
		old       map[string]channelInfo
		changes   []portalChange
		noSubs    bool
		held      []string
		wantApply []portalChange
		wantHeld  []string
	}{
		"a few removals": {ten, removals("a", "b"), false, nil, removals("a", "b"), nil},
		"exactly half":   {ten, removals("a", "b", "c", "d", "e"), false, nil, removals("a", "b", "c", "d", "e"), nil},
		"more than half": {
			ten, append([]portalChange{upsert(pub("new"))}, removals("a", "b", "c", "d", "e", "f")...), false, nil,
			[]portalChange{upsert(pub("new"))}, []string{"a", "b", "c", "d", "e", "f"},
		},
		"more than half again": {
			ten, removals("a", "b", "c", "d", "e", "f"), false, []string{"a", "b", "c", "d", "e", "f"},
			removals("a", "b", "c", "d", "e", "f"), nil,
		},
		"more than half, but other ones than before": {
			ten, removals("a", "b", "c", "d", "e", "g"), false, []string{"a", "b", "c", "d", "e", "f"},
			nil, []string{"a", "b", "c", "d", "e", "g"},
		},
		"all of fewer than the minimum": {snap(pubs("a", "b", "c", "d")...), removals("a", "b", "c", "d"), false, nil, removals("a", "b", "c", "d"), nil},
		"no subscriptions":              {snap(pubs("a", "b")...), removals("a", "b"), true, nil, nil, []string{"a", "b"}},
		"no subscriptions again":        {snap(pubs("a", "b")...), removals("a", "b"), true, []string{"a", "b"}, removals("a", "b"), nil},
		"no subscriptions and no public portals": {
			snap(dm("d")), nil, true, nil, nil, nil,
		},
		"DMs do not count": {
			snap(append(pubs("a"), dm("d1"), dm("d2"), dm("d3"), dm("d4"), dm("d5"), dm("d6"))...),
			[]portalChange{{Channel: dm("d1"), Kind: changeRemove}, {Channel: dm("d2"), Kind: changeRemove}, {Channel: dm("d3"), Kind: changeRemove}, {Channel: dm("d4"), Kind: changeRemove}, {Channel: dm("d5"), Kind: changeRemove}},
			false, nil,
			[]portalChange{{Channel: dm("d1"), Kind: changeRemove}, {Channel: dm("d2"), Kind: changeRemove}, {Channel: dm("d3"), Kind: changeRemove}, {Channel: dm("d4"), Kind: changeRemove}, {Channel: dm("d5"), Kind: changeRemove}},
			nil,
		},
		"held removals no longer wanted": {ten, nil, false, []string{"a", "b", "c", "d", "e", "f"}, nil, nil},
	} {
		t.Run(name, func(t *testing.T) {
			apply, held := holdRemovals(tc.old, tc.changes, tc.noSubs, tc.held)
			if !reflect.DeepEqual(apply, tc.wantApply) || !reflect.DeepEqual(held, tc.wantHeld) {
				t.Errorf("holdRemovals = %+v, held %v; want %+v, held %v", apply, held, tc.wantApply, tc.wantHeld)
			}
		})
	}
}
