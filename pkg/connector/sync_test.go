package connector

import (
	"context"
	"maps"
	"net/http"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/bridgeconfig"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/id"

	"github.com/kisepichu/traq-matrix-bridge/pkg/traq"
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

// At startup, the portals with a room that are no longer targets are left by the same rule as at a subscription
// sync: when that looks like a bad answer from traQ, none is left, and the first subscription sync that agrees
// (the same removals) leaves them.
func TestStartupRemovals(t *testing.T) {
	pub := func(id string) channelInfo { return channelInfo{ID: id, Path: "p/" + id, Level: 1} }
	dm := func(id string) channelInfo { return channelInfo{ID: id, DMUserID: "user-" + id, Level: dmLevel} }
	pubs := func(ids ...string) []channelInfo {
		var chs []channelInfo
		for _, id := range ids {
			chs = append(chs, pub(id))
		}
		return chs
	}
	for name, tc := range map[string]struct {
		kept, gone []channelInfo
		noSubs     bool
		wantHeld   []string
	}{
		"a few":                  {pubs("a", "b", "c", "d", "e", "f", "g", "h"), pubs("y", "x"), false, nil},
		"exactly half":           {pubs("a", "b", "c", "d", "e"), pubs("v", "w", "x", "y", "z"), false, nil},
		"more than half":         {pubs("a", "b", "c", "d"), pubs("z", "v", "w", "x", "y"), false, []string{"v", "w", "x", "y", "z"}},
		"fewer than the minimum": {nil, pubs("a", "b", "c", "d"), false, nil},
		"no subscriptions":       {[]channelInfo{dm("d")}, append(pubs("a"), dm("e")), true, []string{"a", "e"}},
		"only DMs":               {pubs("a"), []channelInfo{dm("d1"), dm("d2"), dm("d3"), dm("d4"), dm("d5")}, false, nil},
		"nothing to leave":       {pubs("a"), nil, true, nil},
	} {
		t.Run(name, func(t *testing.T) {
			if held := startupRemovals(tc.kept, tc.gone, tc.noSubs); !reflect.DeepEqual(held, tc.wantHeld) {
				t.Fatalf("startupRemovals = %v, want %v", held, tc.wantHeld)
			}
		})
	}

	// The first subscription sync after held startup removals: initialSync kept the gone channels in c.targets,
	// and the sync removes the same ones.
	kept, gone := pubs("a", "b", "c", "d"), pubs("z", "v", "w", "x", "y")
	held := startupRemovals(kept, gone, false)
	targets, snap := map[string]channelInfo{}, map[string]channelInfo{}
	for _, ch := range kept {
		targets[ch.ID], snap[ch.ID] = ch, ch
	}
	for _, ch := range gone {
		targets[ch.ID] = ch
	}
	apply, nowHeld := holdRemovals(targets, portalDiff(targets, snap), false, held)
	var left []string
	for _, change := range apply {
		if change.Kind == changeRemove {
			left = append(left, change.Channel.ID)
		}
	}
	if !slices.Equal(left, []string{"v", "w", "x", "y", "z"}) || nowHeld != nil {
		t.Errorf("agreeing sync leaves %v, holds %v; want [v w x y z], nothing held", left, nowHeld)
	}
}

// A portal whose removal was held back (by the previous subscription sync, or at startup) may have missed
// messages, at least those posted while the bridge was stopped. When a sync keeps it, it is resynced with a
// backfill check, so those messages are fetched into its room. One that is held back again waits.
func TestSyncSubscriptionsCatchesUpKeptHeldPortals(t *testing.T) {
	const channels = `{"public":[{"id":"a","name":"a"},{"id":"b","name":"b"},{"id":"c","name":"c"}],"dm":[]}`
	for name, tc := range map[string]struct {
		subs      string
		wantCheck []string // the channels resynced, each with a backfill check
		wantHeld  []string
	}{
		"kept":       {`[{"channelId":"a","level":1},{"channelId":"b","level":1},{"channelId":"c","level":2}]`, []string{"b", "c"}, nil},
		"held again": {`[]`, nil, []string{"a", "b", "c"}},
	} {
		t.Run(name, func(t *testing.T) {
			mux := http.NewServeMux()
			mux.HandleFunc("GET /api/v3/channels", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(channels)) })
			mux.HandleFunc("GET /api/v3/users/me/subscriptions", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(tc.subs)) })
			c := &TraQClient{Login: testLogin("me", false), API: newMemAPI(t, mux)}
			c.Login.Log = zerolog.Nop()
			// The previous snapshot: a was a target; b and c were kept only because their removal was held back.
			c.targets = map[string]channelInfo{
				"a": {ID: "a", Path: "a", Level: 1},
				"b": {ID: "b", Path: "b", Level: 1},
				"c": {ID: "c", Path: "c", Level: 1},
			}
			c.heldRemovals = []string{"b", "c"}
			p := newResyncPacer(t.Context(), func(context.Context, *pendingResync) {})
			c.resyncs.Store(p)

			if err := c.syncSubscriptions(t.Context()); err != nil {
				t.Fatal(err)
			}
			p.mu.Lock()
			var resynced, checked []string
			for id, r := range p.waiting {
				resynced = append(resynced, id)
				if r.needsBackfill != nil {
					checked = append(checked, id)
				}
			}
			p.mu.Unlock()
			slices.Sort(resynced)
			slices.Sort(checked)
			if !slices.Equal(resynced, tc.wantCheck) || !slices.Equal(checked, tc.wantCheck) {
				t.Errorf("resynced %v, with a backfill check %v; want %v, all with one", resynced, checked, tc.wantCheck)
			}
			if !slices.Equal(c.heldRemovals, tc.wantHeld) {
				t.Errorf("held %v, want %v", c.heldRemovals, tc.wantHeld)
			}
		})
	}
}

// testMatrix and testNetwork are just enough of the two connectors for bridgev2.NewBridge and for loading
// portals from the database.
type testMatrix struct{ bridgev2.MatrixConnector }

func (testMatrix) Init(*bridgev2.Bridge)         {}
func (testMatrix) BotIntent() bridgev2.MatrixAPI { return nil }

type testNetwork struct{ bridgev2.NetworkConnector }

func (testNetwork) Init(*bridgev2.Bridge)              {}
func (testNetwork) GetDBMetaTypes() database.MetaTypes { return (&TraQConnector{}).GetDBMetaTypes() }
func (testNetwork) GetCapabilities() *bridgev2.NetworkGeneralCapabilities {
	return &bridgev2.NetworkGeneralCapabilities{}
}

// At startup, when the portals that are no longer targets look like a bad answer from traQ, initialSync leaves
// none: it keeps them as targets, holds their removals for the first subscription sync, and loads their last
// activity for the poller.
func TestInitialSyncHoldsMassRemovals(t *testing.T) {
	ctx := t.Context()
	br := bridgev2.NewBridge("test", newTestRawDB(t), zerolog.Nop(), &bridgeconfig.BridgeConfig{}, testMatrix{}, testNetwork{},
		func(*bridgev2.Bridge) bridgev2.CommandProcessor { return nil })
	if err := br.DB.Upgrade(ctx); err != nil {
		t.Fatal(err)
	}
	br.BackgroundCtx = ctx
	const mxid = id.UserID("@me:example.com")
	login := &database.UserLogin{BridgeID: "test", UserMXID: mxid, ID: "me", Metadata: &UserLoginMetadata{}}
	if err := br.DB.User.Insert(ctx, &database.User{BridgeID: "test", MXID: mxid}); err != nil {
		t.Fatal(err)
	}
	if err := br.DB.UserLogin.Insert(ctx, login); err != nil {
		t.Fatal(err)
	}
	// a is still subscribed; v-z are not, more than half of the portals.
	posted := time.UnixMilli(1_700_000_000_000)
	for _, ch := range []string{"a", "v", "w", "x", "y", "z"} {
		key := networkid.PortalKey{ID: networkid.PortalID(ch)}
		if err := br.DB.Portal.Insert(ctx, &database.Portal{BridgeID: "test", PortalKey: key, MXID: id.RoomID("!" + ch + ":example.com"), Metadata: &PortalMetadata{}}); err != nil {
			t.Fatal(err)
		}
		if err := br.DB.UserPortal.Put(ctx, &database.UserPortal{BridgeID: "test", UserMXID: mxid, LoginID: "me", Portal: key}); err != nil {
			t.Fatal(err)
		}
	}
	if err := br.DB.Ghost.Insert(ctx, &database.Ghost{BridgeID: "test", ID: "u-bob"}); err != nil {
		t.Fatal(err)
	}
	if err := br.DB.Message.Insert(ctx, &database.Message{
		ID: "m-v", MXID: "$m-v", Room: networkid.PortalKey{ID: "v"}, SenderID: "u-bob", Timestamp: posted, Metadata: &MessageMetadata{},
	}); err != nil {
		t.Fatal(err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v3/activity/timeline", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(`[]`)) })
	c := &TraQClient{
		Login: &bridgev2.UserLogin{UserLogin: login, Bridge: br, Log: zerolog.Nop()},
		API:   newMemAPI(t, mux),
	}
	c.channels = buildChannelTree(&traq.ChannelList{Public: []traq.Channel{{ID: "a", Name: "a"}, {ID: "v", Name: "v"}}})
	targets := map[string]channelInfo{"a": {ID: "a", Path: "a", Level: 1}}
	c.targets = targets
	c.resyncs.Store(newResyncPacer(ctx, func(context.Context, *pendingResync) {}))
	c.loadLastActivity(ctx, targets)

	c.initialSync(ctx, loaded{me: &traq.Me{User: traq.User{ID: "me"}}, targets: targets, subscriptions: 1})

	if want := []string{"v", "w", "x", "y", "z"}; !slices.Equal(c.heldRemovals, want) {
		t.Errorf("heldRemovals = %v, want %v", c.heldRemovals, want)
	}
	for _, id := range []string{"a", "v", "w", "x", "y", "z"} {
		if ch, ok := c.targets[id]; !ok || !ch.isTarget() {
			t.Errorf("targets[%s] = %+v, %v; want a target", id, ch, ok)
		}
	}
	if got := c.lastActivity["v"]; !got.Equal(posted) {
		t.Errorf("lastActivity[v] = %v, want %v", got, posted)
	}
}
