package connector

import (
	"context"
	"strings"
	"sync"

	"github.com/kisepichu/traq-matrix-bridge/pkg/traq"
)

// channelInfo is what the bridge knows about one traQ channel.
type channelInfo struct {
	ID       string
	Path     string // "gps/times/kise" (no leading #)
	Topic    string
	DMUserID string // non-empty for DMs
	Level    int    // subscription level; DMs are always 2
}

func (ch channelInfo) isDM() bool { return ch.DMUserID != "" }

// isTarget reports whether the channel gets a portal: every DM, and public channels with level >= 1.
func (ch channelInfo) isTarget() bool { return ch.isDM() || ch.Level >= 1 }

// dmLevel is the level of every DM: traQ notifies DMs like a level 2 subscription.
const dmLevel = 2

// channelTree indexes the channels of one GET /channels response. It is not modified after
// buildChannelTree, so it is safe for concurrent use; a newer channel list replaces the whole tree.
// Public channels have Level 0 here: levels come from the subscriptions (see subscriptionSnapshot).
type channelTree struct {
	byID   map[string]channelInfo
	byPath map[string]string // path -> channel ID, public channels only
}

// buildChannelTree computes each public channel's path from its ParentID chain. Archived channels are
// included. A parent missing from the list ends the path there.
func buildChannelTree(list *traq.ChannelList) *channelTree {
	t := &channelTree{
		byID:   make(map[string]channelInfo, len(list.Public)+len(list.DM)),
		byPath: make(map[string]string, len(list.Public)),
	}
	public := make(map[string]*traq.Channel, len(list.Public))
	for i := range list.Public {
		public[list.Public[i].ID] = &list.Public[i]
	}
	var segments []string
	for _, ch := range list.Public {
		segments = segments[:0]
		// The step limit stops a malformed parent cycle.
		for cur, steps := &ch, 0; cur != nil && steps <= len(list.Public); steps++ {
			segments = append(segments, cur.Name)
			if cur.ParentID == nil {
				break
			}
			cur = public[*cur.ParentID]
		}
		var b strings.Builder
		for i := len(segments) - 1; i >= 0; i-- {
			b.WriteString(segments[i])
			if i > 0 {
				b.WriteByte('/')
			}
		}
		path := b.String()
		t.byID[ch.ID] = channelInfo{ID: ch.ID, Path: path, Topic: ch.Topic}
		t.byPath[path] = ch.ID
	}
	for _, dm := range list.DM {
		t.byID[dm.ID] = channelInfo{ID: dm.ID, DMUserID: dm.UserID, Level: dmLevel}
	}
	return t
}

func (t *channelTree) Get(channelID string) (channelInfo, bool) {
	ch, ok := t.byID[channelID]
	return ch, ok
}

// ByPath looks up a public channel by its path, with or without a leading '#'.
func (t *channelTree) ByPath(path string) (channelInfo, bool) {
	id, ok := t.byPath[strings.TrimPrefix(path, "#")]
	if !ok {
		return channelInfo{}, false
	}
	return t.byID[id], true
}

// userCache holds traQ users by ID and name. It is filled from Users() on connect and updated by
// lookups of unknown users (and USER_* events). Safe for concurrent use. Stored users are never
// modified, so callers may keep the returned pointers but must not modify them.
type userCache struct {
	mu     sync.RWMutex
	byID   map[string]*traq.User
	byName map[string]*traq.User
}

func newUserCache() *userCache {
	return &userCache{byID: map[string]*traq.User{}, byName: map[string]*traq.User{}}
}

// setAll replaces the cache contents.
func (u *userCache) setAll(users []traq.User) {
	byID := make(map[string]*traq.User, len(users))
	byName := make(map[string]*traq.User, len(users))
	for i := range users {
		user := &users[i]
		byID[user.ID] = user
		byName[user.Name] = user
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	u.byID, u.byName = byID, byName
}

// put adds or replaces one user.
func (u *userCache) put(user *traq.User) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if old, ok := u.byID[user.ID]; ok && old.Name != user.Name {
		delete(u.byName, old.Name)
	}
	u.byID[user.ID] = user
	u.byName[user.Name] = user
}

// Get returns the cached user, or fetches it with User() and caches it.
func (u *userCache) Get(ctx context.Context, api *traq.Client, userID string) (*traq.User, error) {
	u.mu.RLock()
	user, ok := u.byID[userID]
	u.mu.RUnlock()
	if ok {
		return user, nil
	}
	user, err := api.User(ctx, userID)
	if err != nil {
		return nil, err
	}
	u.put(user)
	return user, nil
}

func (u *userCache) ByName(name string) (*traq.User, bool) {
	u.mu.RLock()
	defer u.mu.RUnlock()
	user, ok := u.byName[name]
	return user, ok
}
