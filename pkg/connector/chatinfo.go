package connector

import (
	"context"
	"errors"
	"fmt"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"

	"github.com/kisepichu/traq-matrix-bridge/pkg/traq"
)

var errNotConnected = errors.New("traq: not connected yet")

// lookupChannel finds a channel in the subscription snapshot (which has the level), else in the channel tree.
func (c *TraQClient) lookupChannel(channelID string) (channelInfo, bool) {
	c.stateMu.RLock()
	defer c.stateMu.RUnlock()
	if ch, ok := c.targets[channelID]; ok {
		return ch, true
	}
	if c.channels == nil {
		return channelInfo{}, false
	}
	return c.channels.Get(channelID)
}

func (c *TraQClient) GetChatInfo(ctx context.Context, portal *bridgev2.Portal) (*bridgev2.ChatInfo, error) {
	ch, ok := c.lookupChannel(ChannelID(portal.PortalKey))
	if !ok {
		if c.self() == nil {
			return nil, errNotConnected
		}
		return nil, fmt.Errorf("unknown traQ channel %s", ChannelID(portal.PortalKey))
	}
	return c.chatInfo(ctx, ch)
}

// chatInfo builds the full chat info of a channel. This login is listed as a joined member of every DM and
// of the public channels it subscribes to (level >= 1). A public channel at level 0 (known only from the
// tree, e.g. through bridgev2's bridge/sync commands) leaves this login out of the non-full member list and
// sets no mute, so bridgev2 neither invites nor joins this login's user. The list stays non-nil: a nil list
// makes bridgev2 invite the source login at room creation.
func (c *TraQClient) chatInfo(ctx context.Context, ch channelInfo) (*bridgev2.ChatInfo, error) {
	me := c.self()
	if me == nil {
		return nil, errNotConnected
	}
	members := bridgev2.ChatMemberMap{}
	info := &bridgev2.ChatInfo{
		CanBackfill:  true,
		ExtraUpdates: setPortalMetadata(ch.DMUserID),
	}
	if !ch.isDM() {
		info.Name = &ch.Path
		info.Topic = &ch.Topic
		info.Members = &bridgev2.ChatMemberList{MemberMap: members}
		if ch.Level >= 1 {
			members.Set(bridgev2.ChatMember{EventSender: c.selfSender(me), Membership: event.MembershipJoin})
		}
		switch ch.Level {
		case 1:
			info.UserLocal = &bridgev2.UserLocalPortalInfo{MutedUntil: new(event.MutedForever)}
		case 2:
			info.UserLocal = &bridgev2.UserLocalPortalInfo{MutedUntil: new(bridgev2.Unmuted)}
		}
		return info, nil
	}

	members.Set(bridgev2.ChatMember{EventSender: c.selfSender(me), Membership: event.MembershipJoin})

	other, err := c.users.Get(ctx, c.API, ch.DMUserID)
	if err != nil {
		return nil, fmt.Errorf("failed to get the DM user %s: %w", ch.DMUserID, err)
	}
	if other.ID != me.ID {
		members.Set(bridgev2.ChatMember{
			EventSender: bridgev2.EventSender{Sender: UserID(other.ID)},
			Membership:  event.MembershipJoin,
		})
	}
	info.Type = new(database.RoomTypeDM)
	info.Name = new(displayName(other))
	info.Avatar = c.userAvatar(other)
	info.Members = &bridgev2.ChatMemberList{IsFull: true, OtherUserID: UserID(other.ID), MemberMap: members}
	return info, nil
}

// setPortalMetadata stores whether the portal is a DM and with whom. dmUserID is empty for public channels.
func setPortalMetadata(dmUserID string) bridgev2.ExtraUpdater[*bridgev2.Portal] {
	return func(_ context.Context, portal *bridgev2.Portal) bool {
		meta, ok := portal.Metadata.(*PortalMetadata)
		if !ok {
			meta = &PortalMetadata{}
			portal.Metadata = meta
		}
		isDM := dmUserID != ""
		if meta.IsDM == isDM && meta.DMUserID == dmUserID {
			return false
		}
		meta.IsDM, meta.DMUserID = isDM, dmUserID
		return true
	}
}

func (c *TraQClient) GetUserInfo(ctx context.Context, ghost *bridgev2.Ghost) (*bridgev2.UserInfo, error) {
	if c.API == nil {
		return nil, errNotConnected
	}
	user, err := c.users.Get(ctx, c.API, TraQUserID(ghost.ID))
	if err != nil {
		return nil, fmt.Errorf("failed to get traQ user %s: %w", ghost.ID, err)
	}
	name := displayName(user)
	if user.Bot {
		name += " (bot)"
	}
	return &bridgev2.UserInfo{
		Identifiers: []string{"traq:" + user.Name},
		Name:        &name,
		Avatar:      c.userAvatar(user),
		IsBot:       &user.Bot,
	}, nil
}

// displayName falls back to the user name, as traQ does, when no display name is set.
func displayName(user *traq.User) string {
	if user.DisplayName != "" {
		return user.DisplayName
	}
	return user.Name
}

// userAvatar is keyed by the icon file ID, which changes whenever the user changes their icon.
func (c *TraQClient) userAvatar(user *traq.User) *bridgev2.Avatar {
	api, name := c.API, user.Name
	return &bridgev2.Avatar{
		ID: networkid.AvatarID(user.IconFileID),
		Get: func(ctx context.Context) ([]byte, error) {
			data, _, err := api.PublicIcon(ctx, name)
			return data, err
		},
	}
}

// self returns the logged-in traQ user, or nil before Connect has loaded it.
func (c *TraQClient) self() *traq.Me {
	c.stateMu.RLock()
	defer c.stateMu.RUnlock()
	return c.me
}
