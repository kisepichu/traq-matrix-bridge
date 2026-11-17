package connector

import "maunium.net/go/mautrix/bridgev2/networkid"

// Every ID is the bare traQ UUID. The TraQ* and StampID functions are the inverse conversions.

// PortalKey is the key of a public channel, which is shared by all logins.
func PortalKey(channelID string) networkid.PortalKey {
	return networkid.PortalKey{ID: networkid.PortalID(channelID)}
}

// DMPortalKey is the key of a DM channel. A DM exists once per login, so the login is the receiver.
func DMPortalKey(channelID string, login networkid.UserLoginID) networkid.PortalKey {
	return networkid.PortalKey{ID: networkid.PortalID(channelID), Receiver: login}
}

func ChannelID(key networkid.PortalKey) string { return string(key.ID) }

func UserID(traqUserID string) networkid.UserID { return networkid.UserID(traqUserID) }

func TraQUserID(id networkid.UserID) string { return string(id) }

func MessageID(traqMessageID string) networkid.MessageID {
	return networkid.MessageID(traqMessageID)
}

func TraQMessageID(id networkid.MessageID) string { return string(id) }

func EmojiID(stampID string) networkid.EmojiID { return networkid.EmojiID(stampID) }

func StampID(id networkid.EmojiID) string { return string(id) }

func LoginID(traqUserID string) networkid.UserLoginID { return networkid.UserLoginID(traqUserID) }
