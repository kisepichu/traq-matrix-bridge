package connector

import (
	"testing"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/bridgeconfig"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
)

const testUUID = "0193a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a5b"

func TestIDRoundTrip(t *testing.T) {
	if got := ChannelID(PortalKey(testUUID, "login")); got != testUUID {
		t.Errorf("ChannelID(PortalKey) = %q", got)
	}
	if got := TraQUserID(UserID(testUUID)); got != testUUID {
		t.Errorf("TraQUserID(UserID) = %q", got)
	}
	if got := TraQMessageID(MessageID(testUUID)); got != testUUID {
		t.Errorf("TraQMessageID(MessageID) = %q", got)
	}
	if got := StampID(EmojiID(testUUID)); got != testUUID {
		t.Errorf("StampID(EmojiID) = %q", got)
	}
}

func TestIDsAreUnprefixedUUIDs(t *testing.T) {
	if UserID(testUUID) != networkid.UserID(testUUID) || LoginID(testUUID) != networkid.UserLoginID(testUUID) {
		t.Error("user and login IDs must be the bare traQ UUID")
	}
	if MessageID(testUUID) != networkid.MessageID(testUUID) || EmojiID(testUUID) != networkid.EmojiID(testUUID) {
		t.Error("message and emoji IDs must be the bare traQ UUID")
	}
}

// testLogin is a login of a bridge whose bridge.split_portals is split.
func testLogin(loginID networkid.UserLoginID, split bool) *bridgev2.UserLogin {
	return &bridgev2.UserLogin{
		UserLogin: &database.UserLogin{ID: loginID},
		Bridge:    &bridgev2.Bridge{Config: &bridgeconfig.BridgeConfig{SplitPortals: split}},
	}
}

// A public channel's portal is shared by all logins, unless bridgev2 splits portals: then every key needs a
// receiver, and each login has its own portal. A DM's portal always belongs to the login.
func TestPortalKeyReceiver(t *testing.T) {
	public := channelInfo{ID: testUUID, Path: "gps", Level: 1}
	dm := channelInfo{ID: testUUID, DMUserID: "u-other", Level: dmLevel}
	for _, split := range []bool{false, true} {
		c := &TraQClient{Login: testLogin("me", split)}
		wantPublic := networkid.PortalKey{ID: networkid.PortalID(testUUID)}
		if split {
			wantPublic.Receiver = "me"
		}
		if got := c.portalKey(public); got != wantPublic {
			t.Errorf("split_portals=%v: public channel key = %+v, want %+v", split, got, wantPublic)
		}
		if got, want := c.portalKey(dm), (networkid.PortalKey{ID: networkid.PortalID(testUUID), Receiver: "me"}); got != want {
			t.Errorf("split_portals=%v: DM key = %+v, want %+v", split, got, want)
		}
	}
}

func TestContentHash(t *testing.T) {
	// sha256("abc")
	const want = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
	if got := ContentHash("abc"); got != want {
		t.Errorf("ContentHash(abc) = %q", got)
	}
	if ContentHash("a") == ContentHash("b") {
		t.Error("different content must hash differently")
	}
}
