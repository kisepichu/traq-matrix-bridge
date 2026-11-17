package connector

import (
	"testing"

	"maunium.net/go/mautrix/bridgev2/networkid"
)

const testUUID = "0193a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a5b"

func TestIDRoundTrip(t *testing.T) {
	if got := ChannelID(PortalKey(testUUID)); got != testUUID {
		t.Errorf("ChannelID(PortalKey) = %q", got)
	}
	if got := ChannelID(DMPortalKey(testUUID, "login")); got != testUUID {
		t.Errorf("ChannelID(DMPortalKey) = %q", got)
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

func TestPortalKeyReceiver(t *testing.T) {
	if got := PortalKey(testUUID); got.Receiver != "" {
		t.Errorf("public channel key has Receiver %q", got.Receiver)
	}
	dm := DMPortalKey(testUUID, "me")
	if dm.ID != networkid.PortalID(testUUID) || dm.Receiver != "me" {
		t.Errorf("DMPortalKey = %+v", dm)
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
