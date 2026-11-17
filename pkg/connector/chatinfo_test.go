package connector

import (
	"context"
	"testing"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"

	"github.com/kisepichu/traq-matrix-bridge/pkg/traq"
)

// A public channel the login is not subscribed to must not report the login as joined.
func TestChatInfoSelfMembership(t *testing.T) {
	c := &TraQClient{
		Login: &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: LoginID(testUUID)}},
		me:    &traq.Me{User: traq.User{ID: testUUID}},
	}
	self := UserID(testUUID)
	for level, wantSelf := range map[int]bool{0: false, 1: true, 2: true} {
		info, err := c.chatInfo(context.Background(), channelInfo{ID: "ch", Path: "a/ch", Level: level})
		if err != nil {
			t.Fatalf("level %d: chatInfo: %v", level, err)
		}
		member, ok := info.Members.MemberMap[self]
		if ok != wantSelf {
			t.Errorf("level %d: self in members = %v, want %v", level, ok, wantSelf)
		}
		if ok && !member.IsFromMe {
			t.Errorf("level %d: self member is not IsFromMe", level)
		}
		if (info.UserLocal != nil) != wantSelf {
			t.Errorf("level %d: UserLocal = %+v, want set: %v", level, info.UserLocal, wantSelf)
		}
		if info.Name == nil || *info.Name != "a/ch" {
			t.Errorf("level %d: Name = %v, want a/ch", level, info.Name)
		}
	}
}
