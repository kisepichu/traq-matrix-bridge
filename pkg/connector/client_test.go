package connector

import (
	"context"
	"testing"
	"time"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
)

func TestLoadUserLogin(t *testing.T) {
	c := &TraQConnector{Config: NetworkConfig{ServerURL: "http://localhost:3080"}}
	for name, tc := range map[string]struct {
		meta     *UserLoginMetadata
		wantAPI  bool
		loggedIn bool
	}{
		"valid token":       {&UserLoginMetadata{AccessToken: "t", ExpiresAt: time.Now().Add(time.Hour)}, true, true},
		"no expiry sent":    {&UserLoginMetadata{AccessToken: "t"}, true, true},
		"expired token":     {&UserLoginMetadata{AccessToken: "t", ExpiresAt: time.Now().Add(-time.Minute)}, true, false},
		"missing token":     {&UserLoginMetadata{}, false, false},
		"missing token+exp": {&UserLoginMetadata{ExpiresAt: time.Now().Add(time.Hour)}, false, false},
	} {
		t.Run(name, func(t *testing.T) {
			login := &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: LoginID(testUUID), Metadata: tc.meta}}
			if err := c.LoadUserLogin(context.Background(), login); err != nil {
				t.Fatalf("LoadUserLogin: %v", err)
			}
			client, ok := login.Client.(*TraQClient)
			if !ok {
				t.Fatalf("Client = %T", login.Client)
			}
			if (client.API != nil) != tc.wantAPI {
				t.Errorf("API non-nil = %v, want %v", client.API != nil, tc.wantAPI)
			}
			if client.IsLoggedIn() != tc.loggedIn {
				t.Errorf("IsLoggedIn = %v, want %v", client.IsLoggedIn(), tc.loggedIn)
			}
			if !client.IsThisUser(context.Background(), UserID(testUUID)) || client.IsThisUser(context.Background(), "someone-else") {
				t.Error("IsThisUser does not match the login's traQ user")
			}
		})
	}
}
