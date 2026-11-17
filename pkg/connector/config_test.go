package connector

import (
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

func validConfig() NetworkConfig {
	return NetworkConfig{
		ServerURL:                "https://q.trap.jp",
		OAuth:                    OAuthConfig{ClientID: "cid", RedirectURI: "http://localhost:29399/callback"},
		Poll:                     PollConfig{Interval: 3 * time.Minute, Window: 6 * time.Hour, Depth: 30, MaxChannels: 50},
		ViewIdleTimeout:          10 * time.Minute,
		SubscriptionSyncInterval: 10 * time.Minute,
	}
}

func TestExampleConfigDecodesToDefaults(t *testing.T) {
	var c NetworkConfig
	if err := yaml.Unmarshal([]byte(ExampleConfig), &c); err != nil {
		t.Fatal(err)
	}
	want := validConfig()
	want.ServerURL, want.OAuth.ClientID = "", ""
	if c != want {
		t.Errorf("example config = %+v, want %+v", c, want)
	}
}

func TestValidate(t *testing.T) {
	if err := validConfig().Validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	cases := map[string]func(*NetworkConfig){
		"empty server_url":     func(c *NetworkConfig) { c.ServerURL = "" },
		"server_url with path": func(c *NetworkConfig) { c.ServerURL = "https://q.trap.jp/api/v3" },
		"server_url not http":  func(c *NetworkConfig) { c.ServerURL = "ftp://q.trap.jp" },
		"empty client_id":      func(c *NetworkConfig) { c.OAuth.ClientID = "" },
		"relative redirect":    func(c *NetworkConfig) { c.OAuth.RedirectURI = "/callback" },
		"zero poll interval":   func(c *NetworkConfig) { c.Poll.Interval = 0 },
		"depth over 200":       func(c *NetworkConfig) { c.Poll.Depth = 201 },
		"no channels":          func(c *NetworkConfig) { c.Poll.MaxChannels = 0 },
		"zero idle timeout":    func(c *NetworkConfig) { c.ViewIdleTimeout = 0 },
	}
	for name, mutate := range cases {
		c := validConfig()
		mutate(&c)
		if err := c.Validate(); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestServerURLTrailingSlashIsAccepted(t *testing.T) {
	c := validConfig()
	c.ServerURL = "https://q.trap.jp/"
	if err := c.Validate(); err != nil {
		t.Errorf("trailing slash: %v", err)
	}
}
