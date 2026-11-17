package connector

import (
	_ "embed"
	"errors"
	"fmt"
	"net/url"
	"time"

	up "go.mau.fi/util/configupgrade"
)

// ExampleConfig is the network section of the example config, written without the "network:" key.
//
//go:embed example-config.yaml
var ExampleConfig string

// NetworkConfig is the "network:" section of the bridge config.
type NetworkConfig struct {
	ServerURL                string        `yaml:"server_url"`
	OAuth                    OAuthConfig   `yaml:"oauth"`
	Poll                     PollConfig    `yaml:"poll"`
	ViewIdleTimeout          time.Duration `yaml:"view_idle_timeout"`
	SubscriptionSyncInterval time.Duration `yaml:"subscription_sync_interval"`
}

type OAuthConfig struct {
	ClientID    string `yaml:"client_id"`
	RedirectURI string `yaml:"redirect_uri"`
}

type PollConfig struct {
	Interval    time.Duration `yaml:"interval"`
	Window      time.Duration `yaml:"window"`
	Depth       int           `yaml:"depth"`
	MaxChannels int           `yaml:"max_channels"`
}

const maxPollDepth = 200

func upgradeConfig(helper up.Helper) {
	helper.Copy(up.Str, "server_url")
	helper.Copy(up.Str, "oauth", "client_id")
	helper.Copy(up.Str, "oauth", "redirect_uri")
	helper.Copy(up.Str, "poll", "interval")
	helper.Copy(up.Str, "poll", "window")
	helper.Copy(up.Int, "poll", "depth")
	helper.Copy(up.Int, "poll", "max_channels")
	helper.Copy(up.Str, "view_idle_timeout")
	helper.Copy(up.Str, "subscription_sync_interval")
}

// Validate checks that the config can be used to talk to a traQ server.
// It has a value receiver so that it can be called on non-addressable values.
func (c NetworkConfig) Validate() error {
	if err := validateServerURL(c.ServerURL); err != nil {
		return fmt.Errorf("network.server_url: %w", err)
	}
	if c.OAuth.ClientID == "" {
		return errors.New("network.oauth.client_id must not be empty")
	}
	if u, err := url.Parse(c.OAuth.RedirectURI); err != nil || !u.IsAbs() || u.Host == "" {
		return fmt.Errorf("network.oauth.redirect_uri must be an absolute URL, got %q", c.OAuth.RedirectURI)
	}
	for name, d := range map[string]time.Duration{
		"network.poll.interval":              c.Poll.Interval,
		"network.poll.window":                c.Poll.Window,
		"network.view_idle_timeout":          c.ViewIdleTimeout,
		"network.subscription_sync_interval": c.SubscriptionSyncInterval,
	} {
		if d <= 0 {
			return fmt.Errorf("%s must be positive", name)
		}
	}
	if c.Poll.Depth < 1 || c.Poll.Depth > maxPollDepth {
		return fmt.Errorf("network.poll.depth must be between 1 and %d", maxPollDepth)
	}
	if c.Poll.MaxChannels < 1 {
		return errors.New("network.poll.max_channels must be at least 1")
	}
	return nil
}

func validateServerURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return err
	}
	if (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return fmt.Errorf("must be an absolute http(s) URL, got %q", raw)
	}
	if (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" || u.User != nil {
		return fmt.Errorf("must not contain a path, query, fragment or credentials, got %q", raw)
	}
	return nil
}
