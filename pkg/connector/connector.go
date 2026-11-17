package connector

import (
	"context"

	"go.mau.fi/util/configupgrade"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
)

// TraQConnector implements bridgev2.NetworkConnector (+ ConfigValidatingNetwork).
type TraQConnector struct {
	Bridge *bridgev2.Bridge
	Config NetworkConfig
}

var (
	_ bridgev2.NetworkConnector        = (*TraQConnector)(nil)
	_ bridgev2.ConfigValidatingNetwork = (*TraQConnector)(nil)
)

func (c *TraQConnector) Init(br *bridgev2.Bridge) {
	c.Bridge = br
}

func (c *TraQConnector) Start(ctx context.Context) error {
	return nil
}

// GetName is called before the config is loaded, so it must not read c.Config.
func (c *TraQConnector) GetName() bridgev2.BridgeName {
	return bridgev2.BridgeName{
		DisplayName:          "traQ",
		NetworkURL:           "https://github.com/traPtitech/traQ",
		NetworkID:            "traq",
		BeeperBridgeType:     "github.com/kisepichu/traq-matrix-bridge",
		DefaultPort:          29338,
		DefaultCommandPrefix: "!traq",
	}
}

func (c *TraQConnector) GetDBMetaTypes() database.MetaTypes {
	return database.MetaTypes{
		Portal:    func() any { return &PortalMetadata{} },
		Message:   func() any { return &MessageMetadata{} },
		UserLogin: func() any { return &UserLoginMetadata{} },
	}
}

func (c *TraQConnector) GetCapabilities() *bridgev2.NetworkGeneralCapabilities {
	return &bridgev2.NetworkGeneralCapabilities{}
}

func (c *TraQConnector) GetConfig() (string, any, configupgrade.Upgrader) {
	return ExampleConfig, &c.Config, configupgrade.SimpleUpgrader(upgradeConfig)
}

func (c *TraQConnector) ValidateConfig() error {
	return c.Config.Validate()
}

func (c *TraQConnector) GetBridgeInfoVersion() (info, capabilities int) {
	return 1, 1
}
