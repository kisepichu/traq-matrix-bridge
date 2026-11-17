package connector

import (
	"context"
	"sync/atomic"

	"go.mau.fi/util/configupgrade"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"

	"github.com/kisepichu/traq-matrix-bridge/pkg/stampconv"
	"github.com/kisepichu/traq-matrix-bridge/pkg/traq"
)

// TraQConnector implements bridgev2.NetworkConnector (+ ConfigValidatingNetwork, MaxFileSizeingNetwork).
type TraQConnector struct {
	Bridge *bridgev2.Bridge
	Config NetworkConfig
	// Stamps holds every stamp of the server. Start creates it empty; the first login to connect fills it.
	Stamps *stampconv.Table

	stampsLoaded atomic.Bool
	uploadLimit  atomic.Int64 // the homeserver's upload limit; 0 until SetMaxFileSize
}

var (
	_ bridgev2.NetworkConnector        = (*TraQConnector)(nil)
	_ bridgev2.ConfigValidatingNetwork = (*TraQConnector)(nil)
	_ bridgev2.MaxFileSizeingNetwork   = (*TraQConnector)(nil)
)

func (c *TraQConnector) Init(br *bridgev2.Bridge) {
	c.Bridge = br
}

func (c *TraQConnector) Start(ctx context.Context) error {
	c.Stamps = stampconv.NewTable(nil, false)
	return nil
}

// defaultMaxFileSize is the upload limit bridgev2's Matrix connector assumes until it has read the
// homeserver's media config.
const defaultMaxFileSize = 50 * 1024 * 1024

// SetMaxFileSize is called by bridgev2's Matrix connector with the homeserver's m.upload.size, soon after
// startup and asynchronously.
func (c *TraQConnector) SetMaxFileSize(maxSize int64) {
	c.uploadLimit.Store(maxSize)
}

func (c *TraQConnector) maxFileSize() int64 {
	if size := c.uploadLimit.Load(); size > 0 {
		return size
	}
	return defaultMaxFileSize
}

// loadStamps fills Stamps from Stamps() once per process; later changes arrive as STAMP_* events.
// Concurrent first calls may both load, which is harmless.
func (c *TraQConnector) loadStamps(ctx context.Context, api *traq.Client) error {
	if c.stampsLoaded.Load() {
		return nil
	}
	stamps, err := api.Stamps(ctx)
	if err != nil {
		return err
	}
	for i := range stamps {
		c.Stamps.Put(stampFromTraQ(&stamps[i]))
	}
	c.stampsLoaded.Store(true)
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

// GetBridgeInfoVersion: bump capabilities whenever roomFeatures changes, so bridgev2 resends them to every room.
func (c *TraQConnector) GetBridgeInfoVersion() (info, capabilities int) {
	return 1, 2
}
