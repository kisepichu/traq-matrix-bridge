package connector

import (
	"context"
	"fmt"
	"sync/atomic"

	"go.mau.fi/util/configupgrade"
	"go.mau.fi/util/dbutil"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"

	"github.com/kisepichu/traq-matrix-bridge/pkg/connector/stampdb"
	"github.com/kisepichu/traq-matrix-bridge/pkg/stampconv"
)

// TraQConnector implements bridgev2.NetworkConnector (+ ConfigValidatingNetwork, MaxFileSizeingNetwork,
// StoppableNetwork).
type TraQConnector struct {
	Bridge *bridgev2.Bridge
	Config NetworkConfig
	// Stamps holds every stamp of the server. Start loads it from stampDB; the stamp sync job (stamps.go) and
	// the STAMP_* events keep both up to date.
	Stamps *stampconv.Table

	stampDB     *stampdb.Store
	stampSync   stampSync
	uploadLimit atomic.Int64 // the homeserver's upload limit; 0 until SetMaxFileSize
}

var (
	_ bridgev2.NetworkConnector        = (*TraQConnector)(nil)
	_ bridgev2.ConfigValidatingNetwork = (*TraQConnector)(nil)
	_ bridgev2.MaxFileSizeingNetwork   = (*TraQConnector)(nil)
	_ bridgev2.StoppableNetwork        = (*TraQConnector)(nil)
)

func (c *TraQConnector) Init(br *bridgev2.Bridge) {
	c.Bridge = br
	c.registerCommands()
}

// Start upgrades the connector's tables (bridgev2 has upgraded its own just before) and loads the stamp table.
func (c *TraQConnector) Start(ctx context.Context) error {
	db := c.Bridge.DB.Database.Child(stampdb.VersionTableName, stampdb.UpgradeTable,
		dbutil.ZeroLogger(c.Bridge.Log.With().Str("db_section", "traq").Logger()))
	if err := db.Upgrade(ctx); err != nil {
		return bridgev2.DBUpgradeError{Section: "traq", Err: err}
	}
	c.stampDB = stampdb.New(db)
	stamps, err := c.stampDB.All(ctx)
	if err != nil {
		return fmt.Errorf("load the stamp table: %w", err)
	}
	complete, err := c.stampDB.Complete(ctx)
	if err != nil {
		return fmt.Errorf("load the stamp table: %w", err)
	}
	c.Stamps = stampconv.NewTable(stamps, complete)
	c.Bridge.Log.Info().Int("stamps", len(stamps)).Bool("complete", complete).Msg("Loaded the stamp table")
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
