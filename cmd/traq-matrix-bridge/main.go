package main

import (
	"maunium.net/go/mautrix/bridgev2/matrix/mxmain"

	"github.com/kisepichu/traq-matrix-bridge/pkg/connector"
)

// Set with -ldflags "-X main.Tag=... -X main.Commit=... -X main.BuildTime=...".
var (
	Tag       = "unknown"
	Commit    = "unknown"
	BuildTime = "unknown"
)

func main() {
	m := mxmain.BridgeMain{
		Name:        "traq-matrix-bridge",
		URL:         "https://github.com/kisepichu/traq-matrix-bridge",
		Description: "A Matrix bridge for traQ.",
		Version:     "0.1.0",
		Connector:   &connector.TraQConnector{},
	}
	m.InitVersion(Tag, Commit, BuildTime)
	m.Run()
}
