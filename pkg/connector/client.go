package connector

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/status"
	"maunium.net/go/mautrix/event"

	"github.com/kisepichu/traq-matrix-bridge/pkg/traq"
)

const userAgent = "traq-matrix-bridge"

// httpTimeout bounds every REST and OAuth request, so that a hung traQ cannot block a login or logout.
const httpTimeout = 30 * time.Second

var httpClient = &http.Client{Timeout: httpTimeout}

// Bridge state error codes.
const (
	errCodeNoToken      status.BridgeStateErrorCode = "traq-no-token"
	errCodeTokenExpired status.BridgeStateErrorCode = "traq-token-expired"
	errCodeTokenInvalid status.BridgeStateErrorCode = "traq-token-invalid"
	errCodeConnectFail  status.BridgeStateErrorCode = "traq-connect-failed"
)

var errNotImplemented = errors.New("traq: not implemented yet")

// TraQClient is the per-login client stored in bridgev2.UserLogin.Client.
type TraQClient struct {
	Main  *TraQConnector
	Login *bridgev2.UserLogin
	API   *traq.Client // nil when the stored token is missing
}

var _ bridgev2.NetworkAPI = (*TraQClient)(nil)

func (c *TraQConnector) oauthConfig() traq.OAuthConfig {
	return traq.OAuthConfig{
		ServerURL:   c.Config.ServerURL,
		ClientID:    c.Config.OAuth.ClientID,
		RedirectURI: c.Config.OAuth.RedirectURI,
		HTTPClient:  httpClient,
	}
}

func (c *TraQClient) metadata() *UserLoginMetadata {
	meta, _ := c.Login.Metadata.(*UserLoginMetadata)
	if meta == nil {
		return &UserLoginMetadata{}
	}
	return meta
}

// IsLoggedIn reports whether a token is stored and has not expired. It does no IO, so a token that traQ
// has revoked is still reported as logged in until Connect sees a 401.
func (c *TraQClient) IsLoggedIn() bool {
	if c.API == nil {
		return false
	}
	exp := c.metadata().ExpiresAt
	return exp.IsZero() || time.Now().Before(exp)
}

func (c *TraQClient) Connect(ctx context.Context) {
	log := c.Login.Log
	if c.API == nil {
		c.sendBadCredentials(errCodeNoToken, "No traQ access token is stored, please log in again")
		return
	}
	if !c.IsLoggedIn() {
		c.sendBadCredentials(errCodeTokenExpired, "The traQ access token has expired, please log in again")
		return
	}
	if _, err := c.API.Me(ctx); err != nil {
		if traq.IsStatus(err, http.StatusUnauthorized) {
			c.sendBadCredentials(errCodeTokenInvalid, "traQ rejected the access token, please log in again")
			return
		}
		log.Err(err).Msg("Failed to reach traQ")
		c.Login.BridgeState.Send(status.BridgeState{
			StateEvent: status.StateTransientDisconnect,
			Error:      errCodeConnectFail,
			Message:    "Failed to reach traQ",
		})
		return
	}
	c.Login.BridgeState.Send(status.BridgeState{StateEvent: status.StateConnected})
}

func (c *TraQClient) sendBadCredentials(code status.BridgeStateErrorCode, message string) {
	c.Login.Log.Warn().Str("error_code", string(code)).Msg(message)
	c.Login.BridgeState.Send(status.BridgeState{
		StateEvent: status.StateBadCredentials,
		Error:      code,
		Message:    message,
		UserAction: status.UserActionRelogin,
	})
}

func (c *TraQClient) Disconnect() {}

// LogoutRemote revokes the access token. A failure is logged and otherwise ignored: the login is being
// deleted either way, and traQ expires the token on its own.
func (c *TraQClient) LogoutRemote(ctx context.Context) {
	if token := c.metadata().AccessToken; token != "" {
		if err := c.Main.oauthConfig().Revoke(ctx, token); err != nil {
			c.Login.Log.Warn().Err(err).Msg("Failed to revoke the traQ access token")
		}
	}
	c.Disconnect()
}

func (c *TraQClient) IsThisUser(_ context.Context, userID networkid.UserID) bool {
	return TraQUserID(userID) == string(c.Login.ID)
}

func (c *TraQClient) GetChatInfo(context.Context, *bridgev2.Portal) (*bridgev2.ChatInfo, error) {
	return nil, fmt.Errorf("GetChatInfo: %w", errNotImplemented)
}

func (c *TraQClient) GetUserInfo(context.Context, *bridgev2.Ghost) (*bridgev2.UserInfo, error) {
	return nil, fmt.Errorf("GetUserInfo: %w", errNotImplemented)
}

func (c *TraQClient) GetCapabilities(context.Context, *bridgev2.Portal) *event.RoomFeatures {
	return &event.RoomFeatures{}
}

func (c *TraQClient) HandleMatrixMessage(context.Context, *bridgev2.MatrixMessage) (*bridgev2.MatrixMessageResponse, error) {
	return nil, fmt.Errorf("HandleMatrixMessage: %w", errNotImplemented)
}
