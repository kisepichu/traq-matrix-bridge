package connector

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/status"

	"github.com/kisepichu/traq-matrix-bridge/pkg/traq"
)

const (
	loginFlowID       = "oauth"
	loginStepRedirect = "io.github.kisepichu.traq.oauth.redirect"
	loginStepComplete = "io.github.kisepichu.traq.oauth.complete"
	redirectFieldID   = "redirect_url"
)

func (c *TraQConnector) GetLoginFlows() []bridgev2.LoginFlow {
	return []bridgev2.LoginFlow{{
		ID:          loginFlowID,
		Name:        "OAuth",
		Description: "Log in with your traQ account in a browser",
	}}
}

func (c *TraQConnector) CreateLogin(_ context.Context, user *bridgev2.User, flowID string) (bridgev2.LoginProcess, error) {
	if flowID != loginFlowID {
		return nil, bridgev2.ErrInvalidLoginFlowID
	}
	return &oauthLogin{main: c, user: user}, nil
}

// oauthLogin is the OAuth 2.0 authorization code + PKCE flow. The bridge cannot receive the redirect, so
// the user pastes the URL their browser was sent to. pkce lives only in memory.
type oauthLogin struct {
	main *TraQConnector
	user *bridgev2.User
	pkce traq.PKCE
}

var _ bridgev2.LoginProcessUserInput = (*oauthLogin)(nil)

func (l *oauthLogin) Start(context.Context) (*bridgev2.LoginStep, error) {
	pkce, err := traq.NewPKCE()
	if err != nil {
		return nil, fmt.Errorf("failed to start login: %w", err)
	}
	authorizeURL, err := l.main.oauthConfig().AuthorizeURL(pkce)
	if err != nil {
		return nil, fmt.Errorf("failed to build the authorize URL: %w", err)
	}
	l.pkce = pkce
	return &bridgev2.LoginStep{
		Type:   bridgev2.LoginStepTypeUserInput,
		StepID: loginStepRedirect,
		Instructions: "1. Open this URL in your browser and approve the bridge:\n\n" +
			"<" + authorizeURL + ">\n\n" +
			"2. After you approve, your browser opens a page that does not load. " +
			"Copy that page's full URL from the address bar and paste it here.",
		UserInputParams: &bridgev2.LoginUserInputParams{
			Fields: []bridgev2.LoginInputDataField{{
				Type: bridgev2.LoginInputFieldTypeURL,
				ID:   redirectFieldID,
				Name: "Redirected URL",
			}},
		},
	}, nil
}

func (l *oauthLogin) SubmitUserInput(ctx context.Context, input map[string]string) (*bridgev2.LoginStep, error) {
	log := zerolog.Ctx(ctx)
	code, err := traq.ParseRedirect(input[redirectFieldID], l.pkce.State)
	if err != nil {
		switch {
		case errors.Is(err, traq.ErrStateMismatch):
			log.Warn().Msg("Login redirect URL has a different state")
			return nil, loginError("That URL does not belong to this login attempt. Start over with the `login` command.")
		case errors.Is(err, traq.ErrNoCode):
			return nil, loginError("That URL has no authorization code. Start over with the `login` command.")
		default:
			log.Warn().Err(err).Msg("Login redirect URL rejected")
			return nil, loginError("traQ authorization failed or the URL is invalid. Start over with the `login` command.")
		}
	}

	oauth := l.main.oauthConfig()
	token, err := oauth.Exchange(ctx, code, l.pkce.Verifier)
	if err != nil {
		log.Err(err).Msg("Failed to exchange the authorization code")
		return nil, loginError("traQ did not accept the authorization code. Start over with the `login` command.")
	}
	api, err := traq.NewClient(l.main.Config.ServerURL, httpClient, token.AccessToken, userAgent)
	if err != nil {
		return nil, fmt.Errorf("failed to create the traQ client: %w", err)
	}
	me, err := api.Me(ctx)
	if err != nil {
		log.Err(err).Msg("Failed to fetch the logged-in traQ user")
		return nil, loginError("Failed to fetch your traQ profile with the new token. Start over with the `login` command.")
	}

	ul, err := l.user.NewLogin(ctx, &database.UserLogin{
		ID:         LoginID(me.ID),
		RemoteName: me.Name,
		RemoteProfile: status.RemoteProfile{
			Username: me.Name,
			Name:     me.DisplayName,
		},
		Metadata: &UserLoginMetadata{AccessToken: token.AccessToken, ExpiresAt: token.ExpiresAt},
	}, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to save the login: %w", err)
	}
	// bridgev2 calls Connect only at startup and on reconnects, so start the new login here. The request
	// context ends with this call.
	go ul.Client.Connect(ul.Log.WithContext(ul.Bridge.BackgroundCtx))

	return &bridgev2.LoginStep{
		Type:         bridgev2.LoginStepTypeComplete,
		StepID:       loginStepComplete,
		Instructions: "Successfully logged in as @" + me.Name,
		CompleteParams: &bridgev2.LoginCompleteParams{
			UserLoginID: ul.ID,
			UserLogin:   ul,
		},
	}, nil
}

func (l *oauthLogin) Cancel() {}

func loginError(msg string) error {
	return bridgev2.RespError(mautrix.MInvalidParam.WithMessage(msg))
}

func (c *TraQConnector) LoadUserLogin(_ context.Context, login *bridgev2.UserLogin) error {
	client := &TraQClient{Main: c, Login: login, users: newUserCache()}
	client.view = newViewState(c.Config.ViewIdleTimeout, client.sendViewState, time.Now)
	if meta, _ := login.Metadata.(*UserLoginMetadata); meta != nil && meta.AccessToken != "" {
		api, err := traq.NewClient(c.Config.ServerURL, httpClient, meta.AccessToken, userAgent)
		if err != nil {
			return fmt.Errorf("failed to create the traQ client: %w", err)
		}
		files, err := traq.NewClient(c.Config.ServerURL, fileHTTPClient, meta.AccessToken, userAgent)
		if err != nil {
			return fmt.Errorf("failed to create the traQ file client: %w", err)
		}
		client.API, client.Files = api, files
	}
	login.Client = client
	return nil
}
