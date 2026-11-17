package connector

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"

	"maunium.net/go/mautrix/bridgev2"
)

// newTestLogin returns a login process against a fake traQ whose token endpoint returns tokenStatus and
// counts its calls.
func newTestLogin(t *testing.T, tokenStatus int) (*oauthLogin, *atomic.Int32) {
	t.Helper()
	var tokenCalls atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v3/oauth2/token", func(w http.ResponseWriter, r *http.Request) {
		tokenCalls.Add(1)
		w.WriteHeader(tokenStatus)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	c := &TraQConnector{Config: NetworkConfig{
		ServerURL: srv.URL,
		OAuth:     OAuthConfig{ClientID: "client-id", RedirectURI: "http://localhost:29399/callback"},
	}}
	proc, err := c.CreateLogin(context.Background(), nil, "oauth")
	if err != nil {
		t.Fatalf("CreateLogin: %v", err)
	}
	return proc.(*oauthLogin), &tokenCalls
}

func TestCreateLoginRejectsUnknownFlow(t *testing.T) {
	c := &TraQConnector{}
	if _, err := c.CreateLogin(context.Background(), nil, "password"); !errors.Is(err, bridgev2.ErrInvalidLoginFlowID) {
		t.Errorf("err = %v, want ErrInvalidLoginFlowID", err)
	}
	if flows := c.GetLoginFlows(); len(flows) != 1 || flows[0].ID != "oauth" {
		t.Errorf("flows = %+v", flows)
	}
}

func TestStartShowsAuthorizeURL(t *testing.T) {
	login, _ := newTestLogin(t, http.StatusOK)
	step, err := login.Start(context.Background())
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if step.Type != bridgev2.LoginStepTypeUserInput || step.StepID != "io.github.kisepichu.traq.oauth.redirect" {
		t.Errorf("step = %s / %s", step.Type, step.StepID)
	}
	fields := step.UserInputParams.Fields
	if len(fields) != 1 || fields[0].ID != "redirect_url" || fields[0].Type != bridgev2.LoginInputFieldTypeURL {
		t.Errorf("fields = %+v", fields)
	}

	want, err := login.main.oauthConfig().AuthorizeURL(login.pkce)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(step.Instructions, want) {
		t.Fatalf("Instructions do not contain the authorize URL:\n%s", step.Instructions)
	}
	u, err := url.Parse(want)
	if err != nil {
		t.Fatal(err)
	}
	q := u.Query()
	if q.Get("state") != login.pkce.State || q.Get("code_challenge") != login.pkce.Challenge || login.pkce.State == "" {
		t.Errorf("authorize URL query = %v, pkce state/challenge = %q/%q", q, login.pkce.State, login.pkce.Challenge)
	}
	if strings.Contains(step.Instructions, login.pkce.Verifier) {
		t.Error("Instructions leak the PKCE verifier")
	}
}

func TestSubmitUserInputRejectsBadRedirect(t *testing.T) {
	const secretCode = "secret-code"
	login, tokenCalls := newTestLogin(t, http.StatusOK)
	if _, err := login.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	denied := "http://localhost:29399/callback?error=access_denied&state=" + url.QueryEscape(login.pkce.State)
	noCode := "http://localhost:29399/callback?state=" + url.QueryEscape(login.pkce.State)
	for name, redirect := range map[string]string{
		"wrong state": "http://localhost:29399/callback?code=" + secretCode + "&state=other",
		"no state":    "http://localhost:29399/callback?code=" + secretCode,
		"denied":      denied,
		"no code":     noCode,
		"empty":       "",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := login.SubmitUserInput(context.Background(), map[string]string{"redirect_url": redirect})
			if err == nil {
				t.Fatal("expected an error")
			}
			if strings.Contains(err.Error(), secretCode) {
				t.Errorf("error leaks the authorization code: %v", err)
			}
			if !strings.Contains(err.Error(), "login") {
				t.Errorf("error does not tell the user to start over: %v", err)
			}
		})
	}
	if n := tokenCalls.Load(); n != 0 {
		t.Errorf("token endpoint called %d times for rejected redirects", n)
	}
}

func TestSubmitUserInputExchangeFailure(t *testing.T) {
	const secretCode = "secret-code"
	login, tokenCalls := newTestLogin(t, http.StatusBadRequest)
	if _, err := login.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	redirect := "http://localhost:29399/callback?code=" + secretCode + "&state=" + url.QueryEscape(login.pkce.State)
	_, err := login.SubmitUserInput(context.Background(), map[string]string{"redirect_url": redirect})
	if err == nil {
		t.Fatal("expected an error")
	}
	if n := tokenCalls.Load(); n != 1 {
		t.Errorf("token endpoint called %d times, want 1", n)
	}
	for _, secret := range []string{secretCode, login.pkce.Verifier} {
		if strings.Contains(err.Error(), secret) {
			t.Errorf("error leaks a secret: %v", err)
		}
	}
}

// A token traQ issued for a login that then fails is revoked: it is stored nowhere, but stays valid for a year.
func TestSubmitUserInputRevokesTheTokenOfAFailedLogin(t *testing.T) {
	var revoked atomic.Value
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v3/oauth2/token", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"access_token":"fresh-token","token_type":"Bearer","expires_in":31536000}`))
	})
	mux.HandleFunc("GET /api/v3/users/me", func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, `{"message":"boom"}`, http.StatusInternalServerError)
	})
	mux.HandleFunc("POST /api/v3/oauth2/revoke", func(w http.ResponseWriter, r *http.Request) {
		revoked.Store(r.FormValue("token"))
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	c := &TraQConnector{Config: NetworkConfig{
		ServerURL: srv.URL,
		OAuth:     OAuthConfig{ClientID: "client-id", RedirectURI: "http://localhost:29399/callback"},
	}}
	proc, err := c.CreateLogin(context.Background(), nil, "oauth")
	if err != nil {
		t.Fatal(err)
	}
	login := proc.(*oauthLogin)
	if _, err := login.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	redirect := "http://localhost:29399/callback?code=c&state=" + url.QueryEscape(login.pkce.State)
	if _, err := login.SubmitUserInput(context.Background(), map[string]string{"redirect_url": redirect}); err == nil {
		t.Fatal("expected an error")
	}
	if got, _ := revoked.Load().(string); got != "fresh-token" {
		t.Errorf("revoked %q, want the new token", got)
	}
}
