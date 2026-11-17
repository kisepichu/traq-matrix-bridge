package traq

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// ErrStateMismatch means the redirect carried a different state than the one we sent (possible CSRF or
// a stale redirect URL). ErrNoCode means the redirect carried neither a code nor an error.
var (
	ErrStateMismatch = errors.New("oauth state mismatch")
	ErrNoCode        = errors.New("no authorization code in the redirect URL")
)

// OAuthConfig describes the traQ OAuth 2.0 client used for the authorization code + PKCE flow.
type OAuthConfig struct {
	ServerURL   string // origin, e.g. "https://q.trap.jp"; a trailing slash is accepted
	ClientID    string
	RedirectURI string
	HTTPClient  *http.Client // nil means http.DefaultClient
}

// PKCE holds the per-login secrets. Verifier must never be logged.
type PKCE struct{ Verifier, Challenge, State string }

// Token is the result of a code exchange. traQ issues no refresh token.
type Token struct {
	AccessToken string
	ExpiresAt   time.Time // zero if the server did not send expires_in
}

// NewPKCE returns a fresh verifier and state (32 random bytes each, base64url without padding) and the
// S256 challenge of the verifier.
func NewPKCE() (PKCE, error) {
	verifier, err := randomString(32)
	if err != nil {
		return PKCE{}, err
	}
	state, err := randomString(32)
	if err != nil {
		return PKCE{}, err
	}
	sum := sha256.Sum256([]byte(verifier))
	return PKCE{
		Verifier:  verifier,
		Challenge: base64.RawURLEncoding.EncodeToString(sum[:]),
		State:     state,
	}, nil
}

func randomString(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generate random bytes: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

// endpoint returns <origin>/api/v3<path>, parsing ServerURL exactly as NewClient does.
func (c OAuthConfig) endpoint(path string) (string, error) {
	origin, err := parseOrigin(c.ServerURL)
	if err != nil {
		return "", err
	}
	return origin.String() + apiPath + path, nil
}

func (c OAuthConfig) httpClient() *http.Client {
	if c.HTTPClient != nil {
		return c.HTTPClient
	}
	return http.DefaultClient
}

// AuthorizeURL returns the URL the user opens in a browser to approve the bridge. It fails if ServerURL is
// not an absolute http(s) URL.
func (c OAuthConfig) AuthorizeURL(p PKCE) (string, error) {
	q := url.Values{
		"response_type":         {"code"},
		"client_id":             {c.ClientID},
		"redirect_uri":          {c.RedirectURI},
		"scope":                 {"read write"},
		"state":                 {p.State},
		"code_challenge":        {p.Challenge},
		"code_challenge_method": {"S256"},
	}
	base, err := c.endpoint("/oauth2/authorize")
	if err != nil {
		return "", err
	}
	return base + "?" + q.Encode(), nil
}

// ParseRedirect extracts the authorization code from the URL the browser was redirected to.
func ParseRedirect(redirected, expectedState string) (code string, err error) {
	u, err := url.Parse(strings.TrimSpace(redirected))
	if err != nil {
		// url.Error would echo the input, which contains the authorization code.
		return "", errors.New("invalid redirect URL")
	}
	q := u.Query()
	if q.Get("state") != expectedState {
		return "", ErrStateMismatch
	}
	if e := q.Get("error"); e != "" {
		return "", fmt.Errorf("authorization denied: %s", e)
	}
	code = q.Get("code")
	if code == "" {
		return "", ErrNoCode
	}
	return code, nil
}

// Exchange trades the authorization code for an access token.
func (c OAuthConfig) Exchange(ctx context.Context, code, verifier string) (Token, error) {
	form := url.Values{
		"grant_type":    {"authorization_code"},
		"code":          {code},
		"redirect_uri":  {c.RedirectURI},
		"client_id":     {c.ClientID},
		"code_verifier": {verifier},
	}
	resp, err := c.postForm(ctx, "/oauth2/token", form)
	if err != nil {
		return Token{}, err
	}
	defer resp.Body.Close()
	var body struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int64  `json:"expires_in"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return Token{}, fmt.Errorf("decode token response: %w", err)
	}
	if body.AccessToken == "" {
		return Token{}, errors.New("token response has no access_token")
	}
	tok := Token{AccessToken: body.AccessToken}
	if body.ExpiresIn > 0 {
		tok.ExpiresAt = time.Now().Add(time.Duration(body.ExpiresIn) * time.Second)
	}
	return tok, nil
}

// Revoke invalidates an access token on the server.
func (c OAuthConfig) Revoke(ctx context.Context, token string) error {
	resp, err := c.postForm(ctx, "/oauth2/revoke", url.Values{"token": {token}})
	if err != nil {
		return err
	}
	return resp.Body.Close()
}

// postForm POSTs a form and returns the 2xx response; the caller closes its body.
func (c OAuthConfig) postForm(ctx context.Context, path string, form url.Values) (*http.Response, error) {
	target, err := c.endpoint(path)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", mimeJSON)
	resp, err := c.httpClient().Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		herr := newHTTPError(resp)
		resp.Body.Close()
		return nil, herr
	}
	return resp, nil
}
