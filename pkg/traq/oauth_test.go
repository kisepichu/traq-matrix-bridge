package traq

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestPKCEChallengeIsS256OfVerifier(t *testing.T) {
	p, err := NewPKCE()
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256([]byte(p.Verifier))
	if p.Challenge != base64.RawURLEncoding.EncodeToString(sum[:]) {
		t.Error("challenge is not base64url(sha256(verifier))")
	}
	if len(p.Verifier) < 43 || p.State == "" || p.State == p.Verifier {
		t.Errorf("weak PKCE values: %+v", p)
	}
}

func TestAuthorizeURL(t *testing.T) {
	cfg := OAuthConfig{ServerURL: "https://q.example.jp", ClientID: "cid", RedirectURI: "http://localhost:29399/callback"}
	u, err := url.Parse(cfg.AuthorizeURL(PKCE{Verifier: "v", Challenge: "ch", State: "st"}))
	if err != nil {
		t.Fatal(err)
	}
	q := u.Query()
	if u.Scheme+"://"+u.Host+u.Path != "https://q.example.jp/api/v3/oauth2/authorize" ||
		q.Get("response_type") != "code" || q.Get("client_id") != "cid" ||
		q.Get("redirect_uri") != "http://localhost:29399/callback" || q.Get("scope") != "read write" ||
		q.Get("state") != "st" || q.Get("code_challenge") != "ch" || q.Get("code_challenge_method") != "S256" {
		t.Errorf("authorize URL = %s", u)
	}
}

func TestAuthorizeURLTrailingSlash(t *testing.T) {
	cfg := OAuthConfig{ServerURL: "https://q.example.jp/", ClientID: "cid", RedirectURI: "http://localhost/cb"}
	got := cfg.AuthorizeURL(PKCE{})
	if !strings.HasPrefix(got, "https://q.example.jp/api/v3/oauth2/authorize?") {
		t.Errorf("authorize URL = %s", got)
	}
}

func TestParseRedirect(t *testing.T) {
	code, err := ParseRedirect("  http://localhost:29399/callback?code=abc&state=st \n", "st")
	if err != nil || code != "abc" {
		t.Errorf("code = %q, err = %v", code, err)
	}
	if _, err := ParseRedirect("http://localhost:29399/callback?code=abc&state=other", "st"); !errors.Is(err, ErrStateMismatch) {
		t.Errorf("state mismatch err = %v", err)
	}
	if _, err := ParseRedirect("http://localhost:29399/callback?state=st", "st"); !errors.Is(err, ErrNoCode) {
		t.Errorf("missing code err = %v", err)
	}
	if _, err := ParseRedirect("http://localhost:29399/callback?error=access_denied&state=st", "st"); err == nil {
		t.Error("denied authorization was accepted")
	}
}

func TestExchangeSendsPKCEForm(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Fatal(err)
		}
		f := r.PostForm
		if r.URL.Path != "/api/v3/oauth2/token" || f.Get("grant_type") != "authorization_code" || f.Get("code") != "abc" ||
			f.Get("code_verifier") != "ver" || f.Get("client_id") != "cid" || f.Get("redirect_uri") != "http://localhost:29399/callback" {
			t.Errorf("token request %s %v", r.URL.Path, f)
		}
		w.Header().Set("Content-Type", "application/json")
		// traQ 3.32.1 returns no refresh_token and scope: null
		_, _ = w.Write([]byte(`{"access_token":"at","token_type":"Bearer","expires_in":31536000,"scope":null}`))
	}))
	defer srv.Close()
	cfg := OAuthConfig{ServerURL: srv.URL, ClientID: "cid", RedirectURI: "http://localhost:29399/callback", HTTPClient: srv.Client()}
	tok, err := cfg.Exchange(context.Background(), "abc", "ver")
	if err != nil || tok.AccessToken != "at" || time.Until(tok.ExpiresAt) < 364*24*time.Hour {
		t.Errorf("token = %+v, err = %v", tok, err)
	}
}

func TestExchangeFailureIsHTTPErrorWithTrailingSlash(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"message":"invalid_grant"}`))
	}))
	defer srv.Close()
	cfg := OAuthConfig{ServerURL: srv.URL + "/", ClientID: "cid", RedirectURI: "http://localhost/cb", HTTPClient: srv.Client()}
	_, err := cfg.Exchange(context.Background(), "abc", "ver")
	if !IsStatus(err, http.StatusBadRequest) {
		t.Errorf("err = %v", err)
	}
}

func TestRevokeSendsTokenForm(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Fatal(err)
		}
		if r.Method != http.MethodPost || r.URL.Path != "/api/v3/oauth2/revoke" || r.PostForm.Get("token") != "tok" {
			t.Errorf("revoke request %s %s %v", r.Method, r.URL.Path, r.PostForm)
		}
	}))
	defer srv.Close()
	cfg := OAuthConfig{ServerURL: srv.URL, HTTPClient: srv.Client()}
	if err := cfg.Revoke(context.Background(), "tok"); err != nil {
		t.Fatalf("Revoke: %v", err)
	}
}
