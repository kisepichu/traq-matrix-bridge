//go:build integration

package traq_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"image"
	"image/png"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/cookiejar"
	"net/textproto"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kisepichu/traq-matrix-bridge/pkg/traq"
)

const (
	adminName     = "traq"
	adminPassword = "traq"
	redirectURI   = "http://localhost:29399/callback"
)

// testServerURL is the origin of the traQ under test.
func testServerURL() string {
	if u := os.Getenv("TRAQ_TEST_URL"); u != "" {
		return strings.TrimRight(u, "/")
	}
	return "http://localhost:3080"
}

// waitForServer blocks until traQ answers GET /api/v3/version with 200. Elasticsearch makes the first start slow.
func waitForServer(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(120 * time.Second)
	var last string
	for {
		resp, err := http.Get(testServerURL() + "/api/v3/version")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
			last = resp.Status
		} else {
			last = err.Error()
		}
		if time.Now().After(deadline) {
			t.Fatalf("traQ at %s is not ready after 120s: %s", testServerURL(), last)
		}
		time.Sleep(time.Second)
	}
}

// session is a logged-in browser-like HTTP client (cookie jar, redirects are not followed).
type session struct{ http *http.Client }

// randHex returns 2n random hex characters.
func randHex(t *testing.T, n int) string {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(b)
}

// randLetters returns n random lowercase letters.
func randLetters(t *testing.T, n int) string {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	for i := range b {
		b[i] = 'a' + b[i]%26
	}
	return string(b)
}

// do sends a request and returns the status, headers and body. It fails the test unless the status is one of want.
func (s *session) do(t *testing.T, req *http.Request, want ...int) (int, http.Header, []byte) {
	t.Helper()
	resp, err := s.http.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", req.Method, req.URL.Path, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("%s %s: read body: %v", req.Method, req.URL.Path, err)
	}
	for _, w := range want {
		if resp.StatusCode == w {
			return resp.StatusCode, resp.Header, body
		}
	}
	t.Fatalf("%s %s: status %d, want %v: %s", req.Method, req.URL.Path, resp.StatusCode, want, body)
	return 0, nil, nil
}

// doJSON sends body (if non-nil) as JSON and decodes the response into out (if non-nil).
func (s *session) doJSON(t *testing.T, method, path string, body, out any, want ...int) int {
	t.Helper()
	var r io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		r = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, testServerURL()+path, r)
	if err != nil {
		t.Fatal(err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	status, _, data := s.do(t, req, want...)
	if out != nil {
		if err := json.Unmarshal(data, out); err != nil {
			t.Fatalf("%s %s: decode %q: %v", method, path, data, err)
		}
	}
	return status
}

// doMultipart sends a multipart form with the given text fields and one file part.
func (s *session) doMultipart(t *testing.T, path string, fields map[string]string, fileName string, data []byte, out any, want ...int) {
	t.Helper()
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	for k, v := range fields {
		if err := w.WriteField(k, v); err != nil {
			t.Fatal(err)
		}
	}
	contentType := mime.TypeByExtension(filepath.Ext(fileName))
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	h := textproto.MIMEHeader{}
	h.Set("Content-Disposition", `form-data; name="file"; filename="`+fileName+`"`)
	h.Set("Content-Type", contentType)
	part, err := w.CreatePart(h)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := part.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodPost, testServerURL()+path, &buf)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", w.FormDataContentType())
	_, _, body := s.do(t, req, want...)
	if err := json.Unmarshal(body, out); err != nil {
		t.Fatalf("POST %s: decode %q: %v", path, body, err)
	}
}

// newSession returns a session with an empty cookie jar.
func newSession(t *testing.T) *session {
	t.Helper()
	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	return &session{http: &http.Client{
		Jar:           jar,
		Timeout:       30 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}}
}

// loginSession logs in with a name and password.
func loginSession(t *testing.T, name, password string) *session {
	t.Helper()
	waitForServer(t)
	s := newSession(t)
	s.doJSON(t, http.MethodPost, "/api/v3/login", map[string]string{"name": name, "password": password}, nil, http.StatusNoContent)
	return s
}

// adminSession logs in as the initial admin of `traQ serve --dev`.
func adminSession(t *testing.T) *session {
	t.Helper()
	return loginSession(t, adminName, adminPassword)
}

// ensureUser creates the user (password name+"-password"), or finds it if it exists, and returns its ID.
// With TRAQ_ALLOWSIGNUP=true, POST /users is the public signup endpoint and traQ rejects it with 400 when the
// request carries a session cookie, so the creation is sent without the admin's cookies. admin is used for the lookup.
func ensureUser(t *testing.T, admin *session, name string) string {
	t.Helper()
	var u traq.User
	status := newSession(t).doJSON(t, http.MethodPost, "/api/v3/users", map[string]string{"name": name, "password": name + "-password"}, &u, http.StatusCreated, http.StatusConflict)
	if status == http.StatusCreated {
		return u.ID
	}
	var users []traq.User
	admin.doJSON(t, http.MethodGet, "/api/v3/users?name="+url.QueryEscape(name), nil, &users, http.StatusOK)
	for _, u := range users {
		if u.Name == name {
			return u.ID
		}
	}
	t.Fatalf("user %q: 409 on create but not found", name)
	return ""
}

// newUser creates a fresh user with a random name and returns its name and a session.
func newUser(t *testing.T, admin *session) (name, id string, s *session) {
	t.Helper()
	name = "it" + randHex(t, 6)
	id = ensureUser(t, admin, name)
	return name, id, loginSession(t, name, name+"-password")
}

// newOAuthClient registers a public OAuth client (read + write) as the session's user.
func newOAuthClient(t *testing.T, admin *session) traq.OAuthConfig {
	t.Helper()
	var c struct {
		ID string `json:"id"`
	}
	admin.doJSON(t, http.MethodPost, "/api/v3/clients", map[string]any{
		"name":         "it-" + randHex(t, 4),
		"description":  "integration test",
		"callbackUrl":  redirectURI,
		"scopes":       []string{"read", "write"},
		"confidential": false,
	}, &c, http.StatusCreated)
	return traq.OAuthConfig{ServerURL: testServerURL(), ClientID: c.ID, RedirectURI: redirectURI}
}

// authorize runs the authorization code + PKCE flow for the session's user, approving the consent page.
func authorize(t *testing.T, s *session, cfg traq.OAuthConfig) traq.Token {
	t.Helper()
	p, err := traq.NewPKCE()
	if err != nil {
		t.Fatal(err)
	}
	authURL, err := cfg.AuthorizeURL(p)
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodGet, authURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, h, _ := s.do(t, req, http.StatusFound)
	if loc := h.Get("Location"); !strings.Contains(loc, "/consent") {
		t.Fatalf("authorize redirected to %q, want the consent page", loc)
	}

	req, err = http.NewRequest(http.MethodPost, testServerURL()+"/api/v3/oauth2/authorize/decide", strings.NewReader(url.Values{"submit": {"approve"}}.Encode()))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	_, h, _ = s.do(t, req, http.StatusFound)
	code, err := traq.ParseRedirect(h.Get("Location"), p.State)
	if err != nil {
		t.Fatalf("ParseRedirect: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	tok, err := cfg.Exchange(ctx, code, p.Verifier)
	if err != nil {
		t.Fatalf("Exchange: %v", err)
	}
	return tok
}

// apiFor authorizes cfg for the session's user and returns a Client plus the user (from Me).
func apiFor(t *testing.T, s *session, cfg traq.OAuthConfig) (*traq.Client, *traq.Me) {
	t.Helper()
	tok := authorize(t, s, cfg)
	c, err := traq.NewClient(testServerURL(), nil, tok.AccessToken, "traq-matrix-bridge-integration-test")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	me, err := c.Me(ctx)
	if err != nil {
		t.Fatalf("Me: %v", err)
	}
	return c, me
}

// newChannel creates a public top-level channel with a random name and returns its ID.
func newChannel(t *testing.T, s *session) string {
	t.Helper()
	var ch traq.Channel
	s.doJSON(t, http.MethodPost, "/api/v3/channels", map[string]any{"name": "it-" + randHex(t, 4), "parent": nil}, &ch, http.StatusCreated)
	return ch.ID
}

// subscribe sets the session user's subscription level on a channel.
func subscribe(t *testing.T, s *session, channelID string, level int) {
	t.Helper()
	s.doJSON(t, http.MethodPut, "/api/v3/users/me/subscriptions/"+url.PathEscape(channelID), map[string]int{"level": level}, nil, http.StatusNoContent)
}

// pngBytes is a valid 1x1 PNG.
var pngBytes = func() []byte {
	var buf bytes.Buffer
	if err := png.Encode(&buf, image.NewNRGBA(image.Rect(0, 0, 1, 1))); err != nil {
		panic(err)
	}
	return buf.Bytes()
}()

// createStamp creates a custom stamp and returns its ID.
func createStamp(t *testing.T, s *session, name string) string {
	t.Helper()
	var st traq.Stamp
	s.doMultipart(t, "/api/v3/stamps", map[string]string{"name": name}, name+".png", pngBytes, &st, http.StatusCreated)
	return st.ID
}

// uploadFile uploads a file to a channel and returns the file ID.
func uploadFile(t *testing.T, s *session, channelID, name string, data []byte) string {
	t.Helper()
	var f traq.FileInfo
	s.doMultipart(t, "/api/v3/files", map[string]string{"channelId": channelID}, name, data, &f, http.StatusCreated)
	return f.ID
}

// dmChannel returns the ID of the DM channel between the session's user and userID, creating it if needed.
func dmChannel(t *testing.T, s *session, userID string) string {
	t.Helper()
	var ch traq.DMChannel
	s.doJSON(t, http.MethodGet, "/api/v3/users/"+url.PathEscape(userID)+"/dm-channel", nil, &ch, http.StatusOK)
	return ch.ID
}
