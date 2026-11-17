// Package traq is a thin, stdlib-only client for traQ's REST API v3 and its OAuth 2.0 endpoints.
//
// It deliberately does not use go-traq: go-traq's models decode with DisallowUnknownFields, so the
// bridge would break whenever traQ adds a response field. Types here decode leniently.
package traq

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	apiPath         = "/api/v3"
	maxRetries      = 3
	maxErrorMessage = 200
	maxErrorRead    = 64 << 10
)

// Client talks to one traQ server as one user. It is safe for concurrent use.
type Client struct {
	origin    *url.URL
	apiBase   string // origin + /api/v3, no trailing slash
	http      *http.Client
	token     string
	userAgent string
}

// NewClient: serverURL is the traQ origin ("https://q.trap.jp"); requests go to <serverURL>/api/v3
// with "Authorization: Bearer <accessToken>". A trailing slash on serverURL is accepted.
func NewClient(serverURL string, httpClient *http.Client, accessToken, userAgent string) (*Client, error) {
	origin, err := parseOrigin(serverURL)
	if err != nil {
		return nil, err
	}
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	return &Client{
		origin:    origin,
		apiBase:   origin.String() + apiPath,
		http:      httpClient,
		token:     accessToken,
		userAgent: userAgent,
	}, nil
}

// parseOrigin parses serverURL and strips trailing slashes, so that origin + "/api/v3" never has "//".
func parseOrigin(serverURL string) (*url.URL, error) {
	u, err := url.Parse(strings.TrimSpace(serverURL))
	if err != nil {
		return nil, fmt.Errorf("invalid traQ server URL: %w", err)
	}
	if (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("invalid traQ server URL %q: want an absolute http(s) URL", serverURL)
	}
	u.Path = strings.TrimRight(u.Path, "/")
	u.RawPath = ""
	u.RawQuery = ""
	u.Fragment = ""
	return u, nil
}

// ServerURL returns the origin, without /api/v3.
func (c *Client) ServerURL() *url.URL {
	u := *c.origin
	return &u
}

// HTTPError is returned for every non-2xx response.
type HTTPError struct {
	Status  int
	Message string // the "message" field of traQ's JSON error body, or the raw body
}

func (e *HTTPError) Error() string {
	text := http.StatusText(e.Status)
	if text == "" {
		text = "unknown status"
	}
	if e.Message == "" {
		return fmt.Sprintf("traQ API error: %d %s", e.Status, text)
	}
	return fmt.Sprintf("traQ API error: %d %s: %s", e.Status, text, e.Message)
}

// IsStatus reports whether err wraps an *HTTPError with the given status.
func IsStatus(err error, status int) bool {
	var he *HTTPError
	return errors.As(err, &he) && he.Status == status
}

// newHTTPError builds an HTTPError from a response, consuming at most maxErrorRead bytes of the body.
func newHTTPError(resp *http.Response) *HTTPError {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorRead))
	var parsed struct {
		Message string `json:"message"`
	}
	msg := ""
	if json.Unmarshal(body, &parsed) == nil {
		msg = parsed.Message
	}
	if msg == "" {
		if len(body) > maxErrorMessage {
			body = body[:maxErrorMessage]
		}
		msg = strings.ToValidUTF8(strings.TrimSpace(string(body)), "")
	}
	return &HTTPError{Status: resp.StatusCode, Message: msg}
}

func isRetryable(status int) bool {
	return status == http.StatusTooManyRequests || status == http.StatusBadGateway || status == http.StatusGatewayTimeout
}

// retryDelay returns the Retry-After seconds if present and valid, else 1s, 2s, 4s for attempts 0, 1, 2.
func retryDelay(resp *http.Response, attempt int) time.Duration {
	if s := strings.TrimSpace(resp.Header.Get("Retry-After")); s != "" {
		if secs, err := strconv.Atoi(s); err == nil && secs >= 0 {
			return time.Duration(secs) * time.Second
		}
	}
	return time.Second << attempt
}

// sleep waits for d or until ctx is done.
func sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// do sends a request and returns the 2xx response; the caller closes its body. Non-2xx responses become
// *HTTPError. 429, 502 and 504 are retried up to maxRetries times; 401 and 503 are not retried.
// body, if non-nil, is sent as JSON.
func (c *Client) do(ctx context.Context, method, path string, query url.Values, body any) (*http.Response, error) {
	var payload []byte
	if body != nil {
		var err error
		if payload, err = json.Marshal(body); err != nil {
			return nil, fmt.Errorf("encode request body: %w", err)
		}
	}
	target := c.apiBase + path
	if len(query) > 0 {
		target += "?" + query.Encode()
	}
	for attempt := 0; ; attempt++ {
		var reader io.Reader
		if payload != nil {
			reader = bytes.NewReader(payload)
		}
		req, err := http.NewRequestWithContext(ctx, method, target, reader)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+c.token)
		req.Header.Set("Accept", "application/json")
		if payload != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		if c.userAgent != "" {
			req.Header.Set("User-Agent", c.userAgent)
		}
		resp, err := c.http.Do(req)
		if err != nil {
			return nil, err
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return resp, nil
		}
		if !isRetryable(resp.StatusCode) || attempt >= maxRetries {
			herr := newHTTPError(resp)
			resp.Body.Close()
			return nil, herr
		}
		delay := retryDelay(resp, attempt)
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxErrorRead))
		resp.Body.Close()
		if err := sleep(ctx, delay); err != nil {
			return nil, err
		}
	}
}

// doJSON sends a request and decodes the JSON response into out (skipped if out is nil).
func (c *Client) doJSON(ctx context.Context, method, path string, query url.Values, body, out any) error {
	resp, err := c.do(ctx, method, path, query, body)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if out == nil {
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("decode %s %s response: %w", method, path, err)
	}
	return nil
}
