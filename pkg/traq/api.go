package traq

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

// esc escapes one path segment.
func esc(s string) string { return url.PathEscape(s) }

func formatTime(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }

// Me: GET /users/me
func (c *Client) Me(ctx context.Context) (*Me, error) {
	var out Me
	if err := c.doJSON(ctx, http.MethodGet, "/users/me", nil, nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// Users: GET /users?include-suspended=true
func (c *Client) Users(ctx context.Context) ([]User, error) {
	var out []User
	err := c.doJSON(ctx, http.MethodGet, "/users", url.Values{"include-suspended": {"true"}}, nil, &out)
	return out, err
}

// User: GET /users/{userId}
func (c *Client) User(ctx context.Context, userID string) (*User, error) {
	var out User
	if err := c.doJSON(ctx, http.MethodGet, "/users/"+esc(userID), nil, nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// Channels: GET /channels?include-dm=true
func (c *Client) Channels(ctx context.Context) (*ChannelList, error) {
	var out ChannelList
	if err := c.doJSON(ctx, http.MethodGet, "/channels", url.Values{"include-dm": {"true"}}, nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// Subscriptions: GET /users/me/subscriptions
func (c *Client) Subscriptions(ctx context.Context) ([]Subscription, error) {
	var out []Subscription
	err := c.doJSON(ctx, http.MethodGet, "/users/me/subscriptions", nil, nil, &out)
	return out, err
}

// Unread: GET /users/me/unread
func (c *Client) Unread(ctx context.Context) ([]UnreadChannel, error) {
	var out []UnreadChannel
	err := c.doJSON(ctx, http.MethodGet, "/users/me/unread", nil, nil, &out)
	return out, err
}

// ReadChannel: DELETE /users/me/unread/{channelId}
func (c *Client) ReadChannel(ctx context.Context, channelID string) error {
	return c.doJSON(ctx, http.MethodDelete, "/users/me/unread/"+esc(channelID), nil, nil, nil)
}

// Messages: GET /channels/{channelId}/messages. more is false only when the X-TRAQ-MORE response header is
// exactly "false". A missing or any other value gives true, so a stripped header is never read as a complete
// fetch (callers redact known messages that were not fetched when more is false).
func (c *Client) Messages(ctx context.Context, channelID string, q MessagesQuery) (msgs []Message, more bool, err error) {
	query := url.Values{}
	if q.Limit > 0 {
		query.Set("limit", strconv.Itoa(q.Limit))
	}
	if q.Since != nil {
		query.Set("since", formatTime(*q.Since))
	}
	if q.Until != nil {
		query.Set("until", formatTime(*q.Until))
	}
	if q.Inclusive {
		query.Set("inclusive", "true")
	}
	if q.Order != "" {
		query.Set("order", q.Order)
	}
	path := "/channels/" + esc(channelID) + "/messages"
	resp, err := c.do(ctx, http.MethodGet, path, query, nil, mimeJSON)
	if err != nil {
		return nil, false, err
	}
	defer resp.Body.Close()
	if err := json.NewDecoder(resp.Body).Decode(&msgs); err != nil {
		return nil, false, fmt.Errorf("decode GET %s response: %w", path, err)
	}
	return msgs, resp.Header.Get("X-TRAQ-MORE") != "false", nil
}

// Message: GET /messages/{messageId}
func (c *Client) Message(ctx context.Context, messageID string) (*Message, error) {
	var out Message
	if err := c.doJSON(ctx, http.MethodGet, "/messages/"+esc(messageID), nil, nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

type messageBody struct {
	Content string `json:"content"`
	Embed   bool   `json:"embed"`
}

// PostMessage: POST /channels/{channelId}/messages
func (c *Client) PostMessage(ctx context.Context, channelID, content string) (*Message, error) {
	var out Message
	err := c.doJSON(ctx, http.MethodPost, "/channels/"+esc(channelID)+"/messages", nil, messageBody{content, true}, &out)
	if err != nil {
		return nil, err
	}
	return &out, nil
}

// EditMessage: PUT /messages/{messageId}
func (c *Client) EditMessage(ctx context.Context, messageID, content string) error {
	return c.doJSON(ctx, http.MethodPut, "/messages/"+esc(messageID), nil, messageBody{content, true}, nil)
}

// DeleteMessage: DELETE /messages/{messageId}
func (c *Client) DeleteMessage(ctx context.Context, messageID string) error {
	return c.doJSON(ctx, http.MethodDelete, "/messages/"+esc(messageID), nil, nil, nil)
}

// AddStamp: POST /messages/{messageId}/stamps/{stampId}
func (c *Client) AddStamp(ctx context.Context, messageID, stampID string) error {
	path := "/messages/" + esc(messageID) + "/stamps/" + esc(stampID)
	return c.doJSON(ctx, http.MethodPost, path, nil, map[string]int{"count": 1}, nil)
}

// RemoveStamp: DELETE /messages/{messageId}/stamps/{stampId}
func (c *Client) RemoveStamp(ctx context.Context, messageID, stampID string) error {
	return c.doJSON(ctx, http.MethodDelete, "/messages/"+esc(messageID)+"/stamps/"+esc(stampID), nil, nil, nil)
}

// Stamps: GET /stamps
func (c *Client) Stamps(ctx context.Context) ([]Stamp, error) {
	var out []Stamp
	err := c.doJSON(ctx, http.MethodGet, "/stamps", nil, nil, &out)
	return out, err
}

// Stamp: GET /stamps/{stampId}
func (c *Client) Stamp(ctx context.Context, stampID string) (*Stamp, error) {
	var out Stamp
	if err := c.doJSON(ctx, http.MethodGet, "/stamps/"+esc(stampID), nil, nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// FileMeta: GET /files/{fileId}/meta
func (c *Client) FileMeta(ctx context.Context, fileID string) (*FileInfo, error) {
	var out FileInfo
	if err := c.doJSON(ctx, http.MethodGet, "/files/"+esc(fileID)+"/meta", nil, nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// DownloadFile: GET /files/{fileId}. The body is streamed; the caller closes it.
func (c *Client) DownloadFile(ctx context.Context, fileID string) (io.ReadCloser, error) {
	resp, err := c.do(ctx, http.MethodGet, "/files/"+esc(fileID), nil, nil, "")
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

// PublicIcon: GET /public/icon/{username}. mime is the response Content-Type.
func (c *Client) PublicIcon(ctx context.Context, username string) (data []byte, mime string, err error) {
	resp, err := c.do(ctx, http.MethodGet, "/public/icon/"+esc(username), nil, nil, "")
	if err != nil {
		return nil, "", err
	}
	defer resp.Body.Close()
	data, err = io.ReadAll(resp.Body)
	if err != nil {
		return nil, "", err
	}
	return data, resp.Header.Get("Content-Type"), nil
}

// Search: GET /messages. A 503 means the search engine is unavailable.
func (c *Client) Search(ctx context.Context, q SearchQuery) (*SearchResult, error) {
	query := url.Values{}
	if q.Word != "" {
		query.Set("word", q.Word)
	}
	if q.In != "" {
		query.Set("in", q.In)
	}
	for _, id := range q.From {
		query.Add("from", id)
	}
	if q.Limit > 0 {
		query.Set("limit", strconv.Itoa(q.Limit))
	}
	if q.Offset > 0 {
		query.Set("offset", strconv.Itoa(q.Offset))
	}
	var out SearchResult
	if err := c.doJSON(ctx, http.MethodGet, "/messages", query, nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// ActivityTimeline: GET /activity/timeline?limit=50&all=false&per_channel=true (the newest message of each
// subscribed channel)
func (c *Client) ActivityTimeline(ctx context.Context) ([]ActivityMessage, error) {
	query := url.Values{
		"limit":       {"50"},
		"all":         {"false"},
		"per_channel": {"true"},
	}
	var out []ActivityMessage
	err := c.doJSON(ctx, http.MethodGet, "/activity/timeline", query, nil, &out)
	return out, err
}
