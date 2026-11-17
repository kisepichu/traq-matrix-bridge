package traq

import (
	"encoding/json"
	"time"
)

// Event is one frame from traQ's WebSocket: {"type": "...", "body": {...}}. Decode Body into the type that
// matches Type.
type Event struct {
	Type string          `json:"type"`
	Body json.RawMessage `json:"body"`
}

// Decode unmarshals the event body into v.
func (e Event) Decode(v any) error { return json.Unmarshal(e.Body, v) }

// IDEvent is the body of the events that carry only an ID: MESSAGE_CREATED/UPDATED/DELETED (message ID),
// MESSAGE_READ (channel ID), USER_UPDATED, USER_ICON_UPDATED, STAMP_*, CHANNEL_UPDATED and CHANNEL_DELETED.
type IDEvent struct {
	ID string `json:"id"`
}

// MessageCreatedEvent is the body of MESSAGE_CREATED.
type MessageCreatedEvent struct {
	ID       string `json:"id"`
	IsCiting bool   `json:"is_citing"`
}

// ChannelCreatedEvent is the body of CHANNEL_CREATED.
type ChannelCreatedEvent struct {
	ID       string `json:"id"`
	DMUserID string `json:"dm_user_id"` // empty unless the channel is a DM
}

// MessageStampedEvent is the body of MESSAGE_STAMPED.
type MessageStampedEvent struct {
	MessageID string    `json:"message_id"`
	UserID    string    `json:"user_id"`
	StampID   string    `json:"stamp_id"`
	Count     int       `json:"count"`
	CreatedAt time.Time `json:"created_at"`
}

// MessageUnstampedEvent is the body of MESSAGE_UNSTAMPED.
type MessageUnstampedEvent struct {
	MessageID string `json:"message_id"`
	UserID    string `json:"user_id"`
	StampID   string `json:"stamp_id"`
}
