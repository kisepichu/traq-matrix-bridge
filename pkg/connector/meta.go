package connector

import (
	"crypto/sha256"
	"encoding/hex"
	"time"
)

// UserLoginMetadata is stored in the user_login.metadata column. traQ issues no refresh token, so an
// expired token means the user has to log in again. A zero ExpiresAt means the server sent no expiry.
type UserLoginMetadata struct {
	AccessToken string    `json:"access_token"`
	ExpiresAt   time.Time `json:"expires_at"`
}

type PortalMetadata struct {
	IsDM     bool   `json:"is_dm,omitempty"`
	DMUserID string `json:"dm_user_id,omitempty"` // the other party of a DM
}

type MessageMetadata struct {
	ContentHash string `json:"content_hash,omitempty"` // hex sha256 of the traQ content as last bridged (first part only)
}

// ContentHash returns the hex sha256 of a traQ message's content.
func ContentHash(content string) string {
	sum := sha256.Sum256([]byte(content))
	return hex.EncodeToString(sum[:])
}
