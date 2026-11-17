package traq

import "time"

// The types below carry only the fields the bridge uses. Unknown JSON fields are ignored on decode.

type Message struct {
	ID        string         `json:"id"`
	UserID    string         `json:"userId"`
	ChannelID string         `json:"channelId"`
	Content   string         `json:"content"`
	CreatedAt time.Time      `json:"createdAt"`
	UpdatedAt time.Time      `json:"updatedAt"`
	Stamps    []MessageStamp `json:"stamps"`
}

type MessageStamp struct {
	UserID    string    `json:"userId"`
	StampID   string    `json:"stampId"`
	Count     int       `json:"count"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}

type User struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	DisplayName string    `json:"displayName"`
	IconFileID  string    `json:"iconFileId"`
	Bot         bool      `json:"bot"`
	State       int       `json:"state"` // 0 deactivated, 1 active, 2 suspended
	UpdatedAt   time.Time `json:"updatedAt"`
}

type Me struct {
	User
	Groups      []string `json:"groups"`
	Permissions []string `json:"permissions"`
}

type Channel struct {
	ID       string   `json:"id"`
	ParentID *string  `json:"parentId"`
	Archived bool     `json:"archived"`
	Force    bool     `json:"force"`
	Topic    string   `json:"topic"`
	Name     string   `json:"name"`
	Children []string `json:"children"`
}

type DMChannel struct {
	ID     string `json:"id"`
	UserID string `json:"userId"` // the other party
}

type ChannelList struct {
	Public []Channel   `json:"public"`
	DM     []DMChannel `json:"dm"`
}

type Subscription struct {
	ChannelID string `json:"channelId"`
	Level     int    `json:"level"` // 0 none, 1 unread tracking, 2 unread tracking + notifications
}

type UnreadChannel struct {
	ChannelID       string    `json:"channelId"`
	Count           int       `json:"count"`
	Noticeable      bool      `json:"noticeable"`
	Since           time.Time `json:"since"`
	UpdatedAt       time.Time `json:"updatedAt"`
	OldestMessageID string    `json:"oldestMessageId"`
}

type Stamp struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	FileID    string `json:"fileId"`
	IsUnicode bool   `json:"isUnicode"`
}

type FileInfo struct {
	ID              string `json:"id"`
	Name            string `json:"name"`
	Mime            string `json:"mime"`
	Size            int64  `json:"size"`
	IsAnimatedImage bool   `json:"isAnimatedImage"`
}

type ActivityMessage struct {
	ID        string    `json:"id"`
	UserID    string    `json:"userId"`
	ChannelID string    `json:"channelId"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}

type SearchResult struct {
	TotalHits int64     `json:"totalHits"`
	Hits      []Message `json:"hits"`
}

type MessagesQuery struct {
	Limit     int // 1-200
	Since     *time.Time
	Until     *time.Time
	Inclusive bool
	Order     string // "asc" or "desc" (default "desc")
}

type SearchQuery struct {
	Word   string
	In     string   // channel ID
	From   []string // user IDs
	Limit  int      // 1-100
	Offset int
}
