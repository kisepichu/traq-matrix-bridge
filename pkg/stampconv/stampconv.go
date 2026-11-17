// Package stampconv maps traQ stamps to Matrix reaction keys and back.
package stampconv

import (
	"errors"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"unicode/utf8"
)

// Stamp is a traQ stamp as the bridge needs it.
type Stamp struct {
	ID      string
	Name    string
	Unicode bool
	// Emoji is the emoji text of a Unicode stamp, or "" until its file name has been resolved.
	Emoji string
}

var (
	ErrUnknownStamp         = errors.New("unknown stamp")
	ErrEmojiTableIncomplete = errors.New("the emoji to stamp table is still being built")
)

var namePattern = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,32}$`)

// EmojiFromFileName converts a twemoji file name such as "1f44d.svg" or "1f468-200d-1f4bb.svg" into emoji text.
func EmojiFromFileName(name string) (string, bool) {
	base, ok := strings.CutSuffix(name, ".svg")
	if !ok || base == "" {
		return "", false
	}
	var b strings.Builder
	for _, part := range strings.Split(base, "-") {
		cp, err := strconv.ParseUint(part, 16, 32)
		if err != nil || cp > utf8.MaxRune || !utf8.ValidRune(rune(cp)) {
			return "", false
		}
		b.WriteRune(rune(cp))
	}
	return b.String(), true
}

// Normalize removes U+FE0F (variation selector-16) so that emoji with and without it compare equal.
func Normalize(s string) string {
	return strings.ReplaceAll(s, "\uFE0F", "")
}

// ReactionKey returns the Matrix reaction key of a stamp: the emoji of a resolved Unicode stamp, otherwise ":name:".
func ReactionKey(s Stamp) string {
	if s.Unicode && s.Emoji != "" {
		return s.Emoji
	}
	return ":" + s.Name + ":"
}

// Table indexes stamps by ID, name and normalized emoji. It is safe for concurrent use.
type Table struct {
	mu       sync.RWMutex
	byID     map[string]Stamp
	byName   map[string]string // name -> ID
	byEmoji  map[string]string // normalized emoji -> ID
	complete bool
}

// NewTable builds a table. complete reports whether every Unicode stamp already has its Emoji resolved.
func NewTable(stamps []Stamp, complete bool) *Table {
	t := &Table{
		byID:     make(map[string]Stamp, len(stamps)),
		byName:   make(map[string]string, len(stamps)),
		byEmoji:  make(map[string]string, len(stamps)),
		complete: complete,
	}
	for _, s := range stamps {
		t.putLocked(s)
	}
	return t
}

func (t *Table) putLocked(s Stamp) {
	t.deleteLocked(s.ID)
	t.byID[s.ID] = s
	t.byName[s.Name] = s.ID
	if s.Unicode && s.Emoji != "" {
		t.byEmoji[Normalize(s.Emoji)] = s.ID
	}
}

func (t *Table) deleteLocked(id string) {
	old, ok := t.byID[id]
	if !ok {
		return
	}
	delete(t.byID, id)
	if t.byName[old.Name] == id {
		delete(t.byName, old.Name)
	}
	if old.Emoji != "" && t.byEmoji[Normalize(old.Emoji)] == id {
		delete(t.byEmoji, Normalize(old.Emoji))
	}
}

// Put adds or replaces a stamp.
func (t *Table) Put(s Stamp) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.putLocked(s)
}

// Delete removes a stamp by ID.
func (t *Table) Delete(id string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.deleteLocked(id)
}

// SetComplete marks whether every Unicode stamp has its Emoji resolved.
func (t *Table) SetComplete(complete bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.complete = complete
}

// ByID looks up a stamp by its traQ ID.
func (t *Table) ByID(id string) (Stamp, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	s, ok := t.byID[id]
	return s, ok
}

// Resolve finds the stamp for a Matrix reaction key: an emoji, ":name:" or "name".
func (t *Table) Resolve(key string) (Stamp, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	name := strings.TrimSuffix(strings.TrimPrefix(key, ":"), ":")
	if namePattern.MatchString(name) {
		if id, ok := t.byName[name]; ok {
			return t.byID[id], nil
		}
		return Stamp{}, ErrUnknownStamp
	}
	if key == "" {
		return Stamp{}, ErrUnknownStamp
	}
	if id, ok := t.byEmoji[Normalize(key)]; ok {
		return t.byID[id], nil
	}
	if !t.complete {
		return Stamp{}, ErrEmojiTableIncomplete
	}
	return Stamp{}, ErrUnknownStamp
}
