package stampconv

import (
	"errors"
	"testing"
)

func TestEmojiFromFileName(t *testing.T) {
	cases := []struct {
		in   string
		want string
		ok   bool
	}{
		{"1f44d.svg", "👍", true},
		{"1f468-200d-1f4bb.svg", "👨\u200d💻", true},
		{"2764.svg", "❤", true},
		{"00a9.svg", "©", true},
		{"1f44d.png", "", false},
		{"thumbsup.svg", "", false},
		{".svg", "", false},
		{"110000.svg", "", false}, // beyond the Unicode range
	}
	for _, c := range cases {
		got, ok := EmojiFromFileName(c.in)
		if got != c.want || ok != c.ok {
			t.Errorf("EmojiFromFileName(%q) = %q, %v; want %q, %v", c.in, got, ok, c.want, c.ok)
		}
	}
}

func TestNormalizeRemovesVariationSelector(t *testing.T) {
	if got := Normalize("❤️"); got != "❤" {
		t.Errorf("Normalize = %q, want %q", got, "❤")
	}
	if got := Normalize("👍"); got != "👍" {
		t.Errorf("Normalize changed an emoji without FE0F: %q", got)
	}
}

func newTestTable(complete bool) *Table {
	return NewTable([]Stamp{
		{ID: "s-thumbsup", Name: "thumbsup", Unicode: true, Emoji: "👍"},
		{ID: "s-heart", Name: "heart", Unicode: true, Emoji: "❤"},
		{ID: "s-unresolved", Name: "zzz", Unicode: true},
		{ID: "s-custom", Name: "kise-custom", Unicode: false},
	}, complete)
}

func TestResolve(t *testing.T) {
	tbl := newTestTable(true)
	cases := []struct {
		key    string
		wantID string
	}{
		{":thumbsup:", "s-thumbsup"},
		{"thumbsup", "s-thumbsup"},
		{"👍", "s-thumbsup"},
		{"❤️", "s-heart"},
		{"❤", "s-heart"},
		{":kise-custom:", "s-custom"},
		{"kise-custom", "s-custom"},
	}
	for _, c := range cases {
		got, err := tbl.Resolve(c.key)
		if err != nil || got.ID != c.wantID {
			t.Errorf("Resolve(%q) = %+v, %v; want ID %q", c.key, got, err, c.wantID)
		}
	}
}

func TestResolveUnknown(t *testing.T) {
	tbl := newTestTable(true)
	for _, key := range []string{":nope:", "nope", "🦄", ""} {
		if _, err := tbl.Resolve(key); !errors.Is(err, ErrUnknownStamp) {
			t.Errorf("Resolve(%q) err = %v, want ErrUnknownStamp", key, err)
		}
	}
}

func TestResolveEmojiWhileTableIncomplete(t *testing.T) {
	tbl := newTestTable(false)
	if _, err := tbl.Resolve("🦄"); !errors.Is(err, ErrEmojiTableIncomplete) {
		t.Errorf("err = %v, want ErrEmojiTableIncomplete", err)
	}
	// Names still resolve while the emoji table is being built.
	if got, err := tbl.Resolve(":thumbsup:"); err != nil || got.ID != "s-thumbsup" {
		t.Errorf("Resolve(:thumbsup:) = %+v, %v", got, err)
	}
	tbl.SetComplete(true)
	if _, err := tbl.Resolve("🦄"); !errors.Is(err, ErrUnknownStamp) {
		t.Errorf("after SetComplete err = %v, want ErrUnknownStamp", err)
	}
}

func TestPutAndDeleteUpdateAllIndexes(t *testing.T) {
	tbl := newTestTable(true)
	tbl.Put(Stamp{ID: "s-thumbsup", Name: "good", Unicode: true, Emoji: "👍"}) // renamed
	if _, err := tbl.Resolve("thumbsup"); !errors.Is(err, ErrUnknownStamp) {
		t.Errorf("old name still resolves after rename: %v", err)
	}
	if got, err := tbl.Resolve("good"); err != nil || got.ID != "s-thumbsup" {
		t.Errorf("new name = %+v, %v", got, err)
	}
	tbl.Delete("s-thumbsup")
	if _, ok := tbl.ByID("s-thumbsup"); ok {
		t.Error("ByID still finds a deleted stamp")
	}
	if _, err := tbl.Resolve("👍"); !errors.Is(err, ErrUnknownStamp) {
		t.Errorf("emoji of a deleted stamp still resolves: %v", err)
	}
}

func TestReactionKey(t *testing.T) {
	cases := []struct {
		s    Stamp
		want string
	}{
		{Stamp{Name: "thumbsup", Unicode: true, Emoji: "👍"}, "👍"},
		{Stamp{Name: "zzz", Unicode: true}, ":zzz:"},
		{Stamp{Name: "kise-custom"}, ":kise-custom:"},
	}
	for _, c := range cases {
		if got := ReactionKey(c.s); got != c.want {
			t.Errorf("ReactionKey(%+v) = %q, want %q", c.s, got, c.want)
		}
	}
}
