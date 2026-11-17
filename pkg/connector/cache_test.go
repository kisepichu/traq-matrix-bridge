package connector

import (
	"testing"

	"github.com/kisepichu/traq-matrix-bridge/pkg/traq"
)

func TestBuildChannelTree(t *testing.T) {
	a, b := "ch-a", "ch-b"
	tree := buildChannelTree(&traq.ChannelList{
		// Children are listed before their parents to check that order does not matter.
		Public: []traq.Channel{
			{ID: "ch-c", ParentID: &b, Name: "c", Topic: "deep", Archived: true},
			{ID: b, ParentID: &a, Name: "b"},
			{ID: a, Name: "a"},
		},
		DM: []traq.DMChannel{{ID: "dm-1", UserID: "user-1"}},
	})

	c, ok := tree.Get("ch-c")
	if !ok {
		t.Fatal("Get(ch-c) not found")
	}
	if c.Path != "a/b/c" || c.Topic != "deep" || c.DMUserID != "" {
		t.Errorf("Get(ch-c) = %+v, want Path a/b/c, Topic deep, no DMUserID", c)
	}
	for _, path := range []string{"#a/b", "a/b"} {
		if got, ok := tree.ByPath(path); !ok || got.ID != b {
			t.Errorf("ByPath(%q) = %+v, %v; want %s", path, got, ok, b)
		}
	}
	if _, ok := tree.ByPath("a/x"); ok {
		t.Error("ByPath(a/x) found a channel")
	}

	dm, ok := tree.Get("dm-1")
	if !ok {
		t.Fatal("Get(dm-1) not found")
	}
	if dm.DMUserID != "user-1" || dm.Path != "" || dm.Level != 2 {
		t.Errorf("Get(dm-1) = %+v, want DMUserID user-1, empty Path, Level 2", dm)
	}
}
