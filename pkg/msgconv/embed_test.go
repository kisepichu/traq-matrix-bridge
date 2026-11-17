package msgconv

import "testing"

func TestPlainEmbeds(t *testing.T) {
	cases := []struct{ name, in, want string }{
		{"no embed", "hello !world {x}", "hello !world {x}"},
		{"user", `hi !{"type":"user","raw":"@alice","id":"u1"}!`, "hi @alice!"},
		{"two embeds", `!{"type":"user","raw":"@a","id":"u1"} in !{"type":"channel","raw":"#x/y","id":"c1"}`, "@a in #x/y"},
		{"nested raw braces", `!{"type":"user","raw":"@a}b","id":"u1"} z`, "@a}b z"},
		{"spoiler marks stay", `!!!{"type":"user","raw":"@a","id":"u1"}!!`, "!!@a!!"},
		{"broken stays", `!{"type":"user", x`, `!{"type":"user", x`},
		{"unknown type stays", `!{"type":"foo","raw":"r","id":"i"}`, `!{"type":"foo","raw":"r","id":"i"}`},
		{"invalid then valid", `!{oops !{"type":"group","raw":"@g","id":"g1"}`, "!{oops @g"},
		{"file without id stays", `!{"type":"file","raw":"f"}`, `!{"type":"file","raw":"f"}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := PlainEmbeds(tc.in); got != tc.want {
				t.Errorf("PlainEmbeds(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}
