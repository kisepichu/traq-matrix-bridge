package msgconv

import (
	"bytes"
	"encoding/json"
	"strings"

	"github.com/yuin/goldmark/ast"
	"github.com/yuin/goldmark/parser"
	"github.com/yuin/goldmark/text"
)

// Embed is a traQ JSON embed: !{"type":"user","raw":"@name","id":"<uuid>"}.
type Embed struct {
	Type string `json:"type"` // "user", "group", "channel", "file", "message"
	Raw  string `json:"raw"`
	ID   string `json:"id"`
}

// PlainEmbeds replaces each valid traQ embed in content with its raw text ("@name", "#channel", ...). An
// invalid embed stays as it is. It does not know about code spans: it is for one-line excerpts, not for
// converting messages.
func PlainEmbeds(content string) string {
	var b strings.Builder
	for {
		i := strings.Index(content, "!{")
		if i < 0 {
			break
		}
		e, n, ok := parseEmbed([]byte(content[i:]))
		if !ok {
			b.WriteString(content[:i+2])
			content = content[i+2:]
			continue
		}
		b.WriteString(content[:i])
		b.WriteString(e.Raw)
		content = content[i+n:]
	}
	b.WriteString(content)
	return b.String()
}

// parseEmbed parses an embed at the start of line, which must begin with `!{`.
// It returns the embed and the number of bytes it spans. Embeds are single-line;
// anything that is not a flat string-to-string JSON object of a known type is
// not an embed.
func parseEmbed(line []byte) (Embed, int, bool) {
	if i := bytes.IndexAny(line, "\r\n"); i >= 0 {
		line = line[:i]
	}
	if len(line) < 2 || line[0] != '!' || line[1] != '{' {
		return Embed{}, 0, false
	}
	dec := json.NewDecoder(bytes.NewReader(line[1:]))
	var m map[string]string
	if err := dec.Decode(&m); err != nil {
		return Embed{}, 0, false
	}
	e := Embed{Type: m["type"], Raw: m["raw"], ID: m["id"]}
	switch e.Type {
	case "user", "group", "channel":
	case "file", "message":
		if e.ID == "" {
			return Embed{}, 0, false
		}
	default:
		return Embed{}, 0, false
	}
	return e, 1 + int(dec.InputOffset()), true
}

var kindEmbed = ast.NewNodeKind("TraqEmbed")

// embedNode is an inline node for a traQ embed, or for a bare URL that was
// replaced by a link. Until it is resolved it carries the parsed Embed and its
// source range; the renderer only looks at href and text.
type embedNode struct {
	ast.BaseInline
	embed      Embed
	start, end int // source offsets of the whole `!{...}`
	href, text string
}

func (n *embedNode) Kind() ast.NodeKind { return kindEmbed }

func (n *embedNode) Dump(source []byte, level int) {
	ast.DumpHelper(n, source, level, map[string]string{"Type": n.embed.Type, "Raw": n.embed.Raw}, nil)
}

type embedParser struct{}

func (embedParser) Trigger() []byte { return []byte{'!'} }

func (embedParser) Parse(parent ast.Node, block text.Reader, pc parser.Context) ast.Node {
	line, segment := block.PeekLine()
	e, n, ok := parseEmbed(line)
	if !ok {
		return nil
	}
	block.Advance(n)
	return &embedNode{embed: e, start: segment.Start, end: segment.Start + n}
}

func (embedParser) CloseBlock(parent ast.Node, pc parser.Context) {}
