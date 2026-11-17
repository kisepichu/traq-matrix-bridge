package msgconv

import (
	"github.com/yuin/goldmark"
	"github.com/yuin/goldmark/ast"
	"github.com/yuin/goldmark/extension"
	"github.com/yuin/goldmark/parser"
	"github.com/yuin/goldmark/renderer"
	"github.com/yuin/goldmark/renderer/html"
	"github.com/yuin/goldmark/text"
	"github.com/yuin/goldmark/util"
)

// markdown is stateless and shared. It is GFM (table, strikethrough, task list,
// linkify) with hard wraps plus the traQ embed and spoiler syntax. Raw HTML is
// not handled here; ToMatrix turns it into text before rendering.
var markdown = goldmark.New(
	goldmark.WithExtensions(extension.Table, extension.Strikethrough, extension.TaskList, traqExtension{}),
	goldmark.WithRendererOptions(html.WithHardWraps()),
)

type traqExtension struct{}

func (traqExtension) Extend(m goldmark.Markdown) {
	m.Parser().AddOptions(parser.WithInlineParsers(
		util.Prioritized(embedParser{}, 100),
		util.Prioritized(spoilerParser{}, 500),
		// Same priority as extension.Linkify, which this replaces.
		util.Prioritized(bareLinkParser{extension.NewLinkifyParser()}, 999),
	))
	m.Renderer().AddOptions(renderer.WithNodeRenderers(
		util.Prioritized(nodeRenderer{}, 500),
	))
}

// --- spoiler: !!text!! ---

var kindSpoiler = ast.NewNodeKind("TraqSpoiler")

type spoilerNode struct{ ast.BaseInline }

func (n *spoilerNode) Kind() ast.NodeKind { return kindSpoiler }

func (n *spoilerNode) Dump(source []byte, level int) { ast.DumpHelper(n, source, level, nil, nil) }

type spoilerDelimiters struct{}

func (spoilerDelimiters) IsDelimiter(b byte) bool { return b == '!' }

func (spoilerDelimiters) CanOpenCloser(opener, closer *parser.Delimiter) bool {
	return opener.Char == closer.Char
}

func (spoilerDelimiters) OnMatch(consumes int) ast.Node { return &spoilerNode{} }

type spoilerParser struct{}

func (spoilerParser) Trigger() []byte { return []byte{'!'} }

func (spoilerParser) Parse(parent ast.Node, block text.Reader, pc parser.Context) ast.Node {
	before := block.PrecendingCharacter()
	line, segment := block.PeekLine()
	node := parser.ScanDelimiter(line, before, 2, spoilerDelimiters{})
	if node == nil || node.OriginalLength > 2 || before == '!' {
		return nil
	}
	node.Segment = segment.WithStop(segment.Start + node.OriginalLength)
	block.Advance(node.OriginalLength)
	pc.PushDelimiter(node)
	return node
}

func (spoilerParser) CloseBlock(parent ast.Node, pc parser.Context) {}

// --- bare URLs ---

// span is a half-open source range.
type span struct{ start, end int }

var bareLinkKey = parser.NewContextKey()

// bareLinkParser wraps goldmark's linkify parser to record where each bare URL
// sits in the source, which ast.AutoLink does not expose. Angle-bracket
// autolinks (<https://...>) never pass through linkify and so are not recorded.
type bareLinkParser struct{ parser.InlineParser }

func (p bareLinkParser) Parse(parent ast.Node, block text.Reader, pc parser.Context) ast.Node {
	n := p.InlineParser.Parse(parent, block, pc)
	link, ok := n.(*ast.AutoLink)
	if !ok {
		return n
	}
	spans, _ := pc.Get(bareLinkKey).(map[*ast.AutoLink]span)
	if spans == nil {
		spans = map[*ast.AutoLink]span{}
		pc.Set(bareLinkKey, spans)
	}
	// Linkify has advanced the reader to the end of the URL.
	_, pos := block.Position()
	spans[link] = span{pos.Start - len(link.Label(block.Source())), pos.Start}
	return n
}

// --- rendering ---

type nodeRenderer struct{}

func (nodeRenderer) RegisterFuncs(reg renderer.NodeRendererFuncRegisterer) {
	reg.Register(kindEmbed, renderEmbed)
	reg.Register(kindSpoiler, renderSpoiler)
}

func renderEmbed(w util.BufWriter, source []byte, n ast.Node, entering bool) (ast.WalkStatus, error) {
	if !entering {
		return ast.WalkContinue, nil
	}
	e := n.(*embedNode)
	if e.href != "" {
		_, _ = w.WriteString(`<a href="`)
		_, _ = w.Write(util.EscapeHTML([]byte(e.href)))
		_, _ = w.WriteString(`">`)
	}
	_, _ = w.Write(util.EscapeHTML([]byte(e.text)))
	if e.href != "" {
		_, _ = w.WriteString("</a>")
	}
	return ast.WalkSkipChildren, nil
}

func renderSpoiler(w util.BufWriter, source []byte, n ast.Node, entering bool) (ast.WalkStatus, error) {
	if entering {
		_, _ = w.WriteString("<span data-mx-spoiler>")
	} else {
		_, _ = w.WriteString("</span>")
	}
	return ast.WalkContinue, nil
}
