// Package msgconv converts message content between traQ and Matrix.
package msgconv

import (
	"bytes"
	"net/url"
	"regexp"
	"slices"
	"strings"

	"github.com/yuin/goldmark/ast"
	"github.com/yuin/goldmark/parser"
	"github.com/yuin/goldmark/text"
	"maunium.net/go/mautrix/id"
)

// Resolver answers lookups that need bridge state. Implementations must not block on the network.
type Resolver interface {
	UserMXID(traqUserID string) (id.UserID, bool) // ghost MXID, or the logged-in user's own MXID for themselves
	ChannelLink(channelID string) (string, bool)  // matrix.to URL of the channel's portal room, if it exists
	MessageLink(messageID string) (string, bool)  // matrix.to URL of the bridged event, if bridged
	CanReplyTo(messageID string) bool             // bridged in the same portal as the message being converted
}

type ToMatrixInput struct {
	Content    string
	ServerURL  *url.URL // traQ origin
	SelfGroups []string // group IDs of the logged-in user
	SelfMXID   id.UserID
}

type ToMatrixResult struct {
	Body     string      // plain text
	HTML     string      // formatted_body (org.matrix.custom.html)
	Mentions []id.UserID // for m.mentions (deduplicated, in order of appearance)
	ReplyTo  string      // traQ message ID to reply to, "" if none
	FileIDs  []string    // traQ file IDs to send as separate parts, in order, deduplicated
}

// traQ file and message URLs: /files/<uuid>, /messages/<uuid>. Unlike traQ
// itself this accepts upper-case hex too.
var traqPathRe = regexp.MustCompile(`^/(files|messages)/([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})$`)

// edit replaces src[start:end] in the plain-text body.
type edit struct {
	start, end int
	text       string
}

type converter struct {
	in    ToMatrixInput
	r     Resolver
	src   []byte
	spans map[*ast.AutoLink]span
	res   ToMatrixResult
	edits []edit
}

// ToMatrix converts traQ message content to Matrix body and formatted_body.
func ToMatrix(in ToMatrixInput, r Resolver) ToMatrixResult {
	// CRLF is normalised so that Body and the parsed text agree on line ends.
	c := &converter{in: in, r: r, src: []byte(strings.ReplaceAll(in.Content, "\r\n", "\n"))}
	pc := parser.NewContext()
	doc := markdown.Parser().Parse(text.NewReader(c.src), parser.WithContext(pc))
	c.spans, _ = pc.Get(bareLinkKey).(map[*ast.AutoLink]span)

	var nodes []ast.Node
	_ = ast.Walk(doc, func(n ast.Node, entering bool) (ast.WalkStatus, error) {
		if entering {
			switch n.Kind() {
			case kindEmbed, ast.KindAutoLink, ast.KindRawHTML, ast.KindHTMLBlock:
				nodes = append(nodes, n)
			}
		}
		return ast.WalkContinue, nil
	})
	var emptied []ast.Node // parents that lost a child
	for _, n := range nodes {
		parent := n.Parent()
		if c.convert(n) {
			c.remove(n)
			if !slices.Contains(emptied, parent) {
				emptied = append(emptied, parent)
			}
		}
	}
	for _, p := range emptied {
		c.tidy(p)
	}

	c.res.Body = c.body()
	if c.res.Body == "" {
		return c.res
	}
	c.res.HTML = c.render(doc)
	if len(emptied) > 0 && !visible(c.res.HTML) {
		// Removals left nothing to show; Body would only hold stray Markdown marks.
		c.res.Body, c.res.HTML = "", ""
	}
	return c.res
}

// convert resolves one node, recording its effect on the result. It returns
// true if the node must be removed from the tree.
func (c *converter) convert(n ast.Node) bool {
	switch n := n.(type) {
	case *embedNode:
		return c.embed(n)
	case *ast.AutoLink:
		return c.bareURL(n)
	case *ast.RawHTML:
		c.rawHTMLToText(n)
	case *ast.HTMLBlock:
		c.htmlBlockToText(n)
	}
	return false
}

func (c *converter) embed(n *embedNode) bool {
	e := n.embed
	n.text = e.Raw
	remove := false
	switch e.Type {
	case "user":
		if mxid, ok := c.r.UserMXID(strings.ToLower(e.ID)); ok {
			n.href = "https://matrix.to/#/" + string(mxid)
			c.mention(mxid)
		}
	case "group":
		if slices.ContainsFunc(c.in.SelfGroups, func(g string) bool { return strings.EqualFold(g, e.ID) }) {
			c.mention(c.in.SelfMXID)
		}
	case "channel":
		if link, ok := c.r.ChannelLink(e.ID); ok {
			n.href = link
		} else {
			n.href = c.origin() + "/channels/" + strings.TrimPrefix(strings.TrimPrefix(e.Raw, "#"), "＃")
		}
	case "file":
		c.file(strings.ToLower(e.ID))
		remove = true
	case "message":
		messageID := strings.ToLower(e.ID)
		if c.reply(messageID) {
			remove = true
		} else if link, ok := c.r.MessageLink(messageID); ok {
			n.href = link
		}
	}
	replacement := e.Raw
	if remove {
		replacement = ""
	}
	c.edits = append(c.edits, edit{n.start, n.end, replacement})
	return remove
}

func (c *converter) bareURL(n *ast.AutoLink) bool {
	sp, ok := c.spans[n]
	if !ok {
		return false
	}
	label := string(n.Label(c.src))
	kind, uuid, ok := c.traqURL(string(n.URL(c.src)))
	if !ok {
		return false
	}
	if kind == "files" {
		c.file(uuid)
	} else if !c.reply(uuid) {
		if link, ok := c.r.MessageLink(uuid); ok {
			parent := n.Parent()
			parent.ReplaceChild(parent, n, &embedNode{href: link, text: label})
		}
		return false
	}
	c.edits = append(c.edits, edit{sp.start, sp.end, ""})
	return true
}

// traqURL reports whether raw is exactly <origin>/files/<uuid> or
// <origin>/messages/<uuid>, and returns the kind and the lower-cased UUID.
func (c *converter) traqURL(raw string) (kind, uuid string, ok bool) {
	srv := c.in.ServerURL
	if srv == nil {
		return "", "", false
	}
	u, err := url.Parse(raw)
	if err != nil || u.User != nil || u.RawQuery != "" || u.Fragment != "" ||
		!strings.EqualFold(u.Scheme, srv.Scheme) || !strings.EqualFold(u.Host, srv.Host) {
		return "", "", false
	}
	m := traqPathRe.FindStringSubmatch(u.Path)
	if m == nil {
		return "", "", false
	}
	return m[1], strings.ToLower(m[2]), true
}

func (c *converter) origin() string {
	if c.in.ServerURL == nil {
		return ""
	}
	return c.in.ServerURL.Scheme + "://" + c.in.ServerURL.Host
}

func (c *converter) mention(mxid id.UserID) {
	if mxid != "" && !slices.Contains(c.res.Mentions, mxid) {
		c.res.Mentions = append(c.res.Mentions, mxid)
	}
}

func (c *converter) file(fileID string) {
	if !slices.Contains(c.res.FileIDs, fileID) {
		c.res.FileIDs = append(c.res.FileIDs, fileID)
	}
}

// reply makes messageID the reply target if none is chosen yet and it can be
// replied to.
func (c *converter) reply(messageID string) bool {
	if c.res.ReplyTo == "" && c.r.CanReplyTo(messageID) {
		c.res.ReplyTo = messageID
		return true
	}
	return false
}

// rawHTMLToText turns inline raw HTML into literal text; goldmark would drop it.
func (c *converter) rawHTMLToText(n *ast.RawHTML) {
	parent := n.Parent()
	prev := ast.Node(n)
	for i := range n.Segments.Len() {
		t := ast.NewTextSegment(n.Segments.At(i))
		t.SetRaw(true)
		parent.InsertAfter(parent, prev, t)
		prev = t
	}
	parent.RemoveChild(parent, n)
}

// htmlBlockToText turns a raw HTML block into a paragraph of literal lines.
func (c *converter) htmlBlockToText(n *ast.HTMLBlock) {
	p := ast.NewParagraph()
	lines := n.Lines()
	segs := make([]text.Segment, 0, lines.Len()+1)
	for i := 0; i < lines.Len(); i++ {
		segs = append(segs, lines.At(i))
	}
	if n.HasClosure() {
		segs = append(segs, n.ClosureLine)
	}
	for i, seg := range segs {
		t := ast.NewTextSegment(seg.TrimRightSpace(c.src))
		t.SetRaw(true)
		p.AppendChild(p, t)
		if i < len(segs)-1 {
			// The renderer ignores line breaks on raw text, so the break rides on an empty text node.
			br := ast.NewTextSegment(text.NewSegment(0, 0))
			br.SetSoftLineBreak(true)
			p.AppendChild(p, br)
		}
	}
	parent := n.Parent()
	parent.ReplaceChild(parent, n, p)
}

// remove cuts n out of its parent and tidies the line it leaves: whitespace
// around the gap collapses, and a line that ends up empty goes entirely. That
// is what body() does to the source.
func (c *converter) remove(n ast.Node) {
	parent := n.Parent()
	gap := ast.NewTextSegment(text.NewSegment(0, 0))
	parent.ReplaceChild(parent, n, gap)

	prev, next := gap.PreviousSibling(), gap.NextSibling()
	atStart := prev == nil || endsLine(prev)
	atEnd := next == nil || (endsLine(next) && c.blank(next))
	if t, ok := prev.(*ast.Text); ok && atEnd && !atStart {
		t.Segment = t.Segment.TrimRightSpace(c.src)
		if t.Segment.Len() == 0 && !endsLine(t) {
			parent.RemoveChild(parent, t)
		}
	}
	if t, ok := next.(*ast.Text); ok && !atEnd && (atStart || c.endsInSpace(prev)) {
		t.Segment = t.Segment.TrimLeftSpace(c.src)
		if t.Segment.Len() == 0 && !endsLine(t) {
			parent.RemoveChild(parent, t)
		}
	}

	// The line around the gap, up to and including its line break.
	line := []ast.Node{gap}
	empty := true
	for x := gap.PreviousSibling(); x != nil && !endsLine(x); x = x.PreviousSibling() {
		line = append(line, x)
	}
	for x := ast.Node(gap); x != nil; x = x.NextSibling() {
		if x != gap {
			line = append(line, x)
		}
		if endsLine(x) {
			break
		}
	}
	for _, x := range line {
		if !c.blank(x) {
			empty = false
			break
		}
	}
	if !empty {
		parent.RemoveChild(parent, gap)
		return
	}
	for _, x := range line {
		parent.RemoveChild(parent, x)
	}
}

// endsLine reports whether n is a text that ends a source line.
func endsLine(n ast.Node) bool {
	t, ok := n.(*ast.Text)
	return ok && (t.SoftLineBreak() || t.HardLineBreak())
}

// blank reports whether n is a text with nothing but whitespace in it.
func (c *converter) blank(n ast.Node) bool {
	t, ok := n.(*ast.Text)
	return ok && len(bytes.TrimSpace(t.Segment.Value(c.src))) == 0
}

func (c *converter) endsInSpace(n ast.Node) bool {
	t, ok := n.(*ast.Text)
	return ok && t.Segment.Len() > 0 && isSpace(c.src[t.Segment.Stop-1])
}

// tidy cleans up n after children were removed: edge whitespace and line
// breaks go, and nodes left empty are removed too.
func (c *converter) tidy(n ast.Node) {
	for n != nil && n.Parent() != nil {
		c.trimEdges(n)
		if n.HasChildren() {
			return
		}
		parent := n.Parent()
		parent.RemoveChild(parent, n)
		// An emptied block takes its container with it, unless that is something
		// whose shape matters, such as a table row.
		if n.Type() == ast.TypeBlock {
			switch parent.Kind() {
			case ast.KindBlockquote, ast.KindList, ast.KindListItem:
			default:
				return
			}
		}
		n = parent
	}
}

func (c *converter) trimEdges(n ast.Node) {
	for first := n.FirstChild(); first != nil; first = n.FirstChild() {
		t, ok := first.(*ast.Text)
		if !ok {
			break
		}
		t.Segment = t.Segment.TrimLeftSpace(c.src)
		if t.Segment.Len() > 0 {
			break
		}
		n.RemoveChild(n, t)
	}
	for last := n.LastChild(); last != nil; last = n.LastChild() {
		t, ok := last.(*ast.Text)
		if !ok {
			break
		}
		t.Segment = t.Segment.TrimRightSpace(c.src)
		t.SetSoftLineBreak(false)
		t.SetHardLineBreak(false)
		if t.Segment.Len() > 0 {
			break
		}
		n.RemoveChild(n, t)
	}
}

// body applies the recorded edits to the source. Lines that an edit emptied
// are dropped, and so are the blank lines that would then sit next to each
// other; blank lines of the source stay otherwise.
func (c *converter) body() string {
	src := string(c.src)
	var out []string
	pos, next := 0, 0
	dropped := false // a line was dropped since the last kept line
	for _, line := range strings.Split(src, "\n") {
		end := pos + len(line)
		var b strings.Builder
		cur := pos
		touched := false
		for ; next < len(c.edits) && c.edits[next].end <= end; next++ {
			e := c.edits[next]
			b.WriteString(src[cur:e.start])
			b.WriteString(e.text)
			cur = e.end
			if e.text == "" && (e.start == pos || isSpace(src[e.start-1])) {
				// The removal sat after whitespace: the whitespace behind it goes too.
				for cur < end && isSpace(src[cur]) {
					cur++
				}
			}
			touched = true
		}
		b.WriteString(src[cur:end])
		pos = end + 1
		s := b.String()
		if touched {
			s = strings.TrimRight(s, " \t")
			if strings.TrimSpace(s) == "" {
				dropped = true
				continue
			}
		}
		if dropped && s == "" && len(out) > 0 && out[len(out)-1] == "" {
			continue
		}
		dropped = false
		out = append(out, s)
	}
	return strings.TrimSpace(strings.Join(out, "\n"))
}

func isSpace(b byte) bool { return b == ' ' || b == '\t' }

var tagRe = regexp.MustCompile(`<[^>]*>`)

// visible reports whether rendered HTML shows anything: text, or an image,
// rule or checkbox.
func visible(html string) bool {
	for _, tag := range []string{"<img", "<hr", "<input"} {
		if strings.Contains(html, tag) {
			return true
		}
	}
	return strings.TrimSpace(tagRe.ReplaceAllString(html, "")) != ""
}

func (c *converter) render(doc ast.Node) string {
	// A lone paragraph is not wrapped in <p>; a text block renders its children bare.
	if first := doc.FirstChild(); first != nil && first == doc.LastChild() && first.Kind() == ast.KindParagraph {
		tb := ast.NewTextBlock()
		for ch := first.FirstChild(); ch != nil; ch = first.FirstChild() {
			first.RemoveChild(first, ch)
			tb.AppendChild(tb, ch)
		}
		doc.ReplaceChild(doc, first, tb)
	}
	var buf bytes.Buffer
	if err := markdown.Renderer().Render(&buf, c.src, doc); err != nil {
		// Rendering into a bytes.Buffer cannot fail.
		panic(err)
	}
	// Literal "<br>\n" cannot occur in text (it would be escaped), so this only touches line breaks.
	return strings.TrimSpace(strings.ReplaceAll(buf.String(), "<br>\n", "<br>"))
}
