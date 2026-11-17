package msgconv

import (
	"net/url"
	"reflect"
	"strings"
	"testing"

	"maunium.net/go/mautrix/id"
)

type fakeResolver struct {
	users    map[string]id.UserID
	channels map[string]string
	links    map[string]string
	replies  map[string]bool
}

func (f fakeResolver) UserMXID(u string) (id.UserID, bool) { v, ok := f.users[u]; return v, ok }
func (f fakeResolver) ChannelLink(c string) (string, bool) { v, ok := f.channels[c]; return v, ok }
func (f fakeResolver) MessageLink(m string) (string, bool) { v, ok := f.links[m]; return v, ok }
func (f fakeResolver) CanReplyTo(m string) bool            { return f.replies[m] }

const (
	uAlice = "11111111-1111-1111-1111-111111111111"
	uSelf  = "22222222-2222-2222-2222-222222222222"
	uBob   = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
	gSelf  = "cccccccc-cccc-cccc-cccc-cccccccccccc"
	gOther = "44444444-4444-4444-4444-444444444444"
	cGen   = "55555555-5555-5555-5555-555555555555"
	cNone  = "66666666-6666-6666-6666-666666666666"
	m1     = "77777777-7777-7777-7777-777777777777"
	m2     = "88888888-8888-8888-8888-888888888888"
	f1     = "99999999-9999-9999-9999-999999999999"
)

const fileURL = "https://q.example.jp/files/" + f1

func input(content string) ToMatrixInput {
	return ToMatrixInput{
		Content:    content,
		ServerURL:  &url.URL{Scheme: "https", Host: "q.example.jp"},
		SelfGroups: []string{gSelf},
		SelfMXID:   "@me:example.org",
	}
}

var resolver = fakeResolver{
	users:    map[string]id.UserID{uAlice: "@traq_alice:example.org", uSelf: "@me:example.org", uBob: "@traq_bob:example.org"},
	channels: map[string]string{cGen: "https://matrix.to/#/!gen:example.org"},
	links:    map[string]string{m1: "https://matrix.to/#/!gen:example.org/$ev1", m2: "https://matrix.to/#/!gen:example.org/$ev2"},
	replies:  map[string]bool{m1: true},
}

func TestToMatrix(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want ToMatrixResult
	}{
		{"plain", "hello", ToMatrixResult{Body: "hello", HTML: "hello"}},
		{"hard wrap", "a\nb", ToMatrixResult{Body: "a\nb", HTML: "a<br>b"}},
		{"bold and raw html escaped", "**x** <b>y</b>", ToMatrixResult{Body: "**x** <b>y</b>", HTML: "<strong>x</strong> &lt;b&gt;y&lt;/b&gt;"}},
		{"user mention", `!{"type":"user","raw":"@alice","id":"` + uAlice + `"} hi`,
			ToMatrixResult{Body: "@alice hi", HTML: `<a href="https://matrix.to/#/@traq_alice:example.org">@alice</a> hi`, Mentions: []id.UserID{"@traq_alice:example.org"}}},
		{"self mention", `!{"type":"user","raw":"@me","id":"` + uSelf + `"}`,
			ToMatrixResult{Body: "@me", HTML: `<a href="https://matrix.to/#/@me:example.org">@me</a>`, Mentions: []id.UserID{"@me:example.org"}}},
		{"own group mentions self", `!{"type":"group","raw":"@all","id":"` + gSelf + `"}`,
			ToMatrixResult{Body: "@all", HTML: "@all", Mentions: []id.UserID{"@me:example.org"}}},
		{"other group", `!{"type":"group","raw":"@x","id":"` + gOther + `"}`, ToMatrixResult{Body: "@x", HTML: "@x"}},
		{"bridged channel", `!{"type":"channel","raw":"#gps/general","id":"` + cGen + `"}`,
			ToMatrixResult{Body: "#gps/general", HTML: `<a href="https://matrix.to/#/!gen:example.org">#gps/general</a>`}},
		{"unbridged channel", `!{"type":"channel","raw":"#a/b","id":"` + cNone + `"}`,
			ToMatrixResult{Body: "#a/b", HTML: `<a href="https://q.example.jp/channels/a/b">#a/b</a>`}},
		{"embed in code span stays", "`!{\"type\":\"user\",\"raw\":\"@alice\",\"id\":\"" + uAlice + "\"}`",
			ToMatrixResult{Body: "`!{\"type\":\"user\",\"raw\":\"@alice\",\"id\":\"" + uAlice + "\"}`", HTML: "<code>!{&quot;type&quot;:&quot;user&quot;,&quot;raw&quot;:&quot;@alice&quot;,&quot;id&quot;:&quot;" + uAlice + "&quot;}</code>"}},
		{"broken embed stays", `!{"type":"user",`, ToMatrixResult{Body: `!{"type":"user",`, HTML: `!{&quot;type&quot;:&quot;user&quot;,`}},
		{"spoiler", "a !!secret!! b", ToMatrixResult{Body: "a !!secret!! b", HTML: "a <span data-mx-spoiler>secret</span> b"}},
		{"file url removed", "look\nhttps://q.example.jp/files/" + f1,
			ToMatrixResult{Body: "look", HTML: "look", FileIDs: []string{f1}}},
		{"file embed removed and deduplicated", `!{"type":"file","raw":"x","id":"` + f1 + `"} https://q.example.jp/files/` + f1,
			ToMatrixResult{FileIDs: []string{f1}}},
		{"first reply candidate becomes reply", "https://q.example.jp/messages/" + m1 + "\nyes",
			ToMatrixResult{Body: "yes", HTML: "yes", ReplyTo: m1}},
		{"a reply quote alone stays visible as a link", "https://q.example.jp/messages/" + m1,
			ToMatrixResult{Body: "https://q.example.jp/messages/" + m1, HTML: `<a href="https://matrix.to/#/!gen:example.org/$ev1">https://q.example.jp/messages/` + m1 + `</a>`, ReplyTo: m1}},
		{"a reply quote alone in a quote block stays visible", "> https://q.example.jp/messages/" + m1,
			ToMatrixResult{Body: "https://q.example.jp/messages/" + m1, HTML: `<a href="https://matrix.to/#/!gen:example.org/$ev1">https://q.example.jp/messages/` + m1 + `</a>`, ReplyTo: m1}},
		{"a reply quote with only a file is shown by the file", "https://q.example.jp/messages/" + m1 + "\n" + fileURL,
			ToMatrixResult{ReplyTo: m1, FileIDs: []string{f1}}},
		{"bridged message elsewhere becomes link", "see https://q.example.jp/messages/" + m2,
			ToMatrixResult{Body: "see https://q.example.jp/messages/" + m2, HTML: `see <a href="https://matrix.to/#/!gen:example.org/$ev2">https://q.example.jp/messages/` + m2 + `</a>`}},
		{"explicit link untouched", "[x](https://q.example.jp/files/" + f1 + ")",
			ToMatrixResult{Body: "[x](https://q.example.jp/files/" + f1 + ")", HTML: `<a href="https://q.example.jp/files/` + f1 + `">x</a>`}},
		{"other origin untouched", "https://other.example/files/" + f1,
			ToMatrixResult{Body: "https://other.example/files/" + f1, HTML: `<a href="https://other.example/files/` + f1 + `">https://other.example/files/` + f1 + `</a>`}},
		{"upper-case file url is lower-cased", "https://q.example.jp/files/AAAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA x",
			ToMatrixResult{Body: "x", HTML: "x", FileIDs: []string{"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"}}},
		{"two paragraphs", "a\n\nb", ToMatrixResult{Body: "a\n\nb", HTML: "<p>a</p>\n<p>b</p>"}},
		{"list", "- x\n- y", ToMatrixResult{Body: "- x\n- y", HTML: "<ul>\n<li>x</li>\n<li>y</li>\n</ul>"}},
		{"code block keeps spoiler marks", "```\ncode !!x!!\n```", ToMatrixResult{Body: "```\ncode !!x!!\n```", HTML: "<pre><code>code !!x!!\n</code></pre>"}},
		{"table", "|a|b|\n|-|-|\n|1|2|", ToMatrixResult{Body: "|a|b|\n|-|-|\n|1|2|",
			HTML: "<table>\n<thead>\n<tr>\n<th>a</th>\n<th>b</th>\n</tr>\n</thead>\n<tbody>\n<tr>\n<td>1</td>\n<td>2</td>\n</tr>\n</tbody>\n</table>"}},
		{"table cell holding only a file url stays", "|a|b|\n|-|-|\n|1|" + fileURL + "|", ToMatrixResult{Body: "|a|b|\n|-|-|\n|1||", FileIDs: []string{f1},
			HTML: "<table>\n<thead>\n<tr>\n<th>a</th>\n<th>b</th>\n</tr>\n</thead>\n<tbody>\n<tr>\n<td>1</td>\n<td></td>\n</tr>\n</tbody>\n</table>"}},
		{"raw html block escaped", "<div>\nhi\n</div>\n\nx", ToMatrixResult{Body: "<div>\nhi\n</div>\n\nx", HTML: "<p>&lt;div&gt;<br>hi<br>&lt;/div&gt;</p>\n<p>x</p>"}},
		{"file url alone on a middle line", "a\n" + fileURL + "\nb", ToMatrixResult{Body: "a\nb", HTML: "a<br>b", FileIDs: []string{f1}}},
		{"file url alone on a middle line, CRLF", "a\r\n" + fileURL + "\r\nb", ToMatrixResult{Body: "a\nb", HTML: "a<br>b", FileIDs: []string{f1}}},
		{"file url alone on the first line", fileURL + "\nb", ToMatrixResult{Body: "b", HTML: "b", FileIDs: []string{f1}}},
		{"file url inline leaves one space", "a " + fileURL + " b", ToMatrixResult{Body: "a b", HTML: "a b", FileIDs: []string{f1}}},
		{"file url between paragraphs leaves one blank line", "a\n\n" + fileURL + "\n\nb", ToMatrixResult{Body: "a\n\nb", HTML: "<p>a</p>\n<p>b</p>", FileIDs: []string{f1}}},
		{"only a bold file url", "**" + fileURL + "**", ToMatrixResult{FileIDs: []string{f1}}},
		{"only a quoted file url", "> " + fileURL, ToMatrixResult{FileIDs: []string{f1}}},
		{"only a listed file url", "- " + fileURL, ToMatrixResult{FileIDs: []string{f1}}},
		{"list keeps the text item", "- " + fileURL + "\n- x", ToMatrixResult{Body: "-\n- x", HTML: "<ul>\n<li>x</li>\n</ul>", FileIDs: []string{f1}}},
		{"spoiler around an embed", `!!!{"type":"user","raw":"@alice","id":"` + uAlice + `"}!!`,
			ToMatrixResult{Body: "!!@alice!!", HTML: `<span data-mx-spoiler><a href="https://matrix.to/#/@traq_alice:example.org">@alice</a></span>`, Mentions: []id.UserID{"@traq_alice:example.org"}}},
		{"upper-case user id", `!{"type":"user","raw":"@bob","id":"` + strings.ToUpper(uBob) + `"}`,
			ToMatrixResult{Body: "@bob", HTML: `<a href="https://matrix.to/#/@traq_bob:example.org">@bob</a>`, Mentions: []id.UserID{"@traq_bob:example.org"}}},
		{"upper-case group id", `!{"type":"group","raw":"@all","id":"` + strings.ToUpper(gSelf) + `"}`,
			ToMatrixResult{Body: "@all", HTML: "@all", Mentions: []id.UserID{"@me:example.org"}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := ToMatrix(input(c.in), resolver)
			if !reflect.DeepEqual(got, c.want) {
				t.Errorf("ToMatrix(%q)\n got  %#v\n want %#v", c.in, got, c.want)
			}
		})
	}
}

// Without a Matrix event to link to, a reply quote left alone stays visible as its traQ URL.
func TestToMatrixReplyQuoteAloneWithoutLink(t *testing.T) {
	quote := "https://q.example.jp/messages/" + m1
	got := ToMatrix(input(quote), fakeResolver{replies: map[string]bool{m1: true}})
	want := ToMatrixResult{Body: quote, HTML: `<a href="` + quote + `">` + quote + `</a>`, ReplyTo: m1}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("ToMatrix(%q)\n got  %#v\n want %#v", quote, got, want)
	}
}

// On a traQ whose host has no dotted TLD (the dev environment's http://localhost:3080), bare file and message
// URLs of that origin are found like on any other host. Other dotless URLs are left as text, as before.
func TestToMatrixDotlessOrigin(t *testing.T) {
	const origin = "http://localhost:3080"
	in := func(content string) ToMatrixInput {
		i := input(content)
		i.ServerURL = &url.URL{Scheme: "http", Host: "localhost:3080"}
		return i
	}
	cases := []struct {
		name string
		in   string
		want ToMatrixResult
	}{
		{"file url", "a\n" + origin + "/files/" + f1, ToMatrixResult{Body: "a", HTML: "a", FileIDs: []string{f1}}},
		{"reply quote", origin + "/messages/" + m1 + "\nyes", ToMatrixResult{Body: "yes", HTML: "yes", ReplyTo: m1}},
		{"other quote", "see " + origin + "/messages/" + m2, ToMatrixResult{Body: "see " + origin + "/messages/" + m2,
			HTML: `see <a href="https://matrix.to/#/!gen:example.org/$ev2">` + origin + `/messages/` + m2 + `</a>`}},
		// As with a dotted host, whose upper-case TLD the linkify pattern does not take either.
		{"upper-case host", "http://LOCALHOST:3080/files/" + f1 + " x", ToMatrixResult{Body: "http://LOCALHOST:3080/files/" + f1 + " x", HTML: "http://LOCALHOST:3080/files/" + f1 + " x"}},
		{"longer path stays a link", origin + "/files/" + f1 + "/x", ToMatrixResult{Body: origin + "/files/" + f1 + "/x",
			HTML: `<a href="` + origin + `/files/` + f1 + `/x">` + origin + `/files/` + f1 + `/x</a>`}},
		{"longer port is another origin", "http://localhost:30801/files/" + f1, ToMatrixResult{Body: "http://localhost:30801/files/" + f1, HTML: "http://localhost:30801/files/" + f1}},
		{"other dotless host", "http://other:3080/files/" + f1, ToMatrixResult{Body: "http://other:3080/files/" + f1, HTML: "http://other:3080/files/" + f1}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := ToMatrix(in(c.in), resolver)
			if !reflect.DeepEqual(got, c.want) {
				t.Errorf("ToMatrix(%q)\n got  %#v\n want %#v", c.in, got, c.want)
			}
		})
	}
}
