package connector

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/bridgev2/commands"
	"maunium.net/go/mautrix/format"

	"github.com/kisepichu/traq-matrix-bridge/pkg/traq"
)

// searchLimit is the number of messages the search command lists.
const searchLimit = 20

// registerCommands adds the connector's bot commands. "search" replaces bridgev2's built-in user search,
// which this connector does not implement.
func (c *TraQConnector) registerCommands() {
	c.Bridge.Commands.(*commands.Processor).AddHandler(&commands.FullHandler{
		Func: fnSearch,
		Name: "search",
		Help: commands.HelpMeta{
			Section:     commands.HelpSectionChats,
			Description: "Search traQ messages. Supports in:#channel and from:@user.",
			Args:        "<_query_>",
		},
		RequiresLogin: true,
	})
}

func fnSearch(ce *commands.Event) {
	q := parseSearchQuery(ce.RawArgs)
	if q.Words == "" && q.In == "" && len(q.From) == 0 {
		ce.Reply("Usage: `$cmdprefix search <query>` (supports `in:#channel` and `from:@user`)")
		return
	}
	client := searchClient(ce)
	if client == nil {
		ce.Reply("You're not logged in.")
		return
	}
	text := client.searchMessages(ce.Ctx, q, time.Local)
	ce.ReplyAdvanced(text, true, false)
}

// searchClient finds the login whose traQ account runs the search: the login of the portal the command was
// sent in, else the user's default login. It returns nil if there is none that is logged in.
func searchClient(ce *commands.Event) *TraQClient {
	login := ce.User.GetDefaultLogin()
	if ce.Portal != nil {
		if l, _, err := ce.Portal.FindPreferredLogin(ce.Ctx, ce.User, false); err == nil && l != nil {
			login = l
		}
	}
	if login == nil {
		return nil
	}
	client, ok := login.Client.(*TraQClient)
	if !ok || client.API == nil || !client.IsLoggedIn() {
		return nil
	}
	return client
}

// searchMessages runs the query and returns the markdown reply: the hits, newest first, or why there are none.
func (c *TraQClient) searchMessages(ctx context.Context, q parsedQuery, loc *time.Location) string {
	sq := traq.SearchQuery{Word: q.Words, Limit: searchLimit}
	c.stateMu.RLock()
	tree := c.channels
	c.stateMu.RUnlock()
	if tree == nil {
		return "The bridge is still connecting to traQ. Try again in a moment."
	}
	if q.In != "" {
		ch, ok := tree.ByPath(q.In)
		if !ok {
			// The tree does not learn of new public channels before the next subscription sync.
			tree = c.freshTree(ctx, tree)
			ch, ok = tree.ByPath(q.In)
		}
		if !ok {
			return "Channel #" + format.EscapeMarkdown(q.In) + " not found."
		}
		sq.In = ch.ID
	}
	for _, name := range q.From {
		user, ok := c.users.ByName(name)
		if !ok {
			return "User @" + format.EscapeMarkdown(name) + " not found."
		}
		sq.From = append(sq.From, user.ID)
	}

	res, err := c.API.Search(ctx, sq)
	if traq.IsStatus(err, http.StatusServiceUnavailable) {
		return "This traQ server has search disabled."
	}
	if err != nil {
		zerolog.Ctx(ctx).Err(err).Msg("Failed to search traQ")
		return "Failed to search traQ: " + format.EscapeMarkdown(err.Error())
	}
	if len(res.Hits) == 0 {
		return "No messages found."
	}
	for _, hit := range res.Hits {
		if _, ok := tree.Get(hit.ChannelID); !ok {
			tree = c.freshTree(ctx, tree)
			break
		}
	}
	lines := make([]string, len(res.Hits))
	for i, hit := range res.Hits {
		lines[i] = formatSearchHit(hit, hitChannelPath(tree, hit), c.hitUserName(ctx, hit), c.hitLink(ctx, tree, hit), loc)
	}
	return strings.Join(lines, "\n")
}

// freshTree reads the channel list from traQ and returns a tree of it for this command only, or old if the read
// fails. Channels found by a search may be newer than the stored tree.
func (c *TraQClient) freshTree(ctx context.Context, old *channelTree) *channelTree {
	list, err := c.API.Channels(ctx)
	if err != nil {
		zerolog.Ctx(ctx).Warn().Err(err).Msg("Failed to read the channel list for a search")
		return old
	}
	return buildChannelTree(list)
}

// hitChannelPath is the path of the hit's channel; "" for a DM. A channel the tree does not know shows as its ID.
func hitChannelPath(tree *channelTree, hit traq.Message) string {
	ch, ok := tree.Get(hit.ChannelID)
	if !ok {
		return hit.ChannelID
	}
	return ch.Path
}

// hitUserName is the name of the hit's author; the ID if the user cannot be fetched.
func (c *TraQClient) hitUserName(ctx context.Context, hit traq.Message) string {
	user, err := c.users.Get(ctx, c.API, hit.UserID)
	if err != nil {
		zerolog.Ctx(ctx).Warn().Err(err).Str("traq_user_id", hit.UserID).Msg("Failed to get the author of a search hit")
		return hit.UserID
	}
	return user.Name
}

// hitLink is the matrix.to link of the Matrix event the hit is bridged to, else the traQ link of the message.
func (c *TraQClient) hitLink(ctx context.Context, tree *channelTree, hit traq.Message) string {
	receiver := c.Login.ID
	if ch, ok := tree.Get(hit.ChannelID); ok && !ch.isDM() {
		receiver = ""
	}
	br := c.Login.Bridge
	row, err := br.DB.Message.GetFirstPartByID(ctx, receiver, MessageID(hit.ID))
	if err != nil {
		zerolog.Ctx(ctx).Warn().Err(err).Str("traq_message_id", hit.ID).Msg("Failed to look up a bridged message")
	} else if row != nil {
		portal, err := br.GetExistingPortalByKey(ctx, row.Room)
		if err == nil && portal != nil && portal.MXID != "" {
			return portal.MXID.EventURI(row.MXID, br.Matrix.ServerName()).MatrixToURL()
		}
	}
	return c.origin() + "/messages/" + hit.ID
}
