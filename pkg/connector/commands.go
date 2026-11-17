package connector

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/commands"
	"maunium.net/go/mautrix/bridgev2/networkid"
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
	client, notLoggedIn := searchClient(ce)
	if client == nil {
		ce.Reply(notLoggedIn)
		return
	}
	text := client.searchMessages(ce.Ctx, q, time.Local, client.bridgedEventLink)
	ce.ReplyAdvanced(text, true, false)
}

// searchClient finds the client of the login that runs the search: in a portal the login the portal is
// reached with, else (management room) the user's default login. If there is none, the second result is the
// reply. In a portal, a failure to find the login is not answered with the default login: the search would
// run as another traQ account than the one that sees this room.
func searchClient(ce *commands.Event) (*TraQClient, string) {
	login := ce.User.GetDefaultLogin()
	if ce.Portal != nil {
		l, _, err := ce.Portal.FindPreferredLogin(ce.Ctx, ce.User, false)
		if err != nil && !errors.Is(err, bridgev2.ErrNotLoggedIn) {
			zerolog.Ctx(ce.Ctx).Err(err).Msg("Failed to find the login of the portal for a search")
		}
		if err != nil || l == nil {
			return nil, "You're not logged in to traQ for this room."
		}
		login = l
	}
	if login == nil {
		return nil, "You're not logged in."
	}
	client, ok := login.Client.(*TraQClient)
	if !ok || client.API == nil || !client.IsLoggedIn() {
		return nil, "You're not logged in."
	}
	return client, ""
}

// bridgedLinkFunc returns the matrix.to link of the Matrix event a traQ message is bridged to, if it is.
// receiver is the login a DM portal belongs to, "" for a public channel.
type bridgedLinkFunc func(ctx context.Context, receiver networkid.UserLoginID, messageID string) (string, bool)

// searchMessages runs the query and returns the markdown reply: the hits, newest first, or why there are none.
// traQ may know channels and users the stored state does not yet (new public channels are learned at the next
// subscription sync, new users at the next connect), so a miss reads the list from traQ, each at most once per
// command.
func (c *TraQClient) searchMessages(ctx context.Context, q parsedQuery, loc *time.Location, bridged bridgedLinkFunc) string {
	sq := traq.SearchQuery{Word: q.Words, Limit: searchLimit}
	c.stateMu.RLock()
	tree := c.channels
	c.stateMu.RUnlock()
	if tree == nil {
		return "The bridge is still connecting to traQ. Try again in a moment."
	}
	// tree is replaced for this command only: the stored tree is not changed.
	refreshedChannels, refreshedUsers := false, false
	refreshChannels := func() {
		if !refreshedChannels {
			refreshedChannels = true
			tree = c.freshTree(ctx, tree)
		}
	}
	if q.In != "" {
		ch, ok := tree.ByPath(q.In)
		if !ok {
			refreshChannels()
			ch, ok = tree.ByPath(q.In)
		}
		if !ok {
			return "Channel #" + format.EscapeMarkdown(q.In) + " not found."
		}
		sq.In = ch.ID
	}
	for _, name := range q.From {
		user, ok := c.users.ByName(name)
		if !ok && !refreshedUsers {
			refreshedUsers = true
			c.refreshUsers(ctx)
			user, ok = c.users.ByName(name)
		}
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
			refreshChannels()
			break
		}
	}
	lines := make([]string, len(res.Hits))
	for i, hit := range res.Hits {
		lines[i] = formatSearchHit(hit, hitChannelPath(tree, hit), c.hitUserName(ctx, hit), c.hitLink(ctx, tree, hit, bridged), loc)
	}
	return strings.Join(lines, "\n")
}

// freshTree reads the channel list from traQ and returns a tree of it, or old if the read fails.
func (c *TraQClient) freshTree(ctx context.Context, old *channelTree) *channelTree {
	list, err := c.API.Channels(ctx)
	if err != nil {
		zerolog.Ctx(ctx).Warn().Err(err).Msg("Failed to read the channel list for a search")
		return old
	}
	return buildChannelTree(list)
}

// refreshUsers adds the users traQ lists to the user cache. Users the cache has stay.
func (c *TraQClient) refreshUsers(ctx context.Context) {
	users, err := c.API.Users(ctx)
	if err != nil {
		zerolog.Ctx(ctx).Warn().Err(err).Msg("Failed to read the user list for a search")
		return
	}
	for i := range users {
		c.users.put(&users[i])
	}
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

// hitLink is the link of the Matrix event the hit is bridged to, else the traQ link of the message. The hit is
// looked up with the receiver of its channel's portal; a channel the tree does not know is taken for a DM.
func (c *TraQClient) hitLink(ctx context.Context, tree *channelTree, hit traq.Message, bridged bridgedLinkFunc) string {
	receiver := c.Login.ID
	if ch, ok := tree.Get(hit.ChannelID); ok {
		receiver = c.portalKey(ch).Receiver
	}
	if link, ok := bridged(ctx, receiver, hit.ID); ok {
		return link
	}
	return c.origin() + "/messages/" + hit.ID
}

// bridgedEventLink is the bridgedLinkFunc that reads the bridge's database.
func (c *TraQClient) bridgedEventLink(ctx context.Context, receiver networkid.UserLoginID, messageID string) (string, bool) {
	row, err := c.Login.Bridge.DB.Message.GetFirstPartByID(ctx, receiver, MessageID(messageID))
	if err != nil {
		zerolog.Ctx(ctx).Warn().Err(err).Str("traq_message_id", messageID).Msg("Failed to look up a bridged message")
		return "", false
	}
	if row == nil {
		return "", false
	}
	return c.eventLink(ctx, row, nil)
}
