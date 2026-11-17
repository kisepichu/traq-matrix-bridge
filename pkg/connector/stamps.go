package connector

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"sync"
	"time"

	"github.com/rs/zerolog"

	"github.com/kisepichu/traq-matrix-bridge/pkg/connector/stampdb"
	"github.com/kisepichu/traq-matrix-bridge/pkg/stampconv"
	"github.com/kisepichu/traq-matrix-bridge/pkg/traq"
)

// Pace and logging of the stamp sync job.
const (
	stampResolveInterval = 100 * time.Millisecond // one FileMeta per interval
	stampProgressEvery   = 500                    // log the progress every this many Unicode stamps
)

// errNoStampLogin: no login is connected, so the stamp sync job has no API client to use.
var errNoStampLogin = errors.New("no connected login to sync the stamps with")

// stampSync is the state of the stamp sync job. The job brings the stamp store and Stamps up to date with
// traQ's stamp list, then resolves the emoji of the Unicode stamps from their file names. It belongs to the
// connector, not to a session: it runs on Bridge.BackgroundCtx, at most once at a time, with the API client of
// the oldest login whose session is connected. When that session ends, the job goes on with the next connected
// login; when none is left it stops, and the next login to connect starts it again. Its progress is in the
// database, so a stopped job, or a restarted bridge, continues where it left off. After it has finished once,
// the STAMP_* events keep the table up to date until the bridge stops.
type stampSync struct {
	// writeMu serializes the changes of the store and Stamps that are based on a read from traQ (the job and
	// the STAMP_* events of every login), so an older read cannot overwrite a newer one.
	writeMu sync.Mutex

	mu      sync.Mutex
	clients []*TraQClient // the logins whose session reached CONNECTED and has not ended, oldest first
	running bool
	done    bool // the job has finished in this process
	wg      sync.WaitGroup
}

// stampClientConnected registers a login whose session reached CONNECTED, and starts the job if it has not
// finished and is not running.
func (c *TraQConnector) stampClientConnected(client *TraQClient) {
	s := &c.stampSync
	s.mu.Lock()
	defer s.mu.Unlock()
	if !slices.Contains(s.clients, client) {
		s.clients = append(s.clients, client)
	}
	if s.running || s.done {
		return
	}
	s.running = true
	ctx := c.Bridge.BackgroundCtx
	s.wg.Go(func() { c.runStampSync(ctx) })
}

// stampClientGone unregisters a login whose session has ended.
func (c *TraQConnector) stampClientGone(client *TraQClient) {
	s := &c.stampSync
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clients = slices.DeleteFunc(s.clients, func(x *TraQClient) bool { return x == client })
}

// stampAPI returns the API client of the oldest connected login. With none, it returns nil and marks the job
// stopped in the same step, so that a login that connects afterwards starts it again.
func (c *TraQConnector) stampAPI() *traq.Client {
	s := &c.stampSync
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.clients) == 0 {
		s.running = false
		return nil
	}
	return s.clients[0].API
}

// Stop is called by bridgev2 after it has disconnected every login and cancelled Bridge.BackgroundCtx, which
// ends the job. It waits for the job, so that the job does not use the database after bridgev2 closes it.
func (c *TraQConnector) Stop() {
	c.stampSync.wg.Wait()
}

func (c *TraQConnector) runStampSync(ctx context.Context) {
	log := zerolog.Ctx(ctx).With().Str("component", "stamp_sync").Logger()
	ctx = log.WithContext(ctx)
	done, err := c.syncStamps(ctx)
	if errors.Is(err, errNoStampLogin) {
		// stampAPI has already marked the job stopped; a new job may be running.
		log.Info().Msg("Pausing the stamp sync until a login connects")
		return
	}
	if err != nil && ctx.Err() == nil {
		log.Err(err).Msg("Stamp sync failed, it is retried when a login connects")
	}
	s := &c.stampSync
	s.mu.Lock()
	defer s.mu.Unlock()
	s.running, s.done = false, done
}

// syncStamps is the body of the job. It reports whether the job finished. errNoStampLogin means no login was
// left to continue with.
func (c *TraQConnector) syncStamps(ctx context.Context) (bool, error) {
	log := zerolog.Ctx(ctx)
	var retry backoff
	// call runs fn with the API client of the oldest connected login until it succeeds, retrying with backoff.
	call := func(fn func(api *traq.Client) error) error {
		for {
			api := c.stampAPI()
			if api == nil {
				return errNoStampLogin
			}
			err := fn(api)
			if err == nil {
				retry.reset()
				return nil
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			log.Warn().Err(err).Msg("Stamp sync failed")
			if !retry.wait(ctx, *log) {
				return ctx.Err()
			}
		}
	}

	if err := call(func(api *traq.Client) error { return c.reconcileStamps(ctx, api) }); err != nil {
		return false, err
	}
	pending, err := c.stampDB.PendingUnicode(ctx)
	if err != nil {
		return false, fmt.Errorf("list the pending Unicode stamps: %w", err)
	}
	if len(pending) > 0 {
		log.Info().Int("pending", len(pending)).Msg("Resolving the emoji of Unicode stamps")
	}
	tick := time.NewTicker(stampResolveInterval)
	defer tick.Stop()
	unresolvable := 0
	for i, p := range pending {
		err := call(func(api *traq.Client) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-tick.C:
			}
			ok, err := c.resolveStamp(ctx, api, p)
			if err == nil && !ok {
				unresolvable++
			}
			return err
		})
		if err != nil {
			return false, err
		}
		if n := i + 1; n%stampProgressEvery == 0 {
			log.Info().Int("done", n).Int("total", len(pending)).Msg("Resolving the emoji of Unicode stamps")
		}
	}
	c.Stamps.SetComplete(true)
	log.Info().Int("resolved", len(pending)-unresolvable).Int("unresolvable", unresolvable).
		Msg("The stamp table is complete")
	return true, nil
}

// reconcileStamps brings the store and Stamps up to date with traQ's stamp list: new stamps are added, stamps
// that are gone are deleted, and renamed stamps are updated. A Unicode stamp keeps its resolved emoji.
func (c *TraQConnector) reconcileStamps(ctx context.Context, api *traq.Client) error {
	c.stampSync.writeMu.Lock()
	defer c.stampSync.writeMu.Unlock()
	remote, err := api.Stamps(ctx)
	if err != nil {
		return fmt.Errorf("get stamps: %w", err)
	}
	stored, err := c.stampDB.All(ctx)
	if err != nil {
		return fmt.Errorf("read the stamp table: %w", err)
	}
	old := make(map[string]stampconv.Stamp, len(stored))
	for _, s := range stored {
		old[s.ID] = s
	}
	var added, changed int
	for i := range remote {
		r := &remote[i]
		s := stampFromTraQ(r)
		if prev, ok := old[s.ID]; ok {
			delete(old, s.ID)
			if prev.Name == s.Name && prev.Unicode == s.Unicode {
				continue
			}
			if s.Unicode {
				s.Emoji = prev.Emoji
			}
			changed++
		} else {
			added++
		}
		if err := c.putStampLocked(ctx, s, r.FileID); err != nil {
			return err
		}
	}
	for id := range old {
		if err := c.deleteStampLocked(ctx, id); err != nil {
			return err
		}
	}
	zerolog.Ctx(ctx).Info().Int("stamps", len(remote)).Int("added", added).Int("changed", changed).
		Int("deleted", len(old)).Msg("Synced the stamp list with traQ")
	return nil
}

// resolveStamp reads the file name of a pending Unicode stamp and saves its emoji. It reports false, after
// logging a warning, when the emoji cannot be derived: the stamp stays pending and keeps its ":name:" key.
func (c *TraQConnector) resolveStamp(ctx context.Context, api *traq.Client, p stampdb.Pending) (bool, error) {
	c.stampSync.writeMu.Lock()
	defer c.stampSync.writeMu.Unlock()
	s, ok := c.Stamps.ByID(p.ID)
	if !ok || !s.Unicode || s.Emoji != "" {
		return true, nil // changed by a STAMP_* event meanwhile
	}
	log := zerolog.Ctx(ctx).With().Str("stamp_id", p.ID).Str("stamp_name", s.Name).Str("file_id", p.FileID).Logger()
	meta, err := api.FileMeta(ctx, p.FileID)
	if traq.IsStatus(err, http.StatusNotFound) {
		log.Warn().Msg("The file of a Unicode stamp does not exist, its emoji is unknown")
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("get the file meta of stamp %s: %w", p.ID, err)
	}
	emoji, ok := stampconv.EmojiFromFileName(meta.Name)
	if !ok {
		log.Warn().Str("file_name", meta.Name).Msg("The file name of a Unicode stamp is not an emoji")
		return false, nil
	}
	s.Emoji = emoji
	return true, c.putStampLocked(ctx, s, p.FileID)
}

// refreshStamp reads one stamp from traQ, with the emoji of a Unicode stamp, into the store and Stamps
// (STAMP_CREATED, STAMP_UPDATED). A stamp that traQ no longer has is deleted.
func (c *TraQConnector) refreshStamp(ctx context.Context, api *traq.Client, id string) error {
	c.stampSync.writeMu.Lock()
	defer c.stampSync.writeMu.Unlock()
	st, err := api.Stamp(ctx, id)
	if traq.IsStatus(err, http.StatusNotFound) {
		return c.deleteStampLocked(ctx, id)
	}
	if err != nil {
		return fmt.Errorf("get stamp %s: %w", id, err)
	}
	s := stampFromTraQ(st)
	if st.IsUnicode {
		meta, err := api.FileMeta(ctx, st.FileID)
		if err != nil {
			return fmt.Errorf("get the file meta of stamp %s: %w", id, err)
		}
		var ok bool
		if s.Emoji, ok = stampconv.EmojiFromFileName(meta.Name); !ok {
			zerolog.Ctx(ctx).Warn().Str("stamp_id", id).Str("file_name", meta.Name).
				Msg("The file name of a Unicode stamp is not an emoji")
		}
	}
	return c.putStampLocked(ctx, s, st.FileID)
}

// deleteStamp removes a stamp from the store and Stamps (STAMP_DELETED).
func (c *TraQConnector) deleteStamp(ctx context.Context, id string) error {
	c.stampSync.writeMu.Lock()
	defer c.stampSync.writeMu.Unlock()
	return c.deleteStampLocked(ctx, id)
}

// tableStamp adds s, just read from traQ, to Stamps unless Stamps got the stamp meanwhile, and returns the
// stamp of Stamps. It does not touch the store: the job adds the stamp there.
func (c *TraQConnector) tableStamp(s stampconv.Stamp) stampconv.Stamp {
	c.stampSync.writeMu.Lock()
	defer c.stampSync.writeMu.Unlock()
	if cur, ok := c.Stamps.ByID(s.ID); ok {
		return cur
	}
	c.Stamps.Put(s)
	return s
}

// putStampLocked saves a stamp, then puts it in Stamps, so that Stamps mirrors the store. writeMu must be held.
func (c *TraQConnector) putStampLocked(ctx context.Context, s stampconv.Stamp, fileID string) error {
	if err := c.stampDB.Put(ctx, s, fileID); err != nil {
		return fmt.Errorf("save stamp %s: %w", s.ID, err)
	}
	c.Stamps.Put(s)
	return nil
}

// deleteStampLocked deletes a stamp from the store, then from Stamps. writeMu must be held.
func (c *TraQConnector) deleteStampLocked(ctx context.Context, id string) error {
	if err := c.stampDB.Delete(ctx, id); err != nil {
		return fmt.Errorf("delete stamp %s: %w", id, err)
	}
	c.Stamps.Delete(id)
	return nil
}
