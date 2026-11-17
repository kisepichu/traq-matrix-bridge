package connector

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"slices"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"golang.org/x/sync/singleflight"

	"github.com/kisepichu/traq-matrix-bridge/pkg/stampconv"
	"github.com/kisepichu/traq-matrix-bridge/pkg/traq"
)

// Pace, retries and logging of the stamp sync job.
const (
	stampResolveInterval = 100 * time.Millisecond // one FileMeta per interval
	// stampResolveAttempts is the number of failed FileMeta calls after which a stamp is given up on.
	stampResolveAttempts = 5
	stampProgressEvery   = 500 // log the progress every this many Unicode stamps
)

var (
	// errNoStampLogin: no login is connected, so the stamp sync job has no API client to use.
	errNoStampLogin = errors.New("no connected login to sync the stamps with")
	// errRetryBudget: a stamp's emoji failed to resolve stampResolveAttempts times.
	errRetryBudget = errors.New("the retries are used up")
)

// stampSync is the state of the stamp sync job. The job brings the stamp store and Stamps up to date with
// traQ's stamp list, then resolves the emoji of the Unicode stamps from their file names. It belongs to the
// connector, not to a session: it runs on Bridge.BackgroundCtx, at most once at a time, with the API client of
// the oldest login whose session is connected. When that session ends, the job goes on with the next connected
// login; when none is left it stops, and the next login to connect starts it again. Its progress is in the
// database, so a stopped job, or a restarted bridge, continues where it left off. After it has finished once,
// the STAMP_* events keep the table up to date until the bridge stops. A conversion that meets a pending
// Unicode stamp resolves it on demand (resolveEmoji), so the job does not have to get there first.
type stampSync struct {
	// writeMu serializes the changes of the store and Stamps that are based on a read from traQ (the job, the
	// on-demand resolutions and the STAMP_* events of every login), so an older read cannot overwrite a newer
	// one. The stamp list is read without it, because it is large: touched then collects the stamps that were
	// read from traQ one by one meanwhile (STAMP_* events, stamps new to a conversion), which the older list
	// must leave alone. An emoji resolved meanwhile is kept by the list anyway. touched is non-nil only during
	// that read.
	writeMu sync.Mutex
	touched map[string]struct{}
	// resolving makes the conversions that need the emoji of one stamp at the same time share one resolution.
	resolving singleflight.Group

	mu      sync.Mutex
	clients []*TraQClient // the logins whose session reached CONNECTED and has not ended, oldest first
	running bool
	done    bool // the job has finished in this process
	wg      sync.WaitGroup
	// retryMin is the first delay of the job's backoff; 0 means minBackoff.
	retryMin time.Duration
}

// stampClientConnected registers a login whose session reached CONNECTED, and starts the job if it has not
// finished and is not running. Once Bridge.BackgroundCtx is done the bridge is stopping, and Stop may already
// be waiting for the job, so none is started.
func (c *TraQConnector) stampClientConnected(client *TraQClient) {
	s := &c.stampSync
	s.mu.Lock()
	defer s.mu.Unlock()
	if !slices.Contains(s.clients, client) {
		s.clients = append(s.clients, client)
	}
	ctx := c.Bridge.BackgroundCtx
	if s.running || s.done || ctx.Err() != nil {
		return
	}
	s.running = true
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
	s := &c.stampSync
	// After this, stampClientConnected sees BackgroundCtx done and starts no job, so wg only has the job
	// started before, if any.
	s.mu.Lock()
	s.clients = nil
	s.mu.Unlock()
	s.wg.Wait()
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
//
// A stamp whose emoji keeps failing to resolve is given up on, and stored as unresolvable, after
// stampResolveAttempts failures, so that one stamp cannot keep the table incomplete. Only failures that traQ
// answered count (see countsAgainstBudget): while traQ cannot be reached, the job waits instead of giving up on
// every stamp. The backoff is not reset by giving up, so a traQ that fails every request slows the job down.
func (c *TraQConnector) syncStamps(ctx context.Context) (bool, error) {
	log := zerolog.Ctx(ctx)
	retry := backoff{min: c.stampSync.retryMin}
	// call runs fn with the API client of the oldest connected login until it succeeds, retrying with backoff.
	// With a budget, it returns errRetryBudget once that many failures counted against it.
	call := func(budget int, fn func(api *traq.Client) error) error {
		failures := 0
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
			if budget > 0 && countsAgainstBudget(err) {
				if failures++; failures >= budget {
					return fmt.Errorf("%w: %w", errRetryBudget, err)
				}
			}
			log.Warn().Err(err).Msg("Stamp sync failed")
			if !retry.wait(ctx, *log) {
				return ctx.Err()
			}
		}
	}

	if err := call(0, func(api *traq.Client) error { return c.reconcileStamps(ctx, api) }); err != nil {
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
		if s, ok := c.Stamps.ByID(p.ID); ok && s.EmojiPending() { // else resolved on demand or changed meanwhile
			var res stampconv.Stamp
			err := call(stampResolveAttempts, func(api *traq.Client) error {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-tick.C:
				}
				var err error
				res, err = c.resolveEmoji(ctx, api, p.ID)
				return err
			})
			if errors.Is(err, errRetryBudget) {
				log.Warn().Err(err).Str("stamp_id", p.ID).Msg("Giving up on the emoji of a Unicode stamp")
				res, err = c.markUnresolvable(ctx, p.ID)
			}
			if err != nil {
				return false, err
			}
			if res.Unresolvable {
				unresolvable++
			}
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

// countsAgainstBudget reports whether a failure to resolve a stamp's emoji says something about the stamp: traQ
// answered (an error status, or a response that could not be read). A request that got no answer (traQ or the
// network is down) does not.
func countsAgainstBudget(err error) bool {
	var netErr net.Error
	return !errors.As(err, &netErr)
}

// reconcileStamps brings the store and Stamps up to date with traQ's stamp list: new stamps are added, stamps
// that are gone are deleted, and renamed stamps are updated. A Unicode stamp keeps its resolved emoji (or its
// unresolvable mark). The list is read without writeMu, and the stamps that changed meanwhile keep their newer
// state; the writes are one transaction, applied to Stamps once it is committed.
func (c *TraQConnector) reconcileStamps(ctx context.Context, api *traq.Client) error {
	s := &c.stampSync
	s.writeMu.Lock()
	s.touched = map[string]struct{}{}
	s.writeMu.Unlock()
	remote, err := api.Stamps(ctx)
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	touched := s.touched
	s.touched = nil
	if err != nil {
		return fmt.Errorf("get stamps: %w", err)
	}
	var puts []stampconv.Stamp
	var deletes []string
	var changed int
	err = c.stampDB.DoTxn(ctx, func(ctx context.Context) error {
		stored, err := c.stampDB.All(ctx)
		if err != nil {
			return fmt.Errorf("read the stamp table: %w", err)
		}
		old := make(map[string]stampconv.Stamp, len(stored))
		for _, st := range stored {
			old[st.ID] = st
		}
		for i := range remote {
			r := &remote[i]
			prev, had := old[r.ID]
			delete(old, r.ID)
			if _, ok := touched[r.ID]; ok {
				continue
			}
			st := stampFromTraQ(r)
			if had {
				if prev.Name == st.Name && prev.Unicode == st.Unicode {
					continue
				}
				if st.Unicode {
					st.Emoji, st.Unresolvable = prev.Emoji, prev.Unresolvable
				}
				changed++
			}
			if err := c.stampDB.Put(ctx, st, r.FileID); err != nil {
				return fmt.Errorf("save stamp %s: %w", st.ID, err)
			}
			puts = append(puts, st)
		}
		for id := range old {
			if _, ok := touched[id]; ok {
				continue
			}
			if err := c.stampDB.Delete(ctx, id); err != nil {
				return fmt.Errorf("delete stamp %s: %w", id, err)
			}
			deletes = append(deletes, id)
		}
		return nil
	})
	if err != nil {
		return err
	}
	for _, st := range puts {
		c.Stamps.Put(st)
	}
	for _, id := range deletes {
		c.Stamps.Delete(id)
	}
	zerolog.Ctx(ctx).Info().Int("stamps", len(remote)).Int("added", len(puts)-changed).Int("changed", changed).
		Int("deleted", len(deletes)).Int("changed_meanwhile", len(touched)).Msg("Synced the stamp list with traQ")
	return nil
}

// resolveEmoji resolves the emoji of a pending Unicode stamp from its file (see emojiFromFile), stores it, and
// returns the stamp as Stamps then has it. The job and the conversions that meet a pending stamp both use it:
// concurrent calls for one stamp share one resolution, and a stamp resolved or changed meanwhile is returned
// as it is, without asking traQ. A failure leaves the stamp pending.
func (c *TraQConnector) resolveEmoji(ctx context.Context, api *traq.Client, id string) (stampconv.Stamp, error) {
	v, err, _ := c.stampSync.resolving.Do(id, func() (any, error) {
		c.stampSync.writeMu.Lock()
		defer c.stampSync.writeMu.Unlock()
		s, ok := c.Stamps.ByID(id)
		if !ok || !s.EmojiPending() {
			return s, nil
		}
		fileID, err := c.storedFileID(ctx, id)
		if err != nil {
			return s, err
		}
		resolved, err := emojiFromFile(ctx, api, s, fileID)
		if err != nil {
			return s, err
		}
		return resolved, c.putStampLocked(ctx, resolved, fileID)
	})
	return v.(stampconv.Stamp), err
}

// emojiFromFile derives the emoji of Unicode stamp s from the name of its image file. A file that does not
// exist, or whose name is not an emoji, makes s unresolvable (logged as a warning); any other failure is
// returned.
func emojiFromFile(ctx context.Context, api *traq.Client, s stampconv.Stamp, fileID string) (stampconv.Stamp, error) {
	log := zerolog.Ctx(ctx).With().Str("stamp_id", s.ID).Str("stamp_name", s.Name).Str("file_id", fileID).Logger()
	meta, err := api.FileMeta(ctx, fileID)
	if traq.IsStatus(err, http.StatusNotFound) {
		log.Warn().Msg("The file of a Unicode stamp does not exist, its emoji is unknown")
		s.Unresolvable = true
		return s, nil
	}
	if err != nil {
		return s, fmt.Errorf("get the file meta of stamp %s: %w", s.ID, err)
	}
	emoji, ok := stampconv.EmojiFromFileName(meta.Name)
	if !ok {
		log.Warn().Str("file_name", meta.Name).Msg("The file name of a Unicode stamp is not an emoji")
		s.Unresolvable = true
		return s, nil
	}
	s.Emoji = emoji
	return s, nil
}

// markUnresolvable stores a pending Unicode stamp as unresolvable and returns the stamp as Stamps then has it.
func (c *TraQConnector) markUnresolvable(ctx context.Context, id string) (stampconv.Stamp, error) {
	c.stampSync.writeMu.Lock()
	defer c.stampSync.writeMu.Unlock()
	s, ok := c.Stamps.ByID(id)
	if !ok || !s.EmojiPending() {
		return s, nil
	}
	fileID, err := c.storedFileID(ctx, id)
	if err != nil {
		return s, err
	}
	s.Unresolvable = true
	return s, c.putStampLocked(ctx, s, fileID)
}

// storedFileID returns the ID of the image of a stamp in the store.
func (c *TraQConnector) storedFileID(ctx context.Context, id string) (string, error) {
	fileID, stored, err := c.stampDB.FileID(ctx, id)
	if err == nil && !stored {
		err = errors.New("the stamp is not stored")
	}
	if err != nil {
		return "", fmt.Errorf("read the file of stamp %s: %w", id, err)
	}
	return fileID, nil
}

// refreshStamp reads one stamp from traQ, with the emoji of a Unicode stamp, into the store and Stamps
// (STAMP_CREATED, STAMP_UPDATED). A stamp that traQ no longer has is deleted. If the emoji cannot be read now,
// the stamp is stored pending, and resolved on demand or by the next job.
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
		if resolved, err := emojiFromFile(ctx, api, s, st.FileID); err != nil {
			zerolog.Ctx(ctx).Warn().Err(err).Str("stamp_id", id).
				Msg("Failed to resolve the emoji of a Unicode stamp, it is resolved later")
		} else {
			s = resolved
		}
	}
	c.touchedLocked(id)
	return c.putStampLocked(ctx, s, st.FileID)
}

// deleteStamp removes a stamp from the store and Stamps (STAMP_DELETED).
func (c *TraQConnector) deleteStamp(ctx context.Context, id string) error {
	c.stampSync.writeMu.Lock()
	defer c.stampSync.writeMu.Unlock()
	return c.deleteStampLocked(ctx, id)
}

// addStamp stores s, just read from traQ, unless Stamps got the stamp meanwhile, and returns the stamp of
// Stamps. On a failure to store it, s is returned with the error.
func (c *TraQConnector) addStamp(ctx context.Context, s stampconv.Stamp, fileID string) (stampconv.Stamp, error) {
	c.stampSync.writeMu.Lock()
	defer c.stampSync.writeMu.Unlock()
	if cur, ok := c.Stamps.ByID(s.ID); ok {
		return cur, nil
	}
	c.touchedLocked(s.ID)
	return s, c.putStampLocked(ctx, s, fileID)
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
	c.touchedLocked(id)
	c.Stamps.Delete(id)
	return nil
}

// touchedLocked records a change of a stamp for a reconcileStamps that is reading the stamp list. writeMu must
// be held.
func (c *TraQConnector) touchedLocked(id string) {
	if t := c.stampSync.touched; t != nil {
		t[id] = struct{}{}
	}
}
