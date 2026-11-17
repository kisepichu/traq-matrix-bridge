package connector

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"runtime/debug"
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
	// stampResolveAttempts is the number of failed FileMeta calls after which the walk over the pending stamps
	// gives up on a stamp, to retry it after the walk.
	stampResolveAttempts = 5
	// stampOnDemandRetryDelay is how long the conversions leave a stamp the job has given up on before they try
	// to resolve it again, so that a traQ that is down is not asked once per conversion.
	stampOnDemandRetryDelay = time.Minute
	stampProgressEvery      = 500 // log the progress every this many Unicode stamps
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
	// resolveJoined, if set, is called with the stamp ID once a caller of resolveEmoji has joined the
	// resolution. It is for the tests.
	resolveJoined func(id string)

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
// The walk over the pending Unicode stamps gives up on a stamp whose emoji fails to resolve
// stampResolveAttempts times, for any reason, so that one stamp cannot keep the table incomplete: the table is
// marked complete after the walk. The stamps that the walk leaves pending (given up on, or given another file
// meanwhile whose meta failed) are then retried in rounds, with the backoff, until each is resolved or gets a
// definite answer (see emojiFromFile); only then has the job finished. A round stops at its first failure, so
// that a traQ that is down is asked once per round, and the next round starts after the stamp that failed.
// Meanwhile Stamps has them with a retry time (SetRetryAfter), so Resolve does not call an emoji unknown and
// the conversions do not ask traQ for them at every turn. They stay pending in the store, so a stopped job or
// the next start retries them too. The backoff is not reset by giving up, so a traQ that fails every request
// slows the job down.
func (c *TraQConnector) syncStamps(ctx context.Context) (bool, error) {
	log := zerolog.Ctx(ctx)
	retry := backoff{min: c.stampSync.retryMin}
	// call runs fn with the API client of the oldest connected login until it succeeds, retrying with backoff.
	// With a budget, it returns errRetryBudget once that many attempts failed.
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
			if budget > 0 {
				if failures++; failures >= budget {
					return fmt.Errorf("%w: %w", errRetryBudget, err)
				}
			}
			log.Warn().Err(err).Msg("Stamp sync failed")
			if !retry.wait(ctx, *log, "Retrying the stamp sync") {
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
	var retrying []string
	resolved, unresolvable, deleted := 0, 0, 0
	for i, p := range pending {
		if s, ok := c.Stamps.ByID(p.ID); ok && s.EmojiPending() { // else resolved on demand or changed meanwhile
			err := call(stampResolveAttempts, func(api *traq.Client) error {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-tick.C:
				}
				_, err := c.resolveEmoji(ctx, api, p.ID)
				return err
			})
			if errors.Is(err, errRetryBudget) {
				log.Warn().Err(err).Str("stamp_id", p.ID).
					Msg("Giving up on the emoji of a Unicode stamp for now, it is retried after the others")
			} else if err != nil {
				return false, err
			}
		}
		switch s, ok := c.Stamps.ByID(p.ID); {
		case !ok:
			deleted++
		case s.Emoji != "":
			resolved++
		case s.Unresolvable:
			unresolvable++
		case !s.Unicode:
			// Changed to a custom stamp meanwhile: it has no emoji to resolve.
		default:
			c.Stamps.SetRetryAfter(p.ID, time.Now().Add(stampOnDemandRetryDelay))
			retrying = append(retrying, p.ID)
		}
		if n := i + 1; n%stampProgressEvery == 0 {
			log.Info().Int("done", n).Int("total", len(pending)).Msg("Resolving the emoji of Unicode stamps")
		}
	}
	c.Stamps.SetComplete(true)
	log.Info().Int("resolved", resolved).Int("unresolvable", unresolvable).Int("deleted", deleted).
		Int("retrying", len(retrying)).Msg("The stamp table is complete")
	if len(retrying) == 0 {
		return true, nil
	}

	for len(retrying) > 0 {
		if !retry.wait(ctx, *log, "Retrying the emoji of the Unicode stamps given up on") {
			return false, ctx.Err()
		}
		api := c.stampAPI()
		if api == nil {
			return false, errNoStampLogin
		}
		var failed error
		var left []string
		for i, id := range retrying {
			if s, ok := c.Stamps.ByID(id); !ok || !s.EmojiPending() {
				continue
			}
			select {
			case <-ctx.Done():
				return false, ctx.Err()
			case <-tick.C:
			}
			if _, err := c.resolveEmoji(ctx, api, id); err != nil {
				// traQ is likely still failing, so the others are not tried this round. The next round starts
				// after this stamp, so that one stamp that keeps failing does not keep the others waiting.
				failed = err
				left = append(append(slices.Clone(retrying[i+1:]), left...), id)
				break
			}
			if s, ok := c.Stamps.ByID(id); ok && s.EmojiPending() {
				left = append(left, id)
			}
		}
		retrying = slices.DeleteFunc(left, func(id string) bool {
			s, ok := c.Stamps.ByID(id)
			return !ok || !s.EmojiPending()
		})
		if failed != nil {
			log.Warn().Err(failed).Int("left", len(retrying)).Msg("Failed to resolve the emoji of Unicode stamps again")
		} else {
			retry.reset()
		}
	}
	log.Info().Msg("Resolved the emoji of the Unicode stamps that were retried")
	return true, nil
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
// concurrent calls for one stamp share one resolution (run with the first caller's ctx), and a stamp
// resolved, changed or deleted meanwhile is returned as Stamps has it (the zero Stamp if deleted), without
// storing the resolution. FileMeta runs without writeMu, so that a slow traQ delays only the callers of this
// stamp. A failure leaves the stamp pending. The resolution runs on its own goroutine (DoChan), where nothing
// else would recover a panic, so a panic is returned as an error.
func (c *TraQConnector) resolveEmoji(ctx context.Context, api *traq.Client, id string) (stampconv.Stamp, error) {
	ch := c.stampSync.resolving.DoChan(id, func() (val any, err error) {
		defer func() {
			if p := recover(); p != nil {
				val, _ = c.Stamps.ByID(id)
				err = fmt.Errorf("panic while resolving the emoji of stamp %s: %v\n%s", id, p, debug.Stack())
			}
		}()
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
		c.stampSync.writeMu.Lock()
		defer c.stampSync.writeMu.Unlock()
		cur, ok := c.Stamps.ByID(id)
		if !ok || !cur.EmojiPending() {
			return cur, nil
		}
		// A STAMP_UPDATED that changed the image meanwhile is newer than this resolution.
		if curFileID, err := c.storedFileID(ctx, id); err != nil || curFileID != fileID {
			return cur, err
		}
		cur.Emoji, cur.Unresolvable = resolved.Emoji, resolved.Unresolvable
		return cur, c.putStampLocked(ctx, cur, fileID)
	})
	if joined := c.stampSync.resolveJoined; joined != nil {
		joined(id)
	}
	r := <-ch
	return r.Val.(stampconv.Stamp), r.Err
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
// the stamp is stored pending with a retry time, like a stamp the job has given up on (see SetRetryAfter), and
// resolved on demand after that time, or by the next job.
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
	retryLater := false
	if st.IsUnicode {
		if resolved, err := emojiFromFile(ctx, api, s, st.FileID); err != nil {
			zerolog.Ctx(ctx).Warn().Err(err).Str("stamp_id", id).
				Msg("Failed to resolve the emoji of a Unicode stamp, it is resolved later")
			retryLater = true
		} else {
			s = resolved
		}
	}
	c.touchedLocked(id)
	if err := c.putStampLocked(ctx, s, st.FileID); err != nil {
		return err
	}
	if retryLater {
		c.Stamps.SetRetryAfter(id, time.Now().Add(stampOnDemandRetryDelay))
	}
	return nil
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
