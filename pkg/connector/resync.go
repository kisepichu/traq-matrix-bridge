package connector

import (
	"context"
	"sync"
	"time"

	"maunium.net/go/mautrix/bridgev2"
)

// resyncGap is the least time between two paced portal resyncs, so that at most 4 are queued per second. Each
// one may make bridgev2 create a room and backfill it, or ask traQ for the newest message of a channel.
const resyncGap = 250 * time.Millisecond

// pendingResync is a portal resync waiting in a resyncPacer.
type pendingResync struct {
	ch            channelInfo
	needsBackfill backfillCheck          // see queueResync
	after         []bridgev2.RemoteEvent // queued right after the resync, in the same portal
	revert        func()                 // called if the resync cannot be queued; may be nil
}

// resyncPacer queues the portal resyncs of a session (initial sync, subscription syncs, reconnect catch-ups)
// at most one per resyncGap, so that an account with hundreds of channels does not start hundreds of room
// creations, backfills and traQ requests at once. Its loop runs until the session's ctx ends, so Disconnect
// stops it, and the resyncs still waiting are dropped with the session.
//
// The resync of a channel must still be queued before any later event of its portal: a live message bridged
// before a catch-up would become the anchor of its forward fetch and cut off the missed messages, and one in a
// public channel without a room would be dropped. So the other events of a portal go through flush, which
// queues the channel's waiting resync at once, ahead of the pace, or waits for the one being queued.
type resyncPacer struct {
	ctx  context.Context
	run  func(ctx context.Context, r *pendingResync) // queues the resync and its after events
	wake chan struct{}

	mu      sync.Mutex
	order   []string                  // channel IDs in the order their resyncs were added; may hold stale IDs
	waiting map[string]*pendingResync // by channel ID
	running map[string]chan struct{}  // channel IDs whose resync is being queued; closed when it is
}

func newResyncPacer(ctx context.Context, run func(ctx context.Context, r *pendingResync)) *resyncPacer {
	return &resyncPacer{
		ctx:     ctx,
		run:     run,
		wake:    make(chan struct{}, 1),
		waiting: make(map[string]*pendingResync),
		running: make(map[string]chan struct{}),
	}
}

// add puts a resync behind the waiting ones. If the channel already has one waiting, they are merged: the newer
// channel info, the backfill check of either, the after events of both, and the older revert (it restores the
// state before both).
func (p *resyncPacer) add(r pendingResync) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if cur := p.waiting[r.ch.ID]; cur != nil {
		cur.ch = r.ch
		if r.needsBackfill != nil {
			cur.needsBackfill = r.needsBackfill
		}
		cur.after = append(cur.after, r.after...)
		if cur.revert == nil {
			cur.revert = r.revert
		}
		return
	}
	p.waiting[r.ch.ID] = &r
	p.order = append(p.order, r.ch.ID)
	select {
	case p.wake <- struct{}{}:
	default:
	}
}

// drop forgets the waiting resync of a channel, if any (the portal is being left).
func (p *resyncPacer) drop(channelID string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.waiting, channelID)
}

// flush returns once the channel has no resync waiting or being queued: it queues a waiting one itself, and
// waits for one being queued. It returns at once when the session has ended.
func (p *resyncPacer) flush(channelID string) {
	p.mu.Lock()
	for p.ctx.Err() == nil {
		if done := p.running[channelID]; done != nil {
			p.mu.Unlock()
			select {
			case <-done:
			case <-p.ctx.Done():
			}
			p.mu.Lock()
			continue
		}
		r := p.waiting[channelID]
		if r == nil {
			break
		}
		done := p.claimLocked(channelID)
		p.mu.Unlock()
		p.run(p.ctx, r)
		p.finish(channelID, done)
		p.mu.Lock()
	}
	p.mu.Unlock()
}

// loop queues the waiting resyncs in order, resyncGap apart, until ctx ends.
func (p *resyncPacer) loop() {
	for {
		id, r, done := p.next()
		if r == nil {
			select {
			case <-p.ctx.Done():
				return
			case <-p.wake:
				continue
			}
		}
		p.run(p.ctx, r)
		p.finish(id, done)
		if !sleepCtx(p.ctx, resyncGap) {
			return
		}
	}
}

// next claims the oldest waiting resync whose channel has none being queued; r is nil if there is none.
func (p *resyncPacer) next() (id string, r *pendingResync, done chan struct{}) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for len(p.order) > 0 {
		id, p.order = p.order[0], p.order[1:]
		// A channel whose resync flush is queueing is put back in order by finish.
		if r = p.waiting[id]; r != nil && p.running[id] == nil {
			return id, r, p.claimLocked(id)
		}
	}
	return "", nil, nil
}

// claimLocked takes a channel's waiting resync off the queue and marks it being queued. p.mu must be held.
func (p *resyncPacer) claimLocked(channelID string) chan struct{} {
	delete(p.waiting, channelID)
	done := make(chan struct{})
	p.running[channelID] = done
	return done
}

// finish marks a channel's resync queued. A resync added for the channel meanwhile goes back in order.
func (p *resyncPacer) finish(channelID string, done chan struct{}) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.running, channelID)
	close(done)
	if p.waiting[channelID] != nil {
		p.order = append(p.order, channelID)
		select {
		case p.wake <- struct{}{}:
		default:
		}
	}
}
