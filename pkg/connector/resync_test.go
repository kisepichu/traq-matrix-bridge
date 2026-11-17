package connector

import (
	"context"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/simplevent"
)

// resyncRecorder is a resyncPacer's run function that records which resyncs were queued, and when.
type resyncRecorder struct {
	mu    sync.Mutex
	start time.Time
	runs  []string
	at    []time.Duration
	got   map[string]*pendingResync
	// block, if set, is waited on by the run of that channel.
	block map[string]chan struct{}
}

func newResyncRecorder() *resyncRecorder {
	return &resyncRecorder{start: time.Now(), got: map[string]*pendingResync{}, block: map[string]chan struct{}{}}
}

func (r *resyncRecorder) run(_ context.Context, p *pendingResync) {
	r.mu.Lock()
	r.runs = append(r.runs, p.ch.ID)
	r.at = append(r.at, time.Since(r.start))
	r.got[p.ch.ID] = p
	block := r.block[p.ch.ID]
	r.mu.Unlock()
	if block != nil {
		<-block
	}
}

func (r *resyncRecorder) result() ([]string, []time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.runs), slices.Clone(r.at)
}

func resync(id string) pendingResync {
	return pendingResync{ch: channelInfo{ID: id, Path: "p/" + id, Level: 1}}
}

// The resyncs are queued in the order they were added, at most 4 per second, the first one at once.
func TestResyncPacerPaces(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rec := newResyncRecorder()
		p := newResyncPacer(t.Context(), rec.run)
		for _, id := range []string{"a", "b", "c", "d"} {
			p.add(resync(id))
		}
		go p.loop()
		time.Sleep(time.Minute)
		runs, at := rec.result()
		if want := []string{"a", "b", "c", "d"}; !slices.Equal(runs, want) {
			t.Errorf("runs = %v, want %v", runs, want)
		}
		if len(at) == 0 || at[0] != 0 {
			t.Fatalf("queued at %v, want the first one at once", at)
		}
		for i := 1; i < len(at); i++ {
			if gap := at[i] - at[i-1]; gap < time.Second/4 {
				t.Errorf("queued at %v: %s between two resyncs, want at least 250ms", at, gap)
			}
		}
	})
}

// Another event of a portal is queued after the portal's resync: flush queues the waiting resync at once,
// ahead of the pace, and the loop does not queue it again.
func TestResyncPacerFlush(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rec := newResyncRecorder()
		p := newResyncPacer(t.Context(), rec.run)
		for _, id := range []string{"a", "b", "c"} {
			p.add(resync(id))
		}
		p.flush("c")
		p.flush("x") // nothing waiting
		go p.loop()
		time.Sleep(time.Minute)
		runs, at := rec.result()
		if want := []string{"c", "a", "b"}; !slices.Equal(runs, want) {
			t.Errorf("runs = %v, want %v", runs, want)
		}
		if want := []time.Duration{0, 0, resyncGap}; !slices.Equal(at, want) {
			t.Errorf("queued at %v, want %v", at, want)
		}
	})
}

// flush waits while the loop is queueing the channel's resync, so the event that follows comes after it.
func TestResyncPacerFlushWaitsForTheRunningResync(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rec := newResyncRecorder()
		release := make(chan struct{})
		rec.block["a"] = release
		p := newResyncPacer(t.Context(), rec.run)
		p.add(resync("a"))
		go p.loop()
		synctest.Wait() // the loop is queueing a
		flushed := make(chan struct{})
		go func() {
			p.flush("a")
			close(flushed)
		}()
		synctest.Wait()
		select {
		case <-flushed:
			t.Fatal("flush returned while the resync was being queued")
		default:
		}
		close(release)
		<-flushed
		if runs, _ := rec.result(); !slices.Equal(runs, []string{"a"}) {
			t.Errorf("runs = %v, want [a]", runs)
		}
	})
}

// A portal being left drops the channel's waiting resync and waits for the one being queued, so the leave that
// follows is not overtaken by it.
func TestResyncPacerDropWaitsForTheRunningResync(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rec := newResyncRecorder()
		release := make(chan struct{})
		rec.block["a"] = release
		p := newResyncPacer(t.Context(), rec.run)
		p.add(resync("a"))
		go p.loop()
		synctest.Wait() // the loop is queueing a
		p.add(resync("a"))
		dropped := make(chan struct{})
		go func() {
			p.drop("a")
			close(dropped)
		}()
		synctest.Wait()
		select {
		case <-dropped:
			t.Error("drop returned while the resync was being queued")
		default:
		}
		close(release)
		<-dropped
		time.Sleep(time.Minute)
		if runs, _ := rec.result(); !slices.Equal(runs, []string{"a"}) {
			t.Errorf("runs = %v, want [a] (the waiting one dropped)", runs)
		}
	})
}

// A resync added for a channel while flush is queueing its previous one waits for that one: the loop goes on
// with the others and queues it afterwards, once.
func TestResyncPacerAddWhileFlushing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rec := newResyncRecorder()
		release := make(chan struct{})
		rec.block["a"] = release
		p := newResyncPacer(t.Context(), rec.run)
		p.add(resync("a"))
		go p.flush("a")
		synctest.Wait() // flush is queueing a
		p.add(resync("a"))
		p.add(resync("b"))
		go p.loop()
		synctest.Wait()
		rec.mu.Lock()
		delete(rec.block, "a")
		rec.mu.Unlock()
		close(release)
		time.Sleep(time.Minute)
		if runs, _ := rec.result(); !slices.Equal(runs, []string{"a", "b", "a"}) {
			t.Errorf("runs = %v, want [a b a]", runs)
		}
	})
}

// Two resyncs of a channel added before it is queued become one, with the newer info, either backfill check
// and the after events of both.
func TestResyncPacerMerges(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rec := newResyncRecorder()
		p := newResyncPacer(t.Context(), rec.run)
		e1, e2 := &simplevent.Receipt{}, &simplevent.Receipt{}
		reverted := ""
		first := resync("a")
		first.needsBackfill = func(context.Context, *database.Message) (bool, error) { return true, nil }
		first.after = []bridgev2.RemoteEvent{e1}
		first.revert = func() { reverted = "first" }
		second := resync("a")
		second.ch.Topic = "new"
		second.after = []bridgev2.RemoteEvent{e2}
		second.revert = func() { reverted = "second" }
		p.add(first)
		p.add(second)
		go p.loop()
		time.Sleep(time.Minute)
		runs, _ := rec.result()
		if !slices.Equal(runs, []string{"a"}) {
			t.Fatalf("runs = %v, want [a] once", runs)
		}
		got := rec.got["a"]
		if got.ch.Topic != "new" || got.needsBackfill == nil || !slices.Equal(got.after, []bridgev2.RemoteEvent{e1, e2}) {
			t.Errorf("merged resync = %+v, want the new topic, the backfill check and both events", got)
		}
		got.revert()
		if reverted != "first" {
			t.Errorf("revert = %s's, want the first's (the state before both)", reverted)
		}
	})
}

// When the session ends, the loop stops and the waiting resyncs are not queued; flush no longer waits. A
// dropped resync (a portal being left) is not queued either.
func TestResyncPacerStops(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rec := newResyncRecorder()
		ctx, cancel := context.WithCancel(t.Context())
		p := newResyncPacer(ctx, rec.run)
		for _, id := range []string{"a", "b", "c", "d"} {
			p.add(resync(id))
		}
		p.drop("b")
		stopped := make(chan struct{})
		go func() {
			p.loop()
			close(stopped)
		}()
		time.Sleep(resyncGap + resyncGap/2) // a and c are queued
		cancel()
		<-stopped
		p.flush("d")
		time.Sleep(time.Minute)
		if runs, _ := rec.result(); !slices.Equal(runs, []string{"a", "c"}) {
			t.Errorf("runs = %v, want [a c]", runs)
		}
	})
}
