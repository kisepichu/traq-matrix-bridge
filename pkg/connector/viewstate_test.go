package connector

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"
)

// fakeViewSender records the channels sent and fails while err is set.
type fakeViewSender struct {
	mu   sync.Mutex
	sent []string
	err  error
}

func (f *fakeViewSender) send(_ context.Context, channelID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.sent = append(f.sent, channelID)
	return nil
}

func (f *fakeViewSender) setErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.err = err
}

// takeSent returns the channels sent since the last call.
func (f *fakeViewSender) takeSent() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	sent := f.sent
	f.sent = nil
	return sent
}

type fakeClock struct{ now time.Time }

func (c *fakeClock) Now() time.Time          { return c.now }
func (c *fakeClock) advance(d time.Duration) { c.now = c.now.Add(d) }

const testIdle = 10 * time.Minute

func newTestViewState() (*viewState, *fakeViewSender, *fakeClock) {
	v, sender, clock, _ := newTestViewStateWithSwitches()
	return v, sender, clock
}

// newTestViewStateWithSwitches also returns the channels the viewstate switched to, in order.
func newTestViewStateWithSwitches() (*viewState, *fakeViewSender, *fakeClock, *[]string) {
	sender := &fakeViewSender{}
	clock := &fakeClock{now: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)}
	var switches []string
	v := newViewState(testIdle, sender.send, clock.Now, func(channelID string) { switches = append(switches, channelID) })
	return v, sender, clock, &switches
}

func expectSent(t *testing.T, sender *fakeViewSender, want ...string) {
	t.Helper()
	if got := sender.takeSent(); !slices.Equal(got, want) {
		t.Errorf("sent %q, want %q", got, want)
	}
}

func TestViewStateTouch(t *testing.T) {
	ctx := context.Background()
	v, sender, _ := newTestViewState()
	if v.Current() != "" {
		t.Fatalf("Current() = %q before any Touch, want empty", v.Current())
	}

	v.Touch(ctx, "a")
	expectSent(t, sender, "a")
	if v.Current() != "a" {
		t.Errorf("Current() = %q, want a", v.Current())
	}

	v.Touch(ctx, "a")
	expectSent(t, sender)

	v.Touch(ctx, "b")
	expectSent(t, sender, "b")
	if v.Current() != "b" {
		t.Errorf("Current() = %q, want b", v.Current())
	}
}

// traQ sends the stamp events of the viewed channel only, so a switch to a channel is reported (and the
// channel polled once, for the stamps changed before), whenever a send makes it the viewed channel: not when
// it already was, not for a failed send, not for a clear, and again on a new connection.
func TestViewStateSwitches(t *testing.T) {
	ctx := context.Background()
	v, sender, clock, switches := newTestViewStateWithSwitches()
	expect := func(want ...string) {
		t.Helper()
		if !slices.Equal(*switches, want) {
			t.Errorf("switches = %q, want %q", *switches, want)
		}
		*switches = nil
	}
	v.Touch(ctx, "a")
	v.Touch(ctx, "a")
	expect("a")
	v.Touch(ctx, "b")
	expect("b")

	sender.setErr(errors.New("down"))
	v.Touch(ctx, "c")
	expect()
	sender.setErr(nil)

	v.Resend(ctx) // a new connection: c, wanted since the failed send, is sent
	expect("c")

	clock.advance(testIdle)
	v.Tick(ctx) // cleared
	expect()
	v.Touch(ctx, "c")
	expect("c")
}

func TestViewStateTickIdle(t *testing.T) {
	ctx := context.Background()
	v, sender, clock := newTestViewState()

	v.Tick(ctx)
	expectSent(t, sender) // nothing viewed: nothing to clear

	v.Touch(ctx, "a")
	sender.takeSent()
	clock.advance(testIdle - time.Second)
	v.Tick(ctx)
	expectSent(t, sender)
	if v.Current() != "a" {
		t.Errorf("Current() = %q before the idle timeout, want a", v.Current())
	}

	// A Touch of the same channel sends nothing but restarts the idle time.
	v.Touch(ctx, "a")
	clock.advance(testIdle - time.Second)
	v.Tick(ctx)
	expectSent(t, sender)

	clock.advance(2 * time.Second)
	v.Tick(ctx)
	expectSent(t, sender, "")
	if v.Current() != "" {
		t.Errorf("Current() = %q after the idle timeout, want empty", v.Current())
	}

	v.Tick(ctx)
	expectSent(t, sender) // already cleared

	v.Touch(ctx, "a")
	expectSent(t, sender, "a") // viewing again after a clear sends again
}

func TestViewStateResend(t *testing.T) {
	ctx := context.Background()
	v, sender, _ := newTestViewState()

	v.Resend(ctx)
	expectSent(t, sender) // nothing viewed

	v.Touch(ctx, "a")
	sender.takeSent()
	v.Resend(ctx)
	expectSent(t, sender, "a")
	v.Resend(ctx)
	expectSent(t, sender, "a") // every reconnect sends it again
}

func TestViewStateSendError(t *testing.T) {
	ctx := context.Background()
	v, sender, clock := newTestViewState()
	v.Touch(ctx, "a")
	sender.takeSent()

	sender.setErr(errors.New("not connected"))
	v.Touch(ctx, "b")
	if v.Current() != "a" {
		t.Errorf("Current() = %q after a failed send of b, want a", v.Current())
	}
	clock.advance(testIdle + time.Second)
	v.Tick(ctx)
	if v.Current() != "a" {
		t.Errorf("Current() = %q after a failed clear, want a", v.Current())
	}

	sender.setErr(nil)
	v.Touch(ctx, "b")
	expectSent(t, sender, "b") // the next Touch tries again
	if v.Current() != "b" {
		t.Errorf("Current() = %q, want b", v.Current())
	}
}

// A channel change while the WebSocket was down is sent once it is back.
func TestViewStateResendAfterFailedTouch(t *testing.T) {
	ctx := context.Background()
	v, sender, _ := newTestViewState()
	v.Touch(ctx, "a")
	sender.takeSent()

	sender.setErr(errors.New("not connected"))
	v.Touch(ctx, "b")
	sender.setErr(nil)
	v.Resend(ctx)
	expectSent(t, sender, "b")
	if v.Current() != "b" {
		t.Errorf("Current() = %q after Resend, want b", v.Current())
	}
}

// After an idle clear that failed while the WebSocket was down, the new connection (which starts without a
// viewstate) gets nothing and the viewstate counts as cleared.
func TestViewStateResendAfterFailedClear(t *testing.T) {
	ctx := context.Background()
	v, sender, clock := newTestViewState()
	v.Touch(ctx, "a")
	sender.takeSent()

	sender.setErr(errors.New("not connected"))
	clock.advance(testIdle + time.Second)
	v.Tick(ctx)
	sender.setErr(nil)
	v.Resend(ctx)
	expectSent(t, sender)
	if v.Current() != "" {
		t.Errorf("Current() = %q after Resend, want empty", v.Current())
	}
	v.Tick(ctx)
	expectSent(t, sender) // nothing left to clear
}

// A Resend whose send failed (for example timed out) may still have reached traQ, so the idle Tick sends
// the clear although nothing is current, and again after a failed clear.
func TestViewStateTickAfterFailedResend(t *testing.T) {
	ctx := context.Background()
	v, sender, clock := newTestViewState()
	v.Touch(ctx, "a")
	sender.takeSent()

	sender.setErr(errors.New("timed out"))
	v.Resend(ctx)
	if v.Current() != "" {
		t.Fatalf("Current() = %q after a failed Resend, want empty", v.Current())
	}
	sender.setErr(nil)
	clock.advance(testIdle - time.Second)
	v.Tick(ctx)
	expectSent(t, sender) // still active

	clock.advance(2 * time.Second)
	sender.setErr(errors.New("not connected"))
	v.Tick(ctx)
	sender.setErr(nil)
	v.Tick(ctx)
	expectSent(t, sender, "") // the failed clear is retried
	v.Tick(ctx)
	expectSent(t, sender) // cleared; nothing more to send
}

// A Resend that fails leaves the new connection without a viewstate, so the next Touch of the same channel
// sends it.
func TestViewStateResendFails(t *testing.T) {
	ctx := context.Background()
	v, sender, _ := newTestViewState()
	v.Touch(ctx, "a")
	sender.takeSent()

	sender.setErr(errors.New("not connected"))
	v.Resend(ctx)
	if v.Current() != "" {
		t.Errorf("Current() = %q after a failed Resend, want empty", v.Current())
	}
	sender.setErr(nil)
	v.Touch(ctx, "a")
	expectSent(t, sender, "a")
}
