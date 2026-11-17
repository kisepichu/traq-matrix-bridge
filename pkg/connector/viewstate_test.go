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
	sender := &fakeViewSender{}
	clock := &fakeClock{now: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)}
	return newViewState(testIdle, sender.send, clock.Now), sender, clock
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
