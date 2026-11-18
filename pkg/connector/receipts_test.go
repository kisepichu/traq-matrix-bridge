package connector

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/id"

	"github.com/kisepichu/traq-matrix-bridge/pkg/traq"
)

func TestOwnReceiptEchoes(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)}
	r := ownReceipts{now: clock.Now}
	a, b := PortalKey("ch-a", ""), PortalKey("ch-b", "")
	t1 := time.UnixMilli(1_700_000_000_000)
	t2, t3 := t1.Add(time.Second), t1.Add(2*time.Second)
	older := &database.Message{ID: "m-1", Timestamp: t1}
	target := &database.Message{ID: "m-2", PartID: "file", Timestamp: t2}
	newer := &database.Message{ID: "m-3", Timestamp: t3}

	if r.isEcho(a, target) {
		t.Error("a receipt is an echo before any own receipt was sent")
	}
	r.sent(a, target)
	for _, tc := range []struct {
		name  string
		key   networkid.PortalKey
		exact *database.Message
		want  bool
	}{
		{"the target", a, target, true},
		{"another part of the target", a, &database.Message{ID: "m-2", PartID: "text", Timestamp: t2}, true},
		{"an older message", a, older, true},
		{"a newer message", a, newer, false},
		{"an event that is no bridged message", a, nil, false},
		{"another portal", b, target, false},
	} {
		if got := r.isEcho(tc.key, tc.exact); got != tc.want {
			t.Errorf("receipt on %s: isEcho = %v, want %v", tc.name, got, tc.want)
		}
	}

	clock.advance(ownReceiptEchoWindow - time.Second)
	if !r.isEcho(a, target) {
		t.Error("receipt on the target just inside the window: not an echo")
	}
	clock.advance(time.Second)
	if r.isEcho(a, target) {
		t.Error("receipt on the target once the window has passed: still an echo")
	}

	// A later own receipt replaces the earlier one, and starts its own window.
	r.sent(a, older)
	if r.isEcho(a, target) {
		t.Error("receipt on a message newer than the latest own receipt: an echo")
	}
	if !r.isEcho(a, older) {
		t.Error("receipt on the target of the latest own receipt: not an echo")
	}
}

// The receipt bridged from traQ targets the newest bridged message at or before its time, and its echo from
// the homeserver (which may have dropped the double puppet marker) neither marks the channel read on traQ nor
// moves the viewstate; a receipt on a newer message still does both.
func TestMatrixReadReceiptIgnoresOwnEcho(t *testing.T) {
	ctx := t.Context()
	db := newTestBridgeDB(t)
	key := PortalKey("ch-a", "")
	if err := db.Portal.Insert(ctx, &database.Portal{BridgeID: "test", PortalKey: key, MXID: "!a:example.com", Metadata: &PortalMetadata{}}); err != nil {
		t.Fatal(err)
	}
	if err := db.Ghost.Insert(ctx, &database.Ghost{BridgeID: "test", ID: "u-bob"}); err != nil {
		t.Fatal(err)
	}
	t1 := time.UnixMilli(1_700_000_000_000)
	msgs := map[networkid.MessageID]*database.Message{}
	for i, mid := range []networkid.MessageID{"m-1", "m-2", "m-3"} {
		msg := &database.Message{
			ID: mid, MXID: id.EventID("$" + mid), Room: key, SenderID: "u-bob",
			Timestamp: t1.Add(time.Duration(i) * time.Second), Metadata: &MessageMetadata{},
		}
		if err := db.Message.Insert(ctx, msg); err != nil {
			t.Fatal(err)
		}
		msgs[mid] = msg
	}

	var mu sync.Mutex
	var reads []string
	mux := http.NewServeMux()
	mux.HandleFunc("DELETE /api/v3/users/me/unread/{id}", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		reads = append(reads, r.PathValue("id"))
		mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	})
	takeReads := func() []string {
		mu.Lock()
		defer mu.Unlock()
		got := reads
		reads = nil
		return got
	}
	view, sender, _ := newTestViewState()
	c := &TraQClient{API: newMemAPI(t, mux), Login: testLogin("u-me", false), view: view}
	c.Login.Bridge.DB = db
	portal := &bridgev2.Portal{Portal: &database.Portal{BridgeID: "test", PortalKey: key}, Bridge: c.Login.Bridge}

	// Read on traQ between m-2 and m-3.
	evt := c.ownReceipt(key, &traq.Me{User: traq.User{ID: "u-me"}}, nil, t1.Add(1500*time.Millisecond))
	if res := evt.PreHandleWithResult(ctx, portal); res != nil {
		t.Fatalf("PreHandleWithResult = %+v, want nil (handled by bridgev2)", res)
	}
	if evt.LastTarget != "m-2" {
		t.Errorf("LastTarget = %q, want m-2", evt.LastTarget)
	}

	receipt := func(mid networkid.MessageID) {
		t.Helper()
		if err := c.HandleMatrixReadReceipt(ctx, &bridgev2.MatrixReadReceipt{
			Portal: portal, EventID: msgs[mid].MXID, ExactMessage: msgs[mid], ReadUpTo: msgs[mid].Timestamp,
		}); err != nil {
			t.Fatalf("HandleMatrixReadReceipt(%s): %v", mid, err)
		}
	}
	receipt("m-2")
	if got := takeReads(); len(got) != 0 {
		t.Errorf("echo of the own receipt marked %v read on traQ", got)
	}
	expectSent(t, sender)

	receipt("m-3")
	if got := takeReads(); len(got) != 1 || got[0] != "ch-a" {
		t.Errorf("receipt on a newer message marked %v read, want [ch-a]", got)
	}
	expectSent(t, sender, "ch-a")
}

// Without a bridged message at or before its time, the receipt is left to bridgev2 (which ignores it) and
// nothing is remembered.
func TestOwnReceiptWithoutTarget(t *testing.T) {
	ctx := context.Background()
	c := &TraQClient{Login: testLogin("u-me", false)}
	c.Login.Bridge.DB = newTestBridgeDB(t)
	key := PortalKey("ch-a", "")
	portal := &bridgev2.Portal{Portal: &database.Portal{BridgeID: "test", PortalKey: key}, Bridge: c.Login.Bridge}
	evt := c.ownReceipt(key, &traq.Me{User: traq.User{ID: "u-me"}}, nil, time.Now())
	if res := evt.PreHandleWithResult(ctx, portal); res != nil {
		t.Fatalf("PreHandleWithResult = %+v, want nil", res)
	}
	if evt.LastTarget != "" {
		t.Errorf("LastTarget = %q, want empty", evt.LastTarget)
	}
	if c.ownReceipts.isEcho(key, &database.Message{ID: "m-0"}) {
		t.Error("a receipt is an echo though no own receipt had a target")
	}
}
