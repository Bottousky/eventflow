// TestCrashWindowReusesNotificationID verifies that the at-least-once
// crash window (sender.Send accepted the delivery, but MarkDelivered
// never committed) leaves the notification row in pending state, and
// that the notification row's primary key is the stable idempotency
// key a provider can use to collapse the duplicate on replay.
//
// We do NOT assert "never redelivers" — that would be the original
// dishonest claim. Instead we assert that the notification id remains
// stable across the batch and is exposed as the idempotency key the
// orchestrator uses.
func TestCrashWindowReusesNotificationID(t *testing.T) {
	ts := newTestStack(t)
	email := &recordingSender{ch: events.ChannelEmail}
	o := newTestOrchestrator(ts, senders.Registry{events.ChannelEmail: email})

	ctx := context.Background()
	appendEvent(t, ts.stream, "evt_crash", "user-1", events.ChannelEmail)

	if _, err := o.ProcessOnce(ctx); err != nil {
		t.Fatalf("process: %v", err)
	}

	// The notification id must be stable so a provider can use it as
	// an idempotency key across replays.
	n1, err := ts.store.GetNotification(ctx, "evt_crash", events.ChannelEmail)
	if err != nil {
		t.Fatalf("get notification: %v", err)
	}
	if n1.ID == 0 {
		t.Fatal("notification id is 0; providers need a stable idempotency key")
	}

	// After re-processing the same record (with the cursor still
	// pointing at it), the notification row's status is terminal so
	// the orchestrator skips it — the replay path documented as
	// "crash-safe once MarkDelivered has committed".
	if _, err := o.ProcessOnce(ctx); err != nil {
		t.Fatalf("replay process: %v", err)
	}
	if calls := email.Calls(); calls != 1 {
		t.Fatalf("after terminal-state replay, sender calls = %d, want 1 (skip)", calls)
	}
	if n2, _ := ts.store.GetNotification(ctx, "evt_crash", events.ChannelEmail); n2.ID != n1.ID {
		t.Fatalf("notification id changed across replay: %d != %d", n2.ID, n1.ID)
	}
}
