package store

import (
	"context"
	"errors"
	"testing"

	"github.com/Bottousky/eventflow/internal/events"
)

// TestDeadLetterRollsBackOnInsertFailure is the regression test for the
// original non-atomic MarkDead bug. The old code first updated the
// notification status to "dead" and then inserted the dead_letters row
// in two separate SQL statements; a failure in the second statement
// left a `dead` notification with no DLQ entry, so a future replay
// would skip the notification and silently lose the operator signal.
//
// We force the INSERT to fail by renaming the dead_letters table right
// before MarkDead. SQLite reports "no such table" for the INSERT
// inside the transaction; the UPDATE that ran before it must roll
// back so the notification stays at `pending` and a future
// ProcessOnce retries the delivery.
//
// This test runs in `package store` (not `package store_test`) so it
// can reach the *sql.DB directly to inject the schema failure without
// exposing a public test helper.
func TestDeadLetterRollsBackOnInsertFailure(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	n, err := s.EnsureNotification(ctx, "evt_rollback", events.ChannelEmail)
	if err != nil {
		t.Fatalf("ensure: %v", err)
	}
	if n.Status != events.StatusPending {
		t.Fatalf("new notification status = %s, want pending", n.Status)
	}

	// Rename the table so the INSERT inside MarkDead's transaction
	// fails. Renaming is reversible and lets us verify the rollback
	// without recreating the whole schema mid-test.
	if _, err := s.db.ExecContext(ctx, "ALTER TABLE dead_letters RENAME TO dead_letters_x"); err != nil {
		t.Fatalf("rename dead_letters: %v", err)
	}

	err = s.MarkDead(ctx, n, errors.New("terminal-2"))
	if err == nil {
		t.Fatal("MarkDead must fail when dead_letters table is missing")
	}

	// Roll the table back so the post-conditions can be checked
	// without further SQL errors.
	if _, err := s.db.ExecContext(ctx, "ALTER TABLE dead_letters_x RENAME TO dead_letters"); err != nil {
		t.Fatalf("rename back: %v", err)
	}

	// The notification status must still be pending: the UPDATE was
	// inside the transaction and must have rolled back when the
	// INSERT failed. This is the bug the original code had.
	got, err := s.GetNotification(ctx, "evt_rollback", events.ChannelEmail)
	if err != nil {
		t.Fatalf("get after rollback: %v", err)
	}
	if got.Status != events.StatusPending {
		t.Fatalf("status = %s after rollback, want pending (atomicity broken: UPDATE leaked past failed INSERT)", got.Status)
	}

	// DLQ must be empty for this event_id.
	dlq, err := s.DeadLetters(ctx)
	if err != nil {
		t.Fatalf("dead letters: %v", err)
	}
	for _, d := range dlq {
		if d.EventID == "evt_rollback" {
			t.Fatalf("DLQ row exists for the rolled-back event: %+v", d)
		}
	}
}

// TestMarkDeadHappyPath is the positive control for the atomicity
// guarantee: when both writes succeed, the row is `dead` and a DLQ
// entry exists. Together with TestDeadLetterRollsBackOnInsertFailure
// this pins down both halves of the all-or-nothing contract.
func TestMarkDeadHappyPath(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	n, err := s.EnsureNotification(ctx, "evt_happy", events.ChannelEmail)
	if err != nil {
		t.Fatalf("ensure: %v", err)
	}
	if err := s.MarkDead(ctx, n, errors.New("terminal-3")); err != nil {
		t.Fatalf("mark dead: %v", err)
	}
	got, err := s.GetNotification(ctx, "evt_happy", events.ChannelEmail)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Status != events.StatusDead {
		t.Fatalf("status = %s, want dead", got.Status)
	}
	dlq, err := s.DeadLetters(ctx)
	if err != nil {
		t.Fatalf("dead letters: %v", err)
	}
	found := false
	for _, d := range dlq {
		if d.EventID == "evt_happy" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("DLQ missing row for evt_happy: %+v", dlq)
	}
}
