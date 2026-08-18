package store

import (
	"context"
	"errors"
	"testing"

	"github.com/Bottousky/eventflow/internal/events"
)

func openTestStore(t *testing.T) *Store {
	t.Helper()
	db, err := Open(":memory:")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	s, err := New(db)
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	return s
}

func TestEnsureNotificationIsIdempotent(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	n1, err := s.EnsureNotification(ctx, "evt_1", events.ChannelEmail)
	if err != nil {
		t.Fatalf("ensure: %v", err)
	}
	if n1.Status != events.StatusPending {
		t.Fatalf("new notification status = %s, want pending", n1.Status)
	}
	n2, err := s.EnsureNotification(ctx, "evt_1", events.ChannelEmail)
	if err != nil {
		t.Fatalf("ensure again: %v", err)
	}
	if n1.ID != n2.ID {
		t.Fatalf("EnsureNotification must return the same row, got ids %d and %d", n1.ID, n2.ID)
	}
}

func TestAttemptsDeliveredAndDeadLetterFlow(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	n, err := s.EnsureNotification(ctx, "evt_2", events.ChannelPush)
	if err != nil {
		t.Fatalf("ensure: %v", err)
	}
	if err := s.RecordAttempt(ctx, n.ID, errors.New("boom-1")); err != nil {
		t.Fatalf("record attempt: %v", err)
	}
	if err := s.RecordAttempt(ctx, n.ID, errors.New("boom-2")); err != nil {
		t.Fatalf("record attempt: %v", err)
	}
	got, err := s.GetNotification(ctx, "evt_2", events.ChannelPush)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Attempts != 2 || got.LastError != "boom-2" {
		t.Fatalf("attempts = %d last_error = %q, want 2 / boom-2", got.Attempts, got.LastError)
	}

	if err := s.MarkDead(ctx, got, errors.New("boom-2")); err != nil {
		t.Fatalf("mark dead: %v", err)
	}
	dead, err := s.DeadLetters(ctx)
	if err != nil {
		t.Fatalf("dead letters: %v", err)
	}
	if len(dead) != 1 || dead[0].EventID != "evt_2" {
		t.Fatalf("dead letters = %+v", dead)
	}
}

func TestCursorRoundTrip(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	seq, err := s.Cursor(ctx)
	if err != nil {
		t.Fatalf("cursor: %v", err)
	}
	if seq != 0 {
		t.Fatalf("initial cursor = %d, want 0", seq)
	}
	if err := s.SetCursor(ctx, 41); err != nil {
		t.Fatalf("set cursor: %v", err)
	}
	seq, err = s.Cursor(ctx)
	if err != nil {
		t.Fatalf("cursor: %v", err)
	}
	if seq != 41 {
		t.Fatalf("cursor = %d, want 41", seq)
	}
}

func TestGetNotificationNotFound(t *testing.T) {
	s := openTestStore(t)
	_, err := s.GetNotification(context.Background(), "missing", events.ChannelEmail)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
	_, err = s.GetNotificationByID(context.Background(), 9999)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound by id, got %v", err)
	}
}

func TestNotificationsForEvent(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	if _, err := s.EnsureNotification(ctx, "evt_3", events.ChannelEmail); err != nil {
		t.Fatalf("ensure email: %v", err)
	}
	if _, err := s.EnsureNotification(ctx, "evt_3", events.ChannelPush); err != nil {
		t.Fatalf("ensure push: %v", err)
	}
	got, err := s.NotificationsForEvent(ctx, "evt_3")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("notifications for evt_3 = %d, want 2", len(got))
	}
}
