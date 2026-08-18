package stream

import (
	"context"
	"errors"
	"testing"

	"github.com/Bottousky/eventflow/internal/events"
	"github.com/Bottousky/eventflow/internal/store"
)

func openTestStream(t *testing.T) *Stream {
	t.Helper()
	db, err := store.Open(":memory:")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	st, err := New(db)
	if err != nil {
		t.Fatalf("create stream: %v", err)
	}
	return st
}

func testEvent(id, key string) events.Event {
	return events.Event{
		ID:          id,
		Type:        "order.shipped",
		OrderingKey: key,
		Recipient:   "ana@example.com",
		Channels:    []events.Channel{events.ChannelEmail},
		Payload:     map[string]string{"order_id": "ORD-1"},
	}
}

func TestAppendAssignsIncreasingSequences(t *testing.T) {
	st := openTestStream(t)
	ctx := context.Background()

	var seqs []int64
	for _, id := range []string{"a", "b", "c"} {
		seq, err := st.Append(ctx, testEvent(id, "user-1"))
		if err != nil {
			t.Fatalf("append %s: %v", id, err)
		}
		seqs = append(seqs, seq)
	}
	if !(seqs[0] < seqs[1] && seqs[1] < seqs[2]) {
		t.Fatalf("sequences must increase, got %v", seqs)
	}
}

func TestReadAfterPreservesAppendOrderPerOrderingKey(t *testing.T) {
	st := openTestStream(t)
	ctx := context.Background()

	// Interleave two ordering keys; per-key order must be preserved.
	inputs := []struct{ id, key string }{
		{"u1-first", "user-1"},
		{"u2-first", "user-2"},
		{"u1-second", "user-1"},
		{"u2-second", "user-2"},
		{"u1-third", "user-1"},
	}
	for _, in := range inputs {
		if _, err := st.Append(ctx, testEvent(in.id, in.key)); err != nil {
			t.Fatalf("append %s: %v", in.id, err)
		}
	}

	records, err := st.ReadAfter(ctx, 0, 100)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(records) != len(inputs) {
		t.Fatalf("got %d records, want %d", len(records), len(inputs))
	}
	for i, in := range inputs {
		if records[i].Event.ID != in.id {
			t.Fatalf("record %d = %s, want %s (global append order)", i, records[i].Event.ID, in.id)
		}
	}
	// Per-key view: filter by ordering key, order must be the append order.
	var u1 []string
	for _, r := range records {
		if r.Event.OrderingKey == "user-1" {
			u1 = append(u1, r.Event.ID)
		}
	}
	want := []string{"u1-first", "u1-second", "u1-third"}
	for i := range want {
		if u1[i] != want[i] {
			t.Fatalf("user-1 order = %v, want %v", u1, want)
		}
	}
}

func TestReadAfterCursorAndLimit(t *testing.T) {
	st := openTestStream(t)
	ctx := context.Background()
	for _, id := range []string{"a", "b", "c", "d"} {
		if _, err := st.Append(ctx, testEvent(id, "user-1")); err != nil {
			t.Fatalf("append: %v", err)
		}
	}

	first, err := st.ReadAfter(ctx, 0, 2)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(first) != 2 || first[0].Event.ID != "a" || first[1].Event.ID != "b" {
		t.Fatalf("first page wrong: %+v", first)
	}
	rest, err := st.ReadAfter(ctx, first[len(first)-1].Seq, 100)
	if err != nil {
		t.Fatalf("read rest: %v", err)
	}
	if len(rest) != 2 || rest[0].Event.ID != "c" || rest[1].Event.ID != "d" {
		t.Fatalf("cursor continuation wrong: %+v", rest)
	}
}

func TestAppendDuplicateIDIsRejected(t *testing.T) {
	st := openTestStream(t)
	ctx := context.Background()
	if _, err := st.Append(ctx, testEvent("dup", "user-1")); err != nil {
		t.Fatalf("first append: %v", err)
	}
	if _, err := st.Append(ctx, testEvent("dup", "user-1")); !errors.Is(err, ErrDuplicateID) {
		t.Fatalf("duplicate append must return ErrDuplicateID, got %v", err)
	}
}

func TestGetRoundTrip(t *testing.T) {
	st := openTestStream(t)
	ctx := context.Background()
	e := testEvent("evt-x", "user-9")
	e.Channels = []events.Channel{events.ChannelEmail, events.ChannelInApp}
	if _, err := st.Append(ctx, e); err != nil {
		t.Fatalf("append: %v", err)
	}
	got, err := st.Get(ctx, "evt-x")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.ID != e.ID || got.OrderingKey != e.OrderingKey || len(got.Channels) != 2 {
		t.Fatalf("round trip mismatch: %+v", got)
	}
	if got.Payload["order_id"] != "ORD-1" {
		t.Fatalf("payload lost: %+v", got.Payload)
	}
}
