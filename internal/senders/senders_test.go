package senders

import (
	"context"
	"errors"
	"testing"

	"github.com/Bottousky/eventflow/internal/events"
)

func testNotification() Notification {
	return Notification{
		EventID:   "evt_1",
		Recipient: "ana@example.com",
		Template:  "order.shipped",
		Payload:   map[string]string{"order_id": "ORD-1"},
	}
}

func TestSimulatedFailsFirstNThenDelivers(t *testing.T) {
	s := &Simulated{Ch: events.ChannelEmail, FailFirstN: 2}
	n := testNotification()

	if err := s.Send(context.Background(), n); err == nil {
		t.Fatal("attempt 1 must fail")
	}
	if err := s.Send(context.Background(), n); err == nil {
		t.Fatal("attempt 2 must fail")
	}
	if err := s.Send(context.Background(), n); err != nil {
		t.Fatalf("attempt 3 must succeed, got %v", err)
	}
	if got := s.Calls(); got != 3 {
		t.Fatalf("Calls() = %d, want 3", got)
	}
}

func TestSimulatedFailAlways(t *testing.T) {
	s := &Simulated{Ch: events.ChannelPush, FailFirstN: FailAlways}
	for i := 0; i < 5; i++ {
		if err := s.Send(context.Background(), testNotification()); err == nil {
			t.Fatalf("attempt %d must fail", i+1)
		}
	}
}

func TestSimulatedPermanentErrorIsDetected(t *testing.T) {
	s := &Simulated{Ch: events.ChannelInApp, Permanent: errors.New("bad recipient")}
	err := s.Send(context.Background(), testNotification())
	if err == nil {
		t.Fatal("permanent error must be returned")
	}
	if !events.IsPermanent(err) {
		t.Fatalf("IsPermanent must detect the error, got %v", err)
	}
}

func TestSendRespectsCanceledContext(t *testing.T) {
	s := &Simulated{Ch: events.ChannelInApp}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.Send(ctx, testNotification()); err == nil {
		t.Fatal("Send with canceled context must fail")
	}
	if got := s.Calls(); got != 0 {
		t.Fatalf("canceled Send must not count as an attempt, got %d", got)
	}
}
