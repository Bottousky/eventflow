package kvs

import (
	"testing"
	"time"
)

func TestSetNXSuppressesDuplicates(t *testing.T) {
	current := time.Now()
	s := New(time.Minute, func() time.Time { return current })

	if !s.SetNX("event:1:email") {
		t.Fatal("first SetNX must succeed")
	}
	if s.SetNX("event:1:email") {
		t.Fatal("second SetNX on the same key must be suppressed")
	}
	if !s.SetNX("event:1:push") {
		t.Fatal("SetNX on a different key must succeed")
	}
	if got := s.Len(); got != 2 {
		t.Fatalf("Len() = %d, want 2", got)
	}
}

func TestSetNXExpiresAfterTTL(t *testing.T) {
	current := time.Now()
	s := New(time.Minute, func() time.Time { return current })

	if !s.SetNX("k") {
		t.Fatal("first SetNX must succeed")
	}
	current = current.Add(2 * time.Minute)
	if !s.SetNX("k") {
		t.Fatal("SetNX after TTL expiry must succeed again")
	}
}
