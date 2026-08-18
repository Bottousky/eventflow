// Package kvs provides a small in-memory key-value store used for
// idempotency. In a production deployment this component maps to Redis or
// Memcached; the in-memory implementation keeps the demo self-contained
// while preserving the same check-and-set semantics.
package kvs

import (
	"sync"
	"time"
)

// Store is a goroutine-safe key-value store with per-key TTL.
type Store struct {
	mu    sync.Mutex
	items map[string]time.Time // key -> expiration time
	ttl   time.Duration
	clock func() time.Time
}

// New returns a Store whose keys expire after ttl. clock is injectable so
// tests can control time deterministically; pass time.Now in production.
func New(ttl time.Duration, clock func() time.Time) *Store {
	return &Store{items: make(map[string]time.Time), ttl: ttl, clock: clock}
}

// SetNX sets key only if it is absent or expired, mirroring Redis SET NX PX.
// It reports whether the key was set by this call. A false return means the
// key already exists and the caller must treat the operation as a duplicate.
func (s *Store) SetNX(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if exp, ok := s.items[key]; ok && s.clock().Before(exp) {
		return false
	}
	s.items[key] = s.clock().Add(s.ttl)
	return true
}

// Delete removes key. It exists mainly for tests and operational tooling.
func (s *Store) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.items, key)
}

// Len reports the number of unexpired keys, exposed for metrics and tests.
func (s *Store) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.clock()
	alive := 0
	for _, exp := range s.items {
		if now.Before(exp) {
			alive++
		}
	}
	return alive
}
