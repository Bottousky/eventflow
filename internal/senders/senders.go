// Package senders delivers notifications to downstream channels. Senders are
// simulated: they log deliveries instead of calling real email/push
// providers, and they support deterministic failure injection so retry and
// dead-letter behavior can be demonstrated and tested without flakiness.
package senders

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/Bottousky/eventflow/internal/events"
)

// Notification is the unit of work handed to a channel sender.
type Notification struct {
	EventID   string
	Recipient string
	Template  string // the event type acts as the template name
	Payload   map[string]string
}

// Sender delivers one notification on one channel.
//
// Returning a *events.PermanentError (or any error wrapping one) signals a
// non-retryable failure: the orchestrator will move the delivery to the
// dead-letter queue without spending the full retry budget. Any other
// non-nil error is treated as transient and retried with backoff.
type Sender interface {
	Channel() events.Channel
	Send(ctx context.Context, n Notification) error
}

// Simulated is a Sender that "delivers" by writing a structured log entry.
//
// FailFirstN makes the first FailFirstN calls fail with a transient error,
// which is how demos and tests exercise the retry path deterministically.
// Set FailFirstN to FailAlways to make every call fail and drive events into
// the dead-letter queue.
//
// If Permanent is non-nil, every call returns it wrapped in a
// *events.PermanentError — useful for modeling providers that respond with
// bad recipient / invalid template and should never be retried.
type Simulated struct {
	Ch         events.Channel
	Logger     *slog.Logger
	FailFirstN int
	Permanent  error

	mu    sync.Mutex
	calls int
}

// FailAlways makes a Simulated sender fail on every call.
const FailAlways = -1

// errTransient is the canonical retryable error returned by Simulated
// senders during the failure-injection window.
var errTransient = fmt.Errorf("transient downstream error (simulated)")

// Channel returns the channel this sender is bound to.
func (s *Simulated) Channel() events.Channel { return s.Ch }

// Send simulates one delivery attempt.
func (s *Simulated) Send(ctx context.Context, n Notification) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	s.calls++
	call := s.calls
	fail := s.FailFirstN == FailAlways || call <= s.FailFirstN
	permanent := s.Permanent
	s.mu.Unlock()

	logger := s.Logger
	if logger == nil {
		logger = slog.Default()
	}
	if permanent != nil {
		logger.Warn("send rejected (permanent)",
			"channel", s.Ch, "event_id", n.EventID, "recipient", n.Recipient, "attempt", call,
			"error", permanent.Error())
		return &events.PermanentError{Cause: errors.New(permanent.Error())}
	}
	if fail {
		logger.Warn("send failed (transient)",
			"channel", s.Ch, "event_id", n.EventID, "recipient", n.Recipient, "attempt", call)
		return fmt.Errorf("send %s notification for event %s: %w", s.Ch, n.EventID, errTransient)
	}
	logger.Info("notification delivered",
		"channel", s.Ch, "event_id", n.EventID, "recipient", n.Recipient, "template", n.Template)
	return nil
}

// Calls reports how many Send attempts were made. Used by tests.
func (s *Simulated) Calls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

// Registry maps channels to their senders.
type Registry map[events.Channel]Sender

// DefaultRegistry builds the standard simulated senders, one per channel.
func DefaultRegistry(logger *slog.Logger) Registry {
	return Registry{
		events.ChannelEmail: &Simulated{Ch: events.ChannelEmail, Logger: logger},
		events.ChannelPush:  &Simulated{Ch: events.ChannelPush, Logger: logger},
		events.ChannelInApp: &Simulated{Ch: events.ChannelInApp, Logger: logger},
	}
}
