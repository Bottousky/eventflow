// Package events defines the domain types shared by the API, the event
// stream, the orchestrator and the senders.
package events

import (
	"errors"
	"fmt"
	"slices"
	"time"
)

// Channel is a notification delivery channel.
type Channel string

const (
	ChannelEmail Channel = "email"
	ChannelPush  Channel = "push"
	ChannelInApp Channel = "in_app"
)

// Channels lists every supported delivery channel.
var Channels = []Channel{ChannelEmail, ChannelPush, ChannelInApp}

// Valid reports whether c is a supported channel.
func (c Channel) Valid() bool {
	return slices.Contains(Channels, c)
}

// Event is an inbound domain event accepted by the API and appended to the
// stream. Events sharing an OrderingKey are delivered in append order.
type Event struct {
	ID          string            `json:"event_id"`
	Type        string            `json:"type"`
	OrderingKey string            `json:"ordering_key"`
	Recipient   string            `json:"recipient"`
	Channels    []Channel         `json:"channels"`
	Payload     map[string]string `json:"payload,omitempty"`
	CreatedAt   time.Time         `json:"created_at"`
}

// Validate checks the fields the API is required to enforce.
func (e Event) Validate() error {
	if e.Type == "" {
		return errors.New("type is required")
	}
	if e.OrderingKey == "" {
		return errors.New("ordering_key is required")
	}
	if e.Recipient == "" {
		return errors.New("recipient is required")
	}
	if len(e.Channels) == 0 {
		return errors.New("channels must not be empty")
	}
	for _, ch := range e.Channels {
		if !ch.Valid() {
			return fmt.Errorf("unsupported channel %q", ch)
		}
	}
	return nil
}

// Status is the lifecycle state of a single channel notification.
type Status string

const (
	StatusPending   Status = "pending"
	StatusDelivered Status = "delivered"
	StatusDead      Status = "dead"
)

// Notification tracks one channel notification for an event. Each row
// represents the orchestration history for a (event_id, channel) pair:
// how many attempts were made, the last error and the final status.
type Notification struct {
	ID        int64     `json:"id"`
	EventID   string    `json:"event_id"`
	Channel   Channel   `json:"channel"`
	Status    Status    `json:"status"`
	Attempts  int       `json:"attempts"`
	LastError string    `json:"last_error,omitempty"`
	UpdatedAt time.Time `json:"updated_at"`
}

// PermanentError marks an error as non-retryable. The orchestrator
// short-circuits to the dead-letter queue when a sender returns one,
// which models providers that respond 4xx-style (bad recipient, invalid
// template, etc.) without ever succeeding.
type PermanentError struct {
	Cause error
}

func (e *PermanentError) Error() string { return "permanent: " + e.Cause.Error() }
func (e *PermanentError) Unwrap() error { return e.Cause }

// IsPermanent reports whether err is or wraps a PermanentError.
func IsPermanent(err error) bool {
	var p *PermanentError
	return errors.As(err, &p)
}
