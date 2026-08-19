// Package orchestrator consumes the event stream in order and fans events
// out to channel senders with idempotency, retries, exponential backoff
// and a dead-letter queue.
//
// Delivery semantics: at-least-once with durable/local deduplication and
// reusable provider idempotency keys. The notification row in the store
// is the source of truth for terminal state: once a channel is marked
// `delivered` or `dead`, no further send is attempted for it. The
// in-memory KVS suppresses in-flight duplicates across worker replicas.
//
// The window between Sender.Send() returning success and MarkDelivered()
// committing is still at-least-once: a crash in that window causes the
// next attempt to re-send. The recommended mitigation in production is
// to pass a stable delivery_id / idempotency_key derived from the
// notification row's primary key to the provider, so the provider can
// deduplicate if it supports that capability.
package orchestrator

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/Bottousky/eventflow/internal/events"
	"github.com/Bottousky/eventflow/internal/kvs"
	"github.com/Bottousky/eventflow/internal/obs"
	"github.com/Bottousky/eventflow/internal/senders"
	"github.com/Bottousky/eventflow/internal/store"
	"github.com/Bottousky/eventflow/internal/stream"
)
