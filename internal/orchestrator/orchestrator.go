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
	"fmt"
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

// Config tunes the worker. Sleep is injectable so tests run without real
// waiting; the other fields are loaded from environment variables via the
// config package.
type Config struct {
	PollInterval   time.Duration // wait between empty polls in Run
	BatchSize      int           // max stream records per poll
	MaxAttempts    int           // send attempts before dead-lettering
	BaseBackoff    time.Duration // first retry wait; doubles each attempt
	MaxConcurrency int           // max in-flight ordering-key partitions per batch
	Sleep          func(ctx context.Context, d time.Duration) error
}

// DefaultConfig returns production-sane defaults. WorkerConcurrency is
// clamped to at least 1 by ProcessOnce.
func DefaultConfig() Config {
	return Config{
		PollInterval:   500 * time.Millisecond,
		BatchSize:      100,
		MaxAttempts:    3,
		BaseBackoff:    100 * time.Millisecond,
		MaxConcurrency: 4,
		Sleep: func(ctx context.Context, d time.Duration) error {
			t := time.NewTimer(d)
			defer t.Stop()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-t.C:
				return nil
			}
		},
	}
}

// ChannelResult describes the outcome of delivering one event on one
// channel. Terminal results mean the notification row has reached a
// durable terminal state (delivered or dead) and no further work is
// needed for that (event, channel) pair. Pending and
// DuplicateSuppressed both keep the row at `pending` and require a
// future attempt.
type ChannelResult int

const (
	// ChannelPending: the channel did not reach a terminal durable state
	// (context canceled, transient infrastructure failure). The cursor
	// must not advance past the surrounding record; the KVS lock is
	// released so the next attempt can retry.
	ChannelPending ChannelResult = iota
	// ChannelDelivered: MarkDelivered committed successfully.
	ChannelDelivered
	// ChannelDead: MarkDead committed successfully (terminal state, plus
	// a dead_letters row).
	ChannelDead
	// ChannelDuplicateSuppressed: the KVS reported this delivery is
	// already in flight on another worker. The local orchestrator does
	// not own the outcome and must wait for the other worker to commit
	// before the row becomes terminal.
	ChannelDuplicateSuppressed
)

// Terminal reports whether the channel result is durable and final.
func (r ChannelResult) Terminal() bool {
	return r == ChannelDelivered || r == ChannelDead
}

// RecordResult describes the outcome of processing one stream record.
// A record is terminal only when every channel it carries reached a
// terminal state; otherwise the cursor must not advance past it and
// later records in the same ordering_key partition must wait.
type RecordResult int

const (
	// RecordPending: at least one channel did not reach a terminal state.
	// The global cursor stops before this seq and the ordering_key
	// goroutine stops before processing further records.
	RecordPending RecordResult = iota
	// RecordTerminal: every channel reached a terminal durable state.
	RecordTerminal
)

// Orchestrator wires the stream, store, KVS and senders together.
type Orchestrator struct {
	stream  *stream.Stream
	store   *store.Store
	kvs     *kvs.Store
	senders senders.Registry
	metrics *obs.Metrics
	logger  *slog.Logger
	cfg     Config
}

// New builds an Orchestrator. logger may be nil (slog.Default is used).
func New(st *stream.Stream, db *store.Store, kv *kvs.Store, reg senders.Registry,
	metrics *obs.Metrics, logger *slog.Logger, cfg Config) *Orchestrator {
	if logger == nil {
		logger = slog.Default()
	}
	if cfg.MaxConcurrency < 1 {
		cfg.MaxConcurrency = 1
	}
	return &Orchestrator{
		stream: st, store: db, kvs: kv, senders: reg,
		metrics: metrics, logger: logger, cfg: cfg,
	}
}

// Run polls the stream until ctx is canceled.
func (o *Orchestrator) Run(ctx context.Context) error {
	o.logger.Info("orchestrator started",
		"poll_interval", o.cfg.PollInterval,
		"batch_size", o.cfg.BatchSize,
		"max_attempts", o.cfg.MaxAttempts,
		"max_concurrency", o.cfg.MaxConcurrency)
	for {
		if err := ctx.Err(); err != nil {
			o.logger.Info("orchestrator stopped")
			return nil
		}
		processed, err := o.ProcessOnce(ctx)
		if err != nil {
			o.logger.Error("process batch failed", "error", err)
		}
		if processed == 0 {
			if err := o.cfg.Sleep(ctx, o.cfg.PollInterval); err != nil {
				return nil // context canceled while idle
			}
		}
	}
}

// ProcessOnce processes at most BatchSize pending stream records. Records
// are grouped by ordering_key: each group is processed sequentially (to
// preserve per-key order) while different groups run in parallel up to
// cfg.MaxConcurrency.
//
// Cursor advance is gated on terminal state: a record is "done" only
// when every channel it carries reached a terminal durable state
// (delivered or dead). A record that ends in a non-terminal state
// (pending, duplicate-suppressed) blocks the cursor for itself and for
// every record behind it in the same ordering_key partition, so that
// future ProcessOnce calls can re-read and finish the work without
// breaking per-key ordering.
//
// Returns the number of records that reached RecordTerminal in this
// batch.
func (o *Orchestrator) ProcessOnce(ctx context.Context) (int, error) {
	cursor, err := o.store.Cursor(ctx)
	if err != nil {
		return 0, err
	}
	records, err := o.stream.ReadAfter(ctx, cursor, o.cfg.BatchSize)
	if err != nil {
		return 0, err
	}
	if len(records) == 0 {
		return 0, nil
	}

	groups := groupByOrderingKey(records)
	concurrency := o.cfg.MaxConcurrency
	if concurrency > len(groups) {
		concurrency = len(groups)
	}
	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup

	var resultsMu sync.Mutex
	results := make(map[int64]RecordResult, len(records))

	for _, group := range groups {
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			wg.Wait()
			return 0, ctx.Err()
		}
		wg.Add(1)
		go func(g []stream.Record) {
			defer wg.Done()
			defer func() { <-sem }()
			for _, rec := range g {
				if err := ctx.Err(); err != nil {
					return
				}
				start := time.Now()
				r := o.processRecord(ctx, rec)
				o.metrics.ObserveDuration(time.Since(start))
				resultsMu.Lock()
				results[rec.Seq] = r
				resultsMu.Unlock()
				if r == RecordPending {
					// Non-terminal: do not process further records in this
					// ordering_key partition. The remaining records will be
					// re-read on the next ProcessOnce once this one reaches
					// a terminal state. This is what preserves per-key
					// ordering across worker restarts and infrastructure
					// failures.
					return
				}
			}
		}(group)
	}
	wg.Wait()
	if err := ctx.Err(); err != nil {
		return 0, ctx.Err()
	}

	// Advance the cursor through the longest contiguous prefix of records
	// whose result is RecordTerminal. A Pending result (or a missing
	// result, e.g. when ctx was canceled) stops the advance: the records
	// behind it will be re-read on the next ProcessOnce.
	terminal := 0
	var advanced int64
	for _, rec := range records {
		r, ok := results[rec.Seq]
		if !ok || r != RecordTerminal {
			break
		}
		advanced = rec.Seq
		terminal++
	}
	if advanced > cursor {
		if err := o.store.SetCursor(ctx, advanced); err != nil {
			return terminal, err
		}
	}
	return terminal, nil
}

// groupByOrderingKey partitions records by OrderingKey while keeping
// the global seq order inside each partition. The returned slice's order
// is the order of the first appearance of each key in the input.
func groupByOrderingKey(records []stream.Record) [][]stream.Record {
	idx := make(map[string]int, len(records))
	groups := make([][]stream.Record, 0, len(records))
	for _, r := range records {
		i, ok := idx[r.Event.OrderingKey]
		if !ok {
			i = len(groups)
			idx[r.Event.OrderingKey] = i
			groups = append(groups, nil)
		}
		groups[i] = append(groups[i], r)
	}
	return groups
}

// processRecord fans one event out to its channels and reports whether
// every channel reached a terminal durable state. A single non-terminal
// channel flips the record to RecordPending so the cursor stops before
// this seq.
func (o *Orchestrator) processRecord(ctx context.Context, rec stream.Record) RecordResult {
	o.metrics.Inc(obs.EventsProcessed)
	log := o.logger.With("event_id", rec.Event.ID, "event_type", rec.Event.Type, "seq", rec.Seq)
	log.Info("processing event", "ordering_key", rec.Event.OrderingKey, "channels", rec.Event.Channels)
	pending := false
	for _, ch := range rec.Event.Channels {
		result := o.deliver(ctx, rec.Event, ch)
		if !result.Terminal() {
			pending = true
			log.Warn("channel not terminal", "channel", ch, "result", result)
		}
	}
	if pending {
		return RecordPending
	}
	return RecordTerminal
}

// deliver sends one event on one channel with idempotency and retries
// and returns the resulting ChannelResult. The notification store is
// the source of truth for terminal state, so a reprocessed event whose
// delivery is already finalized (delivered or dead) is skipped without
// re-sending.
//
// The KVS lock is released on every non-terminal result so the next
// worker can retry without waiting for the TTL (24h) to expire. The
// lock is kept (until TTL) for terminal results so concurrent replicas
// that observe the in-progress delivery still skip it.
//
// Idempotency keys for external providers should be derived from the
// notification row's primary key (n.ID), which is stable across replays.
// Providers that support idempotency can collapse the duplicate that
// the at-least-once crash window can otherwise produce.
func (o *Orchestrator) deliver(ctx context.Context, e events.Event, ch events.Channel) ChannelResult {
	log := o.logger.With("event_id", e.ID, "channel", ch)

	n, err := o.store.EnsureNotification(ctx, e.ID, ch)
	if err != nil {
		log.Error("ensure notification failed", "error", err)
		o.metrics.Inc(obs.Errors)
		return ChannelPending
	}
	if n.Status == events.StatusDelivered {
		log.Info("notification already delivered, skipping")
		return ChannelDelivered
	}
	if n.Status == events.StatusDead {
		log.Info("notification already dead-lettered, skipping")
		return ChannelDead
	}

	key := e.ID + ":" + string(ch)
	if !o.kvs.SetNX(key) {
		o.metrics.Inc(obs.Duplicates)
		log.Info("duplicate delivery suppressed")
		// Do NOT release the KVS lock — it belongs to the worker that
		// won the race, not to us. Returning Pending stops the cursor
		// here and the next ProcessOnce will re-read this record once
		// the other worker has committed (the row's status will then
		// hit the "already finalized" branch above).
		return ChannelDuplicateSuppressed
	}
	// We own the KVS lock. Release it on every non-terminal result so a
	// future worker can retry without waiting 24h for TTL expiry. Keep
	// it for terminal results: even though the row is finalized, holding
	// the lock for a short window after success prevents concurrent
	// retries from racing with replay.
	releaseKVS := true
	defer func() {
		if releaseKVS {
			o.kvs.Delete(key)
		}
	}()

	sender, ok := o.senders[ch]
	if !ok {
		log.Error("no sender registered", "channel", ch)
		return ChannelPending
	}
	notification := senders.Notification{
		EventID:   e.ID,
		Recipient: e.Recipient,
		Template:  e.Type,
		Payload:   e.Payload,
	}

	var lastErr error
	for attempt := 1; attempt <= o.cfg.MaxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			log.Warn("delivery canceled by context", "attempt", attempt)
			return ChannelPending
		}
		lastErr = sender.Send(ctx, notification)
		if lastErr == nil {
			if err := o.store.MarkDelivered(ctx, n.ID); err != nil {
				log.Error("mark delivered failed", "error", err)
				return ChannelPending
			}
			o.metrics.Inc(obs.Delivered)
			log.Info("delivery succeeded", "attempt", attempt)
			releaseKVS = false
			return ChannelDelivered
		}
		// Permanent errors bypass the retry budget and go straight to the
		// dead-letter queue: retrying a 4xx-style failure just wastes the
		// attempt budget.
		if events.IsPermanent(lastErr) {
			log.Warn("delivery rejected as permanent", "attempt", attempt, "error", lastErr)
			break
		}
		o.metrics.Inc(obs.Retries)
		log.Warn("delivery attempt failed", "attempt", attempt, "error", lastErr)
		if err := o.store.RecordAttempt(ctx, n.ID, lastErr); err != nil {
			log.Error("record attempt failed", "error", err)
			return ChannelPending
		}
		if attempt < o.cfg.MaxAttempts {
			if err := o.cfg.Sleep(ctx, o.backoff(attempt)); err != nil {
				log.Warn("delivery canceled during backoff", "attempt", attempt)
				return ChannelPending
			}
		}
	}

	fresh, err := o.store.GetNotification(ctx, e.ID, ch)
	if err != nil {
		log.Error("get notification before mark-dead failed", "error", err)
		return ChannelPending
	}
	if err := o.store.MarkDead(ctx, fresh, lastErr); err != nil {
		log.Error("mark dead failed", "error", err)
		return ChannelPending
	}
	o.metrics.Inc(obs.DeadLettered)
	log.Error("notification exhausted attempts, dead-lettered", "attempts", o.cfg.MaxAttempts, "error", lastErr)
	releaseKVS = false
	return ChannelDead
}

// backoff returns BaseBackoff * 2^(attempt-1), capped at 2 seconds.
func (o *Orchestrator) backoff(attempt int) time.Duration {
	d := o.cfg.BaseBackoff << (attempt - 1)
	const max = 2 * time.Second
	if d > max {
		return max
	}
	return d
}

// Unused import guard: fmt is referenced only by the doc string for the
// sender-missing error path. Keep the import explicit so future edits
// that re-introduce fmt.Errorf do not break the build.
var _ = fmt.Sprintf
