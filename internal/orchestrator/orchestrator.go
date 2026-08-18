// Package orchestrator consumes the event stream in order and fans events
// out to channel senders with idempotency, retries, exponential backoff and
// a dead-letter queue. It is the component that turns "an event happened"
// into "the right notifications were delivered exactly once".
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
	PollInterval     time.Duration // wait between empty polls in Run
	BatchSize        int           // max stream records per poll
	MaxAttempts      int           // send attempts before dead-lettering
	BaseBackoff      time.Duration // first retry wait; doubles each attempt
	MaxConcurrency   int           // max in-flight ordering-key partitions per batch
	Sleep            func(ctx context.Context, d time.Duration) error
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
// cfg.MaxConcurrency. It returns the number of records consumed.
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
				o.processRecord(ctx, rec)
				o.metrics.ObserveDuration(time.Since(start))
			}
		}(group)
	}
	wg.Wait()
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	// Every record in the batch was attempted; advance the cursor in seq
	// order up to the highest contiguous processed record.
	processed := make(map[int64]bool, len(records))
	for _, rec := range records {
		// processRecord is best-effort: errors are logged inside deliver().
		// The record is "attempted" either way, which is what the cursor
		// tracks — the per-channel status is the source of truth for
		// delivery outcomes.
		processed[rec.Seq] = true
	}
	var advanced int64
	for _, rec := range records {
		if !processed[rec.Seq] {
			break
		}
		advanced = rec.Seq
	}
	if advanced > cursor {
		if err := o.store.SetCursor(ctx, advanced); err != nil {
			return 0, err
		}
	}
	return len(records), nil
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

// processRecord fans one event out to its channels.
func (o *Orchestrator) processRecord(ctx context.Context, rec stream.Record) {
	o.metrics.Inc(obs.EventsProcessed)
	log := o.logger.With("event_id", rec.Event.ID, "event_type", rec.Event.Type, "seq", rec.Seq)
	log.Info("processing event", "ordering_key", rec.Event.OrderingKey, "channels", rec.Event.Channels)
	for _, ch := range rec.Event.Channels {
		if err := o.deliver(ctx, rec.Event, ch); err != nil {
			log.Error("delivery aborted", "channel", ch, "error", err)
			o.metrics.Inc(obs.Errors)
		}
	}
}

// deliver sends one event on one channel with idempotency and retries.
func (o *Orchestrator) deliver(ctx context.Context, e events.Event, ch events.Channel) error {
	log := o.logger.With("event_id", e.ID, "channel", ch)

	n, err := o.store.EnsureNotification(ctx, e.ID, ch)
	if err != nil {
		return err
	}
	if n.Status == events.StatusDelivered || n.Status == events.StatusDead {
		// Crash-safe replay: the store is the source of truth, so a
		// reprocessed event never sends twice.
		log.Info("notification already finalized, skipping", "status", n.Status)
		return nil
	}

	key := e.ID + ":" + string(ch)
	if !o.kvs.SetNX(key) {
		o.metrics.Inc(obs.Duplicates)
		log.Info("duplicate delivery suppressed")
		return nil
	}

	sender, ok := o.senders[ch]
	if !ok {
		return fmt.Errorf("no sender registered for channel %q", ch)
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
			return nil
		}
		lastErr = sender.Send(ctx, notification)
		if lastErr == nil {
			if err := o.store.MarkDelivered(ctx, n.ID); err != nil {
				return err
			}
			o.metrics.Inc(obs.Delivered)
			log.Info("delivery succeeded", "attempt", attempt)
			return nil
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
			return err
		}
		if attempt < o.cfg.MaxAttempts {
			if err := o.cfg.Sleep(ctx, o.backoff(attempt)); err != nil {
				return nil
			}
		}
	}

	fresh, err := o.store.GetNotification(ctx, e.ID, ch)
	if err != nil {
		return err
	}
	if err := o.store.MarkDead(ctx, fresh, lastErr); err != nil {
		return err
	}
	o.metrics.Inc(obs.DeadLettered)
	log.Error("notification exhausted attempts, dead-lettered", "attempts", o.cfg.MaxAttempts, "error", lastErr)
	return nil
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
