package orchestrator_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Bottousky/eventflow/internal/events"
	"github.com/Bottousky/eventflow/internal/kvs"
	"github.com/Bottousky/eventflow/internal/obs"
	"github.com/Bottousky/eventflow/internal/orchestrator"
	"github.com/Bottousky/eventflow/internal/senders"
	"github.com/Bottousky/eventflow/internal/store"
	"github.com/Bottousky/eventflow/internal/stream"
)

// recordingSender records the event IDs it successfully "delivers", in order.
type recordingSender struct {
	ch         events.Channel
	failFirstN int
	permanent  error
	delay      time.Duration

	mu       sync.Mutex
	calls    int
	inFlight int
	peak     int
	sent     []string
}

func (r *recordingSender) Channel() events.Channel { return r.ch }

func (r *recordingSender) Send(ctx context.Context, n senders.Notification) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r.mu.Lock()
	r.inFlight++
	if r.inFlight > r.peak {
		r.peak = r.inFlight
	}
	r.calls++
	fail := r.permanent != nil || r.failFirstN == senders.FailAlways || r.calls <= r.failFirstN
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		r.inFlight--
		r.mu.Unlock()
	}()
	if r.delay > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(r.delay):
		}
	}
	if fail {
		if r.permanent != nil {
			return &events.PermanentError{Cause: errors.New(r.permanent.Error())}
		}
		return errors.New("boom")
	}
	r.mu.Lock()
	r.sent = append(r.sent, n.EventID)
	r.mu.Unlock()
	return nil
}

func (r *recordingSender) Calls() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls
}

func (r *recordingSender) Sent() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.sent...)
}

func (r *recordingSender) Peak() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.peak
}

type testStack struct {
	stream  *stream.Stream
	store   *store.Store
	kvs     *kvs.Store
	metrics *obs.Metrics
}

func newTestStack(t *testing.T) testStack {
	t.Helper()
	db, err := store.Open(":memory:")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	st, err := stream.New(db)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	dbs, err := store.New(db)
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	return testStack{stream: st, store: dbs, kvs: kvs.New(24*time.Hour, time.Now), metrics: obs.New()}
}

var discardLogger = slog.New(slog.NewTextHandler(io.Discard, nil))

func newTestOrchestrator(ts testStack, reg senders.Registry) *orchestrator.Orchestrator {
	return newTestOrchestratorWith(ts, reg, orchestrator.DefaultConfig())
}

func newTestOrchestratorWith(ts testStack, reg senders.Registry, cfg orchestrator.Config) *orchestrator.Orchestrator {
	cfg.MaxAttempts = 3
	cfg.Sleep = func(context.Context, time.Duration) error { return nil } // no real waiting in tests
	if cfg.MaxConcurrency < 1 {
		cfg.MaxConcurrency = 1
	}
	return orchestrator.New(ts.stream, ts.store, ts.kvs, reg, ts.metrics, discardLogger, cfg)
}

func appendEvent(t *testing.T, st *stream.Stream, id, key string, channels ...events.Channel) {
	t.Helper()
	e := events.Event{
		ID:          id,
		Type:        "order.shipped",
		OrderingKey: key,
		Recipient:   "ana@example.com",
		Channels:    channels,
	}
	if _, err := st.Append(context.Background(), e); err != nil {
		t.Fatalf("append %s: %v", id, err)
	}
}

func TestProcessesEventsInAppendOrder(t *testing.T) {
	ts := newTestStack(t)
	email := &recordingSender{ch: events.ChannelEmail}
	o := newTestOrchestrator(ts, senders.Registry{events.ChannelEmail: email})

	ctx := context.Background()
	appendEvent(t, ts.stream, "evt_1", "user-1", events.ChannelEmail)
	appendEvent(t, ts.stream, "evt_2", "user-1", events.ChannelEmail)
	appendEvent(t, ts.stream, "evt_3", "user-1", events.ChannelEmail)

	n, err := o.ProcessOnce(ctx)
	if err != nil {
		t.Fatalf("process: %v", err)
	}
	if n != 3 {
		t.Fatalf("processed %d, want 3", n)
	}
	want := []string{"evt_1", "evt_2", "evt_3"}
	got := email.Sent()
	if len(got) != len(want) {
		t.Fatalf("sent %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("sent order %v, want %v", got, want)
		}
	}
}

func TestRetriesWithBackoffThenDelivers(t *testing.T) {
	ts := newTestStack(t)
	email := &recordingSender{ch: events.ChannelEmail, failFirstN: 2}
	o := newTestOrchestrator(ts, senders.Registry{events.ChannelEmail: email})

	ctx := context.Background()
	appendEvent(t, ts.stream, "evt_1", "user-1", events.ChannelEmail)

	if _, err := o.ProcessOnce(ctx); err != nil {
		t.Fatalf("process: %v", err)
	}
	if got := email.Calls(); got != 3 {
		t.Fatalf("calls = %d, want 3 (2 failures + 1 success)", got)
	}
	d, err := ts.store.GetNotification(ctx, "evt_1", events.ChannelEmail)
	if err != nil {
		t.Fatalf("get notification: %v", err)
	}
	if d.Status != events.StatusDelivered || d.Attempts != 2 {
		t.Fatalf("notification = %+v, want delivered with 2 recorded attempts", d)
	}
	if got := ts.metrics.Snapshot()[obs.Retries]; got != 2 {
		t.Fatalf("retries metric = %d, want 2", got)
	}
}

func TestExhaustedDeliveryGoesToDeadLetterQueue(t *testing.T) {
	ts := newTestStack(t)
	push := &recordingSender{ch: events.ChannelPush, failFirstN: senders.FailAlways}
	o := newTestOrchestrator(ts, senders.Registry{events.ChannelPush: push})

	ctx := context.Background()
	appendEvent(t, ts.stream, "evt_9", "user-2", events.ChannelPush)

	if _, err := o.ProcessOnce(ctx); err != nil {
		t.Fatalf("process: %v", err)
	}
	n, err := ts.store.GetNotification(ctx, "evt_9", events.ChannelPush)
	if err != nil {
		t.Fatalf("get notification: %v", err)
	}
	if n.Status != events.StatusDead {
		t.Fatalf("status = %s, want dead", n.Status)
	}
	dlq, err := ts.store.DeadLetters(ctx)
	if err != nil {
		t.Fatalf("dead letters: %v", err)
	}
	if len(dlq) != 1 || dlq[0].EventID != "evt_9" {
		t.Fatalf("dead letters = %+v", dlq)
	}
	if got := ts.metrics.Snapshot()[obs.DeadLettered]; got != 1 {
		t.Fatalf("dead lettered metric = %d, want 1", got)
	}
}

func TestPermanentErrorShortCircuitsToDeadLetter(t *testing.T) {
	ts := newTestStack(t)
	email := &recordingSender{ch: events.ChannelEmail, permanent: errors.New("bad recipient")}
	o := newTestOrchestrator(ts, senders.Registry{events.ChannelEmail: email})

	ctx := context.Background()
	appendEvent(t, ts.stream, "evt_p", "user-1", events.ChannelEmail)

	if _, err := o.ProcessOnce(ctx); err != nil {
		t.Fatalf("process: %v", err)
	}
	if got := email.Calls(); got != 1 {
		t.Fatalf("permanent error must not retry: calls = %d, want 1", got)
	}
	n, err := ts.store.GetNotification(ctx, "evt_p", events.ChannelEmail)
	if err != nil {
		t.Fatalf("get notification: %v", err)
	}
	if n.Status != events.StatusDead {
		t.Fatalf("status = %s, want dead", n.Status)
	}
}

func TestReplayAfterRestartNeverRedelivers(t *testing.T) {
	ts := newTestStack(t)
	email := &recordingSender{ch: events.ChannelEmail}
	o := newTestOrchestrator(ts, senders.Registry{events.ChannelEmail: email})

	ctx := context.Background()
	appendEvent(t, ts.stream, "evt_1", "user-1", events.ChannelEmail)

	if _, err := o.ProcessOnce(ctx); err != nil {
		t.Fatalf("first process: %v", err)
	}
	// Simulate a worker restart with a stale cursor: the same event is
	// re-read, but the store says it was already delivered.
	if err := ts.store.SetCursor(ctx, 0); err != nil {
		t.Fatalf("reset cursor: %v", err)
	}
	if _, err := o.ProcessOnce(ctx); err != nil {
		t.Fatalf("second process: %v", err)
	}
	if got := email.Calls(); got != 1 {
		t.Fatalf("calls = %d after replay, want exactly 1 (no redelivery)", got)
	}
}

func TestInFlightDuplicateIsSuppressedByKVS(t *testing.T) {
	ts := newTestStack(t)
	email := &recordingSender{ch: events.ChannelEmail}
	o := newTestOrchestrator(ts, senders.Registry{events.ChannelEmail: email})

	ctx := context.Background()
	appendEvent(t, ts.stream, "evt_1", "user-1", events.ChannelEmail)
	// Another worker instance already claimed this delivery.
	if !ts.kvs.SetNX("evt_1:email") {
		t.Fatal("pre-seed SetNX must succeed")
	}

	if _, err := o.ProcessOnce(ctx); err != nil {
		t.Fatalf("process: %v", err)
	}
	if got := email.Calls(); got != 0 {
		t.Fatalf("calls = %d, want 0 (duplicate suppressed)", got)
	}
	if got := ts.metrics.Snapshot()[obs.Duplicates]; got != 1 {
		t.Fatalf("duplicates metric = %d, want 1", got)
	}
}

func TestFanOutToMultipleChannels(t *testing.T) {
	ts := newTestStack(t)
	email := &recordingSender{ch: events.ChannelEmail}
	push := &recordingSender{ch: events.ChannelPush}
	inApp := &recordingSender{ch: events.ChannelInApp}
	o := newTestOrchestrator(ts, senders.Registry{
		events.ChannelEmail: email,
		events.ChannelPush:  push,
		events.ChannelInApp: inApp,
	})

	ctx := context.Background()
	appendEvent(t, ts.stream, "evt_1", "user-1",
		events.ChannelEmail, events.ChannelPush, events.ChannelInApp)

	if _, err := o.ProcessOnce(ctx); err != nil {
		t.Fatalf("process: %v", err)
	}
	if email.Calls() != 1 || push.Calls() != 1 || inApp.Calls() != 1 {
		t.Fatalf("each channel must be delivered exactly once: email=%d push=%d in_app=%d",
			email.Calls(), push.Calls(), inApp.Calls())
	}
	if got := ts.metrics.Snapshot()[obs.Delivered]; got != 3 {
		t.Fatalf("delivered metric = %d, want 3", got)
	}
}

func TestDifferentOrderingKeysProcessInParallel(t *testing.T) {
	ts := newTestStack(t)
	const keys = 6
	registry := senders.Registry{}
	sendersByKey := make([]*recordingSender, keys)
	for i := 0; i < keys; i++ {
		sendersByKey[i] = &recordingSender{ch: events.ChannelEmail, delay: 40 * time.Millisecond}
		registry[events.ChannelEmail] = sendersByKey[i]
		_ = i
	}
	// Use a single sender for all keys; peak in-flight count tells us how
	// many keys were running in parallel.
	email := &recordingSender{ch: events.ChannelEmail, delay: 40 * time.Millisecond}
	registry = senders.Registry{events.ChannelEmail: email}

	cfg := orchestrator.DefaultConfig()
	cfg.MaxConcurrency = 4
	o := newTestOrchestratorWith(ts, registry, cfg)

	ctx := context.Background()
	for i := 0; i < keys; i++ {
		appendEvent(t, ts.stream,
			"evt_"+string(rune('A'+i)),
			"user-"+string(rune('A'+i)),
			events.ChannelEmail)
	}

	start := time.Now()
	if _, err := o.ProcessOnce(ctx); err != nil {
		t.Fatalf("process: %v", err)
	}
	elapsed := time.Since(start)

	// With 4-way concurrency and 40ms per send, processing 6 keys should
	// take roughly 80ms (2 batches) rather than 240ms (sequential).
	if elapsed > 200*time.Millisecond {
		t.Fatalf("processing 6 keys with MaxConcurrency=4 took %v, want < 200ms", elapsed)
	}
	if peak := email.Peak(); peak < 2 {
		t.Fatalf("peak concurrent calls = %d, want >= 2 to prove parallelism", peak)
	}
}

func TestSameOrderingKeyProcessesSequentially(t *testing.T) {
	ts := newTestStack(t)
	email := &recordingSender{ch: events.ChannelEmail, delay: 20 * time.Millisecond}

	cfg := orchestrator.DefaultConfig()
	cfg.MaxConcurrency = 4
	o := newTestOrchestratorWith(ts, senders.Registry{events.ChannelEmail: email}, cfg)

	ctx := context.Background()
	for i := 0; i < 4; i++ {
		appendEvent(t, ts.stream, "evt_same_"+string(rune('a'+i)), "user-1", events.ChannelEmail)
	}

	if _, err := o.ProcessOnce(ctx); err != nil {
		t.Fatalf("process: %v", err)
	}
	// One ordering key means one in-flight partition, even with concurrency > 1.
	if peak := email.Peak(); peak != 1 {
		t.Fatalf("peak concurrent calls for same key = %d, want 1 (sequential within a key)", peak)
	}
}

func TestGracefulShutdown(t *testing.T) {
	ts := newTestStack(t)
	email := &recordingSender{ch: events.ChannelEmail}
	email.delay = 100 * time.Millisecond
	cfg := orchestrator.DefaultConfig()
	cfg.PollInterval = 5 * time.Millisecond
	o := newTestOrchestratorWith(ts, senders.Registry{events.ChannelEmail: email}, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	appendEvent(t, ts.stream, "evt_1", "user-1", events.ChannelEmail)
	appendEvent(t, ts.stream, "evt_2", "user-1", events.ChannelEmail)

	done := make(chan error, 1)
	go func() { done <- o.Run(ctx) }()

	// Cancel after a short delay; the worker should exit cleanly.
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return within 2s after cancel")
	}
}

// Ensure the orchestrator honors the injected Sleep for graceful shutdown:
// if the worker is sleeping between empty polls when the context is
// canceled, it should return immediately rather than waiting out the
// poll interval. Run starts an empty stream so ProcessOnce returns 0
// and the worker enters its sleep on the first iteration.
func TestRunWakesOnContextCancelDuringSleep(t *testing.T) {
	ts := newTestStack(t)
	email := &recordingSender{ch: events.ChannelEmail}
	sleepCalled := make(chan struct{}, 1)
	cfg := orchestrator.Config{
		PollInterval:   5 * time.Second,
		BatchSize:      100,
		MaxAttempts:    1,
		BaseBackoff:    time.Millisecond,
		MaxConcurrency: 1,
		Sleep: func(ctx context.Context, _ time.Duration) error {
			select {
			case sleepCalled <- struct{}{}:
			default:
			}
			return ctx.Err()
		},
	}
	o := orchestrator.New(ts.stream, ts.store, ts.kvs, senders.Registry{events.ChannelEmail: email}, ts.metrics, discardLogger, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- o.Run(ctx) }()

	// Wait for the orchestrator to enter its sleep.
	select {
	case <-sleepCalled:
	case <-time.After(2 * time.Second):
		t.Fatal("orchestrator never called Sleep")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancel during sleep")
	}
}

// recordAndFailSender is a sender that records the call and returns an
// error. Used by the orchestrator benchmark.
type recordAndFailSender struct {
	calls atomic.Int64
}

func (r *recordAndFailSender) Channel() events.Channel            { return events.ChannelEmail }
func (r *recordAndFailSender) Send(_ context.Context, _ senders.Notification) error {
	r.calls.Add(1)
	return nil
}
