package orchestrator_test

import (
	"context"
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

// BenchmarkEventAppend measures raw event append latency on a fresh
// in-memory database. Results are local and depend on the host.
func BenchmarkEventAppend(b *testing.B) {
	db, err := store.Open(":memory:")
	if err != nil {
		b.Fatalf("open db: %v", err)
	}
	defer db.Close()
	st, err := stream.New(db)
	if err != nil {
		b.Fatalf("stream: %v", err)
	}
	ctx := context.Background()
	e := events.Event{
		Type:        "order.shipped",
		OrderingKey: "user-1",
		Recipient:   "ana@example.com",
		Channels:    []events.Channel{events.ChannelEmail},
		Payload:     map[string]string{"order_id": "ORD-1"},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e.ID = "evt_" + itoa(int64(i))
		if _, err := st.Append(ctx, e); err != nil {
			b.Fatalf("append: %v", err)
		}
	}
}

// BenchmarkOrchestratorProcessing measures the cost of one orchestrator
// pass over a batch of N events with M distinct ordering keys. With
// MaxConcurrency set to M, all groups should run in parallel and the
// throughput upper bound is the slowest single Send.
func BenchmarkOrchestratorProcessing(b *testing.B) {
	const (
		keys     = 8
		perKey   = 32
		channels = 1
	)
	db, err := store.Open(":memory:")
	if err != nil {
		b.Fatalf("open db: %v", err)
	}
	defer db.Close()
	st, err := stream.New(db)
	if err != nil {
		b.Fatalf("stream: %v", err)
	}
	dbs, err := store.New(db)
	if err != nil {
		b.Fatalf("store: %v", err)
	}

	registry := senders.Registry{events.ChannelEmail: &noopSender{}}
	cfg := orchestrator.DefaultConfig()
	cfg.MaxConcurrency = keys
	cfg.MaxAttempts = 1
	cfg.Sleep = func(context.Context, time.Duration) error { return nil }
	o := orchestrator.New(st, dbs, kvs.New(time.Hour, time.Now), registry, obs.New(), nil, cfg)

	ctx := context.Background()
	chs := make([]events.Channel, channels)
	for i := range chs {
		chs[i] = events.ChannelEmail
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Reset state between iterations by truncating the event table.
		if _, err := db.ExecContext(ctx, `DELETE FROM events`); err != nil {
			b.Fatalf("truncate: %v", err)
		}
		if err := dbs.SetCursor(ctx, 0); err != nil {
			b.Fatalf("reset cursor: %v", err)
		}
		for k := 0; k < keys; k++ {
			key := "k" + itoa(int64(k))
			for j := 0; j < perKey; j++ {
				e := events.Event{
					ID:          "e_" + itoa(int64(i)) + "_" + itoa(int64(k)) + "_" + itoa(int64(j)),
					Type:        "order.shipped",
					OrderingKey: key,
					Recipient:   "ana@example.com",
					Channels:    chs,
				}
				if _, err := st.Append(ctx, e); err != nil {
					b.Fatalf("append: %v", err)
				}
			}
		}
		if _, err := o.ProcessOnce(ctx); err != nil {
			b.Fatalf("process: %v", err)
		}
	}
}

// noopSender returns success without any work, so the benchmark measures
// the orchestrator's own coordination overhead.
type noopSender struct {
	calls atomic.Int64
}

func (n *noopSender) Channel() events.Channel            { return events.ChannelEmail }
func (n *noopSender) Send(_ context.Context, _ senders.Notification) error {
	n.calls.Add(1)
	return nil
}

func itoa(v int64) string {
	if v == 0 {
		return "0"
	}
	const digits = "0123456789"
	var buf [20]byte
	i := len(buf)
	neg := v < 0
	if neg {
		v = -v
	}
	for v > 0 {
		i--
		buf[i] = digits[v%10]
		v /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
