package stream

import (
	"context"
	"testing"

	"github.com/Bottousky/eventflow/internal/events"
	"github.com/Bottousky/eventflow/internal/store"
)

// BenchmarkEventAppend measures raw append latency on a fresh in-memory
// database. The result is sensitive to SQLite's WAL/sync settings and
// should be treated as a local reproduction hint, not a production SLA.
func BenchmarkEventAppend(b *testing.B) {
	db, err := store.Open(":memory:")
	if err != nil {
		b.Fatalf("open db: %v", err)
	}
	defer db.Close()
	st, err := New(db)
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
		e.ID = "evt_" + intToStr(int64(i))
		if _, err := st.Append(ctx, e); err != nil {
			b.Fatalf("append: %v", err)
		}
	}
}

func intToStr(v int64) string {
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
