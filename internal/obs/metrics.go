// Package obs provides minimal operational observability: named counters
// and a histogram, both rendered as Prometheus text. The whole point of
// the package is to be small enough to read in a single sitting while
// staying close to the wire format a real Prometheus server would scrape.
package obs

import (
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

// Metrics is a goroutine-safe set of named counters and a duration
// histogram. The Render output is stable: counters are sorted by name.
//
// Process boundaries: the API and the worker each own their own *Metrics
// instance. The API exposes /metrics with events_received; the worker
// exposes its own /metrics endpoint (configurable listen address) with
// the worker-side counters and histogram. Prometheus scrape config
// should target both endpoints independently.
type Metrics struct {
	mu        sync.Mutex
	counters  map[string]uint64
	histogram *Histogram
}

// Well-known counter names. The package uses short names that match the
// historical "service_metric" style, with the "eventflow_" prefix added
// at render time.
const (
	EventsReceived  = "events_received"
	EventsProcessed = "events_processed"
	Delivered       = "notifications_delivered"
	Retries         = "delivery_retries"
	Duplicates      = "duplicates_suppressed"
	DeadLettered    = "events_dead_lettered"
	Errors          = "processing_errors"
)

// New returns an empty Metrics set with a default 5-bucket histogram.
func New() *Metrics {
	return &Metrics{
		counters:  make(map[string]uint64),
		histogram: NewHistogram([]float64{0.005, 0.025, 0.1, 0.5, 2.5}),
	}
}

// Inc increments counter name by one.
func (m *Metrics) Inc(name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.counters[name]++
}

// ObserveDuration records d in the processing duration histogram.
func (m *Metrics) ObserveDuration(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.histogram.Observe(d.Seconds())
}

// Handler serves the metrics in Prometheus text format (the same shape a
// production /metrics endpoint would expose).
func (m *Metrics) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = w.Write([]byte(m.Render()))
	})
}

// Snapshot returns a stable copy of the counter map. The well-known
// counter names are always present (zero if never incremented), and any
// custom counters are appended. It is primarily useful for tests and
// the orchestrator's internal assertions; the production /metrics
// endpoint renders through Render() instead.
func (m *Metrics) Snapshot() map[string]uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := map[string]uint64{
		EventsReceived:  m.counters[EventsReceived],
		EventsProcessed: m.counters[EventsProcessed],
		Delivered:       m.counters[Delivered],
		Retries:         m.counters[Retries],
		Duplicates:      m.counters[Duplicates],
		DeadLettered:    m.counters[DeadLettered],
		Errors:          m.counters[Errors],
	}
	for k, v := range m.counters {
		if _, known := out[k]; !known {
			out[k] = v
		}
	}
	return out
}

// Render returns the metrics in Prometheus text exposition format.
func (m *Metrics) Render() string {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Make the output deterministic: the well-known counters appear in a
	// fixed order, followed by any custom counters in alphabetical order.
	known := []string{
		EventsReceived,
		EventsProcessed,
		Delivered,
		Retries,
		Duplicates,
		DeadLettered,
		Errors,
	}
	seen := make(map[string]bool, len(known))
	var b strings.Builder
	for _, name := range known {
		seen[name] = true
		fmt.Fprintf(&b, "# HELP eventflow_%s_total %s\n", name, name)
		fmt.Fprintf(&b, "# TYPE eventflow_%s_total counter\n", name)
		fmt.Fprintf(&b, "eventflow_%s_total %d\n", name, m.counters[name])
	}
	extras := make([]string, 0)
	for k := range m.counters {
		if !seen[k] {
			extras = append(extras, k)
		}
	}
	sort.Strings(extras)
	for _, name := range extras {
		fmt.Fprintf(&b, "# HELP eventflow_%s_total %s\n", name, name)
		fmt.Fprintf(&b, "# TYPE eventflow_%s_total counter\n", name)
		fmt.Fprintf(&b, "eventflow_%s_total %d\n", name, m.counters[name])
	}
	b.WriteString(m.histogram.Render("eventflow_processing_duration_seconds"))
	return b.String()
}

// Histogram is a tiny Prometheus-style histogram.
//
// bucketHits[i] holds the count of observations that fell into bucket i
// (the smallest bucket whose upper bound is >= the observation). Each
// observation increments exactly one bucket. Render() then emits
// Prometheus-style cumulative bucket counts, plus _sum and _count, so
// each observation is counted once in the output.
type Histogram struct {
	buckets    []float64
	sum        float64
	count      uint64
	bucketHits []uint64 // bucketHits[i] = observations that fell into bucket i (not cumulative)
}

// NewHistogram builds a Histogram with the given bucket upper bounds
// (in seconds). A final +Inf bucket is added automatically.
func NewHistogram(buckets []float64) *Histogram {
	bs := append([]float64(nil), buckets...)
	sort.Float64s(bs)
	return &Histogram{
		buckets:    bs,
		bucketHits: make([]uint64, len(bs)),
	}
}

// Observe records v (in seconds) in the histogram.
//
// Each observation increments exactly one bucket: the smallest bucket
// whose upper bound is >= v. If v is larger than every finite bucket,
// the observation is counted only in the +Inf bucket emitted at render
// time (not in any of the bucketHits entries), so Prometheus sees a
// consistent total. Callers that need to know how many observations
// fell into the +Inf bucket should read Count().
func (h *Histogram) Observe(v float64) {
	h.sum += v
	h.count++
	for i, b := range h.buckets {
		if v <= b {
			h.bucketHits[i]++
			return
		}
	}
}

// Count returns the total number of observations.
func (h *Histogram) Count() uint64 { return h.count }

// Sum returns the sum of all observations in seconds.
func (h *Histogram) Sum() float64 { return h.sum }

// BucketCounts returns the per-bucket counts (non-cumulative) plus the
// bucket upper bounds. It is primarily useful for tests; the production
// /metrics endpoint renders through Render() instead.
func (h *Histogram) BucketCounts() (bounds []float64, counts []uint64) {
	bounds = append([]float64(nil), h.buckets...)
	counts = make([]uint64, len(h.bucketHits))
	copy(counts, h.bucketHits)
	return bounds, counts
}

// Render formats the histogram with the given metric name in Prometheus
// text exposition format. Bucket lines are cumulative (each bucket's
// count includes all observations that fell into smaller buckets).
func (h *Histogram) Render(name string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# HELP %s Distribution of per-record processing time in seconds.\n", name)
	fmt.Fprintf(&b, "# TYPE %s histogram\n", name)
	var cum uint64
	for i, ub := range h.buckets {
		cum += h.bucketHits[i]
		fmt.Fprintf(&b, "%s_bucket{le=\"%g\"} %d\n", name, ub, cum)
	}
	fmt.Fprintf(&b, "%s_bucket{le=\"+Inf\"} %d\n", name, h.count)
	fmt.Fprintf(&b, "%s_sum %g\n", name, h.sum)
	fmt.Fprintf(&b, "%s_count %d\n", name, h.count)
	return b.String()
}
