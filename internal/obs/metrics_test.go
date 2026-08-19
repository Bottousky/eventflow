package obs

import (
	"strings"
	"testing"
)

// TestHistogramSingleObservationInSmallestBucket pins the per-bucket
// increment contract: one observation at 0.001 must land in the
// 0.005 bucket and not be counted in any larger bucket.
func TestHistogramSingleObservationInSmallestBucket(t *testing.T) {
	h := NewHistogram([]float64{0.005, 0.025, 0.1, 0.5, 2.5})
	h.Observe(0.001)

	bounds, counts := h.BucketCounts()
	want := []uint64{1, 0, 0, 0, 0}
	for i, c := range want {
		if counts[i] != c {
			t.Fatalf("bucket[%d] (le=%g) = %d, want %d", i, bounds[i], counts[i], c)
		}
	}
	if got := h.Count(); got != 1 {
		t.Fatalf("count = %d, want 1", got)
	}
	if got := h.Sum(); got != 0.001 {
		t.Fatalf("sum = %g, want 0.001", got)
	}
}

// TestHistogramObservationAtBucketBoundaryIsInclusive verifies that the
// upper bound is inclusive: an observation equal to the bucket bound
// falls into that bucket, not the next one.
func TestHistogramObservationAtBucketBoundaryIsInclusive(t *testing.T) {
	h := NewHistogram([]float64{0.005, 0.025, 0.1, 0.5, 2.5})
	h.Observe(0.005)

	_, counts := h.BucketCounts()
	want := []uint64{1, 0, 0, 0, 0}
	for i, c := range want {
		if counts[i] != c {
			t.Fatalf("bucket[%d] = %d, want %d", i, counts[i], c)
		}
	}
}

// TestHistogramObservationAboveAllFiniteBuckets lands in the +Inf
// bucket only and is not double-counted into any finite bucket.
func TestHistogramObservationAboveAllFiniteBuckets(t *testing.T) {
	h := NewHistogram([]float64{0.005, 0.025, 0.1, 0.5, 2.5})
	h.Observe(5.0)

	_, counts := h.BucketCounts()
	for i, c := range counts {
		if c != 0 {
			t.Fatalf("finite bucket[%d] = %d, want 0 (observation fell into +Inf only)", i, c)
		}
	}
	if got := h.Count(); got != 1 {
		t.Fatalf("count = %d, want 1 (the +Inf bucket carries the observation)", got)
	}
}

// TestHistogramMultipleObservationsAccumulateCorrectly is the regression
// test for the original double-accumulation bug. With the previous
// implementation, observing [0.001, 0.01, 0.2] produced cumulative bucket
// counts [1, 2, 3, 3, 3] because Observe incremented every matching
// bucket and Render added them up again. The fix increments exactly one
// bucket per observation; Render then produces correct cumulative
// counts [1, 2, 3, 3, 3].
func TestHistogramMultipleObservationsAccumulateCorrectly(t *testing.T) {
	h := NewHistogram([]float64{0.005, 0.025, 0.1, 0.5, 2.5})
	h.Observe(0.001) // bucket 0
	h.Observe(0.01)  // bucket 1
	h.Observe(0.2)   // bucket 3

	_, counts := h.BucketCounts()
	// Per-bucket (non-cumulative) counts.
	wantPerBucket := []uint64{1, 1, 0, 1, 0}
	for i, c := range wantPerBucket {
		if counts[i] != c {
			t.Fatalf("bucket[%d] = %d, want %d", i, counts[i], c)
		}
	}
	if got := h.Count(); got != 3 {
		t.Fatalf("count = %d, want 3", got)
	}
	if got := h.Sum(); got != 0.001+0.01+0.2 {
		t.Fatalf("sum = %g, want %g", got, 0.001+0.01+0.2)
	}

	// Cumulative (Prometheus output) counts.
	rendered := h.Render("test_seconds")
	wantLines := []string{
		`test_seconds_bucket{le="0.005"} 1`,
		`test_seconds_bucket{le="0.025"} 2`,
		`test_seconds_bucket{le="0.1"} 2`,
		`test_seconds_bucket{le="0.5"} 3`,
		`test_seconds_bucket{le="2.5"} 3`,
		`test_seconds_bucket{le="+Inf"} 3`,
		`test_seconds_sum 0.211`,
		`test_seconds_count 3`,
	}
	for _, line := range wantLines {
		if !strings.Contains(rendered, line) {
			t.Errorf("rendered output missing line %q\nfull output:\n%s", line, rendered)
		}
	}
}

// TestHistogramRenderDoesNotDoubleAccumulate is the focused regression
// test for the original bug. Observing a single value should produce
// bucket counts of [1, 1, 1, 1, 1] (cumulative, Prometheus-style) —
// not [1, 2, 3, 4, 5] as the previous bug did.
func TestHistogramRenderDoesNotDoubleAccumulate(t *testing.T) {
	h := NewHistogram([]float64{0.005, 0.025, 0.1, 0.5, 2.5})
	h.Observe(0.1)

	rendered := h.Render("test_seconds")
	wantLines := []string{
		`test_seconds_bucket{le="0.005"} 0`,
		`test_seconds_bucket{le="0.025"} 0`,
		`test_seconds_bucket{le="0.1"} 1`,
		`test_seconds_bucket{le="0.5"} 1`,
		`test_seconds_bucket{le="2.5"} 1`,
		`test_seconds_bucket{le="+Inf"} 1`,
	}
	for _, line := range wantLines {
		if !strings.Contains(rendered, line) {
			t.Errorf("rendered output missing line %q\nfull output:\n%s", line, rendered)
		}
	}
	// Specifically verify the original bug shape: bucket counts must not
	// be strictly increasing past the count of observations.
	for _, line := range []string{
		`test_seconds_bucket{le="0.5"} 2`,
		`test_seconds_bucket{le="2.5"} 2`,
	} {
		if strings.Contains(rendered, line) {
			t.Errorf("rendered output shows double-accumulated bucket count %q\nfull output:\n%s", line, rendered)
		}
	}
}

// TestHistogramEmptyRender ensures the histogram renders correctly even
// with zero observations. +Inf bucket, _sum and _count must all be 0.
func TestHistogramEmptyRender(t *testing.T) {
	h := NewHistogram([]float64{0.005, 0.025, 0.1, 0.5, 2.5})

	rendered := h.Render("test_seconds")
	for _, line := range []string{
		`test_seconds_bucket{le="+Inf"} 0`,
		`test_seconds_sum 0`,
		`test_seconds_count 0`,
	} {
		if !strings.Contains(rendered, line) {
			t.Errorf("rendered output missing line %q\nfull output:\n%s", line, rendered)
		}
	}
}

// TestHistogramBucketsAreSorted verifies the histogram defensively
// sorts user-supplied bucket bounds. NewHistogram is called with the
// bounds in descending order, but Observe/Render must still behave.
func TestHistogramBucketsAreSorted(t *testing.T) {
	h := NewHistogram([]float64{2.5, 0.005, 0.5, 0.025, 0.1})
	bounds, _ := h.BucketCounts()
	for i := 1; i < len(bounds); i++ {
		if bounds[i] < bounds[i-1] {
			t.Fatalf("buckets not sorted: %v", bounds)
		}
	}
	h.Observe(0.01)
	_, counts := h.BucketCounts()
	if counts[1] != 1 { // the second bucket, after sort, is 0.025
		t.Fatalf("expected bucket 0.025 to count 1 observation, got counts %v", counts)
	}
}

// TestMetricsRenderContainsAllCountersAndHistogram ensures Render joins
// counters and the histogram into a single Prometheus text payload.
func TestMetricsRenderContainsAllCountersAndHistogram(t *testing.T) {
	m := New()
	m.Inc(EventsReceived)
	m.Inc(Delivered)
	m.ObserveDuration(0)

	rendered := m.Render()
	for _, line := range []string{
		"eventflow_events_received_total 1",
		"eventflow_notifications_delivered_total 1",
		"eventflow_processing_duration_seconds_bucket",
		"eventflow_processing_duration_seconds_count 1",
	} {
		if !strings.Contains(rendered, line) {
			t.Errorf("Render output missing %q\nfull output:\n%s", line, rendered)
		}
	}
}

// TestMetricsSnapshotIncludesAllWellKnownCounters ensures Snapshot
// always returns the well-known counters, even when not incremented.
func TestMetricsSnapshotIncludesAllWellKnownCounters(t *testing.T) {
	m := New()
	snap := m.Snapshot()
	for _, name := range []string{
		EventsReceived, EventsProcessed, Delivered, Retries,
		Duplicates, DeadLettered, Errors,
	} {
		if _, ok := snap[name]; !ok {
			t.Errorf("Snapshot missing well-known counter %q", name)
		}
	}
}
