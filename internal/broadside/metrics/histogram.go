package metrics

import (
	"math"
	"sync"
	"time"
)

// Histogram collects observations into exponential buckets for efficient percentile
// calculation. Buckets double in size: [0, base), [base, 2*base), [2*base, 4*base), etc.
// Buckets are created dynamically as observations arrive, allowing the histogram to
// adapt to any range of values without predefined limits.
type Histogram struct {
	mu      sync.Mutex
	base    float64
	buckets []uint64
	count   uint64
	sum     float64
	min     float64
	max     float64
}

// NewHistogram creates a histogram with exponential buckets starting at the given base.
// The base defines the smallest bucket boundary; subsequent buckets double in size.
// For example, with base=1: buckets cover [0,1), [1,2), [2,4), [4,8), etc.
func NewHistogram(base float64) *Histogram {
	if base <= 0 {
		base = 1
	}
	return &Histogram{
		base:    base,
		buckets: make([]uint64, 1),
		min:     math.MaxFloat64,
		max:     -math.MaxFloat64,
	}
}

// bucketIndex returns the bucket index for a given value using exponential bucketing.
// Values below base go into bucket 0. Values >= base go into bucket floor(log2(value/base))+1.
func (h *Histogram) bucketIndex(value float64) int {
	if value < h.base {
		return 0
	}
	return int(math.Log2(value/h.base)) + 1
}

// bucketBounds returns the lower and upper bounds for a given bucket index.
func (h *Histogram) bucketBounds(index int) (lower, upper float64) {
	if index == 0 {
		return 0, h.base
	}
	lower = h.base * math.Pow(2, float64(index-1))
	upper = h.base * math.Pow(2, float64(index))
	return lower, upper
}

// Observe records a new observation in the histogram.
func (h *Histogram) Observe(value float64) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.count++
	h.sum += value
	if value < h.min {
		h.min = value
	}
	if value > h.max {
		h.max = value
	}

	idx := h.bucketIndex(value)

	// Grow buckets slice if needed
	if idx >= len(h.buckets) {
		newBuckets := make([]uint64, idx+1)
		copy(newBuckets, h.buckets)
		h.buckets = newBuckets
	}

	h.buckets[idx]++
}

// HistogramStats contains computed statistics from a histogram.
type HistogramStats struct {
	Count   uint64
	Sum     float64
	Min     float64
	Max     float64
	Average float64
	P50     float64
	P95     float64
	P99     float64
}

// Stats returns computed statistics from the histogram using linear interpolation
// for percentile estimation.
func (h *Histogram) Stats() HistogramStats {
	h.mu.Lock()
	defer h.mu.Unlock()

	stats := HistogramStats{
		Count: h.count,
		Sum:   h.sum,
	}

	if h.count == 0 {
		return stats
	}

	stats.Min = h.min
	stats.Max = h.max
	stats.Average = h.sum / float64(h.count)
	stats.P50 = h.percentileLocked(0.50)
	stats.P95 = h.percentileLocked(0.95)
	stats.P99 = h.percentileLocked(0.99)

	return stats
}

// percentileLocked calculates the percentile using linear interpolation.
// Caller must hold the mutex.
func (h *Histogram) percentileLocked(p float64) float64 {
	if h.count == 0 {
		return 0
	}

	target := p * float64(h.count)
	var cumulative uint64

	for i, count := range h.buckets {
		prev := cumulative
		cumulative += count

		if float64(cumulative) >= target {
			lower, upper := h.bucketBounds(i)

			// For the last bucket, cap upper at max observed value
			if i == len(h.buckets)-1 && upper > h.max {
				upper = h.max
			}

			if count == 0 {
				return lower
			}
			fraction := (target - float64(prev)) / float64(count)
			return lower + fraction*(upper-lower)
		}
	}

	return h.max
}

// DurationHistogram wraps Histogram to work with time.Duration values.
type DurationHistogram struct {
	histogram *Histogram
}

// NewDurationHistogram creates a histogram for time.Duration values with exponential
// buckets starting at the given base duration. Buckets double in size from the base.
func NewDurationHistogram(base time.Duration) *DurationHistogram {
	return &DurationHistogram{
		histogram: NewHistogram(float64(base.Nanoseconds())),
	}
}

// Observe records a duration observation.
func (h *DurationHistogram) Observe(d time.Duration) {
	h.histogram.Observe(float64(d.Nanoseconds()))
}

// DurationStats contains computed statistics with duration values.
type DurationStats struct {
	Count   uint64
	Sum     time.Duration
	Min     time.Duration
	Max     time.Duration
	Average time.Duration
	P50     time.Duration
	P95     time.Duration
	P99     time.Duration
}

// Stats returns computed statistics with duration values.
func (h *DurationHistogram) Stats() DurationStats {
	stats := h.histogram.Stats()
	return DurationStats{
		Count:   stats.Count,
		Sum:     time.Duration(stats.Sum),
		Min:     time.Duration(stats.Min),
		Max:     time.Duration(stats.Max),
		Average: time.Duration(stats.Average),
		P50:     time.Duration(stats.P50),
		P95:     time.Duration(stats.P95),
		P99:     time.Duration(stats.P99),
	}
}

// IntHistogram wraps Histogram to work with integer values (e.g., backlog size).
type IntHistogram struct {
	histogram *Histogram
}

// NewIntHistogram creates a histogram for integer values with exponential buckets
// starting at the given base. Buckets double in size from the base.
func NewIntHistogram(base int) *IntHistogram {
	return &IntHistogram{
		histogram: NewHistogram(float64(base)),
	}
}

// Observe records an integer observation.
func (h *IntHistogram) Observe(value int) {
	h.histogram.Observe(float64(value))
}

// IntStats contains computed statistics with integer values.
type IntStats struct {
	Count   uint64
	Sum     int64
	Min     int
	Max     int
	Average float64
	P50     float64
	P95     float64
	P99     float64
}

// Stats returns computed statistics for integer observations.
func (h *IntHistogram) Stats() IntStats {
	stats := h.histogram.Stats()
	return IntStats{
		Count:   stats.Count,
		Sum:     int64(stats.Sum),
		Min:     int(stats.Min),
		Max:     int(stats.Max),
		Average: stats.Average,
		P50:     stats.P50,
		P95:     stats.P95,
		P99:     stats.P99,
	}
}
