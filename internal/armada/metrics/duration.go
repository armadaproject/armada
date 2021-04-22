package metrics

import (
	"sort"
	"time"
)

type DurationMetricsRecorder struct {
	min     float64
	max     float64
	sum     float64
	count   uint64
	values  []float64
	buckets map[float64]uint64
}

func NewDurationMetrics(buckets ...float64) *DurationMetricsRecorder {
	bucketsMap := make(map[float64]uint64, len(buckets))
	for _, bucket := range buckets {
		bucketsMap[bucket] = 0
	}
	return &DurationMetricsRecorder{
		values:  make([]float64, 0, 10),
		buckets: bucketsMap,
	}
}

func NewDefaultJobDurationMetricsRecorder() *DurationMetricsRecorder {
	return NewDurationMetrics(
		(time.Minute).Seconds(),
		(time.Minute * 10).Seconds(),
		(time.Minute * 30).Seconds(),
		(time.Hour).Seconds(),
		(time.Hour * 3).Seconds(),
		(time.Hour * 12).Seconds(),
		(time.Hour * 24).Seconds(),
		(time.Hour * 24 * 2).Seconds(),
		(time.Hour * 24 * 7).Seconds())
}

func (d *DurationMetricsRecorder) Record(value float64) {
	if d.count == 0 || value < d.min {
		d.min = value
	}
	if d.count == 0 || value > d.max {
		d.max = value
	}
	d.count++
	d.sum += value
	d.values = append(d.values, value)

	for bucket := range d.buckets {
		if value <= bucket {
			d.buckets[bucket]++
		}
	}
}
func (d *DurationMetricsRecorder) calculateMedian() float64 {
	if len(d.values) == 0 {
		return 0
	}
	sort.Float64s(d.values)
	medianPosition := len(d.values) / 2

	if len(d.values)%2 != 0 {
		return d.values[medianPosition]
	}

	return (d.values[medianPosition-1] + d.values[medianPosition]) / 2
}

func (d *DurationMetricsRecorder) GetMetrics() *DurationMetrics {
	return &DurationMetrics{
		min:     d.min,
		max:     d.max,
		median:  d.calculateMedian(),
		sum:     d.sum,
		count:   d.count,
		buckets: d.buckets,
	}
}

type DurationMetrics struct {
	min     float64
	max     float64
	median  float64
	sum     float64
	count   uint64
	buckets map[float64]uint64
}

func (d *DurationMetrics) GetMin() float64 {
	return d.min
}

func (d *DurationMetrics) GetMax() float64 {
	return d.max
}

func (d *DurationMetrics) GetMedian() float64 {
	return d.median
}

func (d *DurationMetrics) GetCount() uint64 {
	return d.count
}

func (d *DurationMetrics) GetSum() float64 {
	return d.sum
}

func (d *DurationMetrics) GetBuckets() map[float64]uint64 {
	copyBuckets := make(map[float64]uint64, len(d.buckets))
	for key, value := range d.buckets {
		copyBuckets[key] = value
	}
	return copyBuckets
}
