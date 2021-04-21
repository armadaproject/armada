package cache

import (
	"time"
)

type DurationMetrics struct {
	min     float64
	max     float64
	count   uint64
	sum     float64
	buckets map[float64]uint64
}

func NewDurationMetrics(buckets ...float64) *DurationMetrics {
	bucketsMap := make(map[float64]uint64, len(buckets))
	for _, bucket := range buckets {
		bucketsMap[bucket] = 0
	}
	return &DurationMetrics{
		buckets: bucketsMap,
	}
}

func NewDefaultJobDurationMetrics() *DurationMetrics {
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

func (d *DurationMetrics) Record(value float64) {
	if d.count == 0 || value < d.min {
		d.min = value
	}
	if d.count == 0 || value > d.max {
		d.max = value
	}
	d.count++
	d.sum += value

	for bucket := range d.buckets {
		if value <= bucket {
			d.buckets[bucket]++
		}
	}
}

func (d *DurationMetrics) GetMin() float64 {
	return d.min
}

func (d *DurationMetrics) GetMax() float64 {
	return d.max
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
