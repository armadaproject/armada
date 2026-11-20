package replay

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"os"
	"slices"
	"strings"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

const bucketsPerPass = 128

type seriesKind uint8

const (
	arrivalEvent seriesKind = iota
	completionEvent
	preemptionEvent
	queueInterval
	executionInterval
)

type seriesRecord struct {
	Kind       seriesKind
	Queue      string
	Priority   string
	Pool       string
	StartUS    int64
	EndUS      int64
	StartupUS  int64
	RetainedUS int64
	LostUS     int64
	Requests   v1.ResourceList
	Censored   bool
}

type seriesSpool struct {
	file   *os.File
	buffer *bufio.Writer
	budget int64
	size   int64
}

func openSeriesSpool(path string, budget int64) (*seriesSpool, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil, err
	}
	return &seriesSpool{file: file, buffer: bufio.NewWriter(file), budget: budget}, nil
}

func (s *seriesSpool) write(record seriesRecord) error {
	raw, err := json.Marshal(record)
	if err != nil {
		return err
	}
	size := int64(len(raw) + 1)
	if size > s.budget-s.size {
		return fmt.Errorf("series accounting needs more than %d bytes of scratch space", s.budget)
	}
	if _, err := s.buffer.Write(raw); err != nil {
		return err
	}
	if err := s.buffer.WriteByte('\n'); err != nil {
		return err
	}
	s.size += size
	return nil
}

func (s *seriesSpool) flush() error { return s.buffer.Flush() }

type seriesGroup struct {
	queue, priority, pool, resource string
}

type seriesMeasures struct {
	arrivals, completions, preemptions int64
	queued, running, oldestQueueUS     int64
	allocation, startup                big.Int
	retained, lost, provisional        big.Int
}

type seriesRow struct {
	StartUS       int64  `json:"start_us" parquet:"name=start_us, type=INT64"`
	EndUS         int64  `json:"end_us" parquet:"name=end_us, type=INT64"`
	Queue         string `json:"queue" parquet:"name=queue, type=BYTE_ARRAY, convertedtype=UTF8"`
	Priority      string `json:"priority_class" parquet:"name=priority_class, type=BYTE_ARRAY, convertedtype=UTF8"`
	Pool          string `json:"pool" parquet:"name=pool, type=BYTE_ARRAY, convertedtype=UTF8"`
	Resource      string `json:"resource" parquet:"name=resource, type=BYTE_ARRAY, convertedtype=UTF8"`
	Arrivals      int64  `json:"arrivals" parquet:"name=arrivals, type=INT64"`
	Completions   int64  `json:"completions" parquet:"name=completions, type=INT64"`
	Preemptions   int64  `json:"preemptions" parquet:"name=preemptions, type=INT64"`
	Queued        int64  `json:"queued" parquet:"name=queued, type=INT64"`
	Running       int64  `json:"running" parquet:"name=running, type=INT64"`
	OldestQueueUS int64  `json:"oldest_queue_age_us" parquet:"name=oldest_queue_age_us, type=INT64"`
	Allocation    string `json:"allocated_resource_us" parquet:"name=allocated_resource_us, type=BYTE_ARRAY, convertedtype=UTF8"`
	Startup       string `json:"startup_resource_us" parquet:"name=startup_resource_us, type=BYTE_ARRAY, convertedtype=UTF8"`
	Retained      string `json:"retained_resource_us" parquet:"name=retained_resource_us, type=BYTE_ARRAY, convertedtype=UTF8"`
	Lost          string `json:"lost_resource_us" parquet:"name=lost_resource_us, type=BYTE_ARRAY, convertedtype=UTF8"`
	Provisional   string `json:"provisional_resource_us" parquet:"name=provisional_resource_us, type=BYTE_ARRAY, convertedtype=UTF8"`
}

type seriesBucket map[seriesGroup]*seriesMeasures

func (b seriesBucket) measures(key seriesGroup) *seriesMeasures {
	m := b[key]
	if m == nil {
		m = new(seriesMeasures)
		b[key] = m
	}
	return m
}

func (b seriesBucket) keys() []seriesGroup {
	keys := make([]seriesGroup, 0, len(b))
	for key := range b {
		keys = append(keys, key)
	}
	slices.SortFunc(keys, func(a, b seriesGroup) int {
		if order := strings.Compare(a.queue, b.queue); order != 0 {
			return order
		}
		if order := strings.Compare(a.priority, b.priority); order != 0 {
			return order
		}
		if order := strings.Compare(a.pool, b.pool); order != 0 {
			return order
		}
		return strings.Compare(a.resource, b.resource)
	})
	return keys
}

func (b seriesBucket) record(row seriesRecord, from, to int64) {
	queueKey := seriesGroup{queue: row.Queue, priority: row.Priority}
	switch row.Kind {
	case arrivalEvent:
		b.measures(queueKey).arrivals++
	case completionEvent:
		b.measures(queueKey).completions++
	case preemptionEvent:
		b.measures(queueKey).preemptions++
	case queueInterval:
		if row.StartUS < to && row.EndUS >= to {
			m := b.measures(queueKey)
			m.queued++
			m.oldestQueueUS = max(m.oldestQueueUS, to-row.StartUS)
		}
	case executionInterval:
		if row.StartUS < to && row.EndUS >= to {
			b.measures(queueKey).running++
		}
		startUseful := row.StartUS + row.StartupUS
		endRetained := startUseful + row.RetainedUS
		endLost := endRetained + row.LostUS
		allocated := overlap(row.StartUS, row.EndUS, from, to)
		if allocated == 0 {
			return
		}
		startup := overlap(row.StartUS, startUseful, from, to)
		retained := overlap(startUseful, endRetained, from, to)
		lost := overlap(endRetained, endLost, from, to)
		provisional := allocated - startup - retained - lost
		for name, request := range row.Requests {
			if request.IsZero() {
				continue
			}
			m := b.measures(seriesGroup{row.Queue, row.Priority, row.Pool, string(name)})
			units := nanoUnits(request)
			addResourceTime(&m.allocation, units, allocated)
			addResourceTime(&m.startup, units, startup)
			addResourceTime(&m.retained, units, retained)
			addResourceTime(&m.lost, units, lost)
			addResourceTime(&m.provisional, units, provisional)
		}
	}
}

func overlap(start, end, from, to int64) int64 {
	return max(0, min(end, to)-max(start, from))
}

var nanoPerUnit = big.NewInt(1_000_000_000)

func nanoUnits(quantity resource.Quantity) *big.Int {
	dec := quantity.AsDec()
	raw := new(big.Int).Set(dec.UnscaledBig())
	delta := int64(9 - dec.Scale())
	power := new(big.Int).Exp(big.NewInt(10), big.NewInt(max(delta, -delta)), nil)
	if delta < 0 {
		return raw.Quo(raw, power)
	}
	return raw.Mul(raw, power)
}

func addResourceTime(total, units *big.Int, durationUS int64) {
	if durationUS > 0 {
		total.Add(total, new(big.Int).Mul(units, big.NewInt(durationUS)))
	}
}

func formatResourceTime(units *big.Int) string {
	raw := new(big.Rat).SetFrac(units, nanoPerUnit).FloatString(9)
	return strings.TrimRight(strings.TrimRight(raw, "0"), ".")
}

func (m *seriesMeasures) row(key seriesGroup, from, to int64) seriesRow {
	return seriesRow{
		StartUS: from, EndUS: to, Queue: key.queue, Priority: key.priority, Pool: key.pool, Resource: key.resource,
		Arrivals: m.arrivals, Completions: m.completions, Preemptions: m.preemptions,
		Queued: m.queued, Running: m.running, OldestQueueUS: m.oldestQueueUS,
		Allocation: formatResourceTime(&m.allocation), Startup: formatResourceTime(&m.startup),
		Retained: formatResourceTime(&m.retained), Lost: formatResourceTime(&m.lost),
		Provisional: formatResourceTime(&m.provisional),
	}
}

// Aggregate one fixed-size tile of buckets at a time. Closed attempts occupy
// one compact disk record regardless of their lifespan or bucket width.
func (s *seriesSpool) aggregate(ctx context.Context, path string, window Window, bucketUS int64) (_ []seriesRow, result error) {
	output, err := createParquet(path, new(seriesRow))
	if err != nil {
		return nil, err
	}
	defer func() { result = errors.Join(result, output.close()) }()
	totals := make(seriesBucket)
	for from := window.StartUS; from < window.EndUS; {
		required := 1 + (window.EndUS-from-1)/bucketUS
		count := int(min(bucketsPerPass, required))
		to := window.EndUS
		if int64(count) < required {
			to = from + int64(count)*bucketUS
		}
		buckets := make([]seriesBucket, count)
		for i := range buckets {
			buckets[i] = make(seriesBucket)
		}
		decoder := json.NewDecoder(io.NewSectionReader(s.file, 0, s.size))
		for {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			var record seriesRecord
			if err := decoder.Decode(&record); err != nil {
				if err == io.EOF {
					break
				}
				return nil, fmt.Errorf("read series accounting scratch: %w", err)
			}
			last := record.EndUS - 1
			if record.Kind < queueInterval {
				last = record.StartUS
			}
			if last < from || record.StartUS >= to || last < record.StartUS {
				continue
			}
			firstBucket := (max(from, record.StartUS) - from) / bucketUS
			lastBucket := (min(last, to-1) - from) / bucketUS
			for i := firstBucket; i <= lastBucket; i++ {
				start := from + i*bucketUS
				end := start + min(bucketUS, to-start)
				buckets[i].record(record, start, end)
			}
		}
		for i, bucket := range buckets {
			start := from + int64(i)*bucketUS
			end := start + min(bucketUS, to-start)
			for _, key := range bucket.keys() {
				m := bucket[key]
				if err := output.w.Write(m.row(key, start, end)); err != nil {
					return nil, err
				}
				if key.resource != "" {
					total := totals.measures(key)
					total.allocation.Add(&total.allocation, &m.allocation)
					total.startup.Add(&total.startup, &m.startup)
					total.retained.Add(&total.retained, &m.retained)
					total.lost.Add(&total.lost, &m.lost)
					total.provisional.Add(&total.provisional, &m.provisional)
				}
			}
		}
		from = to
	}
	work := make([]seriesRow, 0, len(totals))
	for _, key := range totals.keys() {
		work = append(work, totals[key].row(key, window.StartUS, window.EndUS))
	}
	return work, nil
}
