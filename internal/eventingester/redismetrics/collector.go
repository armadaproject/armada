package redismetrics

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/scheduler/leader"
)

// Metric name constants follow the pattern: ArmadaRedisMetricsPrefix + specific_metric_name
const (
	ArmadaRedisMetricsPrefix = "armada_event_redis_"

	// Stream-level top-N metrics
	RedisStreamMemoryBytesMetricName            = ArmadaRedisMetricsPrefix + "stream_memory_bytes"
	RedisStreamEventCountMetricName             = ArmadaRedisMetricsPrefix + "stream_event_count"
	RedisStreamAgeSecondsMetricName             = ArmadaRedisMetricsPrefix + "stream_age_seconds"
	RedisStreamSizeBytesDistributionMetricName  = ArmadaRedisMetricsPrefix + "stream_size_bytes_distribution"
	RedisStreamSizeEventsDistributionMetricName = ArmadaRedisMetricsPrefix + "stream_size_events_distribution"
	RedisStreamAgeSecondsDistributionMetricName = ArmadaRedisMetricsPrefix + "stream_age_seconds_distribution"

	// Queue-level aggregate metrics
	RedisQueueStreamsMetricName     = ArmadaRedisMetricsPrefix + "queue_streams_total"
	RedisQueueMemoryBytesMetricName = ArmadaRedisMetricsPrefix + "queue_memory_bytes_total"
	RedisQueueEventsMetricName      = ArmadaRedisMetricsPrefix + "queue_events_total"

	// Self-monitoring metrics
	RedisMetricsCollectionDurationMetricName      = ArmadaRedisMetricsPrefix + "metrics_collection_duration_seconds"
	RedisMetricsErrorsTotalMetricName             = ArmadaRedisMetricsPrefix + "metrics_errors_total"
	RedisMetricsLastCollectionTimestampMetricName = ArmadaRedisMetricsPrefix + "metrics_last_collection_timestamp"
	RedisMetricsStreamScannedMetricName           = ArmadaRedisMetricsPrefix + "metrics_streams_scanned_total"
)

// ScannerInterface defines the interface for scanning Redis streams.
type ScannerInterface interface {
	ScanAll(ctx context.Context) ([]StreamInfo, error)
}

// Collector implements prometheus.Collector for Redis stream metrics.
// It periodically scans Redis streams and caches metrics in an atomic snapshot.
type Collector struct {
	scanner ScannerInterface
	config  Config

	// Leadership support for gating metric collection
	leaderController leader.LeaderController

	// Top-N gauges (labels: queue, jobset)
	topNMemoryGauge *prometheus.GaugeVec
	topNEventsGauge *prometheus.GaugeVec
	topNAgeGauge    *prometheus.GaugeVec

	// Histograms (no labels)
	bytesHistogram  prometheus.Histogram
	eventsHistogram prometheus.Histogram
	ageHistogram    prometheus.Histogram

	// Per-queue aggregates (label: queue)
	queueStreamsGauge *prometheus.GaugeVec
	queueMemoryGauge  *prometheus.GaugeVec
	queueEventsGauge  *prometheus.GaugeVec

	// Self-monitoring
	collectionDuration      prometheus.Histogram
	errorsTotal             prometheus.Counter
	lastCollectionTimestamp prometheus.Gauge
	streamsScannedGauge     prometheus.Gauge

	state     atomic.Value // stores []prometheus.Metric
	collectMu sync.Mutex   // skip-if-busy pattern
}

func newBytesHistogram() prometheus.Histogram {
	return prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: RedisStreamSizeBytesDistributionMetricName,
		Help: "Distribution of Redis stream sizes in bytes",
		// Up to 512 GiB with 30 buckets (1 KiB, 2 KiB, 4 KiB, ..., 512 GiB)
		Buckets: prometheus.ExponentialBuckets(1024, 2, 30),
	})
}

func newEventsHistogram() prometheus.Histogram {
	return prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: RedisStreamSizeEventsDistributionMetricName,
		Help: "Distribution of Redis stream event counts",
		// Up to 536 million events
		Buckets: prometheus.ExponentialBuckets(10, 2, 30),
	})
}

func newAgeHistogram() prometheus.Histogram {
	return prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: RedisStreamAgeSecondsDistributionMetricName,
		Help: "Distribution of Redis stream ages in seconds",
		// Up to the 388.36 days (1 year) range with 25 buckets
		Buckets: prometheus.ExponentialBuckets(60, 2, 25),
	})
}

// NewCollector creates a new Collector instance.
func NewCollector(scanner ScannerInterface, config Config, leaderController leader.LeaderController) *Collector {
	collector := &Collector{
		scanner:          scanner,
		config:           config,
		leaderController: leaderController,

		topNMemoryGauge: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: RedisStreamMemoryBytesMetricName,
				Help: "Memory usage of top-N Redis streams by bytes",
			},
			[]string{"queue", "jobset"},
		),
		topNEventsGauge: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: RedisStreamEventCountMetricName,
				Help: "Event count of top-N Redis streams",
			},
			[]string{"queue", "jobset"},
		),
		topNAgeGauge: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: RedisStreamAgeSecondsMetricName,
				Help: "Age of top-N oldest Redis streams in seconds",
			},
			[]string{"queue", "jobset"},
		),
		bytesHistogram:  newBytesHistogram(),
		eventsHistogram: newEventsHistogram(),
		ageHistogram:    newAgeHistogram(),
		queueStreamsGauge: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: RedisQueueStreamsMetricName,
				Help: "Total number of streams per queue",
			},
			[]string{"queue"},
		),
		queueMemoryGauge: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: RedisQueueMemoryBytesMetricName,
				Help: "Total memory usage per queue in bytes",
			},
			[]string{"queue"},
		),
		queueEventsGauge: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: RedisQueueEventsMetricName,
				Help: "Total event count per queue",
			},
			[]string{"queue"},
		),
		collectionDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: RedisMetricsCollectionDurationMetricName,
			Help: "Duration of Redis metrics collection cycles",
		}),
		errorsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: RedisMetricsErrorsTotalMetricName,
			Help: "Total number of Redis metrics collection errors",
		}),
		lastCollectionTimestamp: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: RedisMetricsLastCollectionTimestampMetricName,
			Help: "Timestamp of last successful collection",
		}),
		streamsScannedGauge: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: RedisMetricsStreamScannedMetricName,
			Help: "Total number of streams found in last scan",
		}),
	}

	collector.reallocateHistograms()

	return collector
}

// Run starts the background collection loop.
func (c *Collector) Run(ctx *armadacontext.Context) error {
	ctx.Infof("Will update Redis metrics every %s", c.config.CollectionInterval)

	// Delay to guard against crash loops during startup and to prevent thundering herd on leadership changes
	// The delay is [0, 1 minute) to ensure that in the worst case, all collectors will be staggered by at least 1 minute.]
	initialDelayMax := c.config.InitialCollectionDelayMax
	if initialDelayMax == 0 {
		initialDelayMax = 1 * time.Minute
	}

	initialDelay := time.Duration(0)
	if initialDelayMax > 0 {
		initialDelay = time.Duration(rand.Int64N(int64(initialDelayMax)))
	}
	ctx.Infof("First collection will start in %s", time.Duration(initialDelay))
	select {
	case <-ctx.Done():
		ctx.Debugf("Context cancelled during initial delay, returning")
		return nil
	case <-time.After(initialDelay):
	}

	ticker := time.NewTicker(c.config.CollectionInterval)
	defer ticker.Stop()

	wasLeader := false
	for {
		isLeader := c.leaderController.GetToken().Leader()
		if !isLeader {
			c.ClearState()
			wasLeader = false
		} else {
			if !wasLeader {
				wasLeader = true
			}
			err := c.collectOnce(ctx)
			if err != nil {
				ctx.Logger().WithError(err).Warnf("error collecting Redis metrics")
			}
		}
		select {
		case <-ctx.Done():
			ctx.Debugf("Context cancelled, returning")
			return nil
		case <-ticker.C:
		}
	}
}

// Describe implements prometheus.Collector.
func (c *Collector) Describe(out chan<- *prometheus.Desc) {
	c.topNMemoryGauge.Describe(out)
	c.topNEventsGauge.Describe(out)
	c.topNAgeGauge.Describe(out)
	c.bytesHistogram.Describe(out)
	c.eventsHistogram.Describe(out)
	c.ageHistogram.Describe(out)
	c.queueStreamsGauge.Describe(out)
	c.queueMemoryGauge.Describe(out)
	c.queueEventsGauge.Describe(out)
	c.collectionDuration.Describe(out)
	c.errorsTotal.Describe(out)
	c.lastCollectionTimestamp.Describe(out)
	c.streamsScannedGauge.Describe(out)
}

// Collect implements prometheus.Collector.
// It serves metrics from the cached atomic snapshot without triggering any Redis operations.
// Non-leaders return immediately to prevent stale metrics exposure during leadership transitions.
func (c *Collector) Collect(metrics chan<- prometheus.Metric) {
	// Scrape-time leadership check prevents stale metrics during failover
	if !c.leaderController.GetToken().Leader() {
		return
	}

	state, ok := c.state.Load().([]prometheus.Metric)
	if ok {
		for _, m := range state {
			metrics <- m
		}
	}
}

func (c *Collector) ClearState() {
	c.state.Store([]prometheus.Metric{})
}

func (c *Collector) reallocateHistograms() {
	c.collectMu.Lock()
	defer c.collectMu.Unlock()
	c.reallocateHistogramsLocked()
}

func (c *Collector) reallocateHistogramsLocked() {
	c.bytesHistogram = newBytesHistogram()
	c.eventsHistogram = newEventsHistogram()
	c.ageHistogram = newAgeHistogram()
}

// resetMetricsForNewCycle resets all metrics (histograms and gauges) at the start of a collection cycle.
// Assumes collectMu is already held by the caller.
func (c *Collector) resetMetricsForNewCycle() {
	// Reallocate histograms to get fresh objects (no accumulation across cycles)
	c.bytesHistogram = newBytesHistogram()
	c.eventsHistogram = newEventsHistogram()
	c.ageHistogram = newAgeHistogram()

	// Reset gauges to clear stale labels from previous cycle
	c.topNMemoryGauge.Reset()
	c.topNEventsGauge.Reset()
	c.topNAgeGauge.Reset()
	c.queueStreamsGauge.Reset()
	c.queueMemoryGauge.Reset()
	c.queueEventsGauge.Reset()
}

// collectOnce performs a single collection cycle.
func (c *Collector) collectOnce(ctx context.Context) error {
	// Skip if previous collection still running (skip-if-busy pattern)
	if !c.collectMu.TryLock() {
		return fmt.Errorf("previous collection still running, skipping")
	}
	defer c.collectMu.Unlock()

	// Reset all metrics for fresh collection cycle
	c.resetMetricsForNewCycle()

	start := time.Now()

	// Scan all streams
	streams, err := c.scanner.ScanAll(ctx)
	if err != nil {
		c.errorsTotal.Inc()
		// Update self-monitoring even on error
		c.collectionDuration.Observe(time.Since(start).Seconds())
		c.lastCollectionTimestamp.SetToCurrentTime()
		c.streamsScannedGauge.Set(0)
		// Collect snapshot with error metrics
		c.collectSnapshot()
		return fmt.Errorf("scanner error: %w", err)
	}

	// Sort for top-N computations
	byMemory := make([]StreamInfo, len(streams))
	copy(byMemory, streams)
	sort.Slice(byMemory, func(i, j int) bool {
		return byMemory[i].MemoryBytes > byMemory[j].MemoryBytes
	})

	byEvents := make([]StreamInfo, len(streams))
	copy(byEvents, streams)
	sort.Slice(byEvents, func(i, j int) bool {
		return byEvents[i].Length > byEvents[j].Length
	})

	byAge := make([]StreamInfo, len(streams))
	copy(byAge, streams)
	sort.Slice(byAge, func(i, j int) bool {
		return byAge[i].AgeSeconds > byAge[j].AgeSeconds
	})

	// Update top-N gauges
	topN := min(c.config.TopN, len(byMemory))

	for i := range topN {
		c.topNMemoryGauge.WithLabelValues(byMemory[i].Queue, byMemory[i].JobSetId).Set(float64(byMemory[i].MemoryBytes))
		c.topNEventsGauge.WithLabelValues(byEvents[i].Queue, byEvents[i].JobSetId).Set(float64(byEvents[i].Length))
		c.topNAgeGauge.WithLabelValues(byAge[i].Queue, byAge[i].JobSetId).Set(byAge[i].AgeSeconds)
	}

	// Update histograms
	for _, s := range streams {
		c.bytesHistogram.Observe(float64(s.MemoryBytes))
		c.eventsHistogram.Observe(float64(s.Length))
		c.ageHistogram.Observe(s.AgeSeconds)
	}

	// Aggregate by queue
	queueStats := make(map[string]struct {
		count  int
		bytes  int64
		events int64
	})
	for _, s := range streams {
		stats := queueStats[s.Queue]
		stats.count++
		stats.bytes += s.MemoryBytes
		stats.events += s.Length
		queueStats[s.Queue] = stats
	}

	for queue, stats := range queueStats {
		c.queueStreamsGauge.WithLabelValues(queue).Set(float64(stats.count))
		c.queueMemoryGauge.WithLabelValues(queue).Set(float64(stats.bytes))
		c.queueEventsGauge.WithLabelValues(queue).Set(float64(stats.events))
	}

	// Update self-monitoring
	c.collectionDuration.Observe(time.Since(start).Seconds())
	c.lastCollectionTimestamp.SetToCurrentTime()
	c.streamsScannedGauge.Set(float64(len(streams)))

	// Collect all metrics into snapshot (AFTER updating self-monitoring)
	c.collectSnapshot()

	return nil
}

// collectSnapshot collects all metrics into an atomic snapshot.
func (c *Collector) collectSnapshot() {
	ch := make(chan prometheus.Metric, 10000)
	go func() {
		c.topNMemoryGauge.Collect(ch)
		c.topNEventsGauge.Collect(ch)
		c.topNAgeGauge.Collect(ch)
		c.bytesHistogram.Collect(ch)
		c.eventsHistogram.Collect(ch)
		c.ageHistogram.Collect(ch)
		c.queueStreamsGauge.Collect(ch)
		c.queueMemoryGauge.Collect(ch)
		c.queueEventsGauge.Collect(ch)
		c.collectionDuration.Collect(ch)
		c.errorsTotal.Collect(ch)
		c.lastCollectionTimestamp.Collect(ch)
		c.streamsScannedGauge.Collect(ch)
		close(ch)
	}()

	var allMetrics []prometheus.Metric
	for m := range ch {
		allMetrics = append(allMetrics, m)
	}

	// Store snapshot atomically
	c.state.Store(allMetrics)
}
