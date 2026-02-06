package metrics

import (
	"sync"
	"time"

	"github.com/armadaproject/armada/internal/common/logging"
)

const (
	// latencyBase is the base bucket size for batch execution latency histograms.
	// With exponential buckets, this gives: 62.5ms, 125ms, 250ms, 500ms, 1s, 2s, etc.
	latencyBase = 62500 * time.Microsecond

	// backlogBase is the base bucket size for backlog size histograms.
	// With exponential buckets, this gives: 1, 2, 4, 8, 16, 32, etc.
	backlogBase = 1

	// backlogWaitBase is the base bucket size for backlog wait time histograms.
	// With exponential buckets, this gives: 1s, 2s, 4s, 8s, 16s, etc.
	// We use 1 second as we are primarily interested in long waits.
	backlogWaitBase = time.Second
)

// IngesterMetrics tracks performance metrics for the ingester during a test run.
// It uses histograms with exponential buckets rather than storing individual samples,
// allowing efficient memory usage regardless of test duration whilst still providing
// percentile data. Buckets are created dynamically as observations arrive.
type IngesterMetrics struct {
	mu sync.Mutex

	// Backlog tracking
	currentBacklogSize      int
	peakBacklogSize         int
	backlogHistogram        *IntHistogram
	backlogWaitHistogram    *DurationHistogram
	backlogWarningThreshold int
	lastWarningTime         time.Time
	warningCooldown         time.Duration

	// Execution tracking
	batchesExecuted  int
	batchesFailed    int
	queriesExecuted  int
	latencyHistogram *DurationHistogram
}

// NewIngesterMetrics creates a new IngesterMetrics with exponential histogram buckets.
func NewIngesterMetrics() *IngesterMetrics {
	return &IngesterMetrics{
		backlogHistogram:     NewIntHistogram(backlogBase),
		backlogWaitHistogram: NewDurationHistogram(backlogWaitBase),
		latencyHistogram:     NewDurationHistogram(latencyBase),
		warningCooldown:      10 * time.Second,
	}
}

// SetBacklogWarningThreshold sets the threshold at which backlog warnings are logged.
// If threshold is 0, warnings are disabled.
func (m *IngesterMetrics) SetBacklogWarningThreshold(threshold int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.backlogWarningThreshold = threshold
}

func (m *IngesterMetrics) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.currentBacklogSize = 0
	m.peakBacklogSize = 0
	m.backlogHistogram = NewIntHistogram(backlogBase)
	m.backlogWaitHistogram = NewDurationHistogram(backlogWaitBase)

	m.batchesExecuted = 0
	m.batchesFailed = 0
	m.queriesExecuted = 0
	m.latencyHistogram = NewDurationHistogram(latencyBase)
}

// RecordBacklogSize records the current backlog size (channel length + batch buffer).
func (m *IngesterMetrics) RecordBacklogSize(channelLen, batchBufferLen int) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.currentBacklogSize = channelLen + batchBufferLen
	if m.currentBacklogSize > m.peakBacklogSize {
		m.peakBacklogSize = m.currentBacklogSize
	}

	m.backlogHistogram.Observe(m.currentBacklogSize)

	// Check if we should warn about high backlog
	if m.backlogWarningThreshold > 0 && m.currentBacklogSize >= m.backlogWarningThreshold {
		now := time.Now()
		if now.Sub(m.lastWarningTime) >= m.warningCooldown {
			logging.WithFields(map[string]any{
				"current_backlog": m.currentBacklogSize,
				"threshold":       m.backlogWarningThreshold,
				"peak_backlog":    m.peakBacklogSize,
			}).Warn("Ingester backlog is high - test runner may be bottleneck")
			m.lastWarningTime = now
		}
	}
}

// RecordBacklogWaitTime records how long a query waited in the backlog before execution.
func (m *IngesterMetrics) RecordBacklogWaitTime(duration time.Duration) {
	m.backlogWaitHistogram.Observe(duration)
}

// RecordBatchExecution records metrics for a completed batch execution.
func (m *IngesterMetrics) RecordBatchExecution(queryCount int, duration time.Duration, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.batchesExecuted++
	if err != nil {
		m.batchesFailed++
	}
	m.queriesExecuted += queryCount

	m.latencyHistogram.Observe(duration)
}

// IngesterReport summarises the ingester metrics at the end of a test.
type IngesterReport struct {
	TotalQueriesExecuted int
	TotalBatchesExecuted int
	TotalBatchesFailed   int
	PeakBacklogSize      int
	AverageBacklogSize   float64
	P50BacklogSize       float64
	P95BacklogSize       float64
	P99BacklogSize       float64
	P50BacklogWaitTime   time.Duration
	P95BacklogWaitTime   time.Duration
	P99BacklogWaitTime   time.Duration
	MaxBacklogWaitTime   time.Duration
	P50ExecutionLatency  time.Duration
	P95ExecutionLatency  time.Duration
	P99ExecutionLatency  time.Duration
}

// GenerateReport creates a summary report from the collected metrics.
func (m *IngesterMetrics) GenerateReport() IngesterReport {
	m.mu.Lock()
	defer m.mu.Unlock()

	backlogStats := m.backlogHistogram.Stats()
	backlogWaitStats := m.backlogWaitHistogram.Stats()
	latencyStats := m.latencyHistogram.Stats()

	return IngesterReport{
		TotalQueriesExecuted: m.queriesExecuted,
		TotalBatchesExecuted: m.batchesExecuted,
		TotalBatchesFailed:   m.batchesFailed,
		PeakBacklogSize:      m.peakBacklogSize,
		AverageBacklogSize:   backlogStats.Average,
		P50BacklogSize:       backlogStats.P50,
		P95BacklogSize:       backlogStats.P95,
		P99BacklogSize:       backlogStats.P99,
		P50BacklogWaitTime:   backlogWaitStats.P50,
		P95BacklogWaitTime:   backlogWaitStats.P95,
		P99BacklogWaitTime:   backlogWaitStats.P99,
		MaxBacklogWaitTime:   backlogWaitStats.Max,
		P50ExecutionLatency:  latencyStats.P50,
		P95ExecutionLatency:  latencyStats.P95,
		P99ExecutionLatency:  latencyStats.P99,
	}
}
