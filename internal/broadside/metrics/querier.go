package metrics

import (
	"sync"
	"time"
)

const (
	// queryLatencyBase is the base bucket size for query latency histograms.
	// With exponential buckets, this gives: 10ms, 20ms, 40ms, 80ms, 160ms, etc.
	queryLatencyBase = 10 * time.Millisecond
)

// QueryType represents the type of query being executed.
type QueryType string

const (
	QueryTypeGetJobs               QueryType = "GetJobs"
	QueryTypeGetJobGroups          QueryType = "GetJobGroups"
	QueryTypeGetJobRunDebugMessage QueryType = "GetJobRunDebugMessage"
	QueryTypeGetJobRunError        QueryType = "GetJobRunError"
	QueryTypeGetJobSpec            QueryType = "GetJobSpec"
)

// FilterCombination represents the type of filter combination used in a query.
type FilterCombination string

const (
	FilterCombinationNone         FilterCombination = "none"
	FilterCombinationAnnotations  FilterCombination = "annotations"
	FilterCombinationRunningQueue FilterCombination = "running_queue"
	FilterCombinationQueuedJobSet FilterCombination = "queued_jobset"
	FilterCombinationPriority     FilterCombination = "priority"
	FilterCombinationErroredTime  FilterCombination = "errored_time"
	FilterCombinationClusterNode  FilterCombination = "cluster_node"
)

// queryKey uniquely identifies a query type and filter combination.
type queryKey struct {
	queryType   QueryType
	filterCombo FilterCombination
}

// queryStats holds statistics for a specific query type and filter combination.
type queryStats struct {
	executed         int
	failed           int
	latencyHistogram *DurationHistogram
}

func newQueryStats() *queryStats {
	return &queryStats{
		latencyHistogram: NewDurationHistogram(queryLatencyBase),
	}
}

// QueryError represents a query error with context.
type QueryError struct {
	QueryType   QueryType
	FilterCombo FilterCombination
	Error       error
	Timestamp   time.Time
}

// QuerierMetrics tracks performance metrics for the querier during a test run.
type QuerierMetrics struct {
	mu sync.Mutex

	stats            map[queryKey]*queryStats
	errors           []QueryError
	maxErrors        int
	errorInsertIndex int
}

// NewQuerierMetrics creates a new QuerierMetrics instance.
func NewQuerierMetrics() *QuerierMetrics {
	return &QuerierMetrics{
		stats:     make(map[queryKey]*queryStats),
		errors:    make([]QueryError, 0),
		maxErrors: 2000, // Default max errors
	}
}

// SetMaxErrors sets the maximum number of errors to collect.
// Uses a circular buffer to prevent unbounded memory growth.
func (m *QuerierMetrics) SetMaxErrors(max int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.maxErrors = max
	if max > 0 && len(m.errors) > max {
		m.errors = m.errors[:max]
	}
}

// Reset clears all collected metrics.
func (m *QuerierMetrics) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.stats = make(map[queryKey]*queryStats)
	m.errors = make([]QueryError, 0)
}

// RecordQuery records metrics for a completed query.
func (m *QuerierMetrics) RecordQuery(
	queryType QueryType,
	filterCombo FilterCombination,
	duration time.Duration,
	err error,
) {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := queryKey{queryType: queryType, filterCombo: filterCombo}
	stats, ok := m.stats[key]
	if !ok {
		stats = newQueryStats()
		m.stats[key] = stats
	}

	stats.executed++
	stats.latencyHistogram.Observe(duration)

	if err != nil {
		stats.failed++

		// Add error using circular buffer if max errors configured
		if m.maxErrors > 0 {
			if len(m.errors) < m.maxErrors {
				// Still growing the buffer
				m.errors = append(m.errors, QueryError{
					QueryType:   queryType,
					FilterCombo: filterCombo,
					Error:       err,
					Timestamp:   time.Now(),
				})
			} else {
				// Buffer is full, overwrite oldest error
				m.errors[m.errorInsertIndex] = QueryError{
					QueryType:   queryType,
					FilterCombo: filterCombo,
					Error:       err,
					Timestamp:   time.Now(),
				}
				m.errorInsertIndex = (m.errorInsertIndex + 1) % m.maxErrors
			}
		} else {
			// No limit, append normally (backward compatibility)
			m.errors = append(m.errors, QueryError{
				QueryType:   queryType,
				FilterCombo: filterCombo,
				Error:       err,
				Timestamp:   time.Now(),
			})
		}
	}
}

// QueryStatsReport contains statistics for a specific query type and filter combination.
type QueryStatsReport struct {
	QueryType      QueryType
	FilterCombo    FilterCombination
	TotalExecuted  int
	TotalFailed    int
	P50Latency     time.Duration
	P95Latency     time.Duration
	P99Latency     time.Duration
	MaxLatency     time.Duration
	AverageLatency time.Duration
}

// QuerierReport summarises the querier metrics at the end of a test.
type QuerierReport struct {
	TotalQueriesExecuted int
	TotalQueriesFailed   int
	StatsByQueryType     []QueryStatsReport
	Errors               []QueryError
}

// GenerateReport creates a summary report from the collected metrics.
func (m *QuerierMetrics) GenerateReport() QuerierReport {
	m.mu.Lock()
	defer m.mu.Unlock()

	report := QuerierReport{
		StatsByQueryType: make([]QueryStatsReport, 0, len(m.stats)),
		Errors:           make([]QueryError, len(m.errors)),
	}

	copy(report.Errors, m.errors)

	for key, stats := range m.stats {
		report.TotalQueriesExecuted += stats.executed
		report.TotalQueriesFailed += stats.failed

		latencyStats := stats.latencyHistogram.Stats()
		report.StatsByQueryType = append(report.StatsByQueryType, QueryStatsReport{
			QueryType:      key.queryType,
			FilterCombo:    key.filterCombo,
			TotalExecuted:  stats.executed,
			TotalFailed:    stats.failed,
			P50Latency:     latencyStats.P50,
			P95Latency:     latencyStats.P95,
			P99Latency:     latencyStats.P99,
			MaxLatency:     latencyStats.Max,
			AverageLatency: latencyStats.Average,
		})
	}

	return report
}
