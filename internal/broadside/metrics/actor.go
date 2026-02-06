package metrics

import (
	"sync"
	"time"
)

// ActorMetrics tracks metrics for the action execution component.
type ActorMetrics struct {
	mu                        sync.RWMutex
	totalReprioritisations    int
	totalCancellations        int
	totalJobsReprioritised    int
	totalJobsCancelled        int
	reprioritisationLatencies *DurationHistogram
	cancellationLatencies     *DurationHistogram
	errors                    []ActorError
	maxErrors                 int
}

// ActorError represents an error that occurred during action execution.
type ActorError struct {
	Timestamp time.Time
	Action    string // "reprioritise" or "cancel"
	Queue     string
	JobSet    string
	Error     string
}

// NewActorMetrics creates a new ActorMetrics instance.
func NewActorMetrics() *ActorMetrics {
	return &ActorMetrics{
		reprioritisationLatencies: NewDurationHistogram(time.Millisecond),
		cancellationLatencies:     NewDurationHistogram(time.Millisecond),
		errors:                    make([]ActorError, 0),
		maxErrors:                 100,
	}
}

// SetMaxErrors sets the maximum number of errors to collect.
func (m *ActorMetrics) SetMaxErrors(max int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.maxErrors = max
}

// RecordReprioritisation records a reprioritisation action.
func (m *ActorMetrics) RecordReprioritisation(duration time.Duration, jobCount int, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.totalReprioritisations++
	m.totalJobsReprioritised += jobCount
	m.reprioritisationLatencies.Observe(duration)

	if err != nil && len(m.errors) < m.maxErrors {
		m.errors = append(m.errors, ActorError{
			Timestamp: time.Now(),
			Action:    "reprioritise",
			Error:     err.Error(),
		})
	}
}

// RecordCancellation records a cancellation action.
func (m *ActorMetrics) RecordCancellation(duration time.Duration, jobCount int, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.totalCancellations++
	m.totalJobsCancelled += jobCount
	m.cancellationLatencies.Observe(duration)

	if err != nil && len(m.errors) < m.maxErrors {
		m.errors = append(m.errors, ActorError{
			Timestamp: time.Now(),
			Action:    "cancel",
			Error:     err.Error(),
		})
	}
}

// GenerateReport generates a report of actor metrics.
func (m *ActorMetrics) GenerateReport() *ActorReport {
	m.mu.RLock()
	defer m.mu.RUnlock()

	report := &ActorReport{
		TotalReprioritisations: m.totalReprioritisations,
		TotalCancellations:     m.totalCancellations,
		TotalJobsReprioritised: m.totalJobsReprioritised,
		TotalJobsCancelled:     m.totalJobsCancelled,
		Errors:                 make([]ActorError, len(m.errors)),
	}

	copy(report.Errors, m.errors)

	if m.totalReprioritisations > 0 {
		repriStats := m.reprioritisationLatencies.Stats()
		report.ReprioritisationP50Latency = repriStats.P50
		report.ReprioritisationP95Latency = repriStats.P95
		report.ReprioritisationP99Latency = repriStats.P99
	}

	if m.totalCancellations > 0 {
		cancelStats := m.cancellationLatencies.Stats()
		report.CancellationP50Latency = cancelStats.P50
		report.CancellationP95Latency = cancelStats.P95
		report.CancellationP99Latency = cancelStats.P99
	}

	return report
}

// Reset resets all metrics.
func (m *ActorMetrics) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.totalReprioritisations = 0
	m.totalCancellations = 0
	m.totalJobsReprioritised = 0
	m.totalJobsCancelled = 0
	m.reprioritisationLatencies = NewDurationHistogram(time.Millisecond)
	m.cancellationLatencies = NewDurationHistogram(time.Millisecond)
	m.errors = make([]ActorError, 0)
}

// ActorReport represents a report of actor metrics.
type ActorReport struct {
	TotalReprioritisations     int
	TotalCancellations         int
	TotalJobsReprioritised     int
	TotalJobsCancelled         int
	ReprioritisationP50Latency time.Duration
	ReprioritisationP95Latency time.Duration
	ReprioritisationP99Latency time.Duration
	CancellationP50Latency     time.Duration
	CancellationP95Latency     time.Duration
	CancellationP99Latency     time.Duration
	Errors                     []ActorError
}
