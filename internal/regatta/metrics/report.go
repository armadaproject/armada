package metrics

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	log "github.com/armadaproject/armada/internal/common/logging"
)

// Report is a snapshot of Armada's Prometheus metrics over one window of a run. A nil metric means Prometheus had no
// value for its query; collecting never fails a run that succeeded.
type Report struct {
	// QueueCount is how many queues the run declared; the names are in Scenario.
	QueueCount    int       `json:"queueCount"`
	Start         time.Time `json:"start"`
	End           time.Time `json:"end"`
	WindowSeconds float64   `json:"windowSeconds"`
	// LookbackSeconds is the lookback the rate and quantile queries used: WindowSeconds, but at least a minute. When it is
	// longer, the report reflects samples from before Start.
	LookbackSeconds float64 `json:"lookbackSeconds"`

	// Scenario is the resolved scenario the run used. It is an `any` because config imports this package.
	Scenario any `json:"scenario,omitempty"`

	// ReadinessFailures lists the targets whose readiness check gave up and whose run continued anyway
	// (cluster.continueOnReadinessFailure). Such a report's numbers may include the scheduler's warm-up.
	ReadinessFailures []ReadinessFailure `json:"readinessFailures,omitempty"`

	JobAge            JobAgeTier         `json:"jobAge"`
	Scheduler         SchedulerTier      `json:"scheduler"`
	QueueDepth        QueueDepthTier     `json:"queueDepth"`
	APISurface        APISurfaceTier     `json:"apiSurface"`
	ExecutorAndPulsar ExecutorPulsarTier `json:"executorAndPulsar"`
}

// ReadinessFailure records one target's failed readiness check that the run continued past.
type ReadinessFailure struct {
	Target string `json:"target"`
	// Error is the readiness check's message, with the last canary event and the scheduler's job report if it had one.
	Error string `json:"error"`
}

// JobAgeTier is how old the queues' jobs were at the worst moment of the window: the highest per-scrape quantile of how
// long the jobs then queued had waited (queued*), and the jobs then running had run (running*). Armada exports
// snapshots of the jobs present at each scrape, so these are not the times of jobs that finished.
type JobAgeTier struct {
	QueuedP50  *float64 `json:"queuedP50,omitempty"`
	QueuedP95  *float64 `json:"queuedP95,omitempty"`
	QueuedP99  *float64 `json:"queuedP99,omitempty"`
	RunningP50 *float64 `json:"runningP50,omitempty"`
	RunningP95 *float64 `json:"runningP95,omitempty"`
	RunningP99 *float64 `json:"runningP99,omitempty"`
}

type SchedulerTier struct {
	ScheduleCycleP95 *float64 `json:"scheduleCycleP95,omitempty"`
	ScheduleCycleP99 *float64 `json:"scheduleCycleP99,omitempty"`
	SubmitCheckP95   *float64 `json:"submitCheckP95,omitempty"`
	ScheduledTotal   *float64 `json:"scheduledTotal,omitempty"`
}

type QueueDepthTier struct {
	PeakQueueSize *float64 `json:"peakQueueSize,omitempty"`
	PeakLeased    *float64 `json:"peakLeased,omitempty"`
}

type APISurfaceTier struct {
	SubmitThroughput *float64 `json:"submitThroughput,omitempty"`
	SubmitP95        *float64 `json:"submitP95,omitempty"`
	SubmitErrors     *float64 `json:"submitErrors,omitempty"`
	LookoutP95       *float64 `json:"lookoutP95,omitempty"`
}

type ExecutorPulsarTier struct {
	ExecutorSubmitP95 *float64 `json:"executorSubmitP95,omitempty"`
	ExecutorLeaseP95  *float64 `json:"executorLeaseP95,omitempty"`
	PulsarPublishP95  *float64 `json:"pulsarPublishP95,omitempty"`
	PulsarErrors      *float64 `json:"pulsarErrors,omitempty"`
}

// namedQuery pairs a query with the Report field its result is written to.
type namedQuery struct {
	expr string
	dest **float64
}

// collectConcurrency is how many Prometheus queries Collect runs at once.
const collectConcurrency = 8

// Collect runs the fixed query set at end over the lookback [start, end], at least a minute (see minWindowSeconds), and
// reports the lookback in LookbackSeconds. Per-queue measures cover all the declared queues together, never unfiltered,
// so other tenants on a shared instance are left out. A failed query is logged and leaves its field nil; Collect errors
// only if no query reached Prometheus.
func Collect(ctx context.Context, promURL string, queues []string, start, end time.Time) (*Report, error) {
	windowSeconds := end.Sub(start).Seconds()
	lookbackSeconds := math.Max(windowSeconds, minWindowSeconds)
	window := windowStr(lookbackSeconds)

	report := &Report{
		QueueCount:      len(queues),
		Start:           start,
		End:             end,
		WindowSeconds:   windowSeconds,
		LookbackSeconds: lookbackSeconds,
	}

	queries := []namedQuery{
		{queuedAgeQuery(0.50, queues, window, lookbackSeconds), &report.JobAge.QueuedP50},
		{queuedAgeQuery(0.95, queues, window, lookbackSeconds), &report.JobAge.QueuedP95},
		{queuedAgeQuery(0.99, queues, window, lookbackSeconds), &report.JobAge.QueuedP99},
		{runningAgeQuery(0.50, queues, window, lookbackSeconds), &report.JobAge.RunningP50},
		{runningAgeQuery(0.95, queues, window, lookbackSeconds), &report.JobAge.RunningP95},
		{runningAgeQuery(0.99, queues, window, lookbackSeconds), &report.JobAge.RunningP99},

		{scheduleCycleQuery(0.95, window), &report.Scheduler.ScheduleCycleP95},
		{scheduleCycleQuery(0.99, window), &report.Scheduler.ScheduleCycleP99},
		{submitCheckQuery(0.95, window), &report.Scheduler.SubmitCheckP95},
		{scheduledJobsQuery(queues, window), &report.Scheduler.ScheduledTotal},

		{peakQueueSizeQuery(queues, window, lookbackSeconds), &report.QueueDepth.PeakQueueSize},
		{peakLeasedPodCountQuery(queues, window, lookbackSeconds), &report.QueueDepth.PeakLeased},

		{submitThroughputQuery(window, lookbackSeconds), &report.APISurface.SubmitThroughput},
		{submitLatencyQuery(0.95, window), &report.APISurface.SubmitP95},
		{submitErrorsQuery(window), &report.APISurface.SubmitErrors},
		{lookoutLatencyQuery(0.95, window), &report.APISurface.LookoutP95},

		{executorSubmitLatencyQuery(0.95, window), &report.ExecutorAndPulsar.ExecutorSubmitP95},
		{executorLeaseLatencyQuery(0.95, window), &report.ExecutorAndPulsar.ExecutorLeaseP95},
		{pulsarPublishLatencyQuery(0.95, window), &report.ExecutorAndPulsar.PulsarPublishP95},
		{pulsarPublishErrorsQuery(window), &report.ExecutorAndPulsar.PulsarErrors},
	}

	// Each query writes only its own field, so they can run side by side.
	var failures atomic.Int32
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(collectConcurrency)
	for _, nq := range queries {
		nq := nq
		group.Go(func() error {
			val, err := query(groupCtx, promURL, nq.expr, end)
			if err != nil {
				failures.Add(1)
				log.Warnf("prometheus query %q failed: %s", truncateExpr(nq.expr), err)
				return nil
			}
			*nq.dest = val
			return nil
		})
	}
	_ = group.Wait()

	if int(failures.Load()) == len(queries) {
		return nil, fmt.Errorf("every prometheus query failed against %q - is Prometheus reachable?", promURL)
	}
	return report, nil
}

// truncateExpr shortens a long expression (a regex over hundreds of queue names) for a log line.
func truncateExpr(expr string) string {
	const limit = 200
	if len(expr) <= limit {
		return expr
	}
	return expr[:limit] + fmt.Sprintf("... (%d bytes)", len(expr))
}

// WriteJSON writes the report to path as indented JSON, creating/truncating the file.
func (r *Report) WriteJSON(path string) error {
	data, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return fmt.Errorf("marshaling metrics report: %w", err)
	}
	if dir := filepath.Dir(path); dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("creating directory %q for metrics report: %w", dir, err)
		}
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return fmt.Errorf("writing metrics report to %q: %w", path, err)
	}
	return nil
}
