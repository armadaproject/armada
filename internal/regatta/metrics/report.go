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

// Report is a snapshot of Armada's own Prometheus metrics over one window of a regatta run.
//
// Every metric field is a *float64: nil means Prometheus had no value for that query (metric not
// scraped, no samples in range, or a non-finite result) rather than a hard failure - collecting a
// report never fails a run that already succeeded.
type Report struct {
	// QueueCount is how many queues the run declared. The names are in the embedded scenario.
	QueueCount    int       `json:"queueCount"`
	Start         time.Time `json:"start"`
	End           time.Time `json:"end"`
	WindowSeconds float64   `json:"windowSeconds"`
	// LookbackSeconds is the lookback the report's rate and quantile queries used: WindowSeconds, stretched to at
	// least a minute. When it is longer than WindowSeconds, the report also reflects samples from before Start,
	// which belong to the report before it in a continuous run.
	LookbackSeconds float64 `json:"lookbackSeconds"`

	// Scenario is the fully-resolved scenario config the run used (absolute paths, defaults
	// applied) - not the raw file the user wrote, matching what actually ran. Left as `any` (set
	// by the caller, typically a *regattaconfig.Scenario) rather than that concrete type to avoid
	// an import cycle: regatta/config already imports regatta/metrics for DefaultSettleDelay.
	Scenario any `json:"scenario,omitempty"`

	// ReadinessFailures lists every target whose readiness check gave up but whose run carried on
	// regardless (cluster.continueOnReadinessFailure). Empty means every target passed its check,
	// or had it switched off. A report with entries here comes from an environment the scheduler
	// never confirmed it could place jobs on, so its numbers may include that warm-up or worse.
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
	// Error is the readiness check's own message, including the last canary event seen and the
	// scheduler's job report when it had one.
	Error string `json:"error"`
}

// JobAgeTier is how old the declared queues' jobs were at the worst moment of the window: the highest of the
// per-scrape quantile of the time the jobs queued at that moment had waited (queued*), and of the time the jobs
// running at that moment had been running (running*). Armada exports these as snapshots of the jobs present at
// each scrape, so they are not the wait or run time of jobs that finished.
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

// namedQuery pairs a query expression with where its result should be written in the Report,
// letting Collect run every query the same way instead of repeating error-handling per field.
type namedQuery struct {
	expr string
	dest **float64
}

// collectConcurrency is how many Prometheus queries Collect runs at once.
const collectConcurrency = 8

// Collect runs the full fixed query set against promURL at end, using [start, end] as the lookback
// window, and returns a populated Report. A window shorter than a minute is queried with a one-minute
// lookback (see minWindowSeconds); the report keeps the requested Start, End and WindowSeconds and states the
// lookback it used in LookbackSeconds, so a reader can see when it reaches back before Start. The per-queue measures are taken over the
// declared queues together (one filter naming all of them, never unfiltered, so other tenants on a
// shared instance are left out). A per-query failure is logged and leaves that field nil; Collect itself
// only errors if it never reached Prometheus at all (e.g. every single query failed to even connect).
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

		{peakQueueSizeQuery(queues, window), &report.QueueDepth.PeakQueueSize},
		{peakLeasedPodCountQuery(queues, window), &report.QueueDepth.PeakLeased},

		{submitThroughputQuery(window, lookbackSeconds), &report.APISurface.SubmitThroughput},
		{submitLatencyQuery(0.95, window), &report.APISurface.SubmitP95},
		{submitErrorsQuery(window), &report.APISurface.SubmitErrors},
		{lookoutLatencyQuery(0.95, window), &report.APISurface.LookoutP95},

		{executorSubmitLatencyQuery(0.95, window), &report.ExecutorAndPulsar.ExecutorSubmitP95},
		{executorLeaseLatencyQuery(0.95, window), &report.ExecutorAndPulsar.ExecutorLeaseP95},
		{pulsarPublishLatencyQuery(0.95, window), &report.ExecutorAndPulsar.PulsarPublishP95},
		{pulsarPublishErrorsQuery(window), &report.ExecutorAndPulsar.PulsarErrors},
	}

	// Each query writes only its own field, so they can run side by side without locking.
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
