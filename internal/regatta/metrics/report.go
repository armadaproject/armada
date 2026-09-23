package metrics

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	log "github.com/armadaproject/armada/internal/common/logging"
)

// Report is a single post-run snapshot of Armada's own Prometheus metrics, queried once at the
// end of a regatta run. Schema/tiers/percentiles are ported directly from
// collect-perf-metrics.sh's Markdown tables (see that script for the live-deployment CI
// equivalent) - this is a faithful port, not a considered redesign. A future pass should compare
// this shape against how other large-batch schedulers (Slurm, Kubernetes scheduler benchmarks,
// Volcano, Kueue) report results before treating it as final.
//
// Every metric field is a *float64: nil means Prometheus had no value for that query (metric not
// scraped, no samples in range, or a non-finite result) rather than a hard failure - collecting a
// report never fails a run that already succeeded.
type Report struct {
	Queue         string    `json:"queue"`
	Start         time.Time `json:"start"`
	End           time.Time `json:"end"`
	WindowSeconds float64   `json:"windowSeconds"`

	// Scenario is the fully-resolved scenario config the run used (absolute paths, defaults
	// applied) - not the raw file the user wrote, matching what actually ran. Left as `any` (set
	// by the caller, typically a *regattaconfig.Scenario) rather than that concrete type to avoid
	// an import cycle: regatta/config already imports regatta/metrics for DefaultSettleDelay.
	Scenario any `json:"scenario,omitempty"`

	EndToEndLatency   LatencyTier        `json:"endToEndLatency"`
	Scheduler         SchedulerTier      `json:"scheduler"`
	QueueDepth        QueueDepthTier     `json:"queueDepth"`
	APISurface        APISurfaceTier     `json:"apiSurface"`
	ExecutorAndPulsar ExecutorPulsarTier `json:"executorAndPulsar"`
}

type LatencyTier struct {
	QueuedP50 *float64 `json:"queuedP50,omitempty"`
	QueuedP95 *float64 `json:"queuedP95,omitempty"`
	QueuedP99 *float64 `json:"queuedP99,omitempty"`
	RunP50    *float64 `json:"runP50,omitempty"`
	RunP95    *float64 `json:"runP95,omitempty"`
	RunP99    *float64 `json:"runP99,omitempty"`
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

// Collect runs the full fixed query set against promURL at end, using [start, end] as the
// lookback window (floored at 60s), and returns a populated Report. A per-query failure is
// logged and leaves that field nil; Collect itself only errors if it never reached Prometheus at
// all (e.g. every single query failed to even connect).
func Collect(ctx context.Context, promURL, queue string, start, end time.Time) (*Report, error) {
	windowSeconds := end.Sub(start).Seconds()
	window := windowStr(windowSeconds)

	report := &Report{
		Queue:         queue,
		Start:         start,
		End:           end,
		WindowSeconds: windowSeconds,
	}

	queries := []namedQuery{
		{queuedLatencyQuery(0.50, queue, window), &report.EndToEndLatency.QueuedP50},
		{queuedLatencyQuery(0.95, queue, window), &report.EndToEndLatency.QueuedP95},
		{queuedLatencyQuery(0.99, queue, window), &report.EndToEndLatency.QueuedP99},
		{runLatencyQuery(0.50, queue, window), &report.EndToEndLatency.RunP50},
		{runLatencyQuery(0.95, queue, window), &report.EndToEndLatency.RunP95},
		{runLatencyQuery(0.99, queue, window), &report.EndToEndLatency.RunP99},

		{scheduleCycleQuery(0.95, window), &report.Scheduler.ScheduleCycleP95},
		{scheduleCycleQuery(0.99, window), &report.Scheduler.ScheduleCycleP99},
		{submitCheckQuery(0.95, window), &report.Scheduler.SubmitCheckP95},
		{scheduledJobsQuery(queue, window), &report.Scheduler.ScheduledTotal},

		{peakQueueSizeQuery(queue, window), &report.QueueDepth.PeakQueueSize},
		{peakLeasedPodCountQuery(queue, window), &report.QueueDepth.PeakLeased},

		{submitThroughputQuery(window), &report.APISurface.SubmitThroughput},
		{submitLatencyQuery(0.95, window), &report.APISurface.SubmitP95},
		{submitErrorsQuery(window), &report.APISurface.SubmitErrors},
		{lookoutLatencyQuery(0.95, window), &report.APISurface.LookoutP95},

		{executorSubmitLatencyQuery(0.95, window), &report.ExecutorAndPulsar.ExecutorSubmitP95},
		{executorLeaseLatencyQuery(0.95, window), &report.ExecutorAndPulsar.ExecutorLeaseP95},
		{pulsarPublishLatencyQuery(0.95, window), &report.ExecutorAndPulsar.PulsarPublishP95},
		{pulsarPublishErrorsQuery(window), &report.ExecutorAndPulsar.PulsarErrors},
	}

	failures := 0
	for _, nq := range queries {
		val, err := query(ctx, promURL, nq.expr, end)
		if err != nil {
			failures++
			log.Warnf("prometheus query %q failed: %s", nq.expr, err)
			continue
		}
		*nq.dest = val
	}
	if failures == len(queries) {
		return nil, fmt.Errorf("every prometheus query failed against %q - is Prometheus reachable?", promURL)
	}
	return report, nil
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
