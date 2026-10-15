package metrics

import (
	"fmt"
	"math"
	"regexp"
	"strings"
)

// minWindowSeconds is the shortest lookback a report queries: a rate() needs a few scrape intervals of samples.
const minWindowSeconds = 60

// windowStr formats the lookback used by the queries below.
func windowStr(windowSeconds float64) string {
	return fmt.Sprintf("%ds", int64(windowSeconds))
}

// counterDelta is how much each series of a counter grew over the window: its value now minus its last value at or
// before the window's start. rate() and increase() count only growth between samples inside the window, so a burst that
// falls between two scrapes (a one-shot load against a 5s scrape interval) reads as 0. A series new in the window
// counts from 0, and a reset counts as 0.
func counterDelta(selector, window string) string {
	return fmt.Sprintf("clamp_min(%[1]s - ((%[1]s offset %[2]s) or (%[1]s * 0)), 0)", selector, window)
}

const (
	// minSnapshotStepSeconds is the finest evaluation step of peakQuantile: the snapshots are rebuilt and scraped every 5s,
	// so a coarser step could miss a peak between two evaluations.
	minSnapshotStepSeconds = 5
	// maxSnapshotSteps bounds a query on a very long window; beyond it the step grows.
	maxSnapshotSteps = 720
)

// snapshotStep is the evaluation step for a window: 5s, coarser only when that would take over 720 evaluations.
func snapshotStep(windowSeconds float64) string {
	step := int64(minSnapshotStepSeconds)
	if perStep := int64(math.Ceil(windowSeconds / maxSnapshotSteps)); perStep > step {
		step = perStep
	}
	return fmt.Sprintf("%ds", step)
}

// peakQuantile is the highest, over the window, of the q-quantile of a snapshot histogram. The queued-time and run-time
// histograms are rebuilt at every scrape from the jobs queued or running at that moment, so their buckets fall as jobs
// leave and rate() would read each fall as a counter reset. They say how long the jobs present had waited or run.
func peakQuantile(metric string, q float64, filter, window string, windowSeconds float64) string {
	selector := metric
	if filter != "" {
		selector = fmt.Sprintf("%s{%s}", metric, filter)
	}
	return fmt.Sprintf("max_over_time(histogram_quantile(%g, sum by (le) (%s))[%s:%s])", q, selector, window, snapshotStep(windowSeconds))
}

// histogramQuantile is the q-quantile of the observations a histogram recorded over the window (its buckets' growth,
// see counterDelta). filter may be empty.
func histogramQuantile(metric string, q float64, filter, window string) string {
	selector := metric
	if filter != "" {
		selector = fmt.Sprintf("%s{%s}", metric, filter)
	}
	return fmt.Sprintf("histogram_quantile(%g, sum by (le) (%s))", q, counterDelta(selector, window))
}

// queueMatcher selects the given queues on label: exact for one queue, otherwise a regex. The names are
// regexp-escaped, so "team.a" does not match "teamXa".
func queueMatcher(label string, queues []string) string {
	if len(queues) == 1 {
		return fmt.Sprintf(`%s=%q`, label, queues[0])
	}
	escaped := make([]string, len(queues))
	for i, q := range queues {
		escaped[i] = regexp.QuoteMeta(q)
	}
	return fmt.Sprintf(`%s=~%q`, label, strings.Join(escaped, "|"))
}

func queueFilter(queues []string) string {
	return queueMatcher("queueName", queues)
}

// Tier 1: job age, the age of the jobs queued (or running) at the worst moment of the window. Not completed-job latency.
func queuedAgeQuery(q float64, queues []string, window string, windowSeconds float64) string {
	return peakQuantile("armada_job_queued_seconds_bucket", q, queueFilter(queues), window, windowSeconds)
}

func runningAgeQuery(q float64, queues []string, window string, windowSeconds float64) string {
	return peakQuantile("armada_job_run_time_seconds_bucket", q, queueFilter(queues), window, windowSeconds)
}

// Tier 2: scheduler.
func scheduleCycleQuery(q float64, window string) string {
	return histogramQuantile("armada_scheduler_schedule_cycle_times_bucket", q, "", window)
}

func submitCheckQuery(q float64, window string) string {
	return histogramQuantile("armada_scheduler_submit_check_times_bucket", q, "", window)
}

func scheduledJobsQuery(queues []string, window string) string {
	return fmt.Sprintf(`sum(%s)`, counterDelta(fmt.Sprintf("armada_scheduler_scheduled_jobs{%s}", queueMatcher("queue", queues)), window))
}

// Tier 3: queue depth, summed across label dimensions (e.g. pool) before max_over_time, as in queueSizeQuery. The
// subquery has an explicit step: its default is Prometheus's evaluation interval (a minute unless configured), which
// could step over a short peak.
func peakQueueSizeQuery(queues []string, window string, windowSeconds float64) string {
	return fmt.Sprintf(`max_over_time(sum(armada_queue_size{%s,state="validated"})[%s:%s])`, queueMatcher("queue", queues), window, snapshotStep(windowSeconds))
}

func peakLeasedPodCountQuery(queues []string, window string, windowSeconds float64) string {
	return fmt.Sprintf(`max_over_time(sum(armada_queue_leased_pod_count{%s})[%s:%s])`, queueMatcher("queue", queues), window, snapshotStep(windowSeconds))
}

// queueSizeQuery and leasedPodCountQuery are instant queries for WaitForQueueDrain. They are wrapped in sum() because
// the scheduler emits one series per queue and label dimension, and the caller reads a single float.
func queueSizeQuery(queues []string) string {
	return fmt.Sprintf(`sum(armada_queue_size{%s,state="validated"})`, queueMatcher("queue", queues))
}

func leasedPodCountQuery(queues []string) string {
	return fmt.Sprintf(`sum(armada_queue_leased_pod_count{%s})`, queueMatcher("queue", queues))
}

// Tier 4: API surface.

// submitThroughputQuery is the SubmitJobs calls per second, averaged over the window of windowSeconds.
func submitThroughputQuery(window string, windowSeconds float64) string {
	return fmt.Sprintf(`sum(%s) / %g`, counterDelta(`grpc_server_handled_total{grpc_service="api.Submit",grpc_method="SubmitJobs",grpc_code="OK"}`, window), windowSeconds)
}

func submitLatencyQuery(q float64, window string) string {
	return histogramQuantile("grpc_server_handling_seconds_bucket", q, `grpc_service="api.Submit",grpc_method="SubmitJobs"`, window)
}

func submitErrorsQuery(window string) string {
	// No failing call is a result of 0, not a missing value.
	return fmt.Sprintf(`sum(%s) or vector(0)`, counterDelta(`grpc_server_handled_total{grpc_service="api.Submit",grpc_code!="OK"}`, window))
}

func lookoutLatencyQuery(q float64, window string) string {
	return histogramQuantile("grpc_server_handling_seconds_bucket", q, `job="armada-lookout"`, window)
}

// Tier 5: executor + Pulsar.
func executorSubmitLatencyQuery(q float64, window string) string {
	return histogramQuantile("armada_executor_submit_runs_latency_seconds_bucket", q, "", window)
}

func executorLeaseLatencyQuery(q float64, window string) string {
	return histogramQuantile("armada_executor_request_runs_latency_seconds_bucket", q, "", window)
}

func pulsarPublishLatencyQuery(q float64, window string) string {
	return histogramQuantile("pulsar_client_producer_latency_seconds_bucket", q, "", window)
}

func pulsarPublishErrorsQuery(window string) string {
	return fmt.Sprintf("sum(%s) or vector(0)", counterDelta("pulsar_client_producer_errors", window))
}
