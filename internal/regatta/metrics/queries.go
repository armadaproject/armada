package metrics

import (
	"fmt"
	"regexp"
	"strings"
)

// minWindowSeconds is the shortest lookback window a report queries: a rate() needs a few scrape intervals of
// samples, so a very short window would be meaningless.
const minWindowSeconds = 60

// windowStr formats the lookback window used by every rate()/increase()/max_over_time() query below.
func windowStr(windowSeconds float64) string {
	return fmt.Sprintf("%ds", int64(windowSeconds))
}

// counterDelta is how much each series of a counter grew over the window ending at the evaluation time: its value
// now minus its last value at or before the window's start. rate() and increase() count only the growth between
// samples inside the window, so a burst that falls between the last sample before the window and the first one
// inside it (a one-shot load of a few seconds against a 5s scrape interval) reads as 0, and the rest is
// extrapolated; this counts it exactly. A series that did not exist at the window's start counts from 0, and a
// series that went backwards (a reset) counts as 0.
func counterDelta(selector, window string) string {
	return fmt.Sprintf("clamp_min(%[1]s - ((%[1]s offset %[2]s) or (%[1]s * 0)), 0)", selector, window)
}

// snapshotStep is the resolution at which peakQuantile looks at a snapshot histogram within the window.
const snapshotStep = "10s"

// peakQuantile is the highest, over the window, of the q-quantile of a snapshot histogram. armada_job_queued_seconds
// and armada_job_run_time_seconds are not counters of finished jobs: the scheduler rebuilds them at every scrape
// from the jobs that are queued, or running, at that moment, so their buckets fall as jobs leave and rate() would
// read each fall as a counter reset. What they do say is how long the jobs present at a scrape have waited, or have
// been running, so the worst such quantile over the window is reported.
func peakQuantile(metric string, q float64, filter, window string) string {
	selector := metric
	if filter != "" {
		selector = fmt.Sprintf("%s{%s}", metric, filter)
	}
	return fmt.Sprintf("max_over_time(histogram_quantile(%g, sum by (le) (%s))[%s:%s])", q, selector, window, snapshotStep)
}

// histogramQuantile builds the q-quantile of the observations a histogram that is always exported recorded over
// the window (its buckets' growth, see counterDelta). filter may be empty.
func histogramQuantile(metric string, q float64, filter, window string) string {
	selector := metric
	if filter != "" {
		selector = fmt.Sprintf("%s{%s}", metric, filter)
	}
	return fmt.Sprintf("histogram_quantile(%g, sum by (le) (%s))", q, counterDelta(selector, window))
}

// queueMatcher returns the label matcher that selects the given queues on label: an exact match for one
// queue, otherwise a regex of the names. Prometheus anchors regexes, so each name must match whole. The
// names are regexp-escaped, so a queue called "team.a" does not also match "teamXa".
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

// Tier 1: job age. How long the jobs queued at a moment had waited, and the jobs running at a moment had been
// running, at the worst moment of the window (see peakQuantile). These are not completed-job latencies.
func queuedAgeQuery(q float64, queues []string, window string) string {
	return peakQuantile("armada_job_queued_seconds_bucket", q, queueFilter(queues), window)
}

func runningAgeQuery(q float64, queues []string, window string) string {
	return peakQuantile("armada_job_run_time_seconds_bucket", q, queueFilter(queues), window)
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

// Tier 3: queue depth. Summed across label dimensions (e.g. pool) before max_over_time, same
// reasoning as queueSizeQuery/leasedPodCountQuery below - otherwise this reports one arbitrary
// series's peak rather than the queue's true combined peak.
func peakQueueSizeQuery(queues []string, window string) string {
	return fmt.Sprintf(`max_over_time(sum(armada_queue_size{%s,state="validated"})[%s:])`, queueMatcher("queue", queues), window)
}

func peakLeasedPodCountQuery(queues []string, window string) string {
	return fmt.Sprintf(`max_over_time(sum(armada_queue_leased_pod_count{%s})[%s:])`, queueMatcher("queue", queues), window)
}

// queueSizeQuery and leasedPodCountQuery are unwindowed instant queries (the current value, not a
// windowed max) used by WaitForQueueDrain to detect when a queue has fully drained. Both are
// wrapped in sum() because the scheduler emits one series per queue *and* label dimension (e.g.
// pool) - query()'s caller only ever looks at a single float, so without the sum it would only
// ever see one arbitrary series out of several and could keep reading a stale non-zero value long
// after the queue's real aggregate lease/queue count had actually reached zero.
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
