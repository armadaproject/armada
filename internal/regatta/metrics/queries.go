package metrics

import (
	"fmt"
	"regexp"
	"strings"
)

// windowStr formats the lookback window used by every rate()/increase()/max_over_time() query
// below, mirroring the CI script's WINDOW calc: the run's own wall-clock duration, floored at
// 60s so a very short run still gets a meaningful rate() sample.
func windowStr(windowSeconds float64) string {
	if windowSeconds < 60 {
		windowSeconds = 60
	}
	return fmt.Sprintf("%ds", int64(windowSeconds))
}

// histogramQuantile builds histogram_quantile(q, sum by (le) (rate(metric{filter}[window]))). filter may
// be empty.
func histogramQuantile(metric string, q float64, filter, window string) string {
	if filter == "" {
		return fmt.Sprintf("histogram_quantile(%g, sum by (le) (rate(%s[%s])))", q, metric, window)
	}
	return fmt.Sprintf("histogram_quantile(%g, sum by (le) (rate(%s{%s}[%s])))", q, metric, filter, window)
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

// Tier 1: end-to-end job latency.
func queuedLatencyQuery(q float64, queues []string, window string) string {
	return histogramQuantile("armada_job_queued_seconds_bucket", q, queueFilter(queues), window)
}

func runLatencyQuery(q float64, queues []string, window string) string {
	return histogramQuantile("armada_job_run_time_seconds_bucket", q, queueFilter(queues), window)
}

// Tier 2: scheduler.
func scheduleCycleQuery(q float64, window string) string {
	return histogramQuantile("armada_scheduler_schedule_cycle_times_bucket", q, "", window)
}

func submitCheckQuery(q float64, window string) string {
	return histogramQuantile("armada_scheduler_submit_check_times_bucket", q, "", window)
}

func scheduledJobsQuery(queues []string, window string) string {
	return fmt.Sprintf(`sum(increase(armada_scheduler_scheduled_jobs{%s}[%s]))`, queueMatcher("queue", queues), window)
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
func submitThroughputQuery(window string) string {
	return fmt.Sprintf(`sum(rate(grpc_server_handled_total{grpc_service="api.Submit",grpc_method="SubmitJobs",grpc_code="OK"}[%s]))`, window)
}

func submitLatencyQuery(q float64, window string) string {
	return histogramQuantile("grpc_server_handling_seconds_bucket", q, `grpc_service="api.Submit",grpc_method="SubmitJobs"`, window)
}

func submitErrorsQuery(window string) string {
	return fmt.Sprintf(`sum(increase(grpc_server_handled_total{grpc_service="api.Submit",grpc_code!="OK"}[%s]))`, window)
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
	return fmt.Sprintf("sum(increase(pulsar_client_producer_errors[%s]))", window)
}
