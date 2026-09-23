package metrics

import "fmt"

// windowStr formats the lookback window used by every rate()/increase()/max_over_time() query
// below, mirroring the CI script's WINDOW calc: the run's own wall-clock duration, floored at
// 60s so a very short run still gets a meaningful rate() sample.
func windowStr(windowSeconds float64) string {
	if windowSeconds < 60 {
		windowSeconds = 60
	}
	return fmt.Sprintf("%ds", int64(windowSeconds))
}

// histogramQuantile builds histogram_quantile(q, sum by (le) (rate(metric{filter}[window]))),
// exactly matching collect-perf-metrics.sh's quantile() helper. filter may be empty.
func histogramQuantile(metric string, q float64, filter, window string) string {
	if filter == "" {
		return fmt.Sprintf("histogram_quantile(%g, sum by (le) (rate(%s[%s])))", q, metric, window)
	}
	return fmt.Sprintf("histogram_quantile(%g, sum by (le) (rate(%s{%s}[%s])))", q, metric, filter, window)
}

func queueFilter(queue string) string {
	return fmt.Sprintf(`queueName=%q`, queue)
}

// Tier 1: end-to-end job latency.
func queuedLatencyQuery(q float64, queue, window string) string {
	return histogramQuantile("armada_job_queued_seconds_bucket", q, queueFilter(queue), window)
}

func runLatencyQuery(q float64, queue, window string) string {
	return histogramQuantile("armada_job_run_time_seconds_bucket", q, queueFilter(queue), window)
}

// Tier 2: scheduler.
func scheduleCycleQuery(q float64, window string) string {
	return histogramQuantile("armada_scheduler_schedule_cycle_times_bucket", q, "", window)
}

func submitCheckQuery(q float64, window string) string {
	return histogramQuantile("armada_scheduler_submit_check_times_bucket", q, "", window)
}

func scheduledJobsQuery(queue, window string) string {
	return fmt.Sprintf(`sum(increase(armada_scheduler_scheduled_jobs{queue=%q}[%s]))`, queue, window)
}

// Tier 3: queue depth. Summed across label dimensions (e.g. pool) before max_over_time, same
// reasoning as queueSizeQuery/leasedPodCountQuery below - otherwise this reports one arbitrary
// series's peak rather than the queue's true combined peak.
func peakQueueSizeQuery(queue, window string) string {
	return fmt.Sprintf(`max_over_time(sum(armada_queue_size{queue=%q,state="validated"})[%s:])`, queue, window)
}

func peakLeasedPodCountQuery(queue, window string) string {
	return fmt.Sprintf(`max_over_time(sum(armada_queue_leased_pod_count{queue=%q})[%s:])`, queue, window)
}

// queueSizeQuery and leasedPodCountQuery are unwindowed instant queries (the current value, not a
// windowed max) used by WaitForQueueDrain to detect when a queue has fully drained. Both are
// wrapped in sum() because the scheduler emits one series per queue *and* label dimension (e.g.
// pool) - query()'s caller only ever looks at a single float, so without the sum it would only
// ever see one arbitrary series out of several and could keep reading a stale non-zero value long
// after the queue's real aggregate lease/queue count had actually reached zero.
func queueSizeQuery(queue string) string {
	return fmt.Sprintf(`sum(armada_queue_size{queue=%q,state="validated"})`, queue)
}

func leasedPodCountQuery(queue string) string {
	return fmt.Sprintf(`sum(armada_queue_leased_pod_count{queue=%q})`, queue)
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
