package metrics

import (
	"regexp"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

type jobStateMetrics struct {
	// Pre-compiled regexes for error categorisation.
	errorRegexes []*regexp.Regexp
	// resources we want to provide metrics for
	trackedResourceNames         []v1.ResourceName
	completedRunDurations        *prometheus.HistogramVec
	queueJobStateSeconds         *prometheus.CounterVec
	nodeJobStateSeconds          *prometheus.CounterVec
	queueJobStateResourceSeconds *prometheus.CounterVec
	nodeJobStateResourceSeconds  *prometheus.CounterVec
	queueJobErrors               *prometheus.CounterVec
	nodeJobErrors                *prometheus.CounterVec
}

func newJobStateMetrics(errorRegexes []*regexp.Regexp, trackedResourceNames []v1.ResourceName) *jobStateMetrics {
	return &jobStateMetrics{
		errorRegexes:         errorRegexes,
		trackedResourceNames: trackedResourceNames,
		completedRunDurations: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    prefix + "job_run_completed_duration_seconds",
				Help:    "Time",
				Buckets: prometheus.ExponentialBuckets(2, 2, 20),
			},
			[]string{queueLabel, poolLabel},
		),
		queueJobStateSeconds: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: prefix + "job_state_seconds_by_queue",
				Help: "Resource-seconds spend in different states at queue level",
			},
			[]string{queueLabel, poolLabel, stateLabel, priorStateLabel},
		),
		nodeJobStateSeconds: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: prefix + "job_state_seconds_by_node",
				Help: "Resource-seconds spend in different states at node level",
			},
			[]string{nodeLabel, poolLabel, clusterLabel, stateLabel, priorStateLabel},
		),
		queueJobStateResourceSeconds: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: prefix + "job_state_resource_seconds_by_queue",
				Help: "Resource Seconds spend in different states at queue level",
			},
			[]string{queueLabel, poolLabel, stateLabel, priorStateLabel, resourceLabel},
		),
		nodeJobStateResourceSeconds: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: prefix + "job_state_resource_seconds_by_node",
				Help: "Resource Seconds spend in different states at node level",
			},
			[]string{nodeLabel, poolLabel, clusterLabel, stateLabel, priorStateLabel, resourceLabel},
		),
		queueJobErrors: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: prefix + "job_error_classification_by_queue",
				Help: "Failed jobs ey error classification",
			},
			[]string{queueLabel, poolLabel, errorCategoryLabel, errorSubcategoryLabel},
		),
		nodeJobErrors: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: prefix + "error_classification_by_node",
				Help: "Failed jobs ey error classification",
			},
			[]string{nodeLabel, poolLabel, clusterLabel, errorCategoryLabel, errorSubcategoryLabel},
		),
	}
}

func (m *jobStateMetrics) describe(ch chan<- *prometheus.Desc) {
	m.completedRunDurations.Describe(ch)
	m.queueJobStateSeconds.Describe(ch)
	m.nodeJobStateSeconds.Describe(ch)
	m.queueJobStateResourceSeconds.Describe(ch)
	m.nodeJobStateResourceSeconds.Describe(ch)
	m.queueJobErrors.Describe(ch)
	m.nodeJobErrors.Describe(ch)
}

func (m *jobStateMetrics) collect(ch chan<- prometheus.Metric) {
	m.completedRunDurations.Collect(ch)
	m.queueJobStateSeconds.Collect(ch)
	m.nodeJobStateSeconds.Collect(ch)
	m.queueJobStateResourceSeconds.Collect(ch)
	m.nodeJobStateResourceSeconds.Collect(ch)
	m.queueJobErrors.Collect(ch)
	m.nodeJobErrors.Collect(ch)
}

// ReportJobLeased reports the job as being leasedJob. This has to be reported separately because the state transition
// logic does work for job leased!
func (m *jobStateMetrics) ReportJobLeased(job *jobdb.Job) {
	run := job.LatestRun()
	duration, priorState := stateDuration(job, run, run.LeaseTime())
	m.updateStateDuration(job, leased, priorState, duration)
}

func (m *jobStateMetrics) ReportStateTransitions(
	jsts []jobdb.JobStateTransitions,
	jobRunErrorsByRunId map[uuid.UUID]*armadaevents.Error,
) {
	for _, jst := range jsts {
		job := jst.Job
		run := job.LatestRun()
		if jst.Pending {
			duration, priorState := stateDuration(job, run, run.PendingTime())
			m.updateStateDuration(job, pending, priorState, duration)
		}
		if jst.Running {
			duration, priorState := stateDuration(job, run, run.RunningTime())
			m.updateStateDuration(job, running, priorState, duration)
		}
		if jst.Cancelled {
			duration, priorState := stateDuration(job, run, run.TerminatedTime())
			m.updateStateDuration(job, cancelled, priorState, duration)
			if job.LatestRun() != nil {
				m.completedRunDurations.WithLabelValues(job.Queue(), run.Pool()).Observe(duration)
			}
		}
		if jst.Failed {
			duration, priorState := stateDuration(job, run, run.TerminatedTime())
			m.updateStateDuration(job, failed, priorState, duration)
			m.completedRunDurations.WithLabelValues(job.Queue(), run.Pool()).Observe(duration)
			jobRunError := jobRunErrorsByRunId[run.Id()]
			category, subCategory := m.failedCategoryAndSubCategoryFromJob(jobRunError)
			m.queueJobErrors.WithLabelValues(job.Queue(), run.Executor(), category, subCategory).Inc()
		}
		if jst.Succeeded {
			duration, priorState := stateDuration(job, run, run.TerminatedTime())
			m.updateStateDuration(job, succeeded, priorState, duration)
			m.completedRunDurations.WithLabelValues(job.Queue(), run.Pool()).Observe(duration)
		}
		if jst.Preempted {
			duration, priorState := stateDuration(job, run, run.PreemptedTime())
			m.updateStateDuration(job, preempted, priorState, duration)
			m.completedRunDurations.WithLabelValues(job.Queue(), run.Pool()).Observe(duration)
		}
	}
}

func (m *jobStateMetrics) updateStateDuration(job *jobdb.Job, state string, priorState string, duration float64) {
	if duration <= 0 {
		return
	}

	queue := job.Queue()
	requests := job.ResourceRequirements().Requests
	latestRun := job.LatestRun()
	pool := ""
	node := ""
	cluster := ""
	if latestRun != nil {
		pool = latestRun.Pool()
		node = latestRun.NodeName()
		cluster = latestRun.Executor()
	}

	// Update job state seconds
	m.queueJobStateSeconds.
		WithLabelValues(queue, pool, state, priorState).
		Add(duration)

	m.nodeJobStateSeconds.
		WithLabelValues(node, pool, cluster, state, priorState).
		Add(duration)

	for _, res := range m.trackedResourceNames {
		resQty := requests[res]
		resSeconds := duration * float64(resQty.MilliValue()) / 1000
		m.queueJobStateResourceSeconds.
			WithLabelValues(queue, pool, state, priorState, res.String()).
			Add(resSeconds)
		m.nodeJobStateResourceSeconds.
			WithLabelValues(node, pool, cluster, state, priorState, res.String()).
			Add(resSeconds)
	}
}

func (m *jobStateMetrics) failedCategoryAndSubCategoryFromJob(err *armadaevents.Error) (string, string) {
	category, message := errorTypeAndMessageFromError(err)
	for _, r := range m.errorRegexes {
		if r.MatchString(message) {
			return category, r.String()
		}
	}
	return category, ""
}

// stateDuration returns:
// -  the duration of the current state (stateTime - priorTime)
// -  the prior state name
func stateDuration(job *jobdb.Job, run *jobdb.JobRun, stateTime *time.Time) (float64, string) {
	if stateTime == nil {
		return 0, ""
	}

	queuedTime := time.Unix(0, job.Created())
	diff := stateTime.Sub(queuedTime).Seconds()
	prior := queued
	priorTime := &queuedTime

	if run.LeaseTime() != nil {
		if sub := stateTime.Sub(*run.LeaseTime()).Seconds(); sub < diff && sub > 0 {
			prior = leased
			priorTime = run.LeaseTime()
			diff = sub
		}
	}
	if run.PendingTime() != nil {
		if sub := stateTime.Sub(*run.PendingTime()).Seconds(); sub < diff && sub > 0 {
			prior = pending
			priorTime = run.PendingTime()
			diff = sub
		}
	}
	if run.RunningTime() != nil {
		if sub := stateTime.Sub(*run.RunningTime()).Seconds(); sub < diff && sub > 0 {
			prior = running
			priorTime = run.RunningTime()
		}
	}
	// succeeded, failed, cancelled, preempted are not prior states
	return stateTime.Sub(*priorTime).Seconds(), prior
}

func errorTypeAndMessageFromError(err *armadaevents.Error) (string, string) {
	if err == nil {
		return "", ""
	}
	// The following errors relate to job run failures.
	// We do not process JobRunPreemptedError as there is separate metric for preemption.
	switch reason := err.Reason.(type) {
	case *armadaevents.Error_PodUnschedulable:
		return "podUnschedulable", reason.PodUnschedulable.Message
	case *armadaevents.Error_LeaseExpired:
		return "leaseExpired", ""
	case *armadaevents.Error_PodError:
		return "podError", reason.PodError.Message
	case *armadaevents.Error_PodLeaseReturned:
		return "podLeaseReturned", reason.PodLeaseReturned.Message
	case *armadaevents.Error_PodTerminated:
		return "podTerminated", reason.PodTerminated.Message
	case *armadaevents.Error_JobRunPreemptedError:
		return "jobRunPreempted", ""
	default:
		return "", ""
	}
}
