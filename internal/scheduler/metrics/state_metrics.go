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
	trackedResourceNames           []v1.ResourceName
	completedRunDurations          *prometheus.HistogramVec
	jobStateCounterByQueue         *prometheus.CounterVec
	jobStateCounterByNode          *prometheus.CounterVec
	jobStateSecondsByQueue         *prometheus.CounterVec
	jobStateSecondsByNode          *prometheus.CounterVec
	jobStateResourceSecondsByQueue *prometheus.CounterVec
	jobStateResourceSecondsByNode  *prometheus.CounterVec
	jobErrorsByQueue               *prometheus.CounterVec
	jobErrorsByNode                *prometheus.CounterVec
	allMetrics                     []prometheus.Collector
}

func newJobStateMetrics(errorRegexes []*regexp.Regexp, trackedResourceNames []v1.ResourceName) *jobStateMetrics {
	completedRunDurations := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    prefix + "job_run_completed_duration_seconds",
			Help:    "Time",
			Buckets: prometheus.ExponentialBuckets(2, 2, 20),
		},
		[]string{queueLabel, poolLabel},
	)
	jobStateCounterByQueue := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: prefix + "job_state_seconds_by_queue",
			Help: "Job states at queue level",
		},
		[]string{queueLabel, poolLabel, stateLabel, priorStateLabel},
	)
	jobStateCounterByNode := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: prefix + "job_state_seconds_by_node",
			Help: "Job states at node level",
		},
		[]string{nodeLabel, poolLabel, clusterLabel, stateLabel, priorStateLabel},
	)
	jobStateSecondsByQueue := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: prefix + "job_state_seconds_by_queue",
			Help: "Resource-seconds spend in different states at queue level",
		},
		[]string{queueLabel, poolLabel, stateLabel, priorStateLabel},
	)
	jobStateSecondsByNode := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: prefix + "job_state_seconds_by_node",
			Help: "Resource-seconds spend in different states at node level",
		},
		[]string{nodeLabel, poolLabel, clusterLabel, stateLabel, priorStateLabel},
	)
	jobStateResourceSecondsByQueue := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: prefix + "job_state_resource_seconds_by_queue",
			Help: "Resource Seconds spend in different states at queue level",
		},
		[]string{queueLabel, poolLabel, stateLabel, priorStateLabel, resourceLabel},
	)
	jobStateResourceSecondsByNode := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: prefix + "job_state_resource_seconds_by_node",
			Help: "Resource Seconds spend in different states at node level",
		},
		[]string{nodeLabel, poolLabel, clusterLabel, stateLabel, priorStateLabel, resourceLabel},
	)
	jobErrorsByQueue := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: prefix + "job_error_classification_by_queue",
			Help: "Failed jobs ey error classification",
		},
		[]string{queueLabel, poolLabel, errorCategoryLabel, errorSubcategoryLabel},
	)
	jobErrorsByNode := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: prefix + "error_classification_by_node",
			Help: "Failed jobs ey error classification",
		},
		[]string{nodeLabel, poolLabel, clusterLabel, errorCategoryLabel, errorSubcategoryLabel},
	)
	return &jobStateMetrics{
		errorRegexes:                   errorRegexes,
		trackedResourceNames:           trackedResourceNames,
		completedRunDurations:          completedRunDurations,
		jobStateCounterByQueue:         jobStateCounterByQueue,
		jobStateCounterByNode:          jobStateCounterByNode,
		jobStateSecondsByQueue:         jobStateSecondsByQueue,
		jobStateSecondsByNode:          jobStateSecondsByNode,
		jobStateResourceSecondsByQueue: jobStateResourceSecondsByQueue,
		jobStateResourceSecondsByNode:  jobStateResourceSecondsByNode,
		jobErrorsByQueue:               jobErrorsByQueue,
		jobErrorsByNode:                jobErrorsByNode,
		allMetrics: []prometheus.Collector{
			completedRunDurations,
			jobStateCounterByQueue,
			jobStateCounterByNode,
			jobStateSecondsByQueue,
			jobStateSecondsByNode,
			jobStateResourceSecondsByQueue,
			jobStateResourceSecondsByNode,
			jobErrorsByQueue,
			jobErrorsByNode,
		},
	}
}

func (m *jobStateMetrics) describe(ch chan<- *prometheus.Desc) {
	for _, metric := range m.allMetrics {
		metric.Describe(ch)
	}
}

func (m *jobStateMetrics) collect(ch chan<- prometheus.Metric) {
	for _, metric := range m.allMetrics {
		metric.Collect(ch)
	}
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
			m.jobErrorsByQueue.WithLabelValues(job.Queue(), run.Executor(), category, subCategory).Inc()
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

	// Counters
	m.jobStateCounterByQueue.
		WithLabelValues(queue, pool, state, priorState).Inc()

	m.jobStateCounterByNode.
		WithLabelValues(node, pool, cluster, state, priorState).Inc()

	// State seconds
	m.jobStateSecondsByQueue.
		WithLabelValues(queue, pool, state, priorState).Add(duration)

	m.jobStateSecondsByNode.
		WithLabelValues(node, pool, cluster, state, priorState).Add(duration)

	// Resource Seconds
	for _, res := range m.trackedResourceNames {
		resQty := requests[res]
		resSeconds := duration * float64(resQty.MilliValue()) / 1000
		m.jobStateResourceSecondsByQueue.
			WithLabelValues(queue, pool, state, priorState, res.String()).Add(resSeconds)
		m.jobStateResourceSecondsByNode.
			WithLabelValues(node, pool, cluster, state, priorState, res.String()).Add(resSeconds)
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
