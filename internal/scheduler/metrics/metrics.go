package metrics

import (
	"regexp"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/scheduler/configuration"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

const (
	namespace = "armada"
	subsystem = "scheduler"

	jobsResourceLabel = "jobs"

	trackedErrorRegexMatches      = "1"
	trackedErrorRegexDoesNotMatch = "0"

	unknown          = "unknown"
	podUnschedulable = "PodUnschedulable"
	leaseExpired     = "LeaseExpired"
	podError         = "PodError"
	podLeaseReturned = "PodLeaseReturned"
	podTerminated    = "PodTerminated"
)

type Metrics struct {
	config   configuration.MetricsConfig
	disabled bool

	// Labels of tracked errors. Used to ensure consistent ordering.
	trackedErrorLabels  []string
	trackedErrorRegexes []*regexp.Regexp

	// Job metrics.
	queued    *prometheus.CounterVec
	leased    *prometheus.CounterVec
	preempted *prometheus.CounterVec
	failed    *prometheus.CounterVec
	cancelled *prometheus.CounterVec
	succeeded *prometheus.CounterVec
}

func New(config configuration.MetricsConfig) (*Metrics, error) {
	trackedErrorLabels := maps.Keys(config.TrackedErrorRegexByLabel)
	slices.Sort(trackedErrorLabels)
	trackedErrorRegexes := make([]*regexp.Regexp, len(trackedErrorLabels))
	for i, trackedErrorLabel := range trackedErrorLabels {
		r, err := regexp.Compile(config.TrackedErrorRegexByLabel[trackedErrorLabel])
		if err != nil {
			return nil, errors.WithStack(err)
		}
		trackedErrorRegexes[i] = r
	}

	inactiveJobLabels := []string{"queue", "resource"}
	activeJobLabels := []string{"queue", "cluster", "node", "resource"}
	failedJobLabels := append(
		[]string{"queue", "cluster", "node", "resource", "errorType"},
		trackedErrorLabels...,
	)

	return &Metrics{
		config: config,

		trackedErrorLabels:  trackedErrorLabels,
		trackedErrorRegexes: trackedErrorRegexes,

		queued: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Subsystem: subsystem,
				Name:      "queued",
				Help:      "Queued jobs.",
			},
			inactiveJobLabels,
		),
		leased: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Subsystem: subsystem,
				Name:      "scheduled",
				Help:      "Scheduled jobs.",
			},
			activeJobLabels,
		),
		preempted: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Subsystem: subsystem,
				Name:      "preempted",
				Help:      "Preempted jobs.",
			},
			activeJobLabels,
		),
		cancelled: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Subsystem: subsystem,
				Name:      "cancelled",
				Help:      "Cancelled jobs.",
			},
			activeJobLabels,
		),
		failed: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Subsystem: subsystem,
				Name:      "failed",
				Help:      "Failed jobs.",
			},
			failedJobLabels,
		),
		succeeded: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Subsystem: subsystem,
				Name:      "succeeded",
				Help:      "Successful jobs.",
			},
			activeJobLabels,
		),
	}, nil
}

func (m *Metrics) Disable() {
	m.disabled = true
}

func (m *Metrics) Enable() {
	m.disabled = false
}

func (m *Metrics) Describe(ch chan<- *prometheus.Desc) {
	if m == nil || m.config.Disabled || m.disabled {
		return
	}
	// TODO(albin): Only these metrics are expected to work for now.
	m.queued.Describe(ch)
	m.leased.Describe(ch)
	m.preempted.Describe(ch)
	m.failed.Describe(ch)
}

func (m *Metrics) Collect(ch chan<- prometheus.Metric) {
	if m == nil || m.config.Disabled || m.disabled {
		return
	}
	// TODO(albin): Only these metrics are expected to work for now.
	m.queued.Collect(ch)
	m.leased.Collect(ch)
	m.preempted.Collect(ch)
	m.failed.Collect(ch)
}

func (m *Metrics) UpdateMany(ctx *armadacontext.Context, jsts []jobdb.JobStateTransitions, jobRunErrorsByRunId map[uuid.UUID]*armadaevents.Error) error {
	if m == nil || m.config.Disabled || m.disabled {
		return nil
	}
	for _, jst := range jsts {
		if err := m.Update(ctx, jst, jobRunErrorsByRunId); err != nil {
			return err
		}
	}
	return nil
}

func (m *Metrics) Update(ctx *armadacontext.Context, jst jobdb.JobStateTransitions, jobRunErrorsByRunId map[uuid.UUID]*armadaevents.Error) error {
	if m == nil || m.config.Disabled || m.disabled {
		return nil
	}
	labels := make([]string, 0, 5+len(m.trackedErrorLabels))
	if jst.Queued {
		if err := m.updateQueued(labels[0:0], jst.Job); err != nil {
			return err
		}
	}
	if jst.Leased {
		if err := m.updateLeased(labels[0:0], jst.Job); err != nil {
			return err
		}
	}
	if jst.Preempted {
		if err := m.updatePreempted(labels[0:0], jst.Job); err != nil {
			return err
		}
	}
	if jst.Cancelled {
		if err := m.updateCancelled(labels[0:0], jst.Job); err != nil {
			return err
		}
	}
	if jst.Failed {
		if err := m.updateFailed(ctx, labels[0:0], jst.Job, jobRunErrorsByRunId); err != nil {
			return err
		}
	}
	if jst.Succeeded {
		if err := m.updateSucceeded(labels[0:0], jst.Job); err != nil {
			return err
		}
	}
	return nil
}

func (m *Metrics) updateQueued(labels []string, job *jobdb.Job) error {
	labels = append(labels, job.GetQueue())
	if err := m.updateCounterVecFromJob(m.queued, labels, job); err != nil {
		return err
	}
	return nil
}

func (m *Metrics) updateLeased(labels []string, job *jobdb.Job) error {
	executor, nodeName := executorAndNodeNameFromRun(job.LatestRun())
	labels = append(labels, job.GetQueue())
	labels = append(labels, executor)
	labels = append(labels, nodeName)
	if err := m.updateCounterVecFromJob(m.leased, labels, job); err != nil {
		return err
	}
	return nil
}

func (m *Metrics) updatePreempted(labels []string, job *jobdb.Job) error {
	executor, nodeName := executorAndNodeNameFromRun(job.LatestRun())
	labels = append(labels, job.GetQueue())
	labels = append(labels, executor)
	labels = append(labels, nodeName)
	if err := m.updateCounterVecFromJob(m.preempted, labels, job); err != nil {
		return err
	}
	return nil
}

func (m *Metrics) updateCancelled(labels []string, job *jobdb.Job) error {
	executor, nodeName := executorAndNodeNameFromRun(job.LatestRun())
	labels = append(labels, job.GetQueue())
	labels = append(labels, executor)
	labels = append(labels, nodeName)
	if err := m.updateCounterVecFromJob(m.cancelled, labels, job); err != nil {
		return err
	}
	return nil
}

func (m *Metrics) updateFailed(ctx *armadacontext.Context, labels []string, job *jobdb.Job, jobRunErrorsByRunId map[uuid.UUID]*armadaevents.Error) error {
	run := job.LatestRun()
	executor, nodeName := executorAndNodeNameFromRun(run)
	name, message := errorTypeAndMessageFromError(ctx, jobRunErrorsByRunId[run.Id()])

	labels = append(labels, job.GetQueue())
	labels = append(labels, executor)
	labels = append(labels, nodeName)
	labels = append(labels, name)

	for _, r := range m.trackedErrorRegexes {
		if r.MatchString(message) {
			labels = append(labels, trackedErrorRegexMatches)
		} else {
			labels = append(labels, trackedErrorRegexDoesNotMatch)
		}
	}

	if err := m.updateCounterVecFromJob(m.failed, labels, job); err != nil {
		return err
	}
	return nil
}

func (m *Metrics) updateSucceeded(labels []string, job *jobdb.Job) error {
	executor, nodeName := executorAndNodeNameFromRun(job.LatestRun())
	labels = append(labels, job.GetQueue())
	labels = append(labels, executor)
	labels = append(labels, nodeName)
	if err := m.updateCounterVecFromJob(m.succeeded, labels, job); err != nil {
		return err
	}
	return nil
}

func executorAndNodeNameFromRun(run *jobdb.JobRun) (string, string) {
	if run == nil {
		// This case covers, e.g., jobs failing that have never been leased.
		return "", ""
	}
	return run.Executor(), run.NodeName()
}

func errorTypeAndMessageFromError(ctx *armadacontext.Context, err *armadaevents.Error) (string, string) {
	if err == nil {
		return unknown, ""
	}
	// The following errors relate to job run failures.
	// We do not process JobRunPreemptedError as there is separate metric for preemption.
	switch reason := err.Reason.(type) {
	case *armadaevents.Error_PodUnschedulable:
		return podUnschedulable, reason.PodUnschedulable.Message
	case *armadaevents.Error_LeaseExpired:
		return leaseExpired, ""
	case *armadaevents.Error_PodError:
		return podError, reason.PodError.Message
	case *armadaevents.Error_PodLeaseReturned:
		return podLeaseReturned, reason.PodLeaseReturned.Message
	case *armadaevents.Error_PodTerminated:
		return podTerminated, reason.PodTerminated.Message
	default:
		ctx.Warnf("omitting name and message for unknown error type %T", err.Reason)
		return unknown, ""
	}
}

func (m *Metrics) updateCounterVecFromJob(vec *prometheus.CounterVec, labels []string, job *jobdb.Job) error {
	// Number of jobs.
	i := len(labels)
	labels = append(labels, jobsResourceLabel)
	if c, err := vec.GetMetricWithLabelValues(labels...); err != nil {
		return err
	} else {
		c.Add(1)
	}

	// Total resource requests of jobs.
	requests := job.GetResourceRequirements().Requests
	for _, resourceName := range m.config.TrackedResourceNames {
		labels[i] = string(resourceName)
		q := requests[resourceName]
		v := float64(q.MilliValue()) / 1000
		if c, err := vec.GetMetricWithLabelValues(labels...); err != nil {
			return err
		} else {
			c.Add(v)
		}
	}

	return nil
}
