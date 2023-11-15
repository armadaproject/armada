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
	schedulercontext "github.com/armadaproject/armada/internal/scheduler/context"
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
	config configuration.MetricsConfig

	// Labels of tracked errors. Used to ensure consistent ordering.
	trackedErrorLabels  []string
	trackedErrorRegexes []*regexp.Regexp

	// Indicates whether this instance is the leader.
	isLeader prometheus.Gauge

	// Scheduling cycle time broken up into the operations that make up the overall cycle.
	cycleTime *prometheus.SummaryVec

	// Job metrics.
	queued    *prometheus.CounterVec
	scheduled *prometheus.CounterVec
	preempted *prometheus.CounterVec
	failed    *prometheus.CounterVec
	succeeded *prometheus.CounterVec

	// Fair and actual share for each queue.
	// As measured by the cost of each queue divided by the total cost across all queues.
	fairness *prometheus.GaugeVec
}

// UpdatedJobs maps events to jobs.
type UpdatedJobs struct {
	Queued    []*jobdb.Job
	Leased    []*jobdb.Job
	Pending   []*jobdb.Job
	Running   []*jobdb.Job
	Preempted []*jobdb.Job
	Failed    []*jobdb.Job
	Succeeded []*jobdb.Job

	// Map from job run id to error for that run.
	// Used to enrich failure metrics if provided.
	JobRunErrorsByRunId map[uuid.UUID]*armadaevents.Error

	// Applies to preempted, scheduled, and jobs failed by the scheduler.
	// TODO: For now we pretend there's always exactly one sctx.
	SchedulingContext schedulercontext.SchedulingContext
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

		isLeader: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Subsystem: subsystem,
				Name:      "is_leader",
				Help:      "Indicates whether this instance of the scheduler is the leader.",
			},
		),
		cycleTime: prometheus.NewSummaryVec(
			prometheus.SummaryOpts{
				Namespace:  namespace,
				Subsystem:  subsystem,
				Name:       "cycle_time",
				Help:       "Scheduler cycle time broken up by operation.",
				Objectives: config.CycleTimeConfig.Objectives,
				MaxAge:     config.CycleTimeConfig.MaxAge,
			},
			[]string{"operation"},
		),
		queued: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Subsystem: subsystem,
				Name:      "queued",
				Help:      "Queued jobs.",
			},
			inactiveJobLabels,
		),
		scheduled: prometheus.NewCounterVec(
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
		fairness: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Subsystem: subsystem,
				Name:      "fairness",
				Help:      "Fairness metrics.",
			},
			[]string{"queue", "pool", "measure"},
		),
	}, nil
}

func (m *Metrics) Describe(ch chan<- *prometheus.Desc) {
	if m == nil || m.config.Disabled {
		return
	}
	m.queued.Describe(ch)
	m.scheduled.Describe(ch)
	m.preempted.Describe(ch)
	m.failed.Describe(ch)
	m.succeeded.Describe(ch)
}

func (m *Metrics) Collect(ch chan<- prometheus.Metric) {
	if m == nil || m.config.Disabled {
		return
	}
	m.queued.Collect(ch)
	m.scheduled.Collect(ch)
	m.preempted.Collect(ch)
	m.failed.Collect(ch)
	m.succeeded.Collect(ch)
}

func (m *Metrics) Update(ctx *armadacontext.Context, update UpdatedJobs) error {
	if m == nil || m.config.Disabled {
		return nil
	}
	if err := m.updateQueued(update.Queued); err != nil {
		return err
	}
	if err := m.updateScheduled(update.Leased); err != nil {
		return err
	}
	if err := m.updatePreempted(update.Preempted); err != nil {
		return err
	}
	if err := m.updateFailed(ctx, update.Failed, update.JobRunErrorsByRunId); err != nil {
		return err
	}
	if err := m.updateSucceeded(update.Succeeded); err != nil {
		return err
	}
	return nil
}

func (m *Metrics) updateQueued(jobs []*jobdb.Job) error {
	labels := make([]string, 2)
	for _, job := range jobs {
		labels[0] = job.GetQueue()
		if err := m.updateCounterVecFromJob(m.queued, labels, 1, job); err != nil {
			return err
		}
	}
	return nil
}

func (m *Metrics) updateScheduled(jobs []*jobdb.Job) error {
	labels := make([]string, 4)
	for _, job := range jobs {
		run := job.LatestRun()
		if run == nil {
			continue
		}
		labels[0] = job.GetQueue()
		labels[1] = run.Executor()
		labels[2] = run.NodeName()
		if err := m.updateCounterVecFromJob(m.scheduled, labels, 3, job); err != nil {
			return err
		}
	}
	return nil
}

func (m *Metrics) updatePreempted(jobs []*jobdb.Job) error {
	labels := make([]string, 4)
	for _, job := range jobs {
		run := job.LatestRun()
		if run == nil {
			continue
		}
		labels[0] = job.GetQueue()
		labels[1] = run.Executor()
		labels[2] = run.NodeName()
		if err := m.updateCounterVecFromJob(m.preempted, labels, 3, job); err != nil {
			return err
		}
	}
	return nil
}

func (m *Metrics) updateFailed(ctx *armadacontext.Context, jobs []*jobdb.Job, jobRunErrorsByRunId map[uuid.UUID]*armadaevents.Error) error {
	labels := make([]string, 5+len(m.trackedErrorLabels))
	for _, job := range jobs {
		run := job.LatestRun()
		if run == nil {
			continue
		}
		name, message := nameAndMessageFromError(ctx, jobRunErrorsByRunId[run.Id()])

		labels[0] = job.GetQueue()
		labels[1] = run.Executor()
		labels[2] = run.NodeName()
		labels[3] = name
		for i, r := range m.trackedErrorRegexes {
			if r.MatchString(message) {
				labels[5+i] = trackedErrorRegexMatches
			} else {
				labels[5+i] = trackedErrorRegexDoesNotMatch
			}
		}

		if err := m.updateCounterVecFromJob(m.failed, labels, 4, job); err != nil {
			return err
		}
	}
	return nil
}

func (m *Metrics) updateSucceeded(jobs []*jobdb.Job) error {
	labels := make([]string, 4)
	for _, job := range jobs {
		run := job.LatestRun()
		if run == nil {
			continue
		}
		labels[0] = job.GetQueue()
		labels[1] = run.Executor()
		labels[2] = run.NodeName()
		if err := m.updateCounterVecFromJob(m.succeeded, labels, 3, job); err != nil {
			return err
		}
	}
	return nil
}

func nameAndMessageFromError(ctx *armadacontext.Context, err *armadaevents.Error) (string, string) {
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

func (m *Metrics) updateCounterVecFromJob(vec *prometheus.CounterVec, labels []string, resourceLabelIndex int, job *jobdb.Job) error {
	// Number of jobs.
	labels[resourceLabelIndex] = jobsResourceLabel
	if c, err := vec.GetMetricWithLabelValues(labels...); err != nil {
		return err
	} else {
		c.Add(1)
	}

	// Total resource requests of jobs.
	requests := job.GetResourceRequirements().Requests
	for _, resourceName := range m.config.TrackedResourceNames {
		labels[resourceLabelIndex] = string(resourceName)
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
