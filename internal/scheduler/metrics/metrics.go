package metrics

import (
	"regexp"
	"time"

	"github.com/google/uuid"
	lru "github.com/hashicorp/golang-lru"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"

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

	podUnschedulable = "podUnschedulable"
	leaseExpired     = "leaseExpired"
	podError         = "podError"
	podLeaseReturned = "podLeaseReturned"
	podTerminated    = "podTerminated"

	queued    = "queued"
	cancelled = "cancelled"
	leased    = "leased"
	preempted = "preempted"
	failed    = "failed"
	succeeded = "succeeded"
)

type Metrics struct {
	config configuration.MetricsConfig

	// For disabling metrics at runtime, e.g., if not leader.
	disabled bool

	// Buffer used to avoid allocations when updating metrics.
	buffer []string

	// Reset metrics periodically.
	resetInterval         time.Duration
	timeOfMostRecentReset time.Time

	// Pre-compiled regexes for error categorisation.
	errorRegexes []*regexp.Regexp
	// Map from error message to the index of the first matching regex.
	// Messages that match no regex map to -1.
	matchedRegexIndexByErrorMessage *lru.Cache

	// Job metrics.
	transitions *prometheus.CounterVec
}

func New(config configuration.MetricsConfig) (*Metrics, error) {
	errorRegexes := make([]*regexp.Regexp, len(config.TrackedErrorRegexes))
	for i, errorRegex := range config.TrackedErrorRegexes {
		if r, err := regexp.Compile(errorRegex); err != nil {
			return nil, errors.WithStack(err)
		} else {
			errorRegexes[i] = r
		}
	}

	var matchedRegexIndexByError *lru.Cache
	if config.MatchedRegexIndexByErrorMessageCacheSize > 0 {
		var err error
		matchedRegexIndexByError, err = lru.New(int(config.MatchedRegexIndexByErrorMessageCacheSize))
		if err != nil {
			return nil, errors.WithStack(err)
		}
	}

	return &Metrics{
		config: config,

		resetInterval:         config.ResetInterval,
		timeOfMostRecentReset: time.Now(),

		buffer: make([]string, 0, 8),

		errorRegexes:                    errorRegexes,
		matchedRegexIndexByErrorMessage: matchedRegexIndexByError,

		transitions: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Subsystem: subsystem,
				Name:      "resources_total",
				Help:      "Job state transition resource counters.",
			},
			[]string{"state", "category", "subCategory", "queue", "cluster", "nodeType", "node", "resource"},
		),
	}, nil
}

func (m *Metrics) Disable() {
	if m != nil {
		m.disabled = true
	}
}

func (m *Metrics) Enable() {
	if m != nil {
		m.disabled = false
	}
}

func (m *Metrics) IsDisabled() bool {
	if m == nil {
		return true
	}
	if m.config.Disabled {
		return true
	}
	return m.disabled
}

func (m *Metrics) Describe(ch chan<- *prometheus.Desc) {
	if m.IsDisabled() {
		return
	}
	m.transitions.Describe(ch)
}

// Collect and then reset all metrics.
// Resetting ensures we do not build up a large number of counters over time.
func (m *Metrics) Collect(ch chan<- prometheus.Metric) {
	if m.IsDisabled() {
		return
	}
	m.transitions.Collect(ch)

	// Reset metrics periodically.
	t := time.Now()
	if t.Sub(m.timeOfMostRecentReset) > m.resetInterval {
		m.transitions.Reset()
		m.timeOfMostRecentReset = t
	}
}

func (m *Metrics) UpdateMany(
	ctx *armadacontext.Context,
	jsts []jobdb.JobStateTransitions,
	jobRunErrorsByRunId map[uuid.UUID]*armadaevents.Error,
) error {
	for _, jst := range jsts {
		if err := m.Update(ctx, jst, jobRunErrorsByRunId); err != nil {
			return err
		}
	}
	return nil
}

func (m *Metrics) Update(
	ctx *armadacontext.Context,
	jst jobdb.JobStateTransitions,
	jobRunErrorsByRunId map[uuid.UUID]*armadaevents.Error,
) error {
	if jst.Queued {
		if err := m.UpdateQueued(jst.Job); err != nil {
			return err
		}
	}
	if jst.Cancelled {
		if err := m.UpdateCancelled(jst.Job); err != nil {
			return err
		}
	}
	if jst.Failed {
		if err := m.UpdateFailed(ctx, jst.Job, jobRunErrorsByRunId); err != nil {
			return err
		}
	}
	if jst.Succeeded {
		if err := m.UpdateSucceeded(jst.Job); err != nil {
			return err
		}
	}
	return nil
}

func (m *Metrics) UpdateQueued(job *jobdb.Job) error {
	labels := m.buffer[0:0]
	labels = append(labels, queued)
	labels = append(labels, "") // No category for queued.
	labels = append(labels, "") // No subCategory for queued.
	labels = appendLabelsFromJob(labels, job)
	if err := m.updateCounterVecFromJob(m.transitions, labels, job); err != nil {
		return err
	}
	return nil
}

func (m *Metrics) UpdateCancelled(job *jobdb.Job) error {
	labels := m.buffer[0:0]
	labels = append(labels, cancelled)
	labels = append(labels, "") // No category for cancelled.
	labels = append(labels, "") // No subCategory for cancelled.
	labels = appendLabelsFromJob(labels, job)
	if err := m.updateCounterVecFromJob(m.transitions, labels, job); err != nil {
		return err
	}
	return nil
}

func (m *Metrics) UpdateFailed(ctx *armadacontext.Context, job *jobdb.Job, jobRunErrorsByRunId map[uuid.UUID]*armadaevents.Error) error {
	labels := m.buffer[0:0]
	category, subCategory := m.failedCategoryAndSubCategoryFromJob(ctx, job, jobRunErrorsByRunId)
	labels = append(labels, failed)
	labels = append(labels, category)
	labels = append(labels, subCategory)
	labels = appendLabelsFromJob(labels, job)
	if err := m.updateCounterVecFromJob(m.transitions, labels, job); err != nil {
		return err
	}
	return nil
}

func (m *Metrics) UpdateSucceeded(job *jobdb.Job) error {
	labels := m.buffer[0:0]
	labels = append(labels, succeeded)
	labels = append(labels, "") // No category for succeeded.
	labels = append(labels, "") // No subCategory for succeeded.
	labels = appendLabelsFromJob(labels, job)
	if err := m.updateCounterVecFromJob(m.transitions, labels, job); err != nil {
		return err
	}
	return nil
}

func (m *Metrics) UpdateLeased(jctx *schedulercontext.JobSchedulingContext) error {
	labels := m.buffer[0:0]
	job := jctx.Job.(*jobdb.Job)
	labels = append(labels, leased)
	labels = append(labels, "") // No category for leased.
	labels = append(labels, "") // No subCategory for leased.
	labels = appendLabelsFromJobSchedulingContext(labels, jctx)
	if err := m.updateCounterVecFromJob(m.transitions, labels, job); err != nil {
		return err
	}
	return nil
}

func (m *Metrics) UpdatePreempted(jctx *schedulercontext.JobSchedulingContext) error {
	labels := m.buffer[0:0]
	job := jctx.Job.(*jobdb.Job)
	labels = append(labels, preempted)
	labels = append(labels, "") // No category for preempted.
	labels = append(labels, "") // No subCategory for preempted.
	labels = appendLabelsFromJobSchedulingContext(labels, jctx)
	if err := m.updateCounterVecFromJob(m.transitions, labels, job); err != nil {
		return err
	}
	return nil
}

func (m *Metrics) failedCategoryAndSubCategoryFromJob(ctx *armadacontext.Context, job *jobdb.Job, jobRunErrorsByRunId map[uuid.UUID]*armadaevents.Error) (category, subCategory string) {
	run := job.LatestRun()
	if run == nil {
		return
	}

	category, message := errorTypeAndMessageFromError(ctx, jobRunErrorsByRunId[run.Id()])
	i, ok := m.regexIndexFromErrorMessage(message)
	if ok {
		subCategory = m.config.TrackedErrorRegexes[i]
	}

	return
}

func (m *Metrics) regexIndexFromErrorMessage(message string) (int, bool) {
	i, ok := m.cachedRegexIndexFromErrorMessage(message)
	if !ok {
		i, ok = m.indexOfFirstMatchingRegexFromErrorMessage(message)
		if !ok {
			// Use -1 to indicate that no regex matches.
			i = -1
		}
		if m.matchedRegexIndexByErrorMessage != nil {
			m.matchedRegexIndexByErrorMessage.Add(message, i)
		}
	}
	if i == -1 {
		ok = false
	}
	return i, ok
}

func (m *Metrics) cachedRegexIndexFromErrorMessage(message string) (int, bool) {
	if m.matchedRegexIndexByErrorMessage == nil {
		return 0, false
	}
	i, ok := m.matchedRegexIndexByErrorMessage.Get(message)
	if !ok {
		return 0, false
	}
	return i.(int), true
}

func (m *Metrics) indexOfFirstMatchingRegexFromErrorMessage(message string) (int, bool) {
	for i, r := range m.errorRegexes {
		if r.MatchString(message) {
			return i, true
		}
	}
	return 0, false
}

func appendLabelsFromJob(labels []string, job *jobdb.Job) []string {
	executor, nodeName := executorAndNodeNameFromRun(job.LatestRun())
	labels = append(labels, job.GetQueue())
	labels = append(labels, executor)
	labels = append(labels, "") // No nodeType.
	labels = append(labels, nodeName)
	return labels
}

func appendLabelsFromJobSchedulingContext(labels []string, jctx *schedulercontext.JobSchedulingContext) []string {
	job := jctx.Job.(*jobdb.Job)
	executor, nodeName := executorAndNodeNameFromRun(job.LatestRun())
	labels = append(labels, job.GetQueue())
	labels = append(labels, executor)
	wellKnownNodeType := ""
	if pctx := jctx.PodSchedulingContext; pctx != nil {
		wellKnownNodeType = pctx.WellKnownNodeTypeName
	}
	labels = append(labels, wellKnownNodeType)
	labels = append(labels, nodeName)
	return labels
}

func executorAndNodeNameFromRun(run *jobdb.JobRun) (string, string) {
	if run == nil {
		// This case covers, e.g., jobs failing that have never been scheduled.
		return "", ""
	}
	return run.Executor(), run.NodeName()
}

func errorTypeAndMessageFromError(ctx *armadacontext.Context, err *armadaevents.Error) (string, string) {
	if err == nil {
		return "", ""
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
		return "", ""
	}
}

// updateCounterVecFromJob is a helper method to increment vector counters.
func (m *Metrics) updateCounterVecFromJob(vec *prometheus.CounterVec, labels []string, job *jobdb.Job) error {
	i := len(labels)

	// Number of jobs.
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
		if c, err := vec.GetMetricWithLabelValues(labels...); err != nil {
			return err
		} else {
			q := requests[resourceName]
			v := float64(q.MilliValue()) / 1000
			c.Add(v)
		}
	}

	return nil
}
