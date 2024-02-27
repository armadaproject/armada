package metrics

import (
	"regexp"
	"time"

	"github.com/google/uuid"
	lru "github.com/hashicorp/golang-lru"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"

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
	jobRunPreempted  = "jobRunPreempted"

	queued    = "queued"
	running   = "running"
	pending   = "pending"
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
	jobs             *prometheus.CounterVec
	cpu              *prometheus.CounterVec
	memory           *prometheus.CounterVec
	gpu              *prometheus.CounterVec
	storage          *prometheus.CounterVec
	ephemeralStorage *prometheus.CounterVec

	jobsSeconds             *prometheus.CounterVec
	cpuSeconds              *prometheus.CounterVec
	memorySeconds           *prometheus.CounterVec
	gpuSeconds              *prometheus.CounterVec
	storageSeconds          *prometheus.CounterVec
	ephemeralStorageSeconds *prometheus.CounterVec
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

		buffer: make([]string, 0, 9),

		errorRegexes:                    errorRegexes,
		matchedRegexIndexByErrorMessage: matchedRegexIndexByError,

		jobs: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Subsystem: subsystem,
				Name:      "jobs_total",
				Help:      "Job counters.",
			},
			[]string{"state", "category", "subCategory", "queue", "cluster", "nodeType", "node"},
		),
		cpu: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Subsystem: subsystem,
				Name:      "cpu_total",
				Help:      "Job CPU counters.",
			},
			[]string{"state", "category", "subCategory", "queue", "cluster", "nodeType", "node"},
		),
		memory: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Subsystem: subsystem,
				Name:      "memory_total",
				Help:      "Job memory counters.",
			},
			[]string{"state", "category", "subCategory", "queue", "cluster", "nodeType", "node"},
		),
		gpu: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Subsystem: subsystem,
				Name:      "gpu_total",
				Help:      "Job GPU counters.",
			},
			[]string{"state", "category", "subCategory", "queue", "cluster", "nodeType", "node"},
		),
		storage: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Subsystem: subsystem,
				Name:      "storage_total",
				Help:      "Job storage counters.",
			},
			[]string{"state", "category", "subCategory", "queue", "cluster", "nodeType", "node"},
		),
		ephemeralStorage: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Subsystem: subsystem,
				Name:      "ephemeral_storage_total",
				Help:      "Job ephemeral storage counters",
			},
			[]string{"state", "category", "subCategory", "queue", "cluster", "nodeType", "node"},
		),
		jobsSeconds: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Subsystem: subsystem,
				Name:      "jobs_seconds_total",
				Help:      "Job seconds counters.",
			},
			[]string{"priorState", "state", "category", "subCategory", "queue", "cluster", "nodeType", "node"},
		),
		cpuSeconds: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Subsystem: subsystem,
				Name:      "cpu_seconds_total",
				Help:      "Job CPU-second counters.",
			},
			[]string{"priorState", "state", "category", "subCategory", "queue", "cluster", "nodeType", "node"},
		),
		memorySeconds: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Subsystem: subsystem,
				Name:      "memory_seconds_total",
				Help:      "Job memory-second counters.",
			},
			[]string{"priorState", "state", "category", "subCategory", "queue", "cluster", "nodeType", "node"},
		),
		gpuSeconds: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Subsystem: subsystem,
				Name:      "gpu_seconds_total",
				Help:      "Job GPU-second counters.",
			},
			[]string{"priorState", "state", "category", "subCategory", "queue", "cluster", "nodeType", "node"},
		),
		storageSeconds: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Subsystem: subsystem,
				Name:      "storage_seconds_total",
				Help:      "Job storage-second counters.",
			},
			[]string{"priorState", "state", "category", "subCategory", "queue", "cluster", "nodeType", "node"},
		),
		ephemeralStorageSeconds: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Subsystem: subsystem,
				Name:      "ephemeral_storage_seconds_total",
				Help:      "Job ephemeral storage-second counters",
			},
			[]string{"priorState", "state", "category", "subCategory", "queue", "cluster", "nodeType", "node"},
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

	m.jobs.Describe(ch)
	m.cpu.Describe(ch)
	m.memory.Describe(ch)
	m.gpu.Describe(ch)
	m.storage.Describe(ch)
	m.ephemeralStorage.Describe(ch)

	m.jobsSeconds.Describe(ch)
	m.cpuSeconds.Describe(ch)
	m.memorySeconds.Describe(ch)
	m.gpuSeconds.Describe(ch)
	m.storageSeconds.Describe(ch)
	m.ephemeralStorageSeconds.Describe(ch)
}

// Collect and then reset all metrics.
// Resetting ensures we do not build up a large number of counters over time.
func (m *Metrics) Collect(ch chan<- prometheus.Metric) {
	if m.IsDisabled() {
		return
	}

	m.jobs.Collect(ch)
	m.cpu.Collect(ch)
	m.memory.Collect(ch)
	m.gpu.Collect(ch)
	m.storage.Collect(ch)
	m.ephemeralStorage.Collect(ch)

	m.jobsSeconds.Collect(ch)
	m.cpuSeconds.Collect(ch)
	m.memorySeconds.Collect(ch)
	m.gpuSeconds.Collect(ch)
	m.storageSeconds.Collect(ch)
	m.ephemeralStorageSeconds.Collect(ch)

	// Reset metrics periodically.
	t := time.Now()
	if t.Sub(m.timeOfMostRecentReset) > m.resetInterval {

		m.jobs.Reset()
		m.cpu.Reset()
		m.memory.Reset()
		m.gpu.Reset()
		m.storage.Reset()
		m.ephemeralStorage.Reset()

		m.jobsSeconds.Reset()
		m.cpuSeconds.Reset()
		m.memorySeconds.Reset()
		m.gpuSeconds.Reset()
		m.storageSeconds.Reset()
		m.ephemeralStorageSeconds.Reset()

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
	if jst.Pending {
		if err := m.UpdatePending(jst.Job); err != nil {
			return err
		}
	}
	if jst.Running {
		if err := m.UpdateRunning(jst.Job); err != nil {
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
	if jst.Preempted {
		if err := m.UpdatePreempted(jst.Job); err != nil {
			return err
		}
	}
	// UpdateLeased is called by the scheduler directly once a job is leased.
	// It is not called here to avoid double counting.
	return nil
}

func (m *Metrics) UpdateQueued(job *jobdb.Job) error {
	labels := m.buffer[0:0]
	labels = append(labels, "") // No priorState for queued.
	labels = append(labels, queued)
	labels = append(labels, "") // No category for queued.
	labels = append(labels, "") // No subCategory for queued.
	labels = appendLabelsFromJob(labels, job)

	return m.updateMetrics(labels, job, 0)
}

func (m *Metrics) UpdatePending(job *jobdb.Job) error {
	latestRun := job.LatestRun()
	duration, priorState := stateDuration(job, latestRun, latestRun.PendingTime())
	labels := m.buffer[0:0]
	labels = append(labels, priorState)
	labels = append(labels, pending)
	labels = append(labels, "") // No category for pending.
	labels = append(labels, "") // No subCategory for pending.
	labels = appendLabelsFromJob(labels, job)

	return m.updateMetrics(labels, job, duration)
}

func (m *Metrics) UpdateCancelled(job *jobdb.Job) error {
	latestRun := job.LatestRun()
	duration, priorState := stateDuration(job, latestRun, latestRun.TerminatedTime())
	labels := m.buffer[0:0]
	labels = append(labels, priorState)
	labels = append(labels, cancelled)
	labels = append(labels, "") // No category for cancelled.
	labels = append(labels, "") // No subCategory for cancelled.
	labels = appendLabelsFromJob(labels, job)

	return m.updateMetrics(labels, job, duration)
}

func (m *Metrics) UpdateFailed(ctx *armadacontext.Context, job *jobdb.Job, jobRunErrorsByRunId map[uuid.UUID]*armadaevents.Error) error {
	category, subCategory := m.failedCategoryAndSubCategoryFromJob(ctx, job, jobRunErrorsByRunId)
	if category == jobRunPreempted {
		// It is safer to UpdatePreempted from preemption errors and not from the scheduler cycle result.
		// e.g. The scheduler might decide to preempt a job, but before the job is preempted, it happens to succeed,
		// in which case it should be reported as a success, not a preemption.
		return m.UpdatePreempted(job)
	}
	latestRun := job.LatestRun()
	duration, priorState := stateDuration(job, latestRun, latestRun.TerminatedTime())
	labels := m.buffer[0:0]
	labels = append(labels, priorState)
	labels = append(labels, failed)
	labels = append(labels, category)
	labels = append(labels, subCategory)
	labels = appendLabelsFromJob(labels, job)

	return m.updateMetrics(labels, job, duration)
}

func (m *Metrics) UpdateSucceeded(job *jobdb.Job) error {
	labels := m.buffer[0:0]
	latestRun := job.LatestRun()
	duration, priorState := stateDuration(job, latestRun, latestRun.TerminatedTime())
	labels = append(labels, priorState)
	labels = append(labels, succeeded)
	labels = append(labels, "") // No category for succeeded.
	labels = append(labels, "") // No subCategory for succeeded.
	labels = appendLabelsFromJob(labels, job)

	return m.updateMetrics(labels, job, duration)
}

func (m *Metrics) UpdateLeased(jctx *schedulercontext.JobSchedulingContext) error {
	job := jctx.Job.(*jobdb.Job)
	latestRun := job.LatestRun()
	duration, priorState := stateDuration(job, latestRun, &jctx.Created)
	labels := m.buffer[0:0]
	labels = append(labels, priorState)
	labels = append(labels, leased)
	labels = append(labels, "") // No category for leased.
	labels = append(labels, "") // No subCategory for leased.
	labels = appendLabelsFromJobSchedulingContext(labels, jctx)

	return m.updateMetrics(labels, job, duration)
}

func (m *Metrics) UpdatePreempted(job *jobdb.Job) error {
	latestRun := job.LatestRun()
	duration, priorState := stateDuration(job, latestRun, latestRun.PreemptedTime())
	labels := m.buffer[0:0]
	labels = append(labels, priorState)
	labels = append(labels, preempted)
	labels = append(labels, "") // No category for preempted.
	labels = append(labels, "") // No subCategory for preempted.
	labels = appendLabelsFromJob(labels, job)

	return m.updateMetrics(labels, job, duration)
}

func (m *Metrics) UpdateRunning(job *jobdb.Job) error {
	latestRun := job.LatestRun()
	duration, priorState := stateDuration(job, latestRun, latestRun.RunningTime())
	labels := m.buffer[0:0]
	labels = append(labels, priorState)
	labels = append(labels, running)
	labels = append(labels, "") // No category for running.
	labels = append(labels, "") // No subCategory for running.
	labels = appendLabelsFromJob(labels, job)

	return m.updateMetrics(labels, job, duration)
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
	case *armadaevents.Error_JobRunPreemptedError:
		return jobRunPreempted, ""
	default:
		ctx.Warnf("omitting name and message for unknown error type %T", err.Reason)
		return "", ""
	}
}

func (m *Metrics) updateMetrics(labels []string, job *jobdb.Job, stateDuration time.Duration) error {
	// update jobs counter
	if c, err := m.jobs.GetMetricWithLabelValues(labels[1:]...); err != nil { // we don't need priorState label here
		return err
	} else {
		c.Add(1)
	}
	// update jobs-seconds counter
	if c, err := m.jobsSeconds.GetMetricWithLabelValues(labels...); err != nil {
		return err
	} else {
		c.Add(stateDuration.Seconds())
	}

	requests := job.GetResourceRequirements().Requests
	for _, resourceName := range m.config.TrackedResourceNames {
		metric, metricSeconds := m.resourceToMetrics(resourceName)
		if metric == nil || metricSeconds == nil {
			continue
		}
		c, err := metric.GetMetricWithLabelValues(labels[1:]...) // we don't need priorState label here
		if err != nil {
			return err
		}
		cSeconds, err := metricSeconds.GetMetricWithLabelValues(labels...)
		if err != nil {
			return err
		}
		q := requests[resourceName]
		v := float64(q.MilliValue()) / 1000
		c.Add(v)
		cSeconds.Add(v * stateDuration.Seconds())
	}

	return nil
}

// maps resource names to their respective counter vectors
// e.g. cpu -> m.cpu, m.cpuSeconds
func (m *Metrics) resourceToMetrics(resourceName v1.ResourceName) (*prometheus.CounterVec, *prometheus.CounterVec) {
	switch resourceName {
	case v1.ResourceCPU:
		return m.cpu, m.cpuSeconds
	case v1.ResourceMemory:
		return m.memory, m.memorySeconds
	case v1.ResourceEphemeralStorage:
		return m.ephemeralStorage, m.ephemeralStorageSeconds
	case v1.ResourceStorage:
		return m.storage, m.storageSeconds
	case "nvidia.com/gpu":
		return m.gpu, m.gpuSeconds
	default:
		logrus.Warnf("Unknown resource name: %s", resourceName)
		return nil, nil
	}
}

// stateDuration returns:
// -  the duration of the current state (stateTime - priorTime)
// -  the prior state name
func stateDuration(job *jobdb.Job, run *jobdb.JobRun, stateTime *time.Time) (time.Duration, string) {
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

	return stateTime.Sub(*priorTime), prior
}
