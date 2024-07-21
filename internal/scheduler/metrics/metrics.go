package metrics

import (
	"regexp"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/schedulerresult"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

const (
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
	// Pre-compiled regexes for error categorisation.
	errorRegexes []*regexp.Regexp
	// resources we want to provide metrics for
	trackedResourceNames []v1.ResourceName
}

func New(trackedErrorRegexes []string, trackedResourceNames []v1.ResourceName) (*Metrics, error) {
	errorRegexes := make([]*regexp.Regexp, len(trackedErrorRegexes))
	for i, errorRegex := range trackedErrorRegexes {
		if r, err := regexp.Compile(errorRegex); err != nil {
			return nil, errors.WithStack(err)
		} else {
			errorRegexes[i] = r
		}
	}

	return &Metrics{
		errorRegexes:         errorRegexes,
		trackedResourceNames: trackedResourceNames,
	}, nil
}

func (m *Metrics) ReportScheduleCycleTime(cycleTime time.Duration) {
	scheduleCycleTimeMetric.Observe(float64(cycleTime.Milliseconds()))
}

func (m *Metrics) ReportReconcileCycleTime(cycleTime time.Duration) {
	reconciliationCycleTimeMetric.Observe(float64(cycleTime.Milliseconds()))
}

func (m *Metrics) ReportSchedulerResult(result schedulerresult.SchedulerResult) {
	// Metrics that depend on pool
	for _, schedContext := range result.SchedulingContexts {
		pool := schedContext.Pool
		for queue, queueContext := range schedContext.QueueSchedulingContexts {
			jobsConsidered := float64(len(queueContext.UnsuccessfulJobSchedulingContexts) + len(queueContext.SuccessfulJobSchedulingContexts))
			actualShare := schedContext.FairnessCostProvider.UnweightedCostFromQueue(queueContext)
			demand := schedContext.FairnessCostProvider.UnweightedCostFromAllocation(queueContext.Demand)
			cappedDemand := schedContext.FairnessCostProvider.UnweightedCostFromAllocation(queueContext.CappedDemand)

			consideredJobsMetric.WithLabelValues(queue, pool).Set(jobsConsidered)
			fairShareMetric.WithLabelValues(queue, pool).Set(queueContext.FairShare)
			adjustedFairShareMetric.WithLabelValues(queue, pool).Set(queueContext.AdjustedFairShare)
			actualShareMetric.WithLabelValues(queue, pool).Set(actualShare)
			demandMetric.WithLabelValues(queue, pool).Set(demand)
			cappedDemandMetric.WithLabelValues(queue, pool).Set(cappedDemand)
		}
		fairnessErrorMetric.WithLabelValues(pool).Set(schedContext.FairnessError())
	}

	for _, jobCtx := range result.ScheduledJobs {
		scheduledJobsMetric.WithLabelValues(jobCtx.Job.Queue(), jobCtx.PriorityClassName).Inc()
	}

	for _, jobCtx := range result.PreemptedJobs {
		premptedJobsMetric.WithLabelValues(jobCtx.Job.Queue(), jobCtx.PriorityClassName).Inc()
	}
}

func (m *Metrics) UpdateJobStateTransitinMetrics(
	jst []jobdb.JobStateTransitions,
	jobRunErrorsByRunId map[uuid.UUID]*armadaevents.Error,
) {
	for _, jst := range jst {
		job := jst.Job
		run := job.LatestRun()

		if jst.Leased {
			duration, priorState := stateDuration(job, run, run.LeaseTime())
			m.updateStateDuration(job, leased, priorState, duration)
		}
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
			completedRunDurationsMetric.WithLabelValues(job.Queue()).Observe(duration)
		}
		if jst.Failed {
			duration, priorState := stateDuration(job, run, run.TerminatedTime())
			m.updateStateDuration(job, failed, priorState, duration)
			completedRunDurationsMetric.WithLabelValues(job.Queue()).Observe(duration)
			jobRunError := jobRunErrorsByRunId[run.Id()]
			category, subCategory := m.failedCategoryAndSubCategoryFromJob(jobRunError)
			jobErrorsMetric.WithLabelValues(job.Queue(), run.Executor(), category, subCategory).Inc()
		}
		if jst.Succeeded {
			duration, priorState := stateDuration(job, run, run.TerminatedTime())
			m.updateStateDuration(job, succeeded, priorState, duration)
			completedRunDurationsMetric.WithLabelValues(job.Queue()).Observe(duration)
		}
		if jst.Preempted {
			duration, priorState := stateDuration(job, run, run.PreemptedTime())
			m.updateStateDuration(job, preempted, priorState, duration)
			completedRunDurationsMetric.WithLabelValues(job.Queue()).Observe(duration)
		}
	}
}

func (m *Metrics) updateStateDuration(job *jobdb.Job, state string, priorState string, duration float64) {

	if duration <= 0 {
		return
	}

	queue := job.Queue()
	requests := job.ResourceRequirements().Requests
	latestRun := job.LatestRun()

	// Update job state seconds
	jobStateSecondsMetric.
		WithLabelValues(queue, latestRun.Executor(), state, priorState).
		Add(duration)

	for _, res := range m.trackedResourceNames {
		resQty := requests[res]
		resSeconds := duration * float64(resQty.MilliValue()) / 1000
		jobStateResourceSecondsMetric.
			WithLabelValues(queue, latestRun.Executor(), state, priorState, res.String()).
			Add(resSeconds)
	}
}

func (m *Metrics) failedCategoryAndSubCategoryFromJob(err *armadaevents.Error) (string, string) {
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
