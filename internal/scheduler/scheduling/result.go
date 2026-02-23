package scheduling

import (
	"context"
	"time"

	"github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/nodedb"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/constraints"
	schedulercontext "github.com/armadaproject/armada/internal/scheduler/scheduling/context"
)

type QueueStats struct {
	GangsConsidered                  int
	JobsConsidered                   int
	GangsScheduled                   int
	FirstGangConsideredSampleJobId   string
	FirstGangConsideredResult        string
	FirstGangConsideredQueuePosition int
	LastGangScheduledSampleJobId     string
	LastGangScheduledQueuePosition   int
	LastGangScheduledQueueCost       float64
	LastGangScheduledResources       internaltypes.ResourceList
	LastGangScheduledQueueResources  internaltypes.ResourceList
	Time                             time.Duration
}

type PoolSchedulingTerminationReason string

const (
	PoolSchedulingTerminationReasonCompleted          PoolSchedulingTerminationReason = "completed"
	PoolSchedulingTerminationReasonSchedulingDisabled PoolSchedulingTerminationReason = "scheduling_disabled"
	PoolSchedulingTerminationReasonTimeout            PoolSchedulingTerminationReason = "timeout"
	PoolSchedulingTerminationReasonSoftTimeout        PoolSchedulingTerminationReason = "soft_timeout"
	PoolSchedulingTerminationReasonRateLimit          PoolSchedulingTerminationReason = "rate_limit"
	PoolSchedulingTerminationReasonMaxResources       PoolSchedulingTerminationReason = "max_resources"
	PoolSchedulingTerminationReasonError              PoolSchedulingTerminationReason = "error"
)

func terminationReasonFromString(reason string) PoolSchedulingTerminationReason {
	switch reason {
	case context.Canceled.Error(), context.DeadlineExceeded.Error():
		return PoolSchedulingTerminationReasonTimeout
	case constraints.GlobalNewJobSchedulingDurationExceededUnschedulableReason:
		return PoolSchedulingTerminationReasonSoftTimeout
	case constraints.GlobalRateLimitExceededUnschedulableReason:
		return PoolSchedulingTerminationReasonRateLimit
	case constraints.MaximumResourcesScheduledUnschedulableReason:
		return PoolSchedulingTerminationReasonMaxResources
	default:
		return PoolSchedulingTerminationReasonCompleted
	}
}

type PoolSchedulingOutcome struct {
	error             error
	terminationReason PoolSchedulingTerminationReason
}

func NewPoolSchedulingOutcome(terminationReason PoolSchedulingTerminationReason, error error) *PoolSchedulingOutcome {
	return &PoolSchedulingOutcome{
		terminationReason: terminationReason,
		error:             error,
	}
}

func (p *PoolSchedulingOutcome) Success() bool {
	return p.error == nil
}

func (p *PoolSchedulingOutcome) Error() error {
	return p.error
}

func (p *PoolSchedulingOutcome) TerminationReason() PoolSchedulingTerminationReason {
	return p.terminationReason
}

type SchedulingInformation struct {
	// scheduling stats per queue
	StatsPerQueue map[string]QueueStats
	// number of loops executed in this cycle
	LoopNumber int
	// Result of any eviction in this cycle
	EvictorResult *EvictorResult
	// Value of ProtectedFractionOfFairShare from config
	ProtectedFractionOfFairShare float64
	// The nodeDb used in the scheduling round
	NodeDb *nodedb.NodeDb
	// Scheduling summary for gang shapes we're interested in. Prices are determined if the job is deemed schedulable.
	MarketDrivenIndicativePrices IndicativeGangPricesByJobShape
}

type SchedulingResult struct {
	// Running jobs that should be preempted.
	PreemptedJobs []*schedulercontext.JobSchedulingContext
	// Queued jobs that should be scheduled.
	ScheduledJobs []*schedulercontext.JobSchedulingContext
	// The scheduling context of the scheduling round
	SchedulingContext *schedulercontext.SchedulingContext
	// miscellaneous information about the scheduling round
	// - stats
	// - snapshot shots of intermediate states
	AdditionalSchedulingInfo *SchedulingInformation
}

type PoolSchedulingResult struct {
	// The name of the pool this result is for
	Name string
	// The result of reconciliation on this pool
	ReconciliationResult *ReconciliationResult
	// The result of scheduling new jobs on this pool
	SchedulingResult *SchedulingResult
	// The time the scheduling on this pool started
	// This will include everything in the cycle and not just scheduling
	//  such as setup, reconciliation and scheduling
	StartTime time.Time
	// The time the scheduling on this pool ended
	EndTime time.Time
	// Scheduling outcome
	Outcome PoolSchedulingOutcome
}

func (p *PoolSchedulingResult) GetDuration() time.Duration {
	if p.StartTime.IsZero() || p.EndTime.IsZero() {
		return time.Duration(0)
	}

	if p.EndTime.Before(p.StartTime) {
		return time.Duration(0)
	}

	return p.EndTime.Sub(p.StartTime)
}

func (p *PoolSchedulingResult) GetScheduledJobs() []*schedulercontext.JobSchedulingContext {
	if p.SchedulingResult == nil {
		return []*schedulercontext.JobSchedulingContext{}
	}
	return p.SchedulingResult.ScheduledJobs
}

func (p *PoolSchedulingResult) GetPreemptedJobs() []*schedulercontext.JobSchedulingContext {
	if p.SchedulingResult == nil {
		return []*schedulercontext.JobSchedulingContext{}
	}
	return p.SchedulingResult.PreemptedJobs
}

func (p *PoolSchedulingResult) GetSchedulingContext() *schedulercontext.SchedulingContext {
	if p.SchedulingResult == nil {
		return nil
	}
	return p.SchedulingResult.SchedulingContext
}

type SchedulerResult struct {
	PoolResults []*PoolSchedulingResult
}

func (s *SchedulerResult) GetAllScheduledJobs() []*schedulercontext.JobSchedulingContext {
	result := []*schedulercontext.JobSchedulingContext{}
	for _, poolResult := range s.PoolResults {
		result = append(result, poolResult.GetScheduledJobs()...)
	}
	return result
}

func (s *SchedulerResult) GetAllPreemptedJobs() []*schedulercontext.JobSchedulingContext {
	result := []*schedulercontext.JobSchedulingContext{}
	for _, poolResult := range s.PoolResults {
		result = append(result, poolResult.GetPreemptedJobs()...)
	}
	return result
}

func (s *SchedulerResult) GetAllSchedulingContexts() []*schedulercontext.SchedulingContext {
	result := []*schedulercontext.SchedulingContext{}
	for _, poolResult := range s.PoolResults {
		if poolResult.GetSchedulingContext() != nil {
			result = append(result, poolResult.GetSchedulingContext())
		}
	}
	return result
}

func (s *SchedulerResult) GetCombinedReconciliationResult() *ReconciliationResult {
	result := &ReconciliationResult{
		PreemptedJobs: []*FailedReconciliationResult{},
		FailedJobs:    []*FailedReconciliationResult{},
	}
	for _, poolResult := range s.PoolResults {
		if poolResult.ReconciliationResult != nil {
			reconciliationResult := poolResult.ReconciliationResult
			if reconciliationResult.PreemptedJobs != nil {
				result.PreemptedJobs = append(result.PreemptedJobs, reconciliationResult.PreemptedJobs...)
			}
			if reconciliationResult.FailedJobs != nil {
				result.FailedJobs = append(result.FailedJobs, reconciliationResult.FailedJobs...)
			}
		}
	}
	return result
}

type ReconciliationResult struct {
	PreemptedJobs []*FailedReconciliationResult
	FailedJobs    []*FailedReconciliationResult
}

func JobsFromFailedReconciliationResults(results []*FailedReconciliationResult) []*jobdb.Job {
	return slices.Map(results, func(r *FailedReconciliationResult) *jobdb.Job {
		return r.Job
	})
}

// PreemptedJobsFromSchedulingResult returns the slice of preempted jobs in the result.
func PreemptedJobsFromSchedulingResult(sr *SchedulingResult) []*jobdb.Job {
	rv := make([]*jobdb.Job, len(sr.PreemptedJobs))
	for i, jctx := range sr.PreemptedJobs {
		rv[i] = jctx.Job
	}
	return rv
}

// ScheduledJobsFromSchedulingResult returns the slice of scheduled jobs in the result.
func ScheduledJobsFromSchedulingResult(sr *SchedulingResult) []*jobdb.Job {
	rv := make([]*jobdb.Job, len(sr.ScheduledJobs))
	for i, jctx := range sr.ScheduledJobs {
		rv[i] = jctx.Job
	}
	return rv
}

// PreemptedJobsFromSchedulerResult returns the slice of preempted jobs in the result.
func PreemptedJobsFromSchedulerResult(sr *SchedulerResult) []*jobdb.Job {
	preemptedJobs := sr.GetAllPreemptedJobs()
	rv := make([]*jobdb.Job, len(preemptedJobs))
	for i, jctx := range preemptedJobs {
		rv[i] = jctx.Job
	}
	return rv
}

// ScheduledJobsFromSchedulerResult returns the slice of scheduled jobs in the result.
func ScheduledJobsFromSchedulerResult(sr *SchedulerResult) []*jobdb.Job {
	scheduledJobs := sr.GetAllScheduledJobs()
	rv := make([]*jobdb.Job, len(scheduledJobs))
	for i, jctx := range scheduledJobs {
		rv[i] = jctx.Job
	}
	return rv
}

type FailedReconciliationResult struct {
	Job    *jobdb.Job
	Reason string
}
