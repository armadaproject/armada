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
	PoolSchedulingTerminationReasonCompleted    PoolSchedulingTerminationReason = "completed"
	PoolSchedulingTerminationReasonTimeout      PoolSchedulingTerminationReason = "timeout"
	PoolSchedulingTerminationReasonRateLimit    PoolSchedulingTerminationReason = "rate_limit"
	PoolSchedulingTerminationReasonMaxResources PoolSchedulingTerminationReason = "max_resources"
	PoolSchedulingTerminationReasonError        PoolSchedulingTerminationReason = "error"
)

func terminationReasonFromString(reason string) PoolSchedulingTerminationReason {
	switch reason {
	case context.Canceled.Error(), context.DeadlineExceeded.Error():
		return PoolSchedulingTerminationReasonTimeout
	case constraints.GlobalRateLimitExceededUnschedulableReason:
		return PoolSchedulingTerminationReasonRateLimit
	case constraints.MaximumResourcesScheduledUnschedulableReason:
		return PoolSchedulingTerminationReasonMaxResources
	default:
		return PoolSchedulingTerminationReasonCompleted
	}
}

type PoolSchedulingOutcome struct {
	Pool              string
	Success           bool
	TerminationReason PoolSchedulingTerminationReason
}

type PerPoolSchedulingStats struct {
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
	// The jobs scheduled in this cycle
	ScheduledJobs []*schedulercontext.JobSchedulingContext
	// The jobs preempted in this cycle
	PreemptedJobs []*schedulercontext.JobSchedulingContext
	// Scheduling summary for gang shapes we're interested in. Prices are determined if the job is deemed schedulable.
	MarketDrivenIndicativePrices IndicativeGangPricesByJobShape
}

// SchedulerResult is returned by Rescheduler.Schedule().
type SchedulerResult struct {
	// Running jobs that should be preempted.
	PreemptedJobs []*schedulercontext.JobSchedulingContext
	// Queued jobs that should be scheduled.
	ScheduledJobs []*schedulercontext.JobSchedulingContext
	// Running jobs that failed reconciliation
	FailedReconciliationJobs *ReconciliationResult
	// Each result may bundle the result of several scheduling decisions.
	// These are the corresponding scheduling contexts.
	// TODO: This doesn't seem like the right approach.
	SchedulingContexts []*schedulercontext.SchedulingContext
	// scheduling stats
	PerPoolSchedulingStats map[string]PerPoolSchedulingStats
	// Pool scheduling outcomes for metrics reporting
	PoolSchedulingOutcomes []PoolSchedulingOutcome
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

// PreemptedJobsFromSchedulerResult returns the slice of preempted jobs in the result.
func PreemptedJobsFromSchedulerResult(sr *SchedulerResult) []*jobdb.Job {
	rv := make([]*jobdb.Job, len(sr.PreemptedJobs))
	for i, jctx := range sr.PreemptedJobs {
		rv[i] = jctx.Job
	}
	return rv
}

// ScheduledJobsFromSchedulerResult returns the slice of scheduled jobs in the result.
func ScheduledJobsFromSchedulerResult(sr *SchedulerResult) []*jobdb.Job {
	rv := make([]*jobdb.Job, len(sr.ScheduledJobs))
	for i, jctx := range sr.ScheduledJobs {
		rv[i] = jctx.Job
	}
	return rv
}

type FailedReconciliationResult struct {
	Job    *jobdb.Job
	Reason string
}
