package scheduling

import (
	"time"

	"github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/nodedb"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/context"
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
	ScheduledJobs []*context.JobSchedulingContext
	// The jobs preempted in this cycle
	PreemptedJobs []*context.JobSchedulingContext
	// Scheduling summary for gang shapes we're interested in. Prices are determined if the job is deemed schedulable.
	MarketDrivenIndicativePrices IndicativeGangPricesByJobShape
}

// SchedulerResult is returned by Rescheduler.Schedule().
type SchedulerResult struct {
	// Running jobs that should be preempted.
	PreemptedJobs []*context.JobSchedulingContext
	// Queued jobs that should be scheduled.
	ScheduledJobs []*context.JobSchedulingContext
	// Running jobs that failed reconciliation
	FailedReconciliationJobs *ReconciliationResult
	// Each result may bundle the result of several scheduling decisions.
	// These are the corresponding scheduling contexts.
	// TODO: This doesn't seem like the right approach.
	SchedulingContexts []*context.SchedulingContext
	// scheduling stats
	PerPoolSchedulingStats map[string]PerPoolSchedulingStats
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
