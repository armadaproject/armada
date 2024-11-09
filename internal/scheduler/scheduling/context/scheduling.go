package context

import (
	"fmt"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/openconfig/goyang/pkg/indent"
	"github.com/pkg/errors"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
	"golang.org/x/time/rate"

	"github.com/armadaproject/armada/internal/common/armadaerrors"
	armadamaps "github.com/armadaproject/armada/internal/common/maps"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/fairness"
)

// SchedulingContext contains information necessary for scheduling and records what happened in a scheduling round.
type SchedulingContext struct {
	// Time at which the scheduling cycle started.
	Started time.Time
	// Time at which the scheduling cycle finished.
	Finished time.Time
	// Pool for which we're currently scheduling jobs.
	Pool string
	// Determines how fairness is computed.
	FairnessCostProvider fairness.FairnessCostProvider
	// Limits job scheduling rate globally across all queues.
	// Use the "Started" time to ensure limiter state remains constant within each scheduling round.
	Limiter *rate.Limiter
	// Sum of queue weights across all queues.
	WeightSum float64
	// Per-queue scheduling contexts.
	QueueSchedulingContexts map[string]*QueueSchedulingContext
	// Total resources across all clusters in this pool available at the start of the scheduling cycle.
	TotalResources internaltypes.ResourceList
	// Allocated resources across all clusters in this pool
	Allocated internaltypes.ResourceList
	// Resources assigned across all queues during this scheduling cycle.
	ScheduledResources internaltypes.ResourceList
	// Resources evicted across all queues during this scheduling cycle.
	EvictedResources internaltypes.ResourceList
	// Total number of successfully scheduled jobs.
	NumScheduledJobs int
	// Total number of successfully scheduled gangs.
	NumScheduledGangs int
	// Total number of evicted jobs.
	NumEvictedJobs int
	// TODO(reports): Count the number of evicted gangs.
	// Reason for why the scheduling round finished.
	TerminationReason string
	// Used to efficiently generate scheduling keys.
	SchedulingKeyGenerator *schedulerobjects.SchedulingKeyGenerator
	// Record of job scheduling requirements known to be unfeasible.
	// Used to immediately reject new jobs with identical reqirements.
	// Maps to the JobSchedulingContext of a previous job attempted to schedule with the same key.
	UnfeasibleSchedulingKeys map[schedulerobjects.SchedulingKey]*JobSchedulingContext
}

func NewSchedulingContext(
	pool string,
	fairnessCostProvider fairness.FairnessCostProvider,
	limiter *rate.Limiter,
	totalResources internaltypes.ResourceList,
) *SchedulingContext {
	return &SchedulingContext{
		Started:                  time.Now(),
		Pool:                     pool,
		FairnessCostProvider:     fairnessCostProvider,
		Limiter:                  limiter,
		QueueSchedulingContexts:  make(map[string]*QueueSchedulingContext),
		TotalResources:           totalResources,
		ScheduledResources:       internaltypes.ResourceList{},
		EvictedResources:         internaltypes.ResourceList{},
		SchedulingKeyGenerator:   schedulerobjects.NewSchedulingKeyGenerator(),
		UnfeasibleSchedulingKeys: make(map[schedulerobjects.SchedulingKey]*JobSchedulingContext),
	}
}

func (sctx *SchedulingContext) ClearUnfeasibleSchedulingKeys() {
	sctx.UnfeasibleSchedulingKeys = make(map[schedulerobjects.SchedulingKey]*JobSchedulingContext)
}

func (sctx *SchedulingContext) AddQueueSchedulingContext(
	queue string, weight float64,
	initialAllocatedByPriorityClass map[string]internaltypes.ResourceList,
	demand internaltypes.ResourceList,
	cappedDemand internaltypes.ResourceList,
	limiter *rate.Limiter,
) error {
	if _, ok := sctx.QueueSchedulingContexts[queue]; ok {
		return errors.WithStack(&armadaerrors.ErrInvalidArgument{
			Name:    "queue",
			Value:   queue,
			Message: fmt.Sprintf("there already exists a context for queue %s", queue),
		})
	}
	if initialAllocatedByPriorityClass == nil {
		initialAllocatedByPriorityClass = map[string]internaltypes.ResourceList{}
	} else {
		initialAllocatedByPriorityClass = maps.Clone(initialAllocatedByPriorityClass)
	}
	allocated := internaltypes.ResourceList{}
	for _, rl := range initialAllocatedByPriorityClass {
		allocated = allocated.Add(rl)
	}
	sctx.WeightSum += weight
	sctx.Allocated = sctx.Allocated.Add(allocated)

	qctx := &QueueSchedulingContext{
		SchedulingContext:                 sctx,
		Created:                           time.Now(),
		Queue:                             queue,
		Weight:                            weight,
		Limiter:                           limiter,
		Allocated:                         allocated,
		Demand:                            demand,
		CappedDemand:                      cappedDemand,
		AllocatedByPriorityClass:          initialAllocatedByPriorityClass,
		ScheduledResourcesByPriorityClass: make(schedulerobjects.QuantityByTAndResourceType[string]),
		EvictedResourcesByPriorityClass:   make(schedulerobjects.QuantityByTAndResourceType[string]),
		SuccessfulJobSchedulingContexts:   make(map[string]*JobSchedulingContext),
		UnsuccessfulJobSchedulingContexts: make(map[string]*JobSchedulingContext),
		EvictedJobsById:                   make(map[string]bool),
	}
	sctx.QueueSchedulingContexts[queue] = qctx
	return nil
}

func (sctx *SchedulingContext) String() string {
	return sctx.ReportString(0)
}

// GetQueue is necessary to implement the fairness.QueueRepository interface.
func (sctx *SchedulingContext) GetQueue(queue string) (fairness.Queue, bool) {
	qctx, ok := sctx.QueueSchedulingContexts[queue]
	return qctx, ok
}

// UpdateFairShares updates FairShare and AdjustedFairShare for every QueueSchedulingContext associated with the
// SchedulingContext.  This works by calculating a far share as queue_weight/sum_of_all_queue_weights and an
// AdjustedFairShare by resharing any unused capacity (as determined by a queue's demand)
func (sctx *SchedulingContext) UpdateFairShares() {
	const maxIterations = 5

	type queueInfo struct {
		queueName     string
		adjustedShare float64
		fairShare     float64
		weight        float64
		cappedShare   float64
	}

	queueInfos := make([]*queueInfo, 0, len(sctx.QueueSchedulingContexts))
	for queueName, qctx := range sctx.QueueSchedulingContexts {
		cappedShare := 1.0
		if !sctx.TotalResources.AllZero() {
			cappedShare = sctx.FairnessCostProvider.UnweightedCostFromAllocation(qctx.CappedDemand)
		}
		queueInfos = append(queueInfos, &queueInfo{
			queueName:     queueName,
			adjustedShare: 0,
			fairShare:     qctx.Weight / sctx.WeightSum,
			weight:        qctx.Weight,
			cappedShare:   cappedShare,
		})
	}

	// We do this so that we get deterministic output
	slices.SortFunc(queueInfos, func(a, b *queueInfo) int {
		return strings.Compare(a.queueName, b.queueName)
	})

	unallocated := 1.0 // this is the proportion of the cluster that we can share each time

	// We will reshare unused capacity until we've reshared 99% of all capacity or we've completed 5 iteration
	for i := 0; i < maxIterations && unallocated > 0.01; i++ {
		totalWeight := 0.0
		for _, q := range queueInfos {
			totalWeight += q.weight
		}

		for _, q := range queueInfos {
			if q.weight > 0 {
				share := (q.weight / totalWeight) * unallocated
				q.adjustedShare += share
			}
		}
		unallocated = 0.0
		for _, q := range queueInfos {
			excessShare := q.adjustedShare - q.cappedShare
			if excessShare > 0 {
				q.adjustedShare = q.cappedShare
				q.weight = 0.0
				unallocated += excessShare
			}
		}
	}

	for _, q := range queueInfos {
		qtx := sctx.QueueSchedulingContexts[q.queueName]
		qtx.FairShare = q.fairShare
		qtx.AdjustedFairShare = q.adjustedShare
	}
}

func (sctx *SchedulingContext) ReportString(verbosity int32) string {
	var sb strings.Builder
	w := tabwriter.NewWriter(&sb, 1, 1, 1, ' ', 0)
	fmt.Fprintf(w, "Started:\t%s\n", sctx.Started)
	fmt.Fprintf(w, "Finished:\t%s\n", sctx.Finished)
	fmt.Fprintf(w, "Duration:\t%s\n", sctx.Finished.Sub(sctx.Started))
	fmt.Fprintf(w, "Termination reason:\t%s\n", sctx.TerminationReason)
	fmt.Fprintf(w, "Total capacity:\t%s\n", sctx.TotalResources.String())
	fmt.Fprintf(w, "Scheduled resources:\t%s\n", sctx.ScheduledResources.String())
	fmt.Fprintf(w, "Preempted resources:\t%s\n", sctx.EvictedResources.String())
	fmt.Fprintf(w, "Number of gangs scheduled:\t%d\n", sctx.NumScheduledGangs)
	fmt.Fprintf(w, "Number of jobs scheduled:\t%d\n", sctx.NumScheduledJobs)
	fmt.Fprintf(w, "Number of jobs preempted:\t%d\n", sctx.NumEvictedJobs)
	scheduled := armadamaps.Filter(
		sctx.QueueSchedulingContexts,
		func(_ string, qctx *QueueSchedulingContext) bool {
			return len(qctx.SuccessfulJobSchedulingContexts) > 0
		},
	)
	if verbosity <= 0 {
		fmt.Fprintf(w, "Scheduled queues:\t%v\n", maps.Keys(scheduled))
	} else {
		fmt.Fprint(w, "Scheduled queues:\n")
		for queueName, qctx := range scheduled {
			fmt.Fprintf(w, "\t%s:\n", queueName)
			fmt.Fprint(w, indent.String("\t\t", qctx.ReportString(verbosity-2)))
		}
	}
	preempted := armadamaps.Filter(
		sctx.QueueSchedulingContexts,
		func(_ string, qctx *QueueSchedulingContext) bool {
			return len(qctx.EvictedJobsById) > 0
		},
	)
	if verbosity <= 0 {
		fmt.Fprintf(w, "Preempted queues:\t%v\n", maps.Keys(preempted))
	} else {
		fmt.Fprint(w, "Preempted queues:\n")
		for queueName, qctx := range preempted {
			fmt.Fprintf(w, "\t%s:\n", queueName)
			fmt.Fprint(w, indent.String("\t\t", qctx.ReportString(verbosity-2)))
		}
	}
	w.Flush()
	return sb.String()
}

func (sctx *SchedulingContext) AddGangSchedulingContext(gctx *GangSchedulingContext) (bool, error) {
	allJobsEvictedInThisRound := true
	allJobsSuccessful := true
	for _, jctx := range gctx.JobSchedulingContexts {
		evictedInThisRound, err := sctx.AddJobSchedulingContext(jctx)
		if err != nil {
			return false, err
		}
		allJobsEvictedInThisRound = allJobsEvictedInThisRound && evictedInThisRound
		allJobsSuccessful = allJobsSuccessful && jctx.IsSuccessful()
	}
	if allJobsSuccessful && !allJobsEvictedInThisRound {
		sctx.NumScheduledGangs++
	}
	return allJobsEvictedInThisRound, nil
}

// AddJobSchedulingContext adds a job scheduling context.
// Automatically updates scheduled resources.
func (sctx *SchedulingContext) AddJobSchedulingContext(jctx *JobSchedulingContext) (bool, error) {
	queue := jctx.Job.Queue()
	if !jctx.IsHomeJob(sctx.Pool) {
		queue = CalculateAwayQueueName(jctx.Job.Queue())
	}
	qctx, ok := sctx.QueueSchedulingContexts[queue]
	if !ok {
		return false, errors.Errorf("failed adding job %s to scheduling context: no context for queue %s", jctx.JobId, queue)
	}
	evictedInThisRound, err := qctx.addJobSchedulingContext(jctx)
	if err != nil {
		return false, err
	}
	if jctx.IsSuccessful() {
		if evictedInThisRound {
			sctx.EvictedResources = sctx.EvictedResources.Subtract(jctx.Job.AllResourceRequirements())
			sctx.NumEvictedJobs--
		} else {
			sctx.ScheduledResources = sctx.ScheduledResources.Add(jctx.Job.AllResourceRequirements())
			sctx.NumScheduledJobs++
		}
		sctx.Allocated = sctx.Allocated.Add(jctx.Job.AllResourceRequirements())
	}
	return evictedInThisRound, nil
}

func (sctx *SchedulingContext) EvictGang(gctx *GangSchedulingContext) (bool, error) {
	allJobsScheduledInThisRound := true
	for _, jctx := range gctx.JobSchedulingContexts {
		scheduledInThisRound, err := sctx.EvictJob(jctx)
		if err != nil {
			return false, err
		}
		allJobsScheduledInThisRound = allJobsScheduledInThisRound && scheduledInThisRound
	}
	if allJobsScheduledInThisRound {
		sctx.NumScheduledGangs--
	}
	return allJobsScheduledInThisRound, nil
}

// QueueContextExists returns true if we know about the queue associated with the job. An example of when this can
// return false is when a job is running on a node
func (sctx *SchedulingContext) QueueContextExists(job *jobdb.Job) bool {
	queue := sctx.resolveQueueName(job)
	_, ok := sctx.QueueSchedulingContexts[queue]
	return ok
}

func (sctx *SchedulingContext) EvictJob(jctx *JobSchedulingContext) (bool, error) {
	queue := sctx.resolveQueueName(jctx.Job)
	qctx, ok := sctx.QueueSchedulingContexts[queue]
	if !ok {
		return false, errors.Errorf("failed adding job %s to scheduling context: no context for queue %s", jctx.JobId, queue)
	}

	scheduledInThisRound, err := qctx.evictJob(jctx.Job)
	if err != nil {
		return false, err
	}

	if scheduledInThisRound {
		sctx.ScheduledResources = sctx.ScheduledResources.Subtract(jctx.Job.AllResourceRequirements())
		sctx.NumScheduledJobs--
	} else {
		sctx.EvictedResources = sctx.EvictedResources.Add(jctx.Job.AllResourceRequirements())
		sctx.NumEvictedJobs++
	}
	sctx.Allocated = sctx.Allocated.Subtract(jctx.Job.AllResourceRequirements())
	return scheduledInThisRound, nil
}

// ClearJobSpecs zeroes out job specs to reduce memory usage.
func (sctx *SchedulingContext) ClearJobSpecs() {
	for _, qctx := range sctx.QueueSchedulingContexts {
		qctx.ClearJobSpecs()
	}
}

func (sctx *SchedulingContext) SuccessfulJobSchedulingContexts() []*JobSchedulingContext {
	jctxs := make([]*JobSchedulingContext, 0)
	for _, qctx := range sctx.QueueSchedulingContexts {
		for _, jctx := range qctx.SuccessfulJobSchedulingContexts {
			jctxs = append(jctxs, jctx)
		}
	}
	return jctxs
}

// AllocatedByQueueAndPriority returns map from queue name and priority to resources allocated.
func (sctx *SchedulingContext) AllocatedByQueueAndPriority() map[string]map[string]internaltypes.ResourceList {
	rv := make(
		map[string]map[string]internaltypes.ResourceList,
		len(sctx.QueueSchedulingContexts),
	)
	for queue, qctx := range sctx.QueueSchedulingContexts {
		if !internaltypes.RlMapAllZero(qctx.AllocatedByPriorityClass) {
			rv[queue] = maps.Clone(qctx.AllocatedByPriorityClass)
		}
	}
	return rv
}

// FairnessError returns the cumulative delta between adjusted fair share and actual share for all users who
// are below their fair share
func (sctx *SchedulingContext) FairnessError() float64 {
	fairnessError := 0.0
	for _, qctx := range sctx.QueueSchedulingContexts {
		actualShare := sctx.FairnessCostProvider.UnweightedCostFromQueue(qctx)
		delta := qctx.AdjustedFairShare - actualShare
		if delta > 0 {
			fairnessError += delta
		}
	}
	return fairnessError
}

func (sctx *SchedulingContext) resolveQueueName(job *jobdb.Job) string {
	queue := job.Queue()
	if !IsHomeJob(job, sctx.Pool) {
		queue = CalculateAwayQueueName(job.Queue())
	}
	return queue
}
