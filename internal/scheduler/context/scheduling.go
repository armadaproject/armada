package context

import (
	"fmt"
	"github.com/pkg/errors"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/openconfig/goyang/pkg/indent"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
	"golang.org/x/time/rate"

	"github.com/armadaproject/armada/internal/common/armadaerrors"
	armadamaps "github.com/armadaproject/armada/internal/common/maps"
	"github.com/armadaproject/armada/internal/scheduler/fairness"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
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
	TotalResources schedulerobjects.ResourceList
	// Allocated resources across all clusters in this pool
	Allocated schedulerobjects.ResourceList
	// Resources assigned across all queues during this scheduling cycle.
	ScheduledResources                schedulerobjects.ResourceList
	ScheduledResourcesByPriorityClass schedulerobjects.QuantityByTAndResourceType[string]
	// Resources evicted across all queues during this scheduling cycle.
	EvictedResources                schedulerobjects.ResourceList
	EvictedResourcesByPriorityClass schedulerobjects.QuantityByTAndResourceType[string]
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
	totalResources schedulerobjects.ResourceList,
) *SchedulingContext {
	return &SchedulingContext{
		Started:                           time.Now(),
		Pool:                              pool,
		FairnessCostProvider:              fairnessCostProvider,
		Limiter:                           limiter,
		QueueSchedulingContexts:           make(map[string]*QueueSchedulingContext),
		TotalResources:                    totalResources.DeepCopy(),
		ScheduledResources:                schedulerobjects.NewResourceListWithDefaultSize(),
		ScheduledResourcesByPriorityClass: make(schedulerobjects.QuantityByTAndResourceType[string]),
		EvictedResourcesByPriorityClass:   make(schedulerobjects.QuantityByTAndResourceType[string]),
		SchedulingKeyGenerator:            schedulerobjects.NewSchedulingKeyGenerator(),
		UnfeasibleSchedulingKeys:          make(map[schedulerobjects.SchedulingKey]*JobSchedulingContext),
	}
}

func (sctx *SchedulingContext) ClearUnfeasibleSchedulingKeys() {
	sctx.UnfeasibleSchedulingKeys = make(map[schedulerobjects.SchedulingKey]*JobSchedulingContext)
}

func (sctx *SchedulingContext) AddQueueSchedulingContext(
	queue string, weight float64,
	initialAllocatedByPriorityClass schedulerobjects.QuantityByTAndResourceType[string],
	demand schedulerobjects.ResourceList,
	cappedDemand schedulerobjects.ResourceList,
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
		initialAllocatedByPriorityClass = make(schedulerobjects.QuantityByTAndResourceType[string])
	} else {
		initialAllocatedByPriorityClass = initialAllocatedByPriorityClass.DeepCopy()
	}
	allocated := schedulerobjects.NewResourceListWithDefaultSize()
	for _, rl := range initialAllocatedByPriorityClass {
		allocated.Add(rl)
	}
	sctx.WeightSum += weight
	sctx.Allocated.Add(allocated)

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

// TotalCost returns the sum of the costs across all queues.
func (sctx *SchedulingContext) TotalCost() float64 {
	var rv float64
	for _, qctx := range sctx.QueueSchedulingContexts {
		rv += sctx.FairnessCostProvider.UnweightedCostFromQueue(qctx)
	}
	return rv
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
		if !sctx.TotalResources.IsZero() {
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
	fmt.Fprintf(w, "Total capacity:\t%s\n", sctx.TotalResources.CompactString())
	fmt.Fprintf(w, "Scheduled resources:\t%s\n", sctx.ScheduledResources.CompactString())
	fmt.Fprintf(w, "Preempted resources:\t%s\n", sctx.EvictedResources.CompactString())
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
	qctx, ok := sctx.QueueSchedulingContexts[jctx.Job.Queue()]
	if !ok {
		return false, errors.Errorf("failed adding job %s to scheduling context: no context for queue %s", jctx.JobId, jctx.Job.Queue())
	}
	evictedInThisRound, err := qctx.addJobSchedulingContext(jctx)
	if err != nil {
		return false, err
	}
	if jctx.IsSuccessful() {
		if evictedInThisRound {
			sctx.EvictedResources.SubV1ResourceList(jctx.PodRequirements.ResourceRequirements.Requests)
			sctx.EvictedResourcesByPriorityClass.SubV1ResourceList(jctx.Job.PriorityClassName(), jctx.PodRequirements.ResourceRequirements.Requests)
			sctx.NumEvictedJobs--
		} else {
			sctx.ScheduledResources.AddV1ResourceList(jctx.PodRequirements.ResourceRequirements.Requests)
			sctx.ScheduledResourcesByPriorityClass.AddV1ResourceList(jctx.Job.PriorityClassName(), jctx.PodRequirements.ResourceRequirements.Requests)
			sctx.NumScheduledJobs++
		}
		sctx.Allocated.AddV1ResourceList(jctx.PodRequirements.ResourceRequirements.Requests)
	}
	return evictedInThisRound, nil
}

func (sctx *SchedulingContext) EvictGang(jobs []*jobdb.Job) (bool, error) {
	allJobsScheduledInThisRound := true
	for _, job := range jobs {
		scheduledInThisRound, err := sctx.EvictJob(job)
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

func (sctx *SchedulingContext) EvictJob(job *jobdb.Job) (bool, error) {
	qctx, ok := sctx.QueueSchedulingContexts[job.Queue()]
	if !ok {
		return false, errors.Errorf("failed evicting job %s from scheduling context: no context for queue %s", job.Id(), job.Queue())
	}
	scheduledInThisRound, err := qctx.evictJob(job)
	if err != nil {
		return false, err
	}
	rl := job.ResourceRequirements().Requests
	if scheduledInThisRound {
		sctx.ScheduledResources.SubV1ResourceList(rl)
		sctx.ScheduledResourcesByPriorityClass.SubV1ResourceList(job.PriorityClassName(), rl)
		sctx.NumScheduledJobs--
	} else {
		sctx.EvictedResources.AddV1ResourceList(rl)
		sctx.EvictedResourcesByPriorityClass.AddV1ResourceList(job.PriorityClassName(), rl)
		sctx.NumEvictedJobs++
	}
	sctx.Allocated.SubV1ResourceList(rl)
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
func (sctx *SchedulingContext) AllocatedByQueueAndPriority() map[string]schedulerobjects.QuantityByTAndResourceType[string] {
	rv := make(
		map[string]schedulerobjects.QuantityByTAndResourceType[string],
		len(sctx.QueueSchedulingContexts),
	)
	for queue, qctx := range sctx.QueueSchedulingContexts {
		if !qctx.AllocatedByPriorityClass.IsZero() {
			rv[queue] = qctx.AllocatedByPriorityClass.DeepCopy()
		}
	}
	return rv
}
