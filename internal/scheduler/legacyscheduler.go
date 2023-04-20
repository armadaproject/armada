package scheduler

import (
	"fmt"
	"strconv"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/exp/maps"

	"github.com/armadaproject/armada/internal/armada/configuration"
	armadamaps "github.com/armadaproject/armada/internal/common/maps"
	armadaresource "github.com/armadaproject/armada/internal/common/resource"
	armadaslices "github.com/armadaproject/armada/internal/common/slices"
	schedulerconfig "github.com/armadaproject/armada/internal/scheduler/configuration"
	schedulercontext "github.com/armadaproject/armada/internal/scheduler/context"
	"github.com/armadaproject/armada/internal/scheduler/interfaces"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

// SchedulerResult is returned by Rescheduler.Schedule().
type SchedulerResult struct {
	// Running jobs that should be preempted.
	PreemptedJobs []interfaces.LegacySchedulerJob
	// Queued jobs that should be scheduled.
	ScheduledJobs []interfaces.LegacySchedulerJob
	// For each preempted job, maps the job id to the id of the node on which the job was running.
	// For each scheduled job, maps the job id to the id of the node on which the job should be scheduled.
	NodeIdByJobId map[string]string
}

func NewSchedulerResult[S ~[]T, T interfaces.LegacySchedulerJob](
	preemptedJobs S,
	scheduledJobs S,
	nodeIdByJobId map[string]string,
) *SchedulerResult {
	castPreemptedJobs := make([]interfaces.LegacySchedulerJob, len(preemptedJobs))
	for i, job := range preemptedJobs {
		castPreemptedJobs[i] = job
	}
	castScheduledJobs := make([]interfaces.LegacySchedulerJob, len(scheduledJobs))
	for i, job := range scheduledJobs {
		castScheduledJobs[i] = job
	}
	return &SchedulerResult{
		PreemptedJobs: castPreemptedJobs,
		ScheduledJobs: castScheduledJobs,
		NodeIdByJobId: nodeIdByJobId,
	}
}

// PreemptedJobsFromSchedulerResult returns the slice of preempted jobs in the result,
// cast to type T.
func PreemptedJobsFromSchedulerResult[T interfaces.LegacySchedulerJob](sr *SchedulerResult) []T {
	rv := make([]T, len(sr.PreemptedJobs))
	for i, job := range sr.PreemptedJobs {
		rv[i] = job.(T)
	}
	return rv
}

// ScheduledJobsFromScheduleResult returns the slice of scheduled jobs in the result,
// cast to type T.
func ScheduledJobsFromSchedulerResult[T interfaces.LegacySchedulerJob](sr *SchedulerResult) []T {
	rv := make([]T, len(sr.ScheduledJobs))
	for i, job := range sr.ScheduledJobs {
		rv[i] = job.(T)
	}
	return rv
}

func JobsSummary(jobs []interfaces.LegacySchedulerJob) string {
	if len(jobs) == 0 {
		return ""
	}
	evictedJobsByQueue := armadaslices.GroupByFunc(
		jobs,
		func(job interfaces.LegacySchedulerJob) string { return job.GetQueue() },
	)
	resourcesByQueue := armadamaps.MapValues(
		evictedJobsByQueue,
		func(jobs []interfaces.LegacySchedulerJob) schedulerobjects.ResourceList {
			rv := schedulerobjects.ResourceList{}
			for _, job := range jobs {
				req := PodRequirementFromLegacySchedulerJob(job, nil)
				if req == nil {
					continue
				}
				rl := schedulerobjects.ResourceListFromV1ResourceList(req.ResourceRequirements.Requests)
				rv.Add(rl)
			}
			return rv
		},
	)
	jobIdsByQueue := armadamaps.MapValues(
		evictedJobsByQueue,
		func(jobs []interfaces.LegacySchedulerJob) []string {
			rv := make([]string, len(jobs))
			for i, job := range jobs {
				rv[i] = job.GetId()
			}
			return rv
		},
	)
	return fmt.Sprintf(
		"affected queues %v; resources %v; jobs %v",
		maps.Keys(evictedJobsByQueue),
		armadamaps.MapValues(
			resourcesByQueue,
			func(rl schedulerobjects.ResourceList) string {
				return rl.CompactString()
			},
		),
		jobIdsByQueue,
	)
}

type AddOrSubtract int

const (
	Add AddOrSubtract = iota
	Subtract
)

func UpdateUsage[S ~[]E, E interfaces.LegacySchedulerJob](
	usage map[string]schedulerobjects.QuantityByPriorityAndResourceType,
	jobs S,
	priorityClasses map[string]configuration.PriorityClass,
	addOrSubtract AddOrSubtract,
) map[string]schedulerobjects.QuantityByPriorityAndResourceType {
	if usage == nil {
		usage = make(map[string]schedulerobjects.QuantityByPriorityAndResourceType)
	}
	for _, job := range jobs {
		req := PodRequirementFromLegacySchedulerJob(job, priorityClasses)
		if req == nil {
			continue
		}
		requests := schedulerobjects.ResourceListFromV1ResourceList(req.ResourceRequirements.Requests)
		queue := job.GetQueue()
		m := usage[queue]
		if m == nil {
			m = make(schedulerobjects.QuantityByPriorityAndResourceType)
		}
		switch addOrSubtract {
		case Add:
			m.Add(schedulerobjects.QuantityByPriorityAndResourceType{req.Priority: requests})
		case Subtract:
			m.Sub(schedulerobjects.QuantityByPriorityAndResourceType{req.Priority: requests})
		default:
			panic(fmt.Sprintf("invalid operation %d", addOrSubtract))
		}
		usage[queue] = m
	}
	return usage
}

// type LegacyScheduler struct {
// 	SchedulingConstraints
// 	SchedulingContext *schedulercontext.SchedulingContext
// 	queues            []*Queue
// 	// Resources allocated to each queue.
// 	// Updated at the end of the scheduling cycle.
// 	allocatedByQueueAndPriority map[string]schedulerobjects.QuantityByPriorityAndResourceType
// 	// Contains all nodes to be considered for scheduling.
// 	// Used for matching pods with nodes.
// 	nodeDb *nodedb.NodeDb
// }

// func NewLegacyScheduler(
// 	ctx context.Context,
// 	constraints SchedulingConstraints,
// 	nodeDb *nodedb.NodeDb,
// 	queues []*Queue,
// 	initialResourcesByQueueAndPriority map[string]schedulerobjects.QuantityByPriorityAndResourceType,
// ) (*LegacyScheduler, error) {
// 	if ResourceListAsWeightedApproximateFloat64(constraints.ResourceScarcity, constraints.TotalResources) == 0 {
// 		// This refers to resources available across all clusters, i.e.,
// 		// it may include resources not currently considered for scheduling.
// 		return nil, errors.Errorf(
// 			"no resources with non-zero weight available for scheduling on any cluster: resource scarcity %v, total resources %v",
// 			constraints.ResourceScarcity, constraints.TotalResources,
// 		)
// 	}
// 	if ResourceListAsWeightedApproximateFloat64(constraints.ResourceScarcity, nodeDb.TotalResources()) == 0 {
// 		// This refers to the resources currently considered for schedling.
// 		return nil, errors.Errorf(
// 			"no resources with non-zero weight available for scheduling in NodeDb: resource scarcity %v, total resources %v",
// 			constraints.ResourceScarcity, nodeDb.TotalResources(),
// 		)
// 	}
// 	return &LegacyScheduler{
// 		SchedulingConstraints:       constraints,
// 		queues:                      queues,
// 		allocatedByQueueAndPriority: armadamaps.DeepCopy(initialResourcesByQueueAndPriority),
// 		nodeDb:                      nodeDb,
// 	}, nil
// }

// func (sched *LegacyScheduler) String() string {
// 	var sb strings.Builder
// 	w := tabwriter.NewWriter(&sb, 1, 1, 1, ' ', 0)
// 	fmt.Fprintf(w, "Executor:\t%s\n", sched.ExecutorId)
// 	if len(sched.SchedulingConstraints.TotalResources.Resources) == 0 {
// 		fmt.Fprint(w, "Total resources:\tnone\n")
// 	} else {
// 		fmt.Fprint(w, "Total resources:\n")
// 		for t, q := range sched.SchedulingConstraints.TotalResources.Resources {
// 			fmt.Fprintf(w, "  %s: %s\n", t, q.String())
// 		}
// 	}
// 	fmt.Fprintf(w, "Minimum job size:\t%v\n", sched.MinimumJobSize)
// 	if sched.nodeDb == nil {
// 		fmt.Fprintf(w, "NodeDb:\t%v\n", sched.nodeDb)
// 	} else {
// 		fmt.Fprint(w, "NodeDb:\n")
// 		fmt.Fprint(w, indent.String("\t", sched.nodeDb.String()))
// 	}
// 	w.Flush()
// 	return sb.String()
// }

// func (sch *LegacyScheduler) Schedule(ctx context.Context) (*SchedulerResult, error) {
// 	defer func() {
// 		sch.SchedulingContext.Finished = time.Now()
// 	}()

// 	priorityFactorByQueue := make(map[string]float64)
// 	for _, queue := range sch.queues {
// 		priorityFactorByQueue[queue.name] = queue.priorityFactor
// 	}
// 	sch.SchedulingContext = schedulercontext.NewSchedulingContext(
// 		sch.ExecutorId,
// 		sch.TotalResources,
// 		priorityFactorByQueue,
// 		sch.allocatedByQueueAndPriority,
// 	)

// 	candidateGangIterator, err := sch.setupIterators(ctx)
// 	if err != nil {
// 		return nil, err
// 	}

// 	nodeIdByJobId := make(map[string]string)
// 	jobsToLeaseByQueue := make(map[string][]interfaces.LegacySchedulerJob, 0)
// 	numJobsToLease := 0
// 	for gctx, err := candidateGangIterator.Next(); gctx != nil; gctx, err = candidateGangIterator.Next() {
// 		if err != nil {
// 			sch.SchedulingContext.TerminationReason = err.Error()
// 			return nil, err
// 		}
// 		if len(gctx.JobSchedulingContexts) == 0 {
// 			continue
// 		}
// 		select {
// 		case <-ctx.Done():
// 			sch.SchedulingContext.TerminationReason = ctx.Err().Error()
// 			return nil, err
// 		default:
// 		}

// 		jobs := make([]interfaces.LegacySchedulerJob, len(gctx.JobSchedulingContexts))
// 		for i, jctx := range gctx.JobSchedulingContexts {
// 			jobs[i] = jctx.Job
// 			jctx.NumNodes = sch.nodeDb.NumNodes()
// 		}
// 		reqs := PodRequirementsFromLegacySchedulerJobs(jobs, sch.PriorityClasses)
// 		pctxs, ok, err := sch.nodeDb.ScheduleMany(reqs)
// 		if err != nil {
// 			return nil, err
// 		}
// 		for _, jctx := range gctx.JobSchedulingContexts {
// 			// Store all pod scheduling contexts for all jobs in the gang.
// 			//
// 			// TODO: No longer necessary since we handle gangs explicitly.
// 			jctx.PodSchedulingContexts = pctxs
// 		}
// 		if !ok {
// 			if len(gctx.JobSchedulingContexts) > 1 {
// 				for _, jctx := range gctx.JobSchedulingContexts {
// 					jctx.UnschedulableReason = "at least one pod in the gang did not fit on any node"
// 				}
// 			} else {
// 				for _, jctx := range gctx.JobSchedulingContexts {
// 					jctx.UnschedulableReason = "pod does not fit on any Node"
// 				}
// 			}
// 			for _, jctx := range gctx.JobSchedulingContexts {
// 				sch.SchedulingContext.AddJobSchedulingContext(jctx, false)
// 			}
// 		} else {
// 			for _, pctx := range pctxs {
// 				jobId, err := nodedb.JobIdFromPodRequirements(pctx.Req)
// 				if err != nil {
// 					return nil, err
// 				}
// 				nodeIdByJobId[jobId] = pctx.Node.Id
// 			}
// 			for _, jctx := range gctx.JobSchedulingContexts {
// 				jobsToLeaseByQueue[jctx.Job.GetQueue()] = append(jobsToLeaseByQueue[jctx.Job.GetQueue()], jctx.Job)
// 				sch.SchedulingContext.AddJobSchedulingContext(jctx, isEvictedJob(jctx.Job))
// 			}
// 			numJobsToLease += len(gctx.JobSchedulingContexts)
// 		}
// 	}
// 	sch.SchedulingContext.TerminationReason = "no remaining candidate jobs"
// 	scheduledJobs := make([]interfaces.LegacySchedulerJob, 0)
// 	for _, jobs := range jobsToLeaseByQueue {
// 		scheduledJobs = append(scheduledJobs, jobs...)
// 	}

// 	allocatedByQueueAndPriority := make(
// 		map[string]schedulerobjects.QuantityByPriorityAndResourceType,
// 		len(sch.SchedulingContext.QueueSchedulingContexts),
// 	)
// 	for queue, qctx := range sch.SchedulingContext.QueueSchedulingContexts {
// 		if len(qctx.ResourcesByPriority) > 0 {
// 			allocatedByQueueAndPriority[queue] = qctx.ResourcesByPriority.DeepCopy()
// 		}
// 	}
// 	sch.allocatedByQueueAndPriority = allocatedByQueueAndPriority
// 	if len(scheduledJobs) != len(nodeIdByJobId) {
// 		return nil, errors.Errorf("only %d out of %d jobs mapped to a node", len(nodeIdByJobId), len(scheduledJobs))
// 	}
// 	return &SchedulerResult{
// 		// This scheduler never preempts jobs.
// 		PreemptedJobs:               nil,
// 		ScheduledJobs:               scheduledJobs,
// 		NodeIdByJobId:               nodeIdByJobId,
// 		AllocatedByQueueAndPriority: armadamaps.DeepCopy(allocatedByQueueAndPriority),
// 		SchedulingContext:           sch.SchedulingContext,
// 	}, nil
// }

// func (sch *LegacyScheduler) setupIterators(ctx context.Context) (*CandidateGangIterator, error) {
// 	// Per-queue iterator pipelines.
// 	gangIteratorsByQueue := make(map[string]*QueueCandidateGangIterator)
// 	priorityFactorByQueue := make(map[string]float64)
// 	for _, queue := range sch.queues {
// 		// Group jobs into gangs, to be scheduled together.
// 		queuedGangIterator := NewQueuedGangIterator(
// 			ctx,
// 			queue.jobIterator,
// 			sch.MaxLookbackPerQueue,
// 		)

// 		// Enforce per-queue constraints.
// 		gangIteratorsByQueue[queue.name] = &QueueCandidateGangIterator{
// 			SchedulingConstraints:   sch.SchedulingConstraints,
// 			QueueSchedulingContexts: sch.SchedulingContext.QueueSchedulingContexts[queue.name],
// 			ctx:                     ctx,
// 			queuedGangIterator:      queuedGangIterator,
// 		}

// 		priorityFactorByQueue[queue.name] = queue.priorityFactor
// 	}

// 	// Multiplex between queues and enforce cross-queue constraints.
// 	candidateGangIterator, err := NewCandidateGangIterator(
// 		sch.SchedulingConstraints,
// 		sch.SchedulingContext,
// 		ctx,
// 		gangIteratorsByQueue,
// 		priorityFactorByQueue,
// 	)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return candidateGangIterator, nil
// }

func jobSchedulingContextsFromJobs[T interfaces.LegacySchedulerJob](jobs []T, executorId string, priorityClasses map[string]configuration.PriorityClass) []*schedulercontext.JobSchedulingContext {
	if jobs == nil {
		return nil
	}
	if len(jobs) == 0 {
		return make([]*schedulercontext.JobSchedulingContext, 0)
	}
	jctxs := make([]*schedulercontext.JobSchedulingContext, len(jobs))
	timestamp := time.Now()
	for i, job := range jobs {
		jctxs[i] = &schedulercontext.JobSchedulingContext{
			Created:    timestamp,
			ExecutorId: executorId,
			JobId:      job.GetId(),
			Job:        job,
			Req:        PodRequirementFromJobSchedulingInfo(job.GetRequirements(priorityClasses)),
		}
	}
	return jctxs
}

// // QueueCandidateGangIterator is an iterator over gangs in a queue that could be scheduled
// // without exceeding per-queue limits.
// type QueueCandidateGangIterator struct {
// 	ctx context.Context
// 	SchedulingConstraints
// 	QueueSchedulingContexts *schedulercontext.QueueSchedulingContext
// 	queuedGangIterator      *QueuedGangIterator
// }

// func (it *QueueCandidateGangIterator) Next() (*schedulercontext.GangSchedulingContext, error) {
// 	if v, err := it.Peek(); err != nil {
// 		return nil, err
// 	} else {
// 		if err := it.Clear(); err != nil {
// 			return nil, err
// 		}
// 		return v, nil
// 	}
// }

// func (it *QueueCandidateGangIterator) Clear() error {
// 	if err := it.queuedGangIterator.Clear(); err != nil {
// 		return err
// 	}
// 	return nil
// }

// func (it *QueueCandidateGangIterator) Peek() (*schedulercontext.GangSchedulingContext, error) {
// 	for gang, err := it.queuedGangIterator.Peek(); gang != nil; gang, err = it.queuedGangIterator.Peek() {
// 		if err != nil {
// 			return nil, err
// 		}
// 		if v, ok, err := it.f(gang); err != nil {
// 			return nil, err
// 		} else if ok {
// 			return v, nil
// 		}
// 		if err := it.queuedGangIterator.Clear(); err != nil {
// 			return nil, err
// 		}
// 	}
// 	return nil, nil
// }

// func (it *QueueCandidateGangIterator) f(gctx *schedulercontext.GangSchedulingContext) (*schedulercontext.GangSchedulingContext, bool, error) {
// 	if gctx == nil {
// 		return nil, false, nil
// 	}
// 	ok, err := it.canSchedule(it.ctx, gctx)
// 	if err != nil {
// 		return nil, false, err
// 	}
// 	if !ok {
// 		it.QueueSchedulingContexts.AddGangSchedulingContext(gctx, false)
// 	}
// 	return gctx, ok, nil
// }

// func (it *QueueCandidateGangIterator) jobSchedulingContextsFromJobs(ctx context.Context, jobs []interfaces.LegacySchedulerJob) ([]*schedulercontext.JobSchedulingContext, error) {
// 	if jobs == nil {
// 		return nil, nil
// 	}
// 	if len(jobs) == 0 {
// 		return make([]*schedulercontext.JobSchedulingContext, 0), nil
// 	}

// 	// Create the scheduling contexts and calculate the total requests of the gang,
// 	// where the total request is the sum of the requests over all jobs in the gang.
// 	allGangJobsEvicted := true
// 	jctxs := make([]*schedulercontext.JobSchedulingContext, len(jobs))
// 	timestamp := time.Now()
// 	for i, job := range jobs {
// 		allGangJobsEvicted = allGangJobsEvicted && isEvictedJob(job)
// 		req := PodRequirementFromJobSchedulingInfo(job.GetRequirements(it.PriorityClasses))
// 		jctxs[i] = &schedulercontext.JobSchedulingContext{
// 			Created:    timestamp,
// 			ExecutorId: it.ExecutorId,
// 			JobId:      job.GetId(),
// 			Job:        job,
// 			Req:        req,
// 		}
// 	}

// 	// Perform no checks for evicted jobs.
// 	// Since we don't want to preempt already running jobs if we, e.g., change MinimumJobSize.
// 	if allGangJobsEvicted {
// 		return jctxs, nil
// 	}

// 	// Set the unschedulableReason of all contexts before returning.
// 	// If any job in a gang fails to schedule,
// 	// we assign the unschedulable reason of that job to all jobs in the gang.
// 	unschedulableReason := ""
// 	defer func() {
// 		for _, jctx := range jctxs {
// 			jctx.UnschedulableReason = unschedulableReason
// 		}
// 	}()

// 	// We assume that all jobs in a gang have the same priority class
// 	// (which we enforce at job submission).
// 	priority := jctxs[0].Req.Priority

// 	// Check that the job is large enough for this executor.
// 	gangTotalResourceRequests := totalResourceRequestsFromJobs(jobs, it.PriorityClasses)
// 	if ok, reason := jobIsLargeEnough(gangTotalResourceRequests, it.MinimumJobSize); !ok {
// 		unschedulableReason = reason
// 		return jctxs, nil
// 	}

// 	// MaximalResourceFractionToSchedulePerQueue check.
// 	roundQueueResourcesByPriority := it.QueueSchedulingContexts.ScheduledResourcesByPriority.DeepCopy()
// 	roundQueueResourcesByPriority.AddResourceList(priority, gangTotalResourceRequests)
// 	if exceeded, reason := exceedsResourceLimits(
// 		ctx,
// 		roundQueueResourcesByPriority.AggregateByResource(),
// 		it.SchedulingConstraints.TotalResources,
// 		it.MaximalResourceFractionToSchedulePerQueue,
// 	); exceeded {
// 		unschedulableReason = reason + " (per scheduling round limit for this queue)"
// 		return jctxs, nil
// 	}

// 	// MaximalResourceFractionPerQueue check.
// 	totalQueueResourcesByPriority := it.QueueSchedulingContexts.ResourcesByPriority.DeepCopy()
// 	totalQueueResourcesByPriority.AddResourceList(priority, gangTotalResourceRequests)
// 	if exceeded, reason := exceedsResourceLimits(
// 		ctx,
// 		totalQueueResourcesByPriority.AggregateByResource(),
// 		it.SchedulingConstraints.TotalResources,
// 		it.MaximalResourceFractionPerQueue,
// 	); exceeded {
// 		unschedulableReason = reason + " (total limit for this queue)"
// 		return jctxs, nil
// 	}

// 	// MaximalCumulativeResourceFractionPerQueueAndPriority check.
// 	if exceeded, reason := exceedsPerPriorityResourceLimits(
// 		ctx,
// 		priority,
// 		totalQueueResourcesByPriority,
// 		it.SchedulingConstraints.TotalResources,
// 		it.MaximalCumulativeResourceFractionPerQueueAndPriority,
// 	); exceeded {
// 		unschedulableReason = reason + " (total limit for this queue)"
// 		return jctxs, nil
// 	}

// 	return jctxs, nil
// }

// func (it *QueueCandidateGangIterator) canSchedule(ctx context.Context, gctx *schedulercontext.GangSchedulingContext) (bool, error) {

// 	// The problem here is that there's no gang context.
// 	// A gang context would contain several job contexts.
// 	// Currently, I pass around slices of job contexts.
// 	// It might be worth changing that to be a gang context.
// 	// Then the first iterator would generate a gang context.
// 	// Later, I'd make it so that the checks operate on the gang context.
// 	// This thing should actually schedule.

// 	// Iterators:
// 	// Get jobs from the repo and group into gangs.
// 	// Check per-queue resource limits.
// 	// Global checks.
// 	// Actual scheduling.

// 	// I probably don't need this many iterators.
// 	// I just need a system that does all the checks.
// 	// Meaning, take a gang and say whether it can be scheduled or not.

// 	if len(gctx.JobSchedulingContexts) == 0 {
// 		return true, nil
// 	}

// 	// Perform no checks for evicted jobs.
// 	// Since we don't want to preempt already running jobs if we, e.g., change MinimumJobSize.
// 	allGangJobsEvicted := true
// 	for _, jctx := range gctx.JobSchedulingContexts {
// 		allGangJobsEvicted = allGangJobsEvicted && isEvictedJob(jctx.Job)
// 	}
// 	if allGangJobsEvicted {
// 		return true, nil
// 	}

// 	// If any job in a gang fails to schedule, set the unschedulableReason to the corresponding reason for all gang jobs.
// 	unschedulableReason := ""
// 	defer func() {
// 		for _, jctx := range gctx.JobSchedulingContexts {
// 			jctx.UnschedulableReason = unschedulableReason
// 		}
// 	}()

// 	// We assume that all jobs in a gang have the same priority class (which we enforce at job submission).
// 	// TODO: Add PC-checks.
// 	// priorityClassName := gctx.JobSchedulingContexts[0].Job.GetRequirements(it.PriorityClasses).PriorityClassName
// 	priority := int32(gctx.JobSchedulingContexts[0].Job.GetRequirements(it.PriorityClasses).Priority)

// 	// Check that the job is large enough for this executor.
// 	gangTotalResourceRequests := totalResourceRequestsFromGangSchedulingContext(gctx, it.PriorityClasses)
// 	if ok, reason := jobIsLargeEnough(gangTotalResourceRequests, it.MinimumJobSize); !ok {
// 		unschedulableReason = reason
// 		return false, nil
// 	}

// 	// MaximalResourceFractionToSchedulePerQueue check.
// 	roundQueueResourcesByPriority := it.QueueSchedulingContexts.ScheduledResourcesByPriority.DeepCopy()
// 	roundQueueResourcesByPriority.AddResourceList(priority, gangTotalResourceRequests)
// 	if exceeded, reason := exceedsResourceLimits(
// 		ctx,
// 		roundQueueResourcesByPriority.AggregateByResource(),
// 		it.SchedulingConstraints.TotalResources,
// 		it.MaximalResourceFractionToSchedulePerQueue,
// 	); exceeded {
// 		unschedulableReason = reason + " (per scheduling round limit for this queue)"
// 		return false, nil
// 	}

// 	// MaximalResourceFractionPerQueue check.
// 	totalQueueResourcesByPriority := it.QueueSchedulingContexts.ResourcesByPriority.DeepCopy()
// 	totalQueueResourcesByPriority.AddResourceList(priority, gangTotalResourceRequests)
// 	if exceeded, reason := exceedsResourceLimits(
// 		ctx,
// 		totalQueueResourcesByPriority.AggregateByResource(),
// 		it.SchedulingConstraints.TotalResources,
// 		it.MaximalResourceFractionPerQueue,
// 	); exceeded {
// 		unschedulableReason = reason + " (total limit for this queue)"
// 		return false, nil
// 	}

// 	// MaximalCumulativeResourceFractionPerQueueAndPriority check.
// 	if exceeded, reason := exceedsPerPriorityResourceLimits(
// 		ctx,
// 		priority,
// 		totalQueueResourcesByPriority,
// 		it.SchedulingConstraints.TotalResources,
// 		it.MaximalCumulativeResourceFractionPerQueueAndPriority,
// 	); exceeded {
// 		unschedulableReason = reason + " (total limit for this queue)"
// 		return false, nil
// 	}

// 	return true, nil
// }

func totalResourceRequestsFromJobs(jobs []interfaces.LegacySchedulerJob, priorityClasses map[string]configuration.PriorityClass) schedulerobjects.ResourceList {
	rv := schedulerobjects.ResourceList{}
	for _, job := range jobs {
		for _, reqs := range job.GetRequirements(priorityClasses).GetObjectRequirements() {
			rv.Add(
				schedulerobjects.ResourceListFromV1ResourceList(
					reqs.GetPodRequirements().ResourceRequirements.Requests,
				),
			)
		}
	}
	return rv
}

func totalResourceRequestsFromGangSchedulingContext(gctx *schedulercontext.GangSchedulingContext, priorityClasses map[string]configuration.PriorityClass) schedulerobjects.ResourceList {
	rv := schedulerobjects.ResourceList{}
	for _, jctx := range gctx.JobSchedulingContexts {
		job := jctx.Job
		for _, reqs := range job.GetRequirements(priorityClasses).GetObjectRequirements() {
			rv.Add(
				schedulerobjects.ResourceListFromV1ResourceList(
					reqs.GetPodRequirements().ResourceRequirements.Requests,
				),
			)
		}
	}
	return rv
}

func isEvictedJob(job interfaces.LegacySchedulerJob) bool {
	return job.GetAnnotations()[schedulerconfig.IsEvictedAnnotation] == "true"
}

func targetNodeIdFromLegacySchedulerJob(job interfaces.LegacySchedulerJob) (string, bool) {
	nodeId, ok := job.GetAnnotations()[schedulerconfig.TargetNodeIdAnnotation]
	return nodeId, ok
}

func GangIdAndCardinalityFromLegacySchedulerJob(job interfaces.LegacySchedulerJob, priorityClasses map[string]configuration.PriorityClass) (string, int, bool, error) {
	reqs := job.GetRequirements(priorityClasses)
	if reqs == nil {
		return "", 0, false, nil
	}
	if len(reqs.ObjectRequirements) != 1 {
		return "", 0, false, errors.Errorf("expected exactly one object requirement in %v", reqs)
	}
	podReqs := reqs.ObjectRequirements[0].GetPodRequirements()
	if podReqs == nil {
		return "", 0, false, nil
	}
	return GangIdAndCardinalityFromAnnotations(podReqs.Annotations)
}

func GangIdAndCardinalityFromAnnotations(annotations map[string]string) (string, int, bool, error) {
	if annotations == nil {
		return "", 0, false, nil
	}
	gangId, ok := annotations[configuration.GangIdAnnotation]
	if !ok {
		return "", 0, false, nil
	}
	gangCardinalityString, ok := annotations[configuration.GangCardinalityAnnotation]
	if !ok {
		return "", 0, false, errors.Errorf("missing annotation %s", configuration.GangCardinalityAnnotation)
	}
	gangCardinality, err := strconv.Atoi(gangCardinalityString)
	if err != nil {
		return "", 0, false, errors.WithStack(err)
	}
	if gangCardinality <= 0 {
		return "", 0, false, errors.Errorf("gang cardinality is non-positive %d", gangCardinality)
	}
	return gangId, gangCardinality, true, nil
}

func ResourceListAsWeightedApproximateFloat64(resourceScarcity map[string]float64, rl schedulerobjects.ResourceList) float64 {
	usage := 0.0
	for resourceName, quantity := range rl.Resources {
		scarcity := resourceScarcity[resourceName]
		usage += armadaresource.QuantityAsFloat64(quantity) * scarcity
	}
	return usage
}

func PodRequirementsFromLegacySchedulerJobs[S ~[]E, E interfaces.LegacySchedulerJob](jobs S, priorityClasses map[string]configuration.PriorityClass) []*schedulerobjects.PodRequirements {
	rv := make([]*schedulerobjects.PodRequirements, 0, len(jobs))
	for _, job := range jobs {
		rv = append(rv, PodRequirementFromLegacySchedulerJob(job, priorityClasses))
	}
	return rv
}

func PodRequirementFromLegacySchedulerJob[E interfaces.LegacySchedulerJob](job E, priorityClasses map[string]configuration.PriorityClass) *schedulerobjects.PodRequirements {
	annotations := make(map[string]string)
	for _, key := range configuration.ArmadaManagedAnnotations {
		if value, ok := job.GetAnnotations()[key]; ok {
			annotations[key] = value
		}
	}
	for _, key := range schedulerconfig.ArmadaSchedulerManagedAnnotations {
		if value, ok := job.GetAnnotations()[key]; ok {
			annotations[key] = value
		}
	}
	annotations[schedulerconfig.JobIdAnnotation] = job.GetId()
	annotations[schedulerconfig.QueueAnnotation] = job.GetQueue()
	info := job.GetRequirements(priorityClasses)
	req := PodRequirementFromJobSchedulingInfo(info)
	req.Annotations = annotations
	return req
}

func PodRequirementsFromJobSchedulingInfos(infos []*schedulerobjects.JobSchedulingInfo) []*schedulerobjects.PodRequirements {
	rv := make([]*schedulerobjects.PodRequirements, 0, len(infos))
	for _, info := range infos {
		rv = append(rv, PodRequirementFromJobSchedulingInfo(info))
	}
	return rv
}

func PodRequirementFromJobSchedulingInfo(info *schedulerobjects.JobSchedulingInfo) *schedulerobjects.PodRequirements {
	for _, oreq := range info.ObjectRequirements {
		if preq := oreq.GetPodRequirements(); preq != nil {
			return preq
		}
	}
	return nil
}
