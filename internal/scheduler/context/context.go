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
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/internal/common/armadaerrors"
	armadamaps "github.com/armadaproject/armada/internal/common/maps"
	armadaslices "github.com/armadaproject/armada/internal/common/slices"
	schedulerconfig "github.com/armadaproject/armada/internal/scheduler/configuration"
	"github.com/armadaproject/armada/internal/scheduler/interfaces"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

// SchedulingContext contains information necessary for scheduling and records what happened in a scheduling round.
type SchedulingContext struct {
	// Time at which the scheduling cycle started.
	Started time.Time
	// Time at which the scheduling cycle finished.
	Finished time.Time
	// Executor for which we're currently scheduling jobs.
	ExecutorId string
	// Resource pool of this executor.
	Pool string
	// Allowed priority classes.
	PriorityClasses map[string]configuration.PriorityClass
	// Default priority class.
	DefaultPriorityClass string
	// Determines how fairness is computed.
	FairnessModel configuration.FairnessModel
	// Resources considered when computing DominantResourceFairness.
	DominantResourceFairnessResourcesToConsider []string
	// Weights used when computing AssetFairness.
	ResourceScarcity map[string]float64
	// Per-queue scheduling contexts.
	QueueSchedulingContexts map[string]*QueueSchedulingContext
	// Total resources across all clusters available at the start of the scheduling cycle.
	TotalResources schedulerobjects.ResourceList
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
	executorId string,
	pool string,
	priorityClasses map[string]configuration.PriorityClass,
	defaultPriorityClass string,
	resourceScarcity map[string]float64,
	totalResources schedulerobjects.ResourceList,
) *SchedulingContext {
	return &SchedulingContext{
		Started:                           time.Now(),
		ExecutorId:                        executorId,
		Pool:                              pool,
		PriorityClasses:                   priorityClasses,
		DefaultPriorityClass:              defaultPriorityClass,
		FairnessModel:                     configuration.AssetFairness,
		ResourceScarcity:                  resourceScarcity,
		QueueSchedulingContexts:           make(map[string]*QueueSchedulingContext),
		TotalResources:                    totalResources.DeepCopy(),
		ScheduledResources:                schedulerobjects.NewResourceListWithDefaultSize(),
		ScheduledResourcesByPriorityClass: make(schedulerobjects.QuantityByTAndResourceType[string]),
		EvictedResourcesByPriorityClass:   make(schedulerobjects.QuantityByTAndResourceType[string]),
		SchedulingKeyGenerator:            schedulerobjects.NewSchedulingKeyGenerator(),
		UnfeasibleSchedulingKeys:          make(map[schedulerobjects.SchedulingKey]*JobSchedulingContext),
	}
}

func (sctx *SchedulingContext) EnableDominantResourceFairness(dominantResourceFairnessResourcesToConsider []string) {
	sctx.FairnessModel = configuration.DominantResourceFairness
	sctx.DominantResourceFairnessResourcesToConsider = dominantResourceFairnessResourcesToConsider
}

func (sctx *SchedulingContext) SchedulingKeyFromLegacySchedulerJob(job interfaces.LegacySchedulerJob) schedulerobjects.SchedulingKey {
	var priority int32
	if priorityClass, ok := sctx.PriorityClasses[job.GetPriorityClassName()]; ok {
		priority = priorityClass.Priority
	}
	return sctx.SchedulingKeyGenerator.Key(
		job.GetNodeSelector(),
		job.GetAffinity(),
		job.GetTolerations(),
		job.GetResourceRequirements().Requests,
		priority,
	)
}

func (sctx *SchedulingContext) ClearUnfeasibleSchedulingKeys() {
	sctx.UnfeasibleSchedulingKeys = make(map[schedulerobjects.SchedulingKey]*JobSchedulingContext)
}

func (sctx *SchedulingContext) AddQueueSchedulingContext(queue string, weight float64, initialAllocatedByPriorityClass schedulerobjects.QuantityByTAndResourceType[string]) error {
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
	qctx := &QueueSchedulingContext{
		SchedulingContext:                 sctx,
		Created:                           time.Now(),
		ExecutorId:                        sctx.ExecutorId,
		Queue:                             queue,
		Weight:                            weight,
		Allocated:                         allocated,
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

// TotalCostAndWeight returns the sum of the costs and weights across all queues.
// Only queues with non-zero cost contribute towards the total weight.
func (sctx *SchedulingContext) TotalCostAndWeight() (float64, float64) {
	var cost float64
	var weight float64
	for _, qctx := range sctx.QueueSchedulingContexts {
		queueCost := qctx.TotalCostForQueue()
		if queueCost != 0 {
			cost += queueCost
			weight += qctx.Weight
		}
	}
	return cost, weight
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
			fmt.Fprintf(w, indent.String("\t\t", qctx.ReportString(verbosity-2)))
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
			fmt.Fprintf(w, indent.String("\t\t", qctx.ReportString(verbosity-2)))
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
	qctx, ok := sctx.QueueSchedulingContexts[jctx.Job.GetQueue()]
	if !ok {
		return false, errors.Errorf("failed adding job %s to scheduling context: no context for queue %s", jctx.JobId, jctx.Job.GetQueue())
	}
	evictedInThisRound, err := qctx.AddJobSchedulingContext(jctx)
	if err != nil {
		return false, err
	}
	if jctx.IsSuccessful() {
		if evictedInThisRound {
			sctx.EvictedResources.SubV1ResourceList(jctx.Req.ResourceRequirements.Requests)
			sctx.EvictedResourcesByPriorityClass.SubV1ResourceList(jctx.Job.GetPriorityClassName(), jctx.Req.ResourceRequirements.Requests)
			sctx.NumEvictedJobs--
		} else {
			sctx.ScheduledResources.AddV1ResourceList(jctx.Req.ResourceRequirements.Requests)
			sctx.ScheduledResourcesByPriorityClass.AddV1ResourceList(jctx.Job.GetPriorityClassName(), jctx.Req.ResourceRequirements.Requests)
			sctx.NumScheduledJobs++
		}
	}
	return evictedInThisRound, nil
}

func (sctx *SchedulingContext) EvictGang(jobs []interfaces.LegacySchedulerJob) (bool, error) {
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

func (sctx *SchedulingContext) EvictJob(job interfaces.LegacySchedulerJob) (bool, error) {
	qctx, ok := sctx.QueueSchedulingContexts[job.GetQueue()]
	if !ok {
		return false, errors.Errorf("failed evicting job %s from scheduling context: no context for queue %s", job.GetId(), job.GetQueue())
	}
	scheduledInThisRound, err := qctx.EvictJob(job)
	if err != nil {
		return false, err
	}
	rl := job.GetResourceRequirements().Requests
	if scheduledInThisRound {
		sctx.ScheduledResources.SubV1ResourceList(rl)
		sctx.ScheduledResourcesByPriorityClass.SubV1ResourceList(job.GetPriorityClassName(), rl)
		sctx.NumScheduledJobs--
	} else {
		sctx.EvictedResources.AddV1ResourceList(rl)
		sctx.EvictedResourcesByPriorityClass.AddV1ResourceList(job.GetPriorityClassName(), rl)
		sctx.NumEvictedJobs++
	}
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

// QueueSchedulingContext captures the decisions made by the scheduler during one invocation
// for a particular queue.
type QueueSchedulingContext struct {
	// The scheduling context to which this QueueSchedulingContext belongs.
	SchedulingContext *SchedulingContext
	// Time at which this context was created.
	Created time.Time
	// Executor this job was attempted to be assigned to.
	ExecutorId string
	// Queue name.
	Queue string
	// Determines the fair share of this queue relative to other queues.
	Weight float64
	// Total resources assigned to the queue across all clusters by priority class priority.
	// Includes jobs scheduled during this invocation of the scheduler.
	Allocated schedulerobjects.ResourceList
	// Total resources assigned to the queue across all clusters by priority class.
	// Includes jobs scheduled during this invocation of the scheduler.
	AllocatedByPriorityClass schedulerobjects.QuantityByTAndResourceType[string]
	// Resources assigned to this queue during this scheduling cycle.
	ScheduledResourcesByPriorityClass schedulerobjects.QuantityByTAndResourceType[string]
	// Resources evicted from this queue during this scheduling cycle.
	EvictedResourcesByPriorityClass schedulerobjects.QuantityByTAndResourceType[string]
	// Job scheduling contexts associated with successful scheduling attempts.
	SuccessfulJobSchedulingContexts map[string]*JobSchedulingContext
	// Job scheduling contexts associated with unsuccessful scheduling attempts.
	UnsuccessfulJobSchedulingContexts map[string]*JobSchedulingContext
	// Jobs evicted in this round.
	EvictedJobsById map[string]bool
}

func GetSchedulingContextFromQueueSchedulingContext(qctx *QueueSchedulingContext) *SchedulingContext {
	if qctx == nil {
		return nil
	}
	return qctx.SchedulingContext
}

func (qctx *QueueSchedulingContext) String() string {
	return qctx.ReportString(0)
}

const maxJobIdsToPrint = 1

func (qctx *QueueSchedulingContext) ReportString(verbosity int32) string {
	var sb strings.Builder
	w := tabwriter.NewWriter(&sb, 1, 1, 1, ' ', 0)
	if verbosity >= 0 {
		fmt.Fprintf(w, "Time:\t%s\n", qctx.Created)
		fmt.Fprintf(w, "Queue:\t%s\n", qctx.Queue)
	}
	fmt.Fprintf(w, "Scheduled resources:\t%s\n", qctx.ScheduledResourcesByPriorityClass.AggregateByResource().CompactString())
	fmt.Fprintf(w, "Scheduled resources (by priority):\t%s\n", qctx.ScheduledResourcesByPriorityClass.String())
	fmt.Fprintf(w, "Preempted resources:\t%s\n", qctx.EvictedResourcesByPriorityClass.AggregateByResource().CompactString())
	fmt.Fprintf(w, "Preempted resources (by priority):\t%s\n", qctx.EvictedResourcesByPriorityClass.String())
	if verbosity >= 0 {
		fmt.Fprintf(w, "Total allocated resources after scheduling:\t%s\n", qctx.Allocated.CompactString())
		fmt.Fprintf(w, "Total allocated resources after scheduling by priority class:\t%s\n", qctx.AllocatedByPriorityClass)
		fmt.Fprintf(w, "Number of jobs scheduled:\t%d\n", len(qctx.SuccessfulJobSchedulingContexts))
		fmt.Fprintf(w, "Number of jobs preempted:\t%d\n", len(qctx.EvictedJobsById))
		fmt.Fprintf(w, "Number of jobs that could not be scheduled:\t%d\n", len(qctx.UnsuccessfulJobSchedulingContexts))
		if len(qctx.SuccessfulJobSchedulingContexts) > 0 {
			jobIdsToPrint := maps.Keys(qctx.SuccessfulJobSchedulingContexts)
			if len(jobIdsToPrint) > maxJobIdsToPrint {
				jobIdsToPrint = jobIdsToPrint[0:maxJobIdsToPrint]
			}
			fmt.Fprintf(w, "Scheduled jobs:\t%v", jobIdsToPrint)
			if len(jobIdsToPrint) != len(qctx.SuccessfulJobSchedulingContexts) {
				fmt.Fprintf(w, " (and %d others not shown)\n", len(qctx.SuccessfulJobSchedulingContexts)-len(jobIdsToPrint))
			} else {
				fmt.Fprint(w, "\n")
			}
		}
		if len(qctx.EvictedJobsById) > 0 {
			jobIdsToPrint := maps.Keys(qctx.EvictedJobsById)
			if len(jobIdsToPrint) > maxJobIdsToPrint {
				jobIdsToPrint = jobIdsToPrint[0:maxJobIdsToPrint]
			}
			fmt.Fprintf(w, "Preempted jobs:\t%v", jobIdsToPrint)
			if len(jobIdsToPrint) != len(qctx.EvictedJobsById) {
				fmt.Fprintf(w, " (and %d others not shown)\n", len(qctx.EvictedJobsById)-len(jobIdsToPrint))
			} else {
				fmt.Fprint(w, "\n")
			}
		}
		if len(qctx.UnsuccessfulJobSchedulingContexts) > 0 {
			fmt.Fprint(w, "Unschedulable jobs:\n")
			jobIdsByReason := armadaslices.MapAndGroupByFuncs(
				maps.Values(qctx.UnsuccessfulJobSchedulingContexts),
				func(jctx *JobSchedulingContext) string {
					return jctx.UnschedulableReason
				},
				func(jctx *JobSchedulingContext) string {
					return jctx.JobId
				},
			)
			reasons := maps.Keys(jobIdsByReason)
			slices.SortFunc(reasons, func(a, b string) bool { return len(jobIdsByReason[a]) < len(jobIdsByReason[b]) })
			for i := len(reasons) - 1; i >= 0; i-- {
				reason := reasons[i]
				jobIds := jobIdsByReason[reason]
				if len(jobIds) <= 0 {
					continue
				}
				fmt.Fprintf(w, "\t%d:\t%s (e.g., %s)\n", len(jobIds), reason, jobIds[0])
			}
		}
	}
	w.Flush()
	return sb.String()
}

func (qctx *QueueSchedulingContext) AddGangSchedulingContext(gctx *GangSchedulingContext) error {
	for _, jctx := range gctx.JobSchedulingContexts {
		if _, err := qctx.AddJobSchedulingContext(jctx); err != nil {
			return err
		}
	}
	return nil
}

// AddJobSchedulingContext adds a job scheduling context.
// Automatically updates scheduled resources.
func (qctx *QueueSchedulingContext) AddJobSchedulingContext(jctx *JobSchedulingContext) (bool, error) {
	if _, ok := qctx.SuccessfulJobSchedulingContexts[jctx.JobId]; ok {
		return false, errors.Errorf("failed adding job %s to queue: job already marked successful", jctx.JobId)
	}
	if _, ok := qctx.UnsuccessfulJobSchedulingContexts[jctx.JobId]; ok {
		return false, errors.Errorf("failed adding job %s to queue: job already marked unsuccessful", jctx.JobId)
	}
	_, evictedInThisRound := qctx.EvictedJobsById[jctx.JobId]
	if jctx.IsSuccessful() {
		if jctx.Req == nil {
			return false, errors.Errorf("failed adding job %s to queue: job requirements are missing", jctx.JobId)
		}

		// Always update ResourcesByPriority.
		// Since ResourcesByPriority is used to order queues by fraction of fair share.
		qctx.Allocated.AddV1ResourceList(jctx.Req.ResourceRequirements.Requests)
		qctx.AllocatedByPriorityClass.AddV1ResourceList(jctx.Job.GetPriorityClassName(), jctx.Req.ResourceRequirements.Requests)

		// Only if the job is not evicted, update ScheduledResourcesByPriority.
		// Since ScheduledResourcesByPriority is used to control per-round scheduling constraints.
		if evictedInThisRound {
			delete(qctx.EvictedJobsById, jctx.JobId)
			qctx.EvictedResourcesByPriorityClass.SubV1ResourceList(jctx.Job.GetPriorityClassName(), jctx.Req.ResourceRequirements.Requests)
		} else {
			qctx.SuccessfulJobSchedulingContexts[jctx.JobId] = jctx
			qctx.ScheduledResourcesByPriorityClass.AddV1ResourceList(jctx.Job.GetPriorityClassName(), jctx.Req.ResourceRequirements.Requests)
		}
	} else {
		qctx.UnsuccessfulJobSchedulingContexts[jctx.JobId] = jctx
	}
	return evictedInThisRound, nil
}

func (qctx *QueueSchedulingContext) EvictJob(job interfaces.LegacySchedulerJob) (bool, error) {
	jobId := job.GetId()
	if _, ok := qctx.UnsuccessfulJobSchedulingContexts[jobId]; ok {
		return false, errors.Errorf("failed evicting job %s from queue: job already marked unsuccessful", jobId)
	}
	if _, ok := qctx.EvictedJobsById[jobId]; ok {
		return false, errors.Errorf("failed evicting job %s from queue: job already marked evicted", jobId)
	}
	rl := job.GetResourceRequirements().Requests
	_, scheduledInThisRound := qctx.SuccessfulJobSchedulingContexts[jobId]
	if scheduledInThisRound {
		qctx.ScheduledResourcesByPriorityClass.SubV1ResourceList(job.GetPriorityClassName(), rl)
		delete(qctx.SuccessfulJobSchedulingContexts, jobId)
	} else {
		qctx.EvictedResourcesByPriorityClass.AddV1ResourceList(job.GetPriorityClassName(), rl)
		qctx.EvictedJobsById[jobId] = true
	}
	qctx.Allocated.SubV1ResourceList(rl)
	qctx.AllocatedByPriorityClass.SubV1ResourceList(job.GetPriorityClassName(), rl)
	return scheduledInThisRound, nil
}

// ClearJobSpecs zeroes out job specs to reduce memory usage.
func (qctx *QueueSchedulingContext) ClearJobSpecs() {
	for _, jctx := range qctx.SuccessfulJobSchedulingContexts {
		jctx.Job = nil
	}
	for _, jctx := range qctx.UnsuccessfulJobSchedulingContexts {
		jctx.Job = nil
	}
}

// TotalCostForQueue returns the total cost of this queue.
func (qctx *QueueSchedulingContext) TotalCostForQueue() float64 {
	return qctx.TotalCostForQueueWithAllocation(qctx.Allocated)
}

// TotalCostForQueueWithAllocation returns the total cost of this queue if its total allocation is given by allocated.
func (qctx *QueueSchedulingContext) TotalCostForQueueWithAllocation(allocated schedulerobjects.ResourceList) float64 {
	switch qctx.SchedulingContext.FairnessModel {
	case configuration.AssetFairness:
		return qctx.assetFairnessCostWithAllocation(allocated)
	case configuration.DominantResourceFairness:
		return qctx.dominantResourceFairnessCostWithAllocation(allocated)
	default:
		panic(fmt.Sprintf("unknown fairness type: %s", qctx.SchedulingContext.FairnessModel))
	}
}

func (qctx *QueueSchedulingContext) assetFairnessCostWithAllocation(allocated schedulerobjects.ResourceList) float64 {
	if len(qctx.SchedulingContext.ResourceScarcity) == 0 {
		panic("ResourceScarcity is not set")
	}
	return float64(allocated.AsWeightedMillis(qctx.SchedulingContext.ResourceScarcity)) / qctx.Weight
}

func (qctx *QueueSchedulingContext) dominantResourceFairnessCostWithAllocation(allocated schedulerobjects.ResourceList) float64 {
	if len(qctx.SchedulingContext.DominantResourceFairnessResourcesToConsider) == 0 {
		panic("DominantResourceFairnessResourcesToConsider is not set")
	}
	var cost float64
	for _, t := range qctx.SchedulingContext.DominantResourceFairnessResourcesToConsider {
		capacity := qctx.SchedulingContext.TotalResources.Get(t)
		if capacity.Equal(resource.Quantity{}) {
			// Ignore any resources with zero capacity.
			continue
		}
		q := allocated.Get(t)
		tcost := float64(q.MilliValue()) / float64(capacity.MilliValue())
		if tcost > cost {
			cost = tcost
		}
	}
	return cost / qctx.Weight
}

type GangSchedulingContext struct {
	Created               time.Time
	Queue                 string
	PriorityClassName     string
	JobSchedulingContexts []*JobSchedulingContext
	TotalResourceRequests schedulerobjects.ResourceList
	AllJobsEvicted        bool
}

func NewGangSchedulingContext(jctxs []*JobSchedulingContext) *GangSchedulingContext {
	// We assume that all jobs in a gang are in the same queue and have the same priority class
	// (which we enforce at job submission).
	queue := ""
	priorityClassName := ""
	if len(jctxs) > 0 {
		queue = jctxs[0].Job.GetQueue()
		priorityClassName = jctxs[0].Job.GetPriorityClassName()
	}
	allJobsEvicted := true
	totalResourceRequests := schedulerobjects.NewResourceList(4)
	for _, jctx := range jctxs {
		allJobsEvicted = allJobsEvicted && isEvictedJob(jctx.Job)
		totalResourceRequests.AddV1ResourceList(jctx.Req.ResourceRequirements.Requests)
	}
	return &GangSchedulingContext{
		Created:               time.Now(),
		Queue:                 queue,
		PriorityClassName:     priorityClassName,
		JobSchedulingContexts: jctxs,
		TotalResourceRequests: totalResourceRequests,
		AllJobsEvicted:        allJobsEvicted,
	}
}

func (gctx GangSchedulingContext) PodRequirements() []*schedulerobjects.PodRequirements {
	rv := make([]*schedulerobjects.PodRequirements, len(gctx.JobSchedulingContexts))
	for i, jctx := range gctx.JobSchedulingContexts {
		rv[i] = jctx.Req
	}
	return rv
}

func isEvictedJob(job interfaces.LegacySchedulerJob) bool {
	return job.GetAnnotations()[schedulerconfig.IsEvictedAnnotation] == "true"
}

// JobSchedulingContext is created by the scheduler and contains information
// about the decision made by the scheduler for a particular job.
type JobSchedulingContext struct {
	// Time at which this context was created.
	Created time.Time
	// Executor this job was attempted to be assigned to.
	ExecutorId string
	// Total number of nodes in the cluster when trying to schedule.
	NumNodes int
	// Id of the job this pod corresponds to.
	JobId string
	// Job spec.
	Job interfaces.LegacySchedulerJob
	// Scheduling requirements of this job.
	// We currently require that each job contains exactly one pod spec.
	Req *schedulerobjects.PodRequirements
	// Reason for why the job could not be scheduled.
	// Empty if the job was scheduled successfully.
	UnschedulableReason string
	// Pod scheduling contexts for the individual pods that make up the job.
	PodSchedulingContext *PodSchedulingContext
}

func (jctx *JobSchedulingContext) String() string {
	var sb strings.Builder
	w := tabwriter.NewWriter(&sb, 1, 1, 1, ' ', 0)
	fmt.Fprintf(w, "Time:\t%s\n", jctx.Created)
	fmt.Fprintf(w, "Job ID:\t%s\n", jctx.JobId)
	fmt.Fprintf(w, "Number of nodes in cluster:\t%d\n", jctx.NumNodes)
	if jctx.UnschedulableReason != "" {
		fmt.Fprintf(w, "UnschedulableReason:\t%s\n", jctx.UnschedulableReason)
	} else {
		fmt.Fprint(w, "UnschedulableReason:\tnone\n")
	}
	if jctx.PodSchedulingContext != nil {
		fmt.Fprint(w, jctx.PodSchedulingContext.String())
	}
	w.Flush()
	return sb.String()
}

func (jctx *JobSchedulingContext) IsSuccessful() bool {
	return jctx.UnschedulableReason == ""
}

// PodSchedulingContext is returned by SelectAndBindNodeToPod and
// contains detailed information on the scheduling decision made for this pod.
type PodSchedulingContext struct {
	// Time at which this context was created.
	Created time.Time
	// Node the pod was assigned to.
	// If nil, the pod could not be assigned to any node.
	Node *schedulerobjects.Node
	// Score indicates how well the pod fits on the selected node.
	Score int
	// Node types on which this pod could be scheduled.
	MatchingNodeTypes []*schedulerobjects.NodeType
	// Total number of nodes in the cluster when trying to schedule.
	NumNodes int
	// Number of nodes excluded by reason.
	NumExcludedNodesByReason map[string]int
}

func (pctx *PodSchedulingContext) String() string {
	var sb strings.Builder
	w := tabwriter.NewWriter(&sb, 1, 1, 1, ' ', 0)
	if pctx.Node != nil {
		fmt.Fprintf(w, "Node:\t%s\n", pctx.Node.Id)
	} else {
		fmt.Fprint(w, "Node:\tnone\n")
	}
	if len(pctx.NumExcludedNodesByReason) == 0 {
		fmt.Fprint(w, "Excluded nodes:\tnone\n")
	} else {
		fmt.Fprint(w, "Excluded nodes:\n")
		for reason, count := range pctx.NumExcludedNodesByReason {
			fmt.Fprintf(w, "\t%d:\t%s\n", count, reason)
		}
	}
	w.Flush()
	return sb.String()
}
