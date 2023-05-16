package context

import (
	"fmt"
	"strings"
	"text/tabwriter"
	"time"

	"golang.org/x/exp/maps"

	"github.com/armadaproject/armada/internal/armada/configuration"
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
	// Weights used when computing total resource usage.
	ResourceScarcity map[string]float64
	// Per-queue scheduling contexts.
	QueueSchedulingContexts map[string]*QueueSchedulingContext
	// Total resources across all clusters available at the start of the scheduling cycle.
	TotalResources schedulerobjects.ResourceList
	// Resources assigned across all queues during this scheduling cycle.
	ScheduledResourcesByPriority schedulerobjects.QuantityByPriorityAndResourceType
	// Resources evicted across all queues during this scheduling cycle.
	EvictedResourcesByPriority schedulerobjects.QuantityByPriorityAndResourceType
	// Total number of successfully scheduled jobs.
	NumScheduledJobs int
	// Total number of successfully scheduled gangs.
	NumScheduledGangs int
	// Reason for why the scheduling round finished.
	TerminationReason string
}

func NewSchedulingContext(
	executorId string,
	pool string,
	priorityClasses map[string]configuration.PriorityClass,
	defaultPriorityClass string,
	resourceScarcity map[string]float64,
	priorityFactorByQueue map[string]float64,
	totalResources schedulerobjects.ResourceList,
	initialAllocatedByQueueAndPriority map[string]schedulerobjects.QuantityByPriorityAndResourceType,
) *SchedulingContext {
	queueSchedulingContexts := make(map[string]*QueueSchedulingContext)
	for queue := range priorityFactorByQueue {
		queueSchedulingContexts[queue] = NewQueueSchedulingContext(
			queue,
			executorId,
			priorityFactorByQueue[queue],
			priorityClasses,
			initialAllocatedByQueueAndPriority[queue],
		)
	}
	return &SchedulingContext{
		Started:                      time.Now(),
		ExecutorId:                   executorId,
		Pool:                         pool,
		PriorityClasses:              priorityClasses,
		DefaultPriorityClass:         defaultPriorityClass,
		ResourceScarcity:             resourceScarcity,
		QueueSchedulingContexts:      queueSchedulingContexts,
		TotalResources:               totalResources.DeepCopy(),
		ScheduledResourcesByPriority: make(schedulerobjects.QuantityByPriorityAndResourceType),
		EvictedResourcesByPriority:   make(schedulerobjects.QuantityByPriorityAndResourceType),
	}
}

func (sctx *SchedulingContext) String() string {
	var sb strings.Builder
	w := tabwriter.NewWriter(&sb, 1, 1, 1, ' ', 0)
	fmt.Fprintf(w, "Started:\t%s\n", sctx.Started)
	fmt.Fprintf(w, "Finished:\t%s\n", sctx.Finished)
	fmt.Fprintf(w, "Duration:\t%s\n", sctx.Finished.Sub(sctx.Started))
	fmt.Fprintf(w, "Total capacity:\t%s\n", sctx.TotalResources.CompactString())
	fmt.Fprintf(w, "Jobs scheduled:\t%d\n", sctx.NumScheduledJobs)
	fmt.Fprintf(w, "Total resources scheduled:\t%s\n", sctx.ScheduledResourcesByPriority.AggregateByResource().CompactString())
	fmt.Fprintf(w, "Total resources scheduled (by priority):\t%s\n", sctx.ScheduledResourcesByPriority.String())
	fmt.Fprintf(
		w, "Scheduled queues:\t%v\n",
		maps.Keys(
			armadamaps.Filter(
				sctx.QueueSchedulingContexts,
				func(_ string, qctx *QueueSchedulingContext) bool {
					return len(qctx.SuccessfulJobSchedulingContexts) > 0
				},
			),
		),
	)
	fmt.Fprintf(w, "Termination reason:\t%s\n", sctx.TerminationReason)
	w.Flush()
	return sb.String()
}

func (sctx *SchedulingContext) AddGangSchedulingContext(gctx *GangSchedulingContext) bool {
	allJobsEvictedInThisRound := true
	allJobsSuccessful := true
	for _, jctx := range gctx.JobSchedulingContexts {
		evictedInThisRound := sctx.AddJobSchedulingContext(jctx)
		allJobsEvictedInThisRound = allJobsEvictedInThisRound && evictedInThisRound
		allJobsSuccessful = allJobsSuccessful && jctx.IsSuccessful()
	}
	if !allJobsEvictedInThisRound && allJobsSuccessful {
		sctx.NumScheduledGangs++
	}
	return allJobsEvictedInThisRound
}

// AddJobSchedulingContext adds a job scheduling context.
// Automatically updates scheduled resources.
func (sctx *SchedulingContext) AddJobSchedulingContext(jctx *JobSchedulingContext) bool {
	qctx, ok := sctx.QueueSchedulingContexts[jctx.Job.GetQueue()]
	if !ok {
		panic(fmt.Sprintf("failed adding job %s to scheduling context: no context for queue %s", jctx.JobId, jctx.Job.GetQueue()))
	}
	evictedInThisRound := qctx.AddJobSchedulingContext(jctx)
	if jctx.IsSuccessful() {
		rl := schedulerobjects.ResourceListFromV1ResourceList(jctx.Req.ResourceRequirements.Requests)
		if evictedInThisRound {
			sctx.EvictedResourcesByPriority.SubResourceList(jctx.Req.Priority, rl)
		} else {
			sctx.ScheduledResourcesByPriority.AddResourceList(jctx.Req.Priority, rl)
			sctx.NumScheduledJobs++
		}
	}
	return evictedInThisRound
}

func (sctx *SchedulingContext) EvictGang(jobs []interfaces.LegacySchedulerJob) bool {
	allJobsScheduledInThisRound := true
	for _, job := range jobs {
		allJobsScheduledInThisRound = allJobsScheduledInThisRound && sctx.EvictJob(job)
	}
	if allJobsScheduledInThisRound {
		sctx.NumScheduledGangs--
	}
	return allJobsScheduledInThisRound
}

func (sctx *SchedulingContext) EvictJob(job interfaces.LegacySchedulerJob) bool {
	qctx, ok := sctx.QueueSchedulingContexts[job.GetQueue()]
	if !ok {
		panic(fmt.Sprintf("failed evicting job %s from scheduling context: no context for queue %s", job.GetId(), job.GetQueue()))
	}
	scheduledInThisRound := qctx.EvictJob(job)
	priority, rl := priorityAndRequestsFromLegacySchedulerJob(job, sctx.PriorityClasses)
	if scheduledInThisRound {
		sctx.ScheduledResourcesByPriority.SubResourceList(priority, rl)
		sctx.NumScheduledJobs--
	} else {
		sctx.EvictedResourcesByPriority.AddResourceList(priority, rl)
	}
	return scheduledInThisRound
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
func (sctx *SchedulingContext) AllocatedByQueueAndPriority() map[string]schedulerobjects.QuantityByPriorityAndResourceType {
	rv := make(
		map[string]schedulerobjects.QuantityByPriorityAndResourceType,
		len(sctx.QueueSchedulingContexts),
	)
	for queue, qctx := range sctx.QueueSchedulingContexts {
		if len(qctx.AllocatedByPriority) > 0 {
			rv[queue] = qctx.AllocatedByPriority.DeepCopy()
		}
	}
	return rv
}

// QueueSchedulingContext captures the decisions made by the scheduler during one invocation
// for a particular queue.
type QueueSchedulingContext struct {
	// Time at which this context was created.
	Created time.Time
	// Executor this job was attempted to be assigned to.
	ExecutorId string
	// Queue name.
	Queue string
	// These factors influence the fraction of resources assigned to each queue.
	PriorityFactor float64
	// Allowed priority classes.
	PriorityClasses map[string]configuration.PriorityClass
	// Total resources assigned to the queue across all clusters.
	// Including jobs scheduled during this invocation of the scheduler.
	AllocatedByPriority schedulerobjects.QuantityByPriorityAndResourceType
	// Resources assigned to this queue during this scheduling cycle.
	ScheduledResourcesByPriority schedulerobjects.QuantityByPriorityAndResourceType
	EvictedResourcesByPriority   schedulerobjects.QuantityByPriorityAndResourceType
	// Job scheduling contexts associated with successful scheduling attempts.
	SuccessfulJobSchedulingContexts map[string]*JobSchedulingContext
	// Job scheduling contexts associated with unsuccessful scheduling attempts.
	UnsuccessfulJobSchedulingContexts map[string]*JobSchedulingContext
	// Jobs evicted in this round.
	EvictedJobsById map[string]interfaces.LegacySchedulerJob
}

func NewQueueSchedulingContext(
	queue,
	executorId string,
	priorityFactor float64,
	priorityClasses map[string]configuration.PriorityClass,
	initialAllocatedByPriority schedulerobjects.QuantityByPriorityAndResourceType,
) *QueueSchedulingContext {
	if initialAllocatedByPriority == nil {
		initialAllocatedByPriority = make(schedulerobjects.QuantityByPriorityAndResourceType)
	} else {
		initialAllocatedByPriority = initialAllocatedByPriority.DeepCopy()
	}
	return &QueueSchedulingContext{
		Created:                           time.Now(),
		ExecutorId:                        executorId,
		Queue:                             queue,
		PriorityFactor:                    priorityFactor,
		PriorityClasses:                   priorityClasses,
		AllocatedByPriority:               initialAllocatedByPriority,
		ScheduledResourcesByPriority:      make(schedulerobjects.QuantityByPriorityAndResourceType),
		EvictedResourcesByPriority:        make(schedulerobjects.QuantityByPriorityAndResourceType),
		SuccessfulJobSchedulingContexts:   make(map[string]*JobSchedulingContext),
		UnsuccessfulJobSchedulingContexts: make(map[string]*JobSchedulingContext),
		EvictedJobsById:                   make(map[string]interfaces.LegacySchedulerJob),
	}
}

const maxPrintedJobIdsByReason = 1

// TODO: Update with preemptions.
func (qctx *QueueSchedulingContext) String() string {
	var sb strings.Builder
	w := tabwriter.NewWriter(&sb, 1, 1, 1, ' ', 0)
	fmt.Fprintf(w, "Time:\t%s\n", qctx.Created)
	fmt.Fprintf(w, "Queue:\t%s\n", qctx.Queue)
	fmt.Fprintf(w, "Total allocated resources after scheduling:\t%s\n", qctx.AllocatedByPriority.AggregateByResource().CompactString())
	fmt.Fprintf(w, "Total allocated resources after scheduling (by priority):\t%s\n", qctx.AllocatedByPriority.String())
	fmt.Fprintf(w, "Scheduled resources:\t%s\n", qctx.ScheduledResourcesByPriority.AggregateByResource().CompactString())
	fmt.Fprintf(w, "Scheduled resources (by priority):\t%s\n", qctx.ScheduledResourcesByPriority.String())
	fmt.Fprintf(w, "Number of jobs scheduled:\t%d\n", len(qctx.SuccessfulJobSchedulingContexts))
	fmt.Fprintf(w, "Number of jobs that could not be scheduled:\t%d\n", len(qctx.UnsuccessfulJobSchedulingContexts))
	if len(qctx.SuccessfulJobSchedulingContexts) > 0 {
		jobIdsToPrint := maps.Keys(qctx.SuccessfulJobSchedulingContexts)
		if len(jobIdsToPrint) > maxPrintedJobIdsByReason {
			jobIdsToPrint = jobIdsToPrint[0:maxPrintedJobIdsByReason]
		}
		fmt.Fprintf(w, "Scheduled jobs:\t%v", jobIdsToPrint)
		if len(jobIdsToPrint) != len(qctx.SuccessfulJobSchedulingContexts) {
			fmt.Fprintf(w, " (and %d others not shown)\n", len(qctx.SuccessfulJobSchedulingContexts)-len(jobIdsToPrint))
		} else {
			fmt.Fprint(w, "\n")
		}
	}
	if len(qctx.UnsuccessfulJobSchedulingContexts) > 0 {
		fmt.Fprint(w, "Unschedulable jobs:\n")
		for reason, jobIds := range armadaslices.MapAndGroupByFuncs(
			maps.Values(qctx.UnsuccessfulJobSchedulingContexts),
			func(jctx *JobSchedulingContext) string {
				return jctx.UnschedulableReason
			},
			func(jctx *JobSchedulingContext) string {
				return jctx.JobId
			},
		) {
			jobIdsToPrint := jobIds
			if len(jobIdsToPrint) > maxPrintedJobIdsByReason {
				jobIdsToPrint = jobIds[0:maxPrintedJobIdsByReason]
			}
			fmt.Fprintf(w, "\t%d:\t%s jobs\t%v", len(qctx.UnsuccessfulJobSchedulingContexts), reason, jobIdsToPrint)
			if len(jobIdsToPrint) != len(jobIds) {
				fmt.Fprintf(w, " (and %d others not shown)\n", len(jobIds)-len(jobIdsToPrint))
			} else {
				fmt.Fprint(w, "\n")
			}
		}
	}
	w.Flush()
	return sb.String()
}

func (qctx *QueueSchedulingContext) AddGangSchedulingContext(gctx *GangSchedulingContext) {
	for _, jctx := range gctx.JobSchedulingContexts {
		qctx.AddJobSchedulingContext(jctx)
	}
}

// AddJobSchedulingContext adds a job scheduling context.
// Automatically updates scheduled resources.
func (qctx *QueueSchedulingContext) AddJobSchedulingContext(jctx *JobSchedulingContext) bool {
	if _, ok := qctx.SuccessfulJobSchedulingContexts[jctx.JobId]; ok {
		panic(fmt.Sprintf("failed adding job %s to queue: job already marked successful", jctx.JobId))
	}
	if _, ok := qctx.UnsuccessfulJobSchedulingContexts[jctx.JobId]; ok {
		panic(fmt.Sprintf("failed adding job %s to queue: job already marked unsuccessful", jctx.JobId))
	}
	rl := schedulerobjects.ResourceListFromV1ResourceList(jctx.Req.ResourceRequirements.Requests)
	_, evictedInThisRound := qctx.EvictedJobsById[jctx.JobId]
	if jctx.IsSuccessful() {
		// Always update ResourcesByPriority.
		// Since ResourcesByPriority is used to order queues by fraction of fair share.
		qctx.AllocatedByPriority.AddResourceList(jctx.Req.Priority, rl)

		// Only if the job is not evicted, update ScheduledResourcesByPriority.
		// Since ScheduledResourcesByPriority is used to control per-round scheduling constraints.
		if evictedInThisRound {
			delete(qctx.EvictedJobsById, jctx.JobId)
			qctx.EvictedResourcesByPriority.SubResourceList(jctx.Req.Priority, rl)
		} else {
			qctx.SuccessfulJobSchedulingContexts[jctx.JobId] = jctx
			qctx.ScheduledResourcesByPriority.AddResourceList(jctx.Req.Priority, rl)
		}
	} else {
		qctx.UnsuccessfulJobSchedulingContexts[jctx.JobId] = jctx
	}
	return evictedInThisRound
}

func (qctx *QueueSchedulingContext) EvictJob(job interfaces.LegacySchedulerJob) bool {
	jobId := job.GetId()
	priority, rl := priorityAndRequestsFromLegacySchedulerJob(job, qctx.PriorityClasses)
	if _, ok := qctx.UnsuccessfulJobSchedulingContexts[jobId]; ok {
		panic(fmt.Sprintf("failed evicting job %s from queue: job already marked unsuccessful", jobId))
	}
	if _, ok := qctx.EvictedJobsById[jobId]; ok {
		panic(fmt.Sprintf("failed evicting job %s from queue: job already marked evicted", jobId))
	}
	_, scheduledInThisRound := qctx.SuccessfulJobSchedulingContexts[jobId]
	if scheduledInThisRound {
		qctx.ScheduledResourcesByPriority.SubResourceList(priority, rl)
		delete(qctx.SuccessfulJobSchedulingContexts, jobId)
	} else {
		qctx.EvictedResourcesByPriority.AddResourceList(priority, rl)
		qctx.EvictedJobsById[jobId] = job
	}
	qctx.AllocatedByPriority.SubResourceList(priority, rl)
	return scheduledInThisRound
}

func priorityAndRequestsFromLegacySchedulerJob(job interfaces.LegacySchedulerJob, priorityClasses map[string]configuration.PriorityClass) (int32, schedulerobjects.ResourceList) {
	req := job.GetRequirements(priorityClasses)
	for _, r := range req.ObjectRequirements {
		podReqs := r.GetPodRequirements()
		if podReqs == nil {
			continue
		}
		return podReqs.Priority, schedulerobjects.ResourceListFromV1ResourceList(podReqs.ResourceRequirements.Requests)
	}
	return 0, schedulerobjects.ResourceList{}
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
		priorityClassName = jctxs[0].Job.GetRequirements(nil).PriorityClassName
	}
	totalResourceRequests := schedulerobjects.ResourceList{}
	for _, jctx := range jctxs {
		job := jctx.Job
		jobReqs := job.GetRequirements(nil)
		if jobReqs == nil {
			continue
		}
		for _, reqs := range jobReqs.GetObjectRequirements() {
			if podReq := reqs.GetPodRequirements(); podReq != nil {
				totalResourceRequests.Add(
					schedulerobjects.ResourceListFromV1ResourceList(
						podReq.ResourceRequirements.Requests,
					),
				)
			}
		}
	}

	allJobsEvicted := true
	for _, jctx := range jctxs {
		allJobsEvicted = allJobsEvicted && isEvictedJob(jctx.Job)
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
	fmt.Fprintf(w, "Job id:\t%s\n", jctx.JobId)
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
