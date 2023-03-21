package scheduler

import (
	"fmt"
	"strings"
	"sync"
	"text/tabwriter"
	"time"

	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

// SchedulingContext captures the decisions made by the scheduler during one invocation.
type SchedulingContext struct {
	// Time at which the scheduling cycle started.
	Started time.Time
	// Time at which the scheduling cycle finished.
	Finished time.Time
	// ExecutorId for which the scheduler was invoked.
	ExecutorId string
	// Per-queue scheduling contexts.
	QueueSchedulingContexts map[string]*QueueSchedulingContext
	// Total resources across all clusters available at the start of the scheduling cycle.
	TotalResources schedulerobjects.ResourceList
	// Resources assigned across all queues during this scheduling cycle.
	ScheduledResourcesByPriority schedulerobjects.QuantityByPriorityAndResourceType
	// Total number of jobs successfully scheduled in this round.
	NumScheduledJobs int
	// Reason for why the scheduling round finished.
	TerminationReason string
	// Protects everything in this struct.
	mu sync.Mutex
}

func NewSchedulingContext(
	executorId string,
	totalResources schedulerobjects.ResourceList,
	priorityFactorByQueue map[string]float64,
	initialResourcesByQueueAndPriority map[string]schedulerobjects.QuantityByPriorityAndResourceType,
) *SchedulingContext {
	queueSchedulingContexts := make(map[string]*QueueSchedulingContext)
	for queue := range priorityFactorByQueue {
		queueSchedulingContexts[queue] = NewQueueSchedulingContext(
			executorId,
			priorityFactorByQueue[queue],
			initialResourcesByQueueAndPriority[queue],
		)
	}
	return &SchedulingContext{
		Started:                      time.Now(),
		QueueSchedulingContexts:      queueSchedulingContexts,
		TotalResources:               totalResources.DeepCopy(),
		ScheduledResourcesByPriority: make(schedulerobjects.QuantityByPriorityAndResourceType),
	}
}

func (sctx *SchedulingContext) String() string {
	var sb strings.Builder
	w := tabwriter.NewWriter(&sb, 1, 1, 1, ' ', 0)
	fmt.Fprintf(w, "Started:\t%s\n", sctx.Started)
	fmt.Fprintf(w, "Finished:\t%s\n", sctx.Finished)
	fmt.Fprintf(w, "Duration:\t%s\n", sctx.Finished.Sub(sctx.Started))
	fmt.Fprintf(w, "Total capacity:\t%s\n", sctx.TotalResources.CompactString())
	totalJobsScheduled := 0
	totalResourcesScheduled := make(schedulerobjects.QuantityByPriorityAndResourceType)
	fmt.Fprintf(w, "Total jobs scheduled:\t%d\n", totalJobsScheduled)
	fmt.Fprintf(w, "Total resources scheduled:\t%s\n", totalResourcesScheduled)
	fmt.Fprintf(w, "Termination reason:\t%s\n", sctx.TerminationReason)
	w.Flush()
	return sb.String()
}

// AddJobSchedulingContext adds a job scheduling context.
// Automatically updates scheduled resources
func (sctx *SchedulingContext) AddJobSchedulingContext(jctx *JobSchedulingContext, isEvictedJob bool) {
	sctx.mu.Lock()
	defer sctx.mu.Unlock()
	if !isEvictedJob && jctx.UnschedulableReason == "" {
		sctx.ScheduledResourcesByPriority.AddResourceList(
			jctx.Req.Priority,
			schedulerobjects.ResourceListFromV1ResourceList(jctx.Req.ResourceRequirements.Requests),
		)
		sctx.NumScheduledJobs++
	}
	if qctx := sctx.QueueSchedulingContexts[jctx.Job.GetQueue()]; qctx != nil {
		qctx.AddJobSchedulingContext(jctx, isEvictedJob)
	}
}

// ClearJobSpecs zeroes out job specs to reduce memory usage.
func (sctx *SchedulingContext) ClearJobSpecs() {
	sctx.mu.Lock()
	defer sctx.mu.Unlock()
	for _, qctx := range sctx.QueueSchedulingContexts {
		qctx.ClearJobSpecs()
	}
}

func (sctx *SchedulingContext) SuccessfulJobSchedulingContexts() []*JobSchedulingContext {
	sctx.mu.Lock()
	defer sctx.mu.Unlock()
	jctxs := make([]*JobSchedulingContext, 0)
	for _, qctx := range sctx.QueueSchedulingContexts {
		for _, jctx := range qctx.SuccessfulJobSchedulingContexts {
			jctxs = append(jctxs, jctx)
		}
	}
	return jctxs
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
	// Total resources assigned to the queue across all clusters.
	// Including jobs scheduled during this invocation of the scheduler.
	ResourcesByPriority schedulerobjects.QuantityByPriorityAndResourceType
	// Resources assigned to this queue during this scheduling cycle.
	ScheduledResourcesByPriority schedulerobjects.QuantityByPriorityAndResourceType
	// Job scheduling contexts associated with successful scheduling attempts.
	SuccessfulJobSchedulingContexts map[string]*JobSchedulingContext
	// Job scheduling contexts associated with unsuccessful scheduling attempts.
	UnsuccessfulJobSchedulingContexts map[string]*JobSchedulingContext
	// Protects the above maps.
	mu sync.Mutex
}

func NewQueueSchedulingContext(executorId string, priorityFactor float64, initialResourcesByPriority schedulerobjects.QuantityByPriorityAndResourceType) *QueueSchedulingContext {
	if initialResourcesByPriority == nil {
		initialResourcesByPriority = make(schedulerobjects.QuantityByPriorityAndResourceType)
	} else {
		initialResourcesByPriority = initialResourcesByPriority.DeepCopy()
	}
	return &QueueSchedulingContext{
		Created:                           time.Now(),
		ExecutorId:                        executorId,
		PriorityFactor:                    priorityFactor,
		ResourcesByPriority:               initialResourcesByPriority,
		ScheduledResourcesByPriority:      make(schedulerobjects.QuantityByPriorityAndResourceType),
		SuccessfulJobSchedulingContexts:   make(map[string]*JobSchedulingContext),
		UnsuccessfulJobSchedulingContexts: make(map[string]*JobSchedulingContext),
	}
}

func (qctx *QueueSchedulingContext) String() string {
	var sb strings.Builder
	w := tabwriter.NewWriter(&sb, 1, 1, 1, ' ', 0)
	fmt.Fprintf(w, "Time:\t%s\n", qctx.Created)
	fmt.Fprintf(w, "Queue:\t%s\n", qctx.Queue)
	fmt.Fprintf(w, "Priority factor:\t%f\n", qctx.PriorityFactor)
	fmt.Fprintf(w, "Allocated resources after scheduling:\t%s\n", qctx.ResourcesByPriority.String())
	fmt.Fprintf(w, "Scheduled resources:\t%s\n", qctx.ScheduledResourcesByPriority.String())
	fmt.Fprintf(w, "Successfully scheduled jobs:\t%d\n", len(qctx.SuccessfulJobSchedulingContexts))
	fmt.Fprintf(w, "Unsuccessful scheduling attempts:\t%d\n", len(qctx.UnsuccessfulJobSchedulingContexts))
	w.Flush()
	return sb.String()
}

// AddJobSchedulingContext adds a job scheduling context.
// Automatically updates scheduled resources.
func (qctx *QueueSchedulingContext) AddJobSchedulingContext(jctx *JobSchedulingContext, isEvictedJob bool) {
	qctx.mu.Lock()
	defer qctx.mu.Unlock()
	if jctx.UnschedulableReason == "" {
		// Always update ResourcesByPriority.
		// Since ResourcesByPriority is used to order queues by fraction of fair share.
		rl := qctx.ResourcesByPriority[jctx.Req.Priority]
		rl.Add(schedulerobjects.ResourceListFromV1ResourceList(jctx.Req.ResourceRequirements.Requests))
		qctx.ResourcesByPriority[jctx.Req.Priority] = rl

		// Only if the job is not evicted, update ScheduledResourcesByPriority.
		// Since ScheduledResourcesByPriority is used to control per-round scheduling constraints.
		if !isEvictedJob {
			qctx.SuccessfulJobSchedulingContexts[jctx.JobId] = jctx
			rl := qctx.ScheduledResourcesByPriority[jctx.Req.Priority]
			rl.Add(schedulerobjects.ResourceListFromV1ResourceList(jctx.Req.ResourceRequirements.Requests))
			qctx.ScheduledResourcesByPriority[jctx.Req.Priority] = rl
		}
	} else {
		qctx.UnsuccessfulJobSchedulingContexts[jctx.JobId] = jctx
	}
}

// ClearJobSpecs zeroes out job specs to reduce memory usage.
func (qctx *QueueSchedulingContext) ClearJobSpecs() {
	qctx.mu.Lock()
	defer qctx.mu.Unlock()
	for _, jctx := range qctx.SuccessfulJobSchedulingContexts {
		jctx.Job = nil
	}
	for _, jctx := range qctx.UnsuccessfulJobSchedulingContexts {
		jctx.Job = nil
	}
}

// JobSchedulingContext is created by the scheduler and contains information
// about the decision made by the scheduler for a particular job.
type JobSchedulingContext struct {
	// Time at which this context was created.
	Created time.Time
	// Executor this job was attempted to be assigned to.
	ExecutorId string
	// Id of the job this pod corresponds to.
	JobId string
	// Job spec.
	Job LegacySchedulerJob
	// Scheduling requirements of this job.
	// We currently require that each job contains exactly one pod spec.
	Req *schedulerobjects.PodRequirements
	// Reason for why the job could not be scheduled.
	// Empty if the job was scheduled successfully.
	UnschedulableReason string
	// Pod scheduling contexts for the individual pods that make up the job.
	PodSchedulingContexts []*PodSchedulingContext
}

func (jctx *JobSchedulingContext) String() string {
	var sb strings.Builder
	w := tabwriter.NewWriter(&sb, 1, 1, 1, ' ', 0)
	fmt.Fprintf(w, "Time:\t%s\n", jctx.Created)
	fmt.Fprintf(w, "Job id:\t%s\n", jctx.JobId)
	if jctx.UnschedulableReason != "" {
		fmt.Fprintf(w, "UnschedulableReason:\t%s\n", jctx.UnschedulableReason)
	} else {
		fmt.Fprint(w, "UnschedulableReason:\tnone\n")
	}
	for _, pctx := range jctx.PodSchedulingContexts {
		fmt.Fprint(w, pctx.String())
	}
	w.Flush()
	return sb.String()
}

// PodSchedulingContext is returned by SelectAndBindNodeToPod and
// contains detailed information on the scheduling decision made for this pod.
type PodSchedulingContext struct {
	// Time at which this context was created.
	Created time.Time
	// Pod scheduling requirements.
	Req *schedulerobjects.PodRequirements
	// Resource type determined by the scheduler to be the hardest to satisfy
	// the scheduling requirements for.
	DominantResourceType string
	// Node the pod was assigned to.
	// If nil, the pod could not be assigned to any Node.
	Node *schedulerobjects.Node
	// Score indicates how well the pod fits on the selected Node.
	Score int
	// Total number of nodes.
	NumNodes int
	// Node types on which this pod could be scheduled.
	MatchingNodeTypes []*schedulerobjects.NodeType
	// Number of Node types excluded by reason.
	NumExcludedNodeTypesByReason map[string]int
	// Number of nodes excluded by reason.
	NumExcludedNodesByReason map[string]int
	// Set if an error occurred while attempting to schedule this pod.
	Err error
}

func (pctx *PodSchedulingContext) String() string {
	var sb strings.Builder
	w := tabwriter.NewWriter(&sb, 1, 1, 1, ' ', 0)
	if pctx.Node != nil {
		fmt.Fprintf(w, "Node:\t%s\n", pctx.Node.Id)
	} else {
		fmt.Fprint(w, "Node:\tnone\n")
	}
	fmt.Fprintf(w, "Number of matched Node types:\t%d\n", len(pctx.MatchingNodeTypes))
	if len(pctx.NumExcludedNodeTypesByReason) == 0 {
		fmt.Fprint(w, "Excluded Node types:\tnone\n")
	} else {
		fmt.Fprint(w, "Excluded Node types:\n")
		for reason, count := range pctx.NumExcludedNodeTypesByReason {
			fmt.Fprintf(w, "\t%d:\t%s\n", count, reason)
		}
	}
	fmt.Fprintf(w, "Number of nodes in cluster:\t%d\n", pctx.NumNodes)
	requestForDominantResourceType := pctx.Req.ResourceRequirements.Requests[v1.ResourceName(pctx.DominantResourceType)]
	fmt.Fprintf(
		w,
		"Node filter:\tconsidering nodes with %s %s allocatable at priority %d\n",
		requestForDominantResourceType.String(),
		pctx.DominantResourceType,
		pctx.Req.Priority,
	)
	if len(pctx.NumExcludedNodesByReason) == 0 && requestForDominantResourceType.IsZero() {
		fmt.Fprint(w, "Excluded nodes:\tnone\n")
	} else {
		fmt.Fprint(w, "Excluded nodes:\n")
		for reason, count := range pctx.NumExcludedNodesByReason {
			fmt.Fprintf(w, "\t%d:\t%s\n", count, reason)
		}
	}
	if pctx.Err != nil {
		fmt.Fprintf(w, "Error:\t%s\n", pctx.Err)
	} else {
		fmt.Fprintf(w, "Error:\tnone\n")
	}
	w.Flush()
	return sb.String()
}
