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
	v1 "k8s.io/api/core/v1"

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
	// Weights used when computing total resource usage.
	ResourceScarcity map[string]float64
	// Per-queue scheduling contexts.
	QueueSchedulingContexts map[string]*QueueSchedulingContext
	// Total resources across all clusters available at the start of the scheduling cycle.
	TotalResources schedulerobjects.ResourceList
	// Resources assigned across all queues during this scheduling cycle.
	ScheduledResources           schedulerobjects.ResourceList
	ScheduledResourcesByPriority schedulerobjects.QuantityByPriorityAndResourceType
	// Resources evicted across all queues during this scheduling cycle.
	EvictedResources           schedulerobjects.ResourceList
	EvictedResourcesByPriority schedulerobjects.QuantityByPriorityAndResourceType
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
		Started:                      time.Now(),
		ExecutorId:                   executorId,
		Pool:                         pool,
		PriorityClasses:              priorityClasses,
		DefaultPriorityClass:         defaultPriorityClass,
		ResourceScarcity:             resourceScarcity,
		QueueSchedulingContexts:      make(map[string]*QueueSchedulingContext),
		TotalResources:               totalResources.DeepCopy(),
		ScheduledResources:           schedulerobjects.NewResourceListWithDefaultSize(),
		ScheduledResourcesByPriority: make(schedulerobjects.QuantityByPriorityAndResourceType),
		EvictedResourcesByPriority:   make(schedulerobjects.QuantityByPriorityAndResourceType),
		SchedulingKeyGenerator:       schedulerobjects.NewSchedulingKeyGenerator(),
		UnfeasibleSchedulingKeys:     make(map[schedulerobjects.SchedulingKey]*JobSchedulingContext),
	}
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

func (sctx *SchedulingContext) AddQueueSchedulingContext(queue string, priorityFactor float64, initialAllocatedByPriority schedulerobjects.QuantityByPriorityAndResourceType) error {
	if _, ok := sctx.QueueSchedulingContexts[queue]; ok {
		return errors.WithStack(&armadaerrors.ErrInvalidArgument{
			Name:    "queue",
			Value:   queue,
			Message: fmt.Sprintf("there already exists a context for queue %s", queue),
		})
	}
	if initialAllocatedByPriority == nil {
		initialAllocatedByPriority = make(schedulerobjects.QuantityByPriorityAndResourceType)
	} else {
		initialAllocatedByPriority = initialAllocatedByPriority.DeepCopy()
	}
	qctx := &QueueSchedulingContext{
		SchedulingContext:                 sctx,
		Created:                           time.Now(),
		ExecutorId:                        sctx.ExecutorId,
		Queue:                             queue,
		PriorityFactor:                    priorityFactor,
		Allocated:                         initialAllocatedByPriority.AggregateByResource(),
		AllocatedByPriority:               initialAllocatedByPriority,
		ScheduledResourcesByPriority:      make(schedulerobjects.QuantityByPriorityAndResourceType),
		EvictedResourcesByPriority:        make(schedulerobjects.QuantityByPriorityAndResourceType),
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
			fmt.Fprintf(w, indent.String("\t\t", qctx.ReportString(verbosity-1)))
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
			fmt.Fprintf(w, indent.String("\t\t", qctx.ReportString(verbosity-1)))
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
			sctx.EvictedResourcesByPriority.SubV1ResourceList(jctx.Req.Priority, jctx.Req.ResourceRequirements.Requests)
			sctx.NumEvictedJobs--
		} else {
			sctx.ScheduledResources.AddV1ResourceList(jctx.Req.ResourceRequirements.Requests)
			sctx.ScheduledResourcesByPriority.AddV1ResourceList(jctx.Req.Priority, jctx.Req.ResourceRequirements.Requests)
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
	priority, rl := priorityAndRequestsFromLegacySchedulerJob(job, sctx.PriorityClasses)
	if scheduledInThisRound {
		sctx.ScheduledResources.SubV1ResourceList(rl)
		sctx.ScheduledResourcesByPriority.SubV1ResourceList(priority, rl)
		sctx.NumScheduledJobs--
	} else {
		sctx.EvictedResources.AddV1ResourceList(rl)
		sctx.EvictedResourcesByPriority.AddV1ResourceList(priority, rl)
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
	// The scheduling context to which this QueueSchedulingContext belongs.
	SchedulingContext *SchedulingContext
	// Time at which this context was created.
	Created time.Time
	// Executor this job was attempted to be assigned to.
	ExecutorId string
	// Queue name.
	Queue string
	// These factors influence the fraction of resources assigned to each queue.
	PriorityFactor float64
	// Total resources assigned to the queue across all clusters by priority class priority.
	// Includes jobs scheduled during this invocation of the scheduler.
	Allocated schedulerobjects.ResourceList
	// Total resources assigned to the queue across all clusters by priority class priority.
	// Includes jobs scheduled during this invocation of the scheduler.
	AllocatedByPriority schedulerobjects.QuantityByPriorityAndResourceType
	// Resources assigned to this queue during this scheduling cycle.
	ScheduledResourcesByPriority schedulerobjects.QuantityByPriorityAndResourceType
	// Resources evicted from this queue during this scheduling cycle.
	EvictedResourcesByPriority schedulerobjects.QuantityByPriorityAndResourceType
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

const maxPrintedJobIdsByReason = 1

func (qctx *QueueSchedulingContext) ReportString(verbosity int32) string {
	var sb strings.Builder
	w := tabwriter.NewWriter(&sb, 1, 1, 1, ' ', 0)
	if verbosity > 0 {
		fmt.Fprintf(w, "Created:\t%s\n", qctx.Created)
	}
	fmt.Fprintf(w, "Scheduled resources:\t%s\n", qctx.ScheduledResourcesByPriority.AggregateByResource().CompactString())
	fmt.Fprintf(w, "Scheduled resources (by priority):\t%s\n", qctx.ScheduledResourcesByPriority.String())
	fmt.Fprintf(w, "Preempted resources:\t%s\n", qctx.EvictedResourcesByPriority.AggregateByResource().CompactString())
	fmt.Fprintf(w, "Preempted resources (by priority):\t%s\n", qctx.EvictedResourcesByPriority.String())
	if verbosity > 0 {
		fmt.Fprintf(w, "Total allocated resources after scheduling:\t%s\n", qctx.AllocatedByPriority.AggregateByResource().CompactString())
		fmt.Fprintf(w, "Total allocated resources after scheduling (by priority):\t%s\n", qctx.AllocatedByPriority.String())
		fmt.Fprintf(w, "Number of jobs scheduled:\t%d\n", len(qctx.SuccessfulJobSchedulingContexts))
		fmt.Fprintf(w, "Number of jobs preempted:\t%d\n", len(qctx.EvictedJobsById))
		fmt.Fprintf(w, "Number of jobs that could not be scheduled:\t%d\n", len(qctx.UnsuccessfulJobSchedulingContexts))
		if len(qctx.SuccessfulJobSchedulingContexts) > 0 {
			jobIdsToPrint := maps.Keys(qctx.SuccessfulJobSchedulingContexts)
			if verbosity <= 1 && len(jobIdsToPrint) > maxPrintedJobIdsByReason {
				jobIdsToPrint = jobIdsToPrint[0:maxPrintedJobIdsByReason]
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
			if verbosity <= 1 && len(jobIdsToPrint) > maxPrintedJobIdsByReason {
				jobIdsToPrint = jobIdsToPrint[0:maxPrintedJobIdsByReason]
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
		qctx.AllocatedByPriority.AddV1ResourceList(jctx.Req.Priority, jctx.Req.ResourceRequirements.Requests)

		// Only if the job is not evicted, update ScheduledResourcesByPriority.
		// Since ScheduledResourcesByPriority is used to control per-round scheduling constraints.
		if evictedInThisRound {
			delete(qctx.EvictedJobsById, jctx.JobId)
			qctx.EvictedResourcesByPriority.SubV1ResourceList(jctx.Req.Priority, jctx.Req.ResourceRequirements.Requests)
		} else {
			qctx.SuccessfulJobSchedulingContexts[jctx.JobId] = jctx
			qctx.ScheduledResourcesByPriority.AddV1ResourceList(jctx.Req.Priority, jctx.Req.ResourceRequirements.Requests)
		}
	} else {
		qctx.UnsuccessfulJobSchedulingContexts[jctx.JobId] = jctx
	}
	return evictedInThisRound, nil
}

func (qctx *QueueSchedulingContext) EvictJob(job interfaces.LegacySchedulerJob) (bool, error) {
	jobId := job.GetId()
	priority, rl := priorityAndRequestsFromLegacySchedulerJob(job, qctx.SchedulingContext.PriorityClasses)
	if _, ok := qctx.UnsuccessfulJobSchedulingContexts[jobId]; ok {
		return false, errors.Errorf("failed evicting job %s from queue: job already marked unsuccessful", jobId)
	}
	if _, ok := qctx.EvictedJobsById[jobId]; ok {
		return false, errors.Errorf("failed evicting job %s from queue: job already marked evicted", jobId)
	}
	_, scheduledInThisRound := qctx.SuccessfulJobSchedulingContexts[jobId]
	if scheduledInThisRound {
		qctx.ScheduledResourcesByPriority.SubV1ResourceList(priority, rl)
		delete(qctx.SuccessfulJobSchedulingContexts, jobId)
	} else {
		qctx.EvictedResourcesByPriority.AddV1ResourceList(priority, rl)
		qctx.EvictedJobsById[jobId] = true
	}
	qctx.Allocated.SubV1ResourceList(rl)
	qctx.AllocatedByPriority.SubV1ResourceList(priority, rl)
	return scheduledInThisRound, nil
}

func priorityAndRequestsFromLegacySchedulerJob(job interfaces.LegacySchedulerJob, priorityClasses map[string]configuration.PriorityClass) (int32, v1.ResourceList) {
	req := job.GetRequirements(priorityClasses)
	for _, r := range req.ObjectRequirements {
		podReqs := r.GetPodRequirements()
		if podReqs == nil {
			continue
		}
		return podReqs.Priority, podReqs.ResourceRequirements.Requests
	}
	return 0, nil
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
