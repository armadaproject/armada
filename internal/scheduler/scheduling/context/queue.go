package context

import (
	"fmt"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
	"golang.org/x/time/rate"

	armadaslices "github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

// QueueSchedulingContext captures the decisions made by the scheduler during one invocation
// for a particular queue.
type QueueSchedulingContext struct {
	// The scheduling context to which this QueueSchedulingContext belongs.
	SchedulingContext *SchedulingContext
	// Time at which this context was created.
	Created time.Time
	// Queue name.
	Queue string
	// Determines the fair share of this queue relative to other queues.
	Weight float64
	// Limits job scheduling rate for this queue.
	// Use the "Started" time to ensure limiter state remains constant within each scheduling round.
	Limiter *rate.Limiter
	// Total resources assigned to the queue across all clusters by priority class priority.
	// Includes jobs scheduled during this invocation of the scheduler.
	Allocated internaltypes.ResourceList
	// Total demand from this queue.  This is essentially the cumulative resources of all non-terminal jobs at the
	// start of the scheduling cycle
	Demand internaltypes.ResourceList
	// Capped Demand for this queue. This differs from Demand in that it takes into account any limits that we have
	// placed on the queue
	CappedDemand internaltypes.ResourceList
	// Fair share is the weight of this queue over the sum of the weights of all queues
	FairShare float64
	// AdjustedFairShare modifies fair share such that queues that have a demand cost less than their fair share, have their fair share reallocated.
	AdjustedFairShare float64
	// Total resources assigned to the queue across all clusters by priority class.
	// Includes jobs scheduled during this invocation of the scheduler.
	AllocatedByPriorityClass map[string]internaltypes.ResourceList
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

func (qctx *QueueSchedulingContext) String() string {
	return qctx.ReportString(0)
}

// GetAllocation is necessary to implement the fairness.Queue interface.
func (qctx *QueueSchedulingContext) GetAllocation() internaltypes.ResourceList {
	return qctx.Allocated
}

// GetWeight is necessary to implement the fairness.Queue interface.
func (qctx *QueueSchedulingContext) GetWeight() float64 {
	return qctx.Weight
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
		fmt.Fprintf(w, "Total allocated resources after scheduling:\t%s\n", qctx.Allocated.String())
		for pc, res := range qctx.AllocatedByPriorityClass {
			fmt.Fprintf(w, "Total allocated resources after scheduling by for priority class %s:\t%s\n", pc, res.String())
		}
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
			slices.SortFunc(reasons, func(a, b string) int {
				if len(jobIdsByReason[a]) < len(jobIdsByReason[b]) {
					return -1
				} else if len(jobIdsByReason[a]) > len(jobIdsByReason[b]) {
					return 1
				} else {
					return 0
				}
			})
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

// addJobSchedulingContext adds a job scheduling context.
// Automatically updates scheduled resources.
func (qctx *QueueSchedulingContext) addJobSchedulingContext(jctx *JobSchedulingContext) (bool, error) {
	if _, ok := qctx.SuccessfulJobSchedulingContexts[jctx.JobId]; ok {
		return false, errors.Errorf("failed adding job %s to queue: job already marked successful", jctx.JobId)
	}
	if _, ok := qctx.UnsuccessfulJobSchedulingContexts[jctx.JobId]; ok {
		return false, errors.Errorf("failed adding job %s to queue: job already marked unsuccessful", jctx.JobId)
	}
	_, evictedInThisRound := qctx.EvictedJobsById[jctx.JobId]
	if jctx.IsSuccessful() {
		if jctx.PodRequirements == nil {
			return false, errors.Errorf("failed adding job %s to queue: job requirements are missing", jctx.JobId)
		}

		// Always update ResourcesByPriority.
		// Since ResourcesByPriority is used to order queues by fraction of fair share.
		pcName := jctx.Job.PriorityClassName()
		qctx.AllocatedByPriorityClass[pcName] = qctx.AllocatedByPriorityClass[pcName].Add(jctx.Job.AllResourceRequirements())
		qctx.Allocated = qctx.Allocated.Add(jctx.Job.AllResourceRequirements())

		// Only if the job is not evicted, update ScheduledResourcesByPriority.
		// Since ScheduledResourcesByPriority is used to control per-round scheduling constraints.
		if evictedInThisRound {
			delete(qctx.EvictedJobsById, jctx.JobId)
			qctx.EvictedResourcesByPriorityClass.SubV1ResourceList(jctx.Job.PriorityClassName(), jctx.PodRequirements.ResourceRequirements.Requests)
		} else {
			qctx.SuccessfulJobSchedulingContexts[jctx.JobId] = jctx
			qctx.ScheduledResourcesByPriorityClass.AddV1ResourceList(jctx.Job.PriorityClassName(), jctx.PodRequirements.ResourceRequirements.Requests)
		}
	} else {
		qctx.UnsuccessfulJobSchedulingContexts[jctx.JobId] = jctx
	}
	return evictedInThisRound, nil
}

func (qctx *QueueSchedulingContext) evictJob(job *jobdb.Job) (bool, error) {
	jobId := job.Id()
	if _, ok := qctx.UnsuccessfulJobSchedulingContexts[jobId]; ok {
		return false, errors.Errorf("failed evicting job %s from queue: job already marked unsuccessful", jobId)
	}
	if _, ok := qctx.EvictedJobsById[jobId]; ok {
		return false, errors.Errorf("failed evicting job %s from queue: job already marked evicted", jobId)
	}
	rl := job.ResourceRequirements().Requests
	_, scheduledInThisRound := qctx.SuccessfulJobSchedulingContexts[jobId]
	if scheduledInThisRound {
		qctx.ScheduledResourcesByPriorityClass.SubV1ResourceList(job.PriorityClassName(), rl)
		delete(qctx.SuccessfulJobSchedulingContexts, jobId)
	} else {
		qctx.EvictedResourcesByPriorityClass.AddV1ResourceList(job.PriorityClassName(), rl)
		qctx.EvictedJobsById[jobId] = true
	}
	pcName := job.PriorityClassName()
	qctx.AllocatedByPriorityClass[pcName] = qctx.AllocatedByPriorityClass[pcName].Subtract(job.AllResourceRequirements())
	qctx.Allocated = qctx.Allocated.Subtract(job.AllResourceRequirements())

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
