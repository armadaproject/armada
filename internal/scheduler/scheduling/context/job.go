package context

import (
	"fmt"
	"strings"
	"text/tabwriter"
	"time"

	"golang.org/x/exp/maps"
	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	armadamaps "github.com/armadaproject/armada/internal/common/maps"
	armadaslices "github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/internal/common/util"
	schedulerconfig "github.com/armadaproject/armada/internal/scheduler/configuration"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
)

// JobSchedulingContext is created by the scheduler and contains information
// about the decision made by the scheduler for a particular job.
type JobSchedulingContext struct {
	// Time at which this context was created.
	Created time.Time
	// Id of the job this pod corresponds to.
	JobId string
	// Indicates whether this context is for re-scheduling an evicted job.
	IsEvicted bool
	// Job spec.
	Job *jobdb.Job
	// Scheduling requirements of this job.
	// We currently require that each job contains exactly one pod spec.
	PodRequirements *internaltypes.PodRequirements
	// Resource requirements in an efficient internaltypes.ResourceList
	KubernetesResourceRequirements internaltypes.ResourceList
	// Node selectors to consider in addition to those included with the PodRequirements.
	// These are added as part of scheduling to further constrain where nodes are scheduled,
	// e.g., to ensure evicted jobs are re-scheduled onto the same node.
	//
	// If some key appears in both PodRequirements.NodeSelector and AdditionalNodeSelectors,
	// the value in AdditionalNodeSelectors trumps that of PodRequirements.NodeSelector.
	AdditionalNodeSelectors map[string]string
	// Tolerations to consider in addition to those included with the PodRequirements.
	// These are added as part of scheduling to expand the set of nodes a job can be scheduled on.
	AdditionalTolerations []v1.Toleration
	// Reason for why the job could not be scheduled.
	// Empty if the job was scheduled successfully.
	UnschedulableReason string
	// Pod scheduling contexts for the individual pods that make up the job.
	PodSchedulingContext *PodSchedulingContext
	// The number of active jobs in the gang this jctx belongs to
	// This may differ from the jobs GangInfo cardinality, as it finished jobs are not included in this count
	CurrentGangCardinality int
	// This is the node the pod is assigned to.
	// This is only set for evicted jobs and is set alongside adding an additionalNodeSelector for the node
	AssignedNode *internaltypes.Node
	// job that preempted this pod
	PreemptingJob *jobdb.Job
	// The type of preemption used to preempt this job (i.e fairshare, urgency)
	PreemptionType PreemptionType
	// Description of the cause of preemption
	PreemptionDescription string
	// If this job context should contribute to the billable resource of the queue
	Billable bool
}

func (jctx *JobSchedulingContext) IsHomeJob(currentPool string) bool {
	return IsHomeJob(jctx.Job, currentPool)
}

func (jctx *JobSchedulingContext) String() string {
	var sb strings.Builder
	w := tabwriter.NewWriter(&sb, 1, 1, 1, ' ', 0)
	fmt.Fprintf(w, "Time:\t%s\n", jctx.Created)
	fmt.Fprintf(w, "Job ID:\t%s\n", jctx.JobId)
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

// SchedulingKey returns the scheduling key of the embedded job.
// If the jctx contains additional node selectors or tolerations,
// the key is invalid and the second return value is false.
func (jctx *JobSchedulingContext) SchedulingKey() (internaltypes.SchedulingKey, bool) {
	if len(jctx.AdditionalNodeSelectors) != 0 || len(jctx.AdditionalTolerations) != 0 {
		return internaltypes.EmptySchedulingKey, false
	}
	return jctx.Job.SchedulingKey(), true
}

func (jctx *JobSchedulingContext) IsSuccessful() bool {
	return jctx.UnschedulableReason == ""
}

func (jctx *JobSchedulingContext) Fail(unschedulableReason string) {
	jctx.UnschedulableReason = unschedulableReason
	if pctx := jctx.PodSchedulingContext; pctx != nil {
		pctx.NodeId = ""
		pctx.SchedulingMethod = None
	}
}

func (jctx *JobSchedulingContext) GetAssignedNode() *internaltypes.Node {
	return jctx.AssignedNode
}

func (jctx *JobSchedulingContext) GetAssignedNodeId() string {
	if jctx.AssignedNode == nil {
		return ""
	}
	return jctx.AssignedNode.GetId()
}

func (jctx *JobSchedulingContext) SetAssignedNode(assignedNode *internaltypes.Node) {
	if assignedNode != nil {
		jctx.AssignedNode = assignedNode
		jctx.AddNodeSelector(schedulerconfig.NodeIdLabel, assignedNode.GetId())
	}
}

func (jctx *JobSchedulingContext) AddNodeSelector(key, value string) {
	if jctx.AdditionalNodeSelectors == nil {
		jctx.AdditionalNodeSelectors = map[string]string{key: value}
	} else {
		jctx.AdditionalNodeSelectors[key] = value
	}
}

func JobSchedulingContextsFromJobs[J *jobdb.Job](jobs []J) []*JobSchedulingContext {
	jctxs := make([]*JobSchedulingContext, len(jobs))
	for i, job := range jobs {
		jctxs[i] = JobSchedulingContextFromJob(job)
	}
	return jctxs
}

func JobSchedulingContextFromJob(job *jobdb.Job) *JobSchedulingContext {
	return &JobSchedulingContext{
		Created:                        time.Now(),
		JobId:                          job.Id(),
		Job:                            job,
		PodRequirements:                job.PodRequirements(),
		KubernetesResourceRequirements: job.KubernetesResourceRequirements(),
		CurrentGangCardinality:         job.GetGangInfo().Cardinality(),
	}
}

// PrintJobSummary logs a summary of the job scheduling context
// It will log a high level summary at Info level, and a list of all queues + jobs affected at debug level
func PrintJobSummary(ctx *armadacontext.Context, prefix string, jctxs []*JobSchedulingContext) {
	if len(jctxs) == 0 {
		return
	}
	jobsByQueue := armadaslices.MapAndGroupByFuncs(
		jctxs,
		func(jctx *JobSchedulingContext) string {
			return jctx.Job.Queue()
		},
		func(jctx *JobSchedulingContext) *jobdb.Job {
			return jctx.Job
		},
	)
	resourcesByQueue := armadamaps.MapValues(
		jobsByQueue,
		func(jobs []*jobdb.Job) internaltypes.ResourceList {
			rv := internaltypes.ResourceList{}
			for _, job := range jobs {
				rv = rv.Add(job.AllResourceRequirements())
			}
			return rv
		},
	)
	jobCountPerQueue := armadamaps.MapValues(
		jobsByQueue,
		func(jobs []*jobdb.Job) int {
			return len(jobs)
		},
	)
	jobIdsByQueue := armadamaps.MapValues(
		jobsByQueue,
		func(jobs []*jobdb.Job) []string {
			rv := make([]string, len(jobs))
			for i, job := range jobs {
				rv[i] = job.Id()
			}
			return rv
		},
	)
	summary := fmt.Sprintf(
		"affected queues %v; resources %v; jobs per queue %v",
		maps.Keys(jobsByQueue),
		armadamaps.MapValues(
			resourcesByQueue,
			func(rl internaltypes.ResourceList) string {
				return rl.String()
			},
		),
		jobCountPerQueue,
	)
	verbose := fmt.Sprintf("affected jobs %v", jobIdsByQueue)

	ctx.Infof("%s %s", prefix, summary)
	ctx.Debugf("%s %s", prefix, verbose)
}

// PrintJobSchedulingDetails prints details of where jobs were scheduled
// It will log the first 100 at info level, and all at debug level
func PrintJobSchedulingDetails(ctx *armadacontext.Context, prefix string, evictedJctx []*JobSchedulingContext) {
	if len(evictedJctx) == 0 {
		return
	}

	type summary struct {
		jobId             string
		node              string
		scheduledPriority int32
	}

	infoDisplayLimit := 100
	infoDisplayLimit = util.Min(infoDisplayLimit, len(evictedJctx))

	summaries := make([]summary, 0, len(evictedJctx))
	for _, jctx := range evictedJctx {
		podSummary := summary{jobId: jctx.JobId}
		if jctx.PodSchedulingContext != nil {
			podSummary.node = jctx.PodSchedulingContext.NodeId
			podSummary.scheduledPriority = jctx.PodSchedulingContext.ScheduledAtPriority
		}
		summaries = append(summaries, podSummary)
	}

	ctx.Infof("%s - total %d, showing first %d -  %+v", prefix, len(evictedJctx), infoDisplayLimit, summaries[:infoDisplayLimit])
	ctx.Debugf("%s - %+v", prefix, summaries)
}
