package context

import (
	"fmt"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/exp/maps"
	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	armadamaps "github.com/armadaproject/armada/internal/common/maps"
	armadaslices "github.com/armadaproject/armada/internal/common/slices"
	schedulerconfig "github.com/armadaproject/armada/internal/scheduler/configuration"
	"github.com/armadaproject/armada/internal/scheduler/interfaces"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/internal/server/configuration"
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
	PodRequirements *schedulerobjects.PodRequirements
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
	// GangInfo holds all the information that is necessary to schedule a gang,
	// such as the lower and upper bounds on its size.
	GangInfo
	// This is the node the pod is assigned to.
	// This is only set for evicted jobs and is set alongside adding an additionalNodeSelector for the node
	AssignedNodeId string
	// Id of job that preempted this pod
	PreemptingJobId string
	// Description of the cause of preemption
	PreemptionDescription string
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
func (jctx *JobSchedulingContext) SchedulingKey() (schedulerobjects.SchedulingKey, bool) {
	if len(jctx.AdditionalNodeSelectors) != 0 || len(jctx.AdditionalTolerations) != 0 {
		return schedulerobjects.EmptySchedulingKey, false
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

func (jctx *JobSchedulingContext) GetAssignedNodeId() string {
	return jctx.AssignedNodeId
}

func (jctx *JobSchedulingContext) SetAssignedNodeId(assignedNodeId string) {
	if assignedNodeId != "" {
		jctx.AssignedNodeId = assignedNodeId
		jctx.AddNodeSelector(schedulerconfig.NodeIdLabel, assignedNodeId)
	}
}

func (jctx *JobSchedulingContext) AddNodeSelector(key, value string) {
	if jctx.AdditionalNodeSelectors == nil {
		jctx.AdditionalNodeSelectors = map[string]string{key: value}
	} else {
		jctx.AdditionalNodeSelectors[key] = value
	}
}

type GangInfo struct {
	Id                string
	Cardinality       int
	PriorityClassName string
	NodeUniformity    string
}

// EmptyGangInfo returns a GangInfo for a job that is not in a gang.
func EmptyGangInfo(job interfaces.MinimalJob) GangInfo {
	return GangInfo{
		// An Id of "" indicates that this job is not in a gang; we set
		// Cardinality (as well as the other fields,
		// which all make sense in this context) accordingly.
		Id:                "",
		Cardinality:       1,
		PriorityClassName: job.PriorityClassName(),
		NodeUniformity:    job.Annotations()[configuration.GangNodeUniformityLabelAnnotation],
	}
}

func GangInfoFromLegacySchedulerJob(job interfaces.MinimalJob) (GangInfo, error) {
	gangInfo := EmptyGangInfo(job)

	annotations := job.Annotations()

	gangId, ok := annotations[configuration.GangIdAnnotation]
	if !ok {
		return gangInfo, nil
	}
	if gangId == "" {
		return gangInfo, errors.Errorf("gang id is empty")
	}

	gangCardinalityString, ok := annotations[configuration.GangCardinalityAnnotation]
	if !ok {
		return gangInfo, errors.Errorf("annotation %s is missing", configuration.GangCardinalityAnnotation)
	}
	gangCardinality, err := strconv.Atoi(gangCardinalityString)
	if err != nil {
		return gangInfo, errors.WithStack(err)
	}
	if gangCardinality <= 0 {
		return gangInfo, errors.Errorf("gang cardinality %d is non-positive", gangCardinality)
	}

	gangInfo.Id = gangId
	gangInfo.Cardinality = gangCardinality
	return gangInfo, nil
}

func JobSchedulingContextsFromJobs[J *jobdb.Job](jobs []J) []*JobSchedulingContext {
	jctxs := make([]*JobSchedulingContext, len(jobs))
	for i, job := range jobs {
		jctxs[i] = JobSchedulingContextFromJob(job)
	}
	return jctxs
}

func JobSchedulingContextFromJob(job *jobdb.Job) *JobSchedulingContext {
	gangInfo, err := GangInfoFromLegacySchedulerJob(job)
	if err != nil {
		logrus.Errorf("failed to extract gang info from job %s: %s", job.Id(), err)
	}
	return &JobSchedulingContext{
		Created:                        time.Now(),
		JobId:                          job.Id(),
		Job:                            job,
		PodRequirements:                job.PodRequirements(),
		KubernetesResourceRequirements: job.KubernetesResourceRequirements(),
		GangInfo:                       gangInfo,
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
		func(jobs []*jobdb.Job) schedulerobjects.ResourceList {
			rv := schedulerobjects.NewResourceListWithDefaultSize()
			for _, job := range jobs {
				rv.AddV1ResourceList(job.ResourceRequirements().Requests)
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
			func(rl schedulerobjects.ResourceList) string {
				return rl.CompactString()
			},
		),
		jobCountPerQueue,
	)
	verbose := fmt.Sprintf("affected jobs %v", jobIdsByQueue)

	ctx.Infof("%s %s", prefix, summary)
	ctx.Debugf("%s %s", prefix, verbose)
}
