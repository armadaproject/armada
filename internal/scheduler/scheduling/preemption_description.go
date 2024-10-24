package scheduling

import (
	"fmt"
	"strings"

	armadaslices "github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/context"
)

const (
	unknownPreemptionCause            = "Preempted by scheduler due to the job failing to reschedule - possibly node resource changed caused this job to be unschedulable"
	fairSharePreemptionTemplate       = "Preempted by scheduler using fair share preemption - preempting job %s"
	urgencyPreemptionTemplate         = "Preempted by scheduler using urgency preemption - preempting job %s"
	urgencyPreemptionMultiJobTemplate = "Preempted by scheduler using urgency preemption - preemption caused by one of the following jobs %s"
)

func PopulatePreemptionDescriptions(preemptedJobs []*context.JobSchedulingContext, scheduledJobs []*context.JobSchedulingContext) {
	jobsScheduledWithUrgencyBasedPreemptionByNode := map[string][]*context.JobSchedulingContext{}
	for _, schedJctx := range scheduledJobs {
		if schedJctx.PodSchedulingContext == nil {
			continue
		}
		if schedJctx.PodSchedulingContext.SchedulingMethod != context.ScheduledWithUrgencyBasedPreemption {
			continue
		}

		nodeId := schedJctx.PodSchedulingContext.NodeId
		if _, ok := jobsScheduledWithUrgencyBasedPreemptionByNode[nodeId]; !ok {
			jobsScheduledWithUrgencyBasedPreemptionByNode[nodeId] = []*context.JobSchedulingContext{}
		}
		jobsScheduledWithUrgencyBasedPreemptionByNode[nodeId] = append(jobsScheduledWithUrgencyBasedPreemptionByNode[nodeId], schedJctx)
	}

	for _, preemptedJctx := range preemptedJobs {
		if preemptedJctx.PreemptingJobId != "" {
			preemptedJctx.PreemptionDescription = fmt.Sprintf(fairSharePreemptionTemplate, preemptedJctx.PreemptingJobId)
		} else {
			potentialPreemptingJobs := jobsScheduledWithUrgencyBasedPreemptionByNode[preemptedJctx.GetAssignedNodeId()]

			if len(potentialPreemptingJobs) == 0 {
				preemptedJctx.PreemptionDescription = fmt.Sprintf(unknownPreemptionCause)
			} else if len(potentialPreemptingJobs) == 1 {
				preemptedJctx.PreemptionDescription = fmt.Sprintf(urgencyPreemptionTemplate, potentialPreemptingJobs[0].JobId)
			} else {
				jobIds := armadaslices.Map(potentialPreemptingJobs, func(job *context.JobSchedulingContext) string {
					return job.JobId
				})
				preemptedJctx.PreemptionDescription = fmt.Sprintf(urgencyPreemptionMultiJobTemplate, strings.Join(jobIds, ","))
			}
		}
	}
}
