package scheduling

import (
	"fmt"
	"strings"

	armadaslices "github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/context"
	"github.com/armadaproject/armada/internal/server/configuration"
)

const (
	unknownPreemptionCause            = "Preempted by scheduler due to the job failing to reschedule - possibly node resource changed causing this job to be unschedulable\nNode Summary:\n%s"
	unknownGangPreemptionCause        = "Preempted by scheduler due to the job failing to reschedule - possibly another job in the gang was preempted or the node resource changed causing this job to be unschedulable"
	fairSharePreemptionTemplate       = "Preempted by scheduler using fair share preemption - preempting job %s"
	marketBasedPreemptionTemplate     = "Preempted by scheduler using marked based preemption - current job has a bid of %f - preempting job %s has a bid of %f"
	urgencyPreemptionTemplate         = "Preempted by scheduler using urgency preemption - preempting job %s"
	urgencyPreemptionMultiJobTemplate = "Preempted by scheduler using urgency preemption - preemption caused by one of the following jobs %s"
)

func PopulatePreemptionDescriptions(marketBasedScheduling bool, pool string, preemptedJobs []*context.JobSchedulingContext, scheduledJobs []*context.JobSchedulingContext) {
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
		if preemptedJctx.PreemptionDescription != "" {
			continue
		}
		if preemptedJctx.PreemptingJob != nil {
			if marketBasedScheduling {
				preemptedJctx.PreemptionDescription = fmt.Sprintf(marketBasedPreemptionTemplate,
					preemptedJctx.Job.GetBidPrice(pool), preemptedJctx.PreemptingJob.Id(), preemptedJctx.PreemptingJob.GetBidPrice(pool))
				preemptedJctx.PreemptionType = context.PreemptedWithFairsharePreemption
			} else {
				preemptedJctx.PreemptionDescription = fmt.Sprintf(fairSharePreemptionTemplate, preemptedJctx.PreemptingJob.Id())
				preemptedJctx.PreemptionType = context.PreemptedWithFairsharePreemption
			}
		} else {
			potentialPreemptingJobs := jobsScheduledWithUrgencyBasedPreemptionByNode[preemptedJctx.GetAssignedNodeId()]

			if len(potentialPreemptingJobs) == 0 {
				_, isGang := preemptedJctx.Job.Annotations()[configuration.GangIdAnnotation]
				if isGang {
					preemptedJctx.PreemptionDescription = fmt.Sprintf(unknownGangPreemptionCause)
					preemptedJctx.PreemptionType = context.UnknownGangJob
				} else {
					preemptedJctx.PreemptionDescription = fmt.Sprintf(unknownPreemptionCause, preemptedJctx.GetAssignedNode().SummaryString())
					preemptedJctx.PreemptionType = context.Unknown
				}
			} else if len(potentialPreemptingJobs) == 1 {
				preemptedJctx.PreemptionDescription = fmt.Sprintf(urgencyPreemptionTemplate, potentialPreemptingJobs[0].JobId)
				preemptedJctx.PreemptionType = context.PreemptedWithUrgencyPreemption
			} else {
				jobIds := armadaslices.Map(potentialPreemptingJobs, func(job *context.JobSchedulingContext) string {
					return job.JobId
				})
				preemptedJctx.PreemptionDescription = fmt.Sprintf(urgencyPreemptionMultiJobTemplate, strings.Join(jobIds, ","))
				preemptedJctx.PreemptionType = context.PreemptedWithUrgencyPreemption
			}
		}
	}
}
