package scheduling

import (
	"fmt"
	"strings"

	armadaslices "github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/context"
)

const (
	unknownPreemptionCause                 = "Preempted by scheduler due to the job failing to reschedule - possibly node resource changed causing this job to be unschedulable\nNode Summary:\n%s"
	unknownGangPreemptionCause             = "Preempted by scheduler due to the job failing to reschedule - possibly another job in the gang was preempted or the node resource changed causing this job to be unschedulable"
	gangSiblingFairSharePreemptionTemplate = "Preempted by scheduler using fair share preemption because the fellow gang member %s was preempted by %s"
	fairSharePreemptionTemplate            = "Preempted by scheduler using fair share preemption - preempting job %s"
	marketBasedPreemptionTemplate          = "Preempted by scheduler using market based preemption - current job has a bid of %f - preempting job %s has a bid of %f"
	urgencyPreemptionTemplate              = "Preempted by scheduler using urgency preemption - preempting job %s"
	urgencyPreemptionMultiJobTemplate      = "Preempted by scheduler using urgency preemption - preemption caused by one of the following jobs %s"
)

func PopulatePreemptionDescriptions(marketBasedScheduling bool, pool string, preemptedJobs []*context.JobSchedulingContext, scheduledJobs []*context.JobSchedulingContext) {
	jobsScheduledWithUrgencyBasedPreemptionByNode := calculateJobsScheduledWithUrgencyBasedPreemptionByNode(scheduledJobs)

	for _, preemptedJctx := range preemptedJobs {
		if preemptedJctx.PreemptionDescription != "" {
			continue
		}
		if preemptedJctx.PreemptionDetails != nil && preemptedJctx.PreemptionDetails.PreemptingJob != nil {
			preemptingJob := preemptedJctx.PreemptionDetails.PreemptingJob
			causedBySiblingPreemption := preemptedJctx.PreemptionDetails.CausedBySiblingPreemption()
			preemptedJctx.PreemptionType = context.PreemptedWithFairsharePreemption

			if causedBySiblingPreemption {
				preemptedJctx.PreemptionDescription = fmt.Sprintf(gangSiblingFairSharePreemptionTemplate, preemptedJctx.PreemptionDetails.PreemptedSiblingJob.Id(), preemptingJob.Id())
			} else {
				if marketBasedScheduling {
					preemptedJctx.PreemptionDescription = fmt.Sprintf(marketBasedPreemptionTemplate,
						preemptedJctx.Job.GetBidPrice(pool), preemptingJob.Id(), preemptingJob.GetBidPrice(pool))
				} else {
					preemptedJctx.PreemptionDescription = fmt.Sprintf(fairSharePreemptionTemplate, preemptingJob.Id())
				}
			}
		} else {
			potentialPreemptingJobs := jobsScheduledWithUrgencyBasedPreemptionByNode[preemptedJctx.GetAssignedNodeId()]
			if len(potentialPreemptingJobs) == 0 {
				if preemptedJctx.Job.IsInGang() {
					preemptedJctx.PreemptionDescription = unknownGangPreemptionCause
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

func calculateJobsScheduledWithUrgencyBasedPreemptionByNode(scheduledJobs []*context.JobSchedulingContext) map[string][]*context.JobSchedulingContext {
	jobsScheduledWithUrgencyBasedPreemptionByNode := map[string][]*context.JobSchedulingContext{}
	for _, schedJctx := range scheduledJobs {
		if schedJctx.PodSchedulingContext == nil {
			continue
		}
		if schedJctx.PodSchedulingContext.SchedulingMethod != context.ScheduledWithUrgencyBasedPreemption {
			continue
		}

		nodeId := schedJctx.PodSchedulingContext.NodeId
		jobsScheduledWithUrgencyBasedPreemptionByNode[nodeId] = append(jobsScheduledWithUrgencyBasedPreemptionByNode[nodeId], schedJctx)
	}
	return jobsScheduledWithUrgencyBasedPreemptionByNode
}
