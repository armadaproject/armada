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
	gangSiblingFairSharePreemptionTemplate = "Preempted by scheduler using fair share preemption because the following gang members were preempted: %s"
	fairSharePreemptionTemplate            = "Preempted by scheduler using fair share preemption - preempting job %s"
	marketBasedPreemptionTemplate          = "Preempted by scheduler using market based preemption - current job has a bid of %f - preempting job %s has a bid of %f"
	urgencyPreemptionTemplate              = "Preempted by scheduler using urgency preemption - preempting job %s"
	urgencyPreemptionMultiJobTemplate      = "Preempted by scheduler using urgency preemption - preemption caused by one of the following jobs %s"
)

type preemptionInfo struct {
	preemptedJobId  string
	preemptingJobId string
}

func PopulatePreemptionDescriptions(marketBasedScheduling bool, pool string, preemptedJobs []*context.JobSchedulingContext, scheduledJobs []*context.JobSchedulingContext) {
	preemptedGangMembersByGangKey := calculatePreemptedGangMembersByGangKey(preemptedJobs)
	jobsScheduledWithUrgencyBasedPreemptionByNode := calculateJobsScheduledWithUrgencyBasedPreemptionByNode(scheduledJobs)

	for _, preemptedJctx := range preemptedJobs {
		if preemptedJctx.PreemptionDescription != "" {
			continue
		}
		siblingPreemptions := gangSiblingPreemptions(preemptedGangMembersByGangKey, preemptedJctx)
		if preemptedJctx.PreemptingJob != nil {
			if marketBasedScheduling {
				preemptedJctx.PreemptionDescription = fmt.Sprintf(marketBasedPreemptionTemplate,
					preemptedJctx.Job.GetBidPrice(pool), preemptedJctx.PreemptingJob.Id(), preemptedJctx.PreemptingJob.GetBidPrice(pool))
				preemptedJctx.PreemptionType = context.PreemptedWithFairsharePreemption
			} else {
				preemptedJctx.PreemptionDescription = fmt.Sprintf(fairSharePreemptionTemplate, preemptedJctx.PreemptingJob.Id())
				preemptedJctx.PreemptionType = context.PreemptedWithFairsharePreemption
			}
		} else if preemptedJctx.Job.IsInGang() && len(siblingPreemptions) > 0 {
			if siblingPreemptions := gangSiblingPreemptions(preemptedGangMembersByGangKey, preemptedJctx); len(siblingPreemptions) > 0 {
				preemptedJctx.PreemptionDescription = fmt.Sprintf(gangSiblingFairSharePreemptionTemplate, describeGangMemberPreemptions(siblingPreemptions))
				preemptedJctx.PreemptionType = context.PreemptedWithFairsharePreemption
				continue
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

func calculatePreemptedGangMembersByGangKey(preemptedJobs []*context.JobSchedulingContext) map[gangKey][]preemptionInfo {
	preemptedGangMembersByGangKey := map[gangKey][]preemptionInfo{}
	for _, preemptedJctx := range preemptedJobs {
		if preemptedJctx.PreemptingJob == nil || preemptedJctx.Job == nil || !preemptedJctx.Job.IsInGang() {
			continue
		}
		key := gangKeyForJob(preemptedJctx)
		preemptedGangMembersByGangKey[key] = append(preemptedGangMembersByGangKey[key], preemptionInfo{
			preemptedJobId:  preemptedJctx.JobId,
			preemptingJobId: preemptedJctx.PreemptingJob.Id(),
		})
	}
	return preemptedGangMembersByGangKey
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

func gangSiblingPreemptions(preemptedGangMembersByGangKey map[gangKey][]preemptionInfo, jctx *context.JobSchedulingContext) []preemptionInfo {
	if !jctx.Job.IsInGang() {
		return nil
	}
	preemptions := preemptedGangMembersByGangKey[gangKeyForJob(jctx)]
	siblingPreemptions := make([]preemptionInfo, 0, len(preemptions))
	for _, preemption := range preemptions {
		if preemption.preemptedJobId == jctx.JobId {
			continue
		}
		siblingPreemptions = append(siblingPreemptions, preemption)
	}
	return siblingPreemptions
}

func gangKeyForJob(jctx *context.JobSchedulingContext) gangKey {
	return gangKey{queue: jctx.Job.Queue(), gangId: jctx.Job.GetGangInfo().Id()}
}

func describeGangMemberPreemptions(preemptions []preemptionInfo) string {
	descriptions := armadaslices.Map(preemptions, func(p preemptionInfo) string {
		return fmt.Sprintf("%s (preempted by job %s)", p.preemptedJobId, p.preemptingJobId)
	})
	return strings.Join(descriptions, ", ")
}
