package scheduler

import (
	"fmt"

	"golang.org/x/exp/maps"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	armadamaps "github.com/armadaproject/armada/internal/common/maps"
	armadaslices "github.com/armadaproject/armada/internal/common/slices"
	schedulercontext "github.com/armadaproject/armada/internal/scheduler/context"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)


func PopulatePreemptionDescriptions(preemptedJobs []*schedulercontext.JobSchedulingContext, scheduledJobs []*schedulercontext.JobSchedulingContext) {
	jobsScheduledWithUrgencyBasedPreemptionByNode := map[string][]*schedulercontext.JobSchedulingContext{}
	for _, schedJob := range scheduledJobs {
		if schedJob.PodSchedulingContext.SchedulingMethod != schedulercontext.ScheduledWithUrgencyBasedPreemption {
			continue
		}

		nodeId := schedJob.PodSchedulingContext.NodeId
		if _, ok := jobsScheduledWithUrgencyBasedPreemptionByNode[nodeId]; !ok {
			jobsScheduledWithUrgencyBasedPreemptionByNode[nodeId] = []*schedulercontext.JobSchedulingContext{}
		}
		jobsScheduledWithUrgencyBasedPreemptionByNode[nodeId] = append(jobsScheduledWithUrgencyBasedPreemptionByNode[nodeId], schedJob)
	}
	for _, job := range preemptedJobs {
		if job.PreemptingJobId == "" {
			potentialPreemptingJobs := jobsScheduledWithUrgencyBasedPreemptionByNode[job.GetAssignedNodeId()]

			if len(potentialPreemptingJobs) == 0 {
				job.PreemptionDescription = fmt.Sprintf("Preempted by scheduler using urgency preemption - unknown preempting job")
			} else if len(potentialPreemptingJobs) == 1 {
				job.PreemptionDescription = fmt.Sprintf("Preempted by scheduler using urgency preemption - preempting job %s", potentialPreemptingJobs[0].JobId)
			} else {
				jobIds := armadaslices.Map(potentialPreemptingJobs, func(job *schedulercontext.JobSchedulingContext) string {
					return job.JobId
				})
				job.PreemptionDescription = fmt.Sprintf("Preempted by scheduler using urgency preemption - preemption caused by one of the following jobs %s", strings.Join(jobIds, ","))
			}
		} else {
			job.PreemptionDescription = fmt.Sprintf("Preempted by scheduler using fair share preemption - preempting job %s", job.PreemptingJobId)
		}
	}
}
