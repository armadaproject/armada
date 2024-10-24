package scheduling

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/armadaproject/armada/internal/scheduler/scheduling/context"
)

func TestPopulatePreemptionDescriptions_UnknownCause(t *testing.T) {
	scheduledJobContexts := []*context.JobSchedulingContext{}
	expectedScheduleJobContexts := []*context.JobSchedulingContext{}

	preemptedJobContexts := []*context.JobSchedulingContext{
		{
			JobId:          "job-1",
			AssignedNodeId: "node-1",
		},
	}
	expectedPreemptedJobContexts := []*context.JobSchedulingContext{
		{
			JobId:                 "job-1",
			AssignedNodeId:        "node-1",
			PreemptionDescription: unknownPreemptionCause,
		},
	}

	PopulatePreemptionDescriptions(preemptedJobContexts, scheduledJobContexts)
	assert.Equal(t, expectedScheduleJobContexts, scheduledJobContexts)
	assert.Equal(t, expectedPreemptedJobContexts, preemptedJobContexts)
}

func TestPopulatePreemptionDescriptions_PreemptedByUrgencyBasedPreemption_Single(t *testing.T) {
	scheduledJobContexts := []*context.JobSchedulingContext{
		makeJobSchedulingContext("job-2", "node-1", context.ScheduledWithUrgencyBasedPreemption),
	}
	expectedScheduleJobContexts := []*context.JobSchedulingContext{
		makeJobSchedulingContext("job-2", "node-1", context.ScheduledWithUrgencyBasedPreemption),
	}

	preemptedJobContexts := []*context.JobSchedulingContext{
		{
			JobId:          "job-1",
			AssignedNodeId: "node-1",
		},
	}

	expectedPreemptedJobContexts := []*context.JobSchedulingContext{
		{
			JobId:                 "job-1",
			AssignedNodeId:        "node-1",
			PreemptionDescription: fmt.Sprintf(urgencyPreemptionTemplate, "job-2"),
		},
	}

	PopulatePreemptionDescriptions(preemptedJobContexts, scheduledJobContexts)
	assert.Equal(t, expectedScheduleJobContexts, scheduledJobContexts)
	assert.Equal(t, expectedPreemptedJobContexts, preemptedJobContexts)
}

func TestPopulatePreemptionDescriptions_PreemptedByUrgencyBasedPreemption_Multiple(t *testing.T) {
	scheduledJobContexts := []*context.JobSchedulingContext{
		makeJobSchedulingContext("job-2", "node-1", context.ScheduledWithUrgencyBasedPreemption),
		makeJobSchedulingContext("job-3", "node-1", context.ScheduledWithUrgencyBasedPreemption),
	}
	expectedScheduleJobContexts := []*context.JobSchedulingContext{
		makeJobSchedulingContext("job-2", "node-1", context.ScheduledWithUrgencyBasedPreemption),
		makeJobSchedulingContext("job-3", "node-1", context.ScheduledWithUrgencyBasedPreemption),
	}

	preemptedJobContexts := []*context.JobSchedulingContext{
		{
			JobId:          "job-1",
			AssignedNodeId: "node-1",
		},
	}

	expectedPreemptedJobContexts := []*context.JobSchedulingContext{
		{
			JobId:                 "job-1",
			AssignedNodeId:        "node-1",
			PreemptionDescription: fmt.Sprintf(urgencyPreemptionMultiJobTemplate, "job-2,job-3"),
		},
	}

	PopulatePreemptionDescriptions(preemptedJobContexts, scheduledJobContexts)
	assert.Equal(t, expectedScheduleJobContexts, scheduledJobContexts)
	assert.Equal(t, expectedPreemptedJobContexts, preemptedJobContexts)
}

func TestPopulatePreemptionDescriptions_PreemptedByFairsharePreemption(t *testing.T) {
	scheduledJobContexts := []*context.JobSchedulingContext{}
	expectedScheduleJobContexts := []*context.JobSchedulingContext{}

	preemptedJobContexts := []*context.JobSchedulingContext{
		{
			JobId:           "job-1",
			AssignedNodeId:  "node-1",
			PreemptingJobId: "job-2",
		},
	}
	expectedPreemptedJobContexts := []*context.JobSchedulingContext{
		{
			JobId:                 "job-1",
			AssignedNodeId:        "node-1",
			PreemptingJobId:       "job-2",
			PreemptionDescription: fmt.Sprintf(fairSharePreemptionTemplate, "job-2"),
		},
	}

	PopulatePreemptionDescriptions(preemptedJobContexts, scheduledJobContexts)
	assert.Equal(t, expectedScheduleJobContexts, scheduledJobContexts)
	assert.Equal(t, expectedPreemptedJobContexts, preemptedJobContexts)
}

func makeJobSchedulingContext(jobId string, nodeId string, schedulingMethod context.SchedulingType) *context.JobSchedulingContext {
	return &context.JobSchedulingContext{
		PodSchedulingContext: &context.PodSchedulingContext{
			SchedulingMethod: schedulingMethod,
			NodeId:           nodeId,
		},
		JobId: jobId,
	}
}
