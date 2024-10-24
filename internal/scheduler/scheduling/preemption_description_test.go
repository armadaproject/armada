package scheduling

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/context"
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
	"github.com/armadaproject/armada/internal/server/configuration"
)

func TestPopulatePreemptionDescriptions_UnknownCause(t *testing.T) {
	scheduledJobContexts := []*context.JobSchedulingContext{}
	expectedScheduleJobContexts := []*context.JobSchedulingContext{}

	preemptedJobContexts := []*context.JobSchedulingContext{
		{
			JobId:          "job-1",
			AssignedNodeId: "node-1",
			Job:            makeJob(t, "job-1", false),
		},
	}
	expectedPreemptedJobContexts := []*context.JobSchedulingContext{
		{
			JobId:                 "job-1",
			AssignedNodeId:        "node-1",
			Job:                   makeJob(t, "job-1", false),
			PreemptionDescription: unknownPreemptionCause,
		},
	}

	PopulatePreemptionDescriptions(preemptedJobContexts, scheduledJobContexts)
	assert.Equal(t, expectedScheduleJobContexts, scheduledJobContexts)
	assert.Equal(t, expectedPreemptedJobContexts, preemptedJobContexts)
}

func TestPopulatePreemptionDescriptions_UnknownGangCause(t *testing.T) {
	scheduledJobContexts := []*context.JobSchedulingContext{}
	expectedScheduleJobContexts := []*context.JobSchedulingContext{}

	preemptedJobContexts := []*context.JobSchedulingContext{
		{
			JobId:          "job-1",
			AssignedNodeId: "node-1",
			Job:            makeJob(t, "job-1", true),
		},
	}
	expectedPreemptedJobContexts := []*context.JobSchedulingContext{
		{
			JobId:                 "job-1",
			AssignedNodeId:        "node-1",
			Job:                   makeJob(t, "job-1", true),
			PreemptionDescription: unknownGangPreemptionCause,
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

func makeJob(t *testing.T, jobId string, isGang bool) *jobdb.Job {
	annotations := map[string]string{}
	if isGang {
		annotations[configuration.GangIdAnnotation] = "gang"
	}
	schedulingInfo := &schedulerobjects.JobSchedulingInfo{
		ObjectRequirements: []*schedulerobjects.ObjectRequirements{
			{
				Requirements: &schedulerobjects.ObjectRequirements_PodRequirements{
					PodRequirements: &schedulerobjects.PodRequirements{
						Annotations: annotations,
					},
				},
			},
		},
	}

	job, err := testfixtures.JobDb.NewJob(jobId, "jobset", "queue", 1, schedulingInfo, false, 1, false, false, false, 0, true, []string{})
	require.NoError(t, err)
	return job
}
