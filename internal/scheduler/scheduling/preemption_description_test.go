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

func TestPopulatePreemptionDescriptions(t *testing.T) {
	scheduledJobContexts := []*context.JobSchedulingContext{
		makeJobSchedulingContext("job-2", "node-1", context.ScheduledWithUrgencyBasedPreemption),
		makeJobSchedulingContext("job-3", "node-2", context.ScheduledWithUrgencyBasedPreemption),
		makeJobSchedulingContext("job-4", "node-2", context.ScheduledWithUrgencyBasedPreemption),
		makeJobSchedulingContext("job-5", "node-1", context.ScheduledWithFairSharePreemption),
		makeJobSchedulingContext("job-5", "node-2", context.ScheduledWithFairSharePreemption),
		makeJobSchedulingContext("job-6", "node-3", context.ScheduledWithFairSharePreemption),
	}
	expectedScheduleJobContexts := []*context.JobSchedulingContext{
		makeJobSchedulingContext("job-2", "node-1", context.ScheduledWithUrgencyBasedPreemption),
		makeJobSchedulingContext("job-3", "node-2", context.ScheduledWithUrgencyBasedPreemption),
		makeJobSchedulingContext("job-4", "node-2", context.ScheduledWithUrgencyBasedPreemption),
		makeJobSchedulingContext("job-5", "node-1", context.ScheduledWithFairSharePreemption),
		makeJobSchedulingContext("job-5", "node-2", context.ScheduledWithFairSharePreemption),
		makeJobSchedulingContext("job-6", "node-3", context.ScheduledWithFairSharePreemption),
	}

	tests := map[string]struct {
		preemptedJobContext         *context.JobSchedulingContext
		expectedPreemptedJobContext *context.JobSchedulingContext
	}{
		"unknown cause - basic job": {
			preemptedJobContext: &context.JobSchedulingContext{
				JobId:        "job-1",
				AssignedNode: testfixtures.TestSimpleNode("node-3"),
				Job:          makeJob(t, "job-1", false),
			},
			expectedPreemptedJobContext: &context.JobSchedulingContext{
				JobId:                 "job-1",
				AssignedNode:          testfixtures.TestSimpleNode("node-3"),
				Job:                   makeJob(t, "job-1", false),
				PreemptionDescription: fmt.Sprintf(unknownPreemptionCause, testfixtures.TestSimpleNode("node-3").SummaryString()),
			},
		},
		"unknown cause - gang job": {
			preemptedJobContext: &context.JobSchedulingContext{
				JobId:        "job-1",
				AssignedNode: testfixtures.TestSimpleNode("node-3"),
				Job:          makeJob(t, "job-1", true),
			},
			expectedPreemptedJobContext: &context.JobSchedulingContext{
				JobId:                 "job-1",
				AssignedNode:          testfixtures.TestSimpleNode("node-3"),
				Job:                   makeJob(t, "job-1", true),
				PreemptionDescription: unknownGangPreemptionCause,
			},
		},
		"urgency preemption - single preempting job": {
			preemptedJobContext: &context.JobSchedulingContext{
				JobId:        "job-1",
				AssignedNode: testfixtures.TestSimpleNode("node-1"),
			},
			expectedPreemptedJobContext: &context.JobSchedulingContext{
				JobId:                 "job-1",
				AssignedNode:          testfixtures.TestSimpleNode("node-1"),
				PreemptionDescription: fmt.Sprintf(urgencyPreemptionTemplate, "job-2"),
			},
		},
		"urgency preemption - multiple preempting jobs": {
			preemptedJobContext: &context.JobSchedulingContext{
				JobId:        "job-1",
				AssignedNode: testfixtures.TestSimpleNode("node-2"),
			},
			expectedPreemptedJobContext: &context.JobSchedulingContext{
				JobId:                 "job-1",
				AssignedNode:          testfixtures.TestSimpleNode("node-2"),
				PreemptionDescription: fmt.Sprintf(urgencyPreemptionMultiJobTemplate, "job-3,job-4"),
			},
		},
		"fairshare": {
			preemptedJobContext: &context.JobSchedulingContext{
				JobId:           "job-1",
				AssignedNode:    testfixtures.TestSimpleNode("node-4"),
				PreemptingJobId: "job-7",
			},
			expectedPreemptedJobContext: &context.JobSchedulingContext{
				JobId:                 "job-1",
				AssignedNode:          testfixtures.TestSimpleNode("node-4"),
				PreemptingJobId:       "job-7",
				PreemptionDescription: fmt.Sprintf(fairSharePreemptionTemplate, "job-7"),
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			PopulatePreemptionDescriptions([]*context.JobSchedulingContext{tc.preemptedJobContext}, scheduledJobContexts)
			assert.Equal(t, expectedScheduleJobContexts, scheduledJobContexts)
			assert.Equal(t, []*context.JobSchedulingContext{tc.expectedPreemptedJobContext}, []*context.JobSchedulingContext{tc.preemptedJobContext})
		})
	}
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

	job, err := testfixtures.JobDb.NewJob(jobId, "jobset", "queue", 1, 0.0, schedulingInfo, false, 1, false, false, false, 0, true, []string{})
	require.NoError(t, err)
	return job
}
