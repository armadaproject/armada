package scheduling

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/pricing"
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
		marketBased                 bool
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
				PreemptionType:        context.Unknown,
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
				PreemptionType:        context.UnknownGangJob,
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
				PreemptionType:        context.PreemptedWithUrgencyPreemption,
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
				PreemptionType:        context.PreemptedWithUrgencyPreemption,
			},
		},
		"fairshare": {
			preemptedJobContext: &context.JobSchedulingContext{
				JobId:         "job-1",
				AssignedNode:  testfixtures.TestSimpleNode("node-4"),
				PreemptingJob: makeJob(t, "job-7", false),
			},
			expectedPreemptedJobContext: &context.JobSchedulingContext{
				JobId:                 "job-1",
				AssignedNode:          testfixtures.TestSimpleNode("node-4"),
				PreemptingJob:         makeJob(t, "job-7", false),
				PreemptionDescription: fmt.Sprintf(fairSharePreemptionTemplate, "job-7"),
				PreemptionType:        context.PreemptedWithFairsharePreemption,
			},
		},
		"fairshare - market based": {
			marketBased: true,
			preemptedJobContext: &context.JobSchedulingContext{
				JobId:         "job-1",
				Job:           makeJobWithPrice(t, "job-1", false, 0),
				AssignedNode:  testfixtures.TestSimpleNode("node-4"),
				PreemptingJob: makeJobWithPrice(t, "job-7", false, 5),
			},
			expectedPreemptedJobContext: &context.JobSchedulingContext{
				JobId:                 "job-1",
				Job:                   makeJobWithPrice(t, "job-1", false, 0),
				AssignedNode:          testfixtures.TestSimpleNode("node-4"),
				PreemptingJob:         makeJobWithPrice(t, "job-7", false, 5),
				PreemptionDescription: fmt.Sprintf(marketBasedPreemptionTemplate, float64(0), "job-7", float64(5)),
				PreemptionType:        context.PreemptedWithFairsharePreemption,
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			PopulatePreemptionDescriptions(tc.marketBased, testfixtures.TestPool, []*context.JobSchedulingContext{tc.preemptedJobContext}, scheduledJobContexts)
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
	schedulingInfo := &internaltypes.JobSchedulingInfo{
		PodRequirements: &internaltypes.PodRequirements{
			Annotations: annotations,
		},
	}

	job, err := testfixtures.JobDb.NewJob(jobId, "jobset", "queue", 1, schedulingInfo, false, 1, false, false, false, 0, true, []string{}, 0)
	require.NoError(t, err)
	return job
}

func makeJobWithPrice(t *testing.T, jobId string, isGang bool, price float64) *jobdb.Job {
	annotations := map[string]string{}
	if isGang {
		annotations[configuration.GangIdAnnotation] = "gang"
	}
	schedulingInfo := &internaltypes.JobSchedulingInfo{
		PodRequirements: &internaltypes.PodRequirements{
			Annotations: annotations,
		},
	}

	job, err := testfixtures.JobDb.NewJob(jobId, "jobset", "queue", 1, schedulingInfo, false, 1, false, false, false, 0, true, []string{}, 0)
	require.NoError(t, err)
	job = job.WithBidPrices(map[string]pricing.Bid{testfixtures.TestPool: {QueuedBid: price, RunningBid: price}})
	return job
}
