package scheduling

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/internal/common/constants"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/pricing"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/context"
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
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
		marketBased                  bool
		preemptedJobContexts         []*context.JobSchedulingContext
		expectedPreemptedJobContexts []*context.JobSchedulingContext
	}{
		"unknown cause - basic job": {
			preemptedJobContexts: []*context.JobSchedulingContext{{
				JobId:        "job-1",
				AssignedNode: testfixtures.TestSimpleNode("node-3"),
				Job:          makeJob(t, "job-1", false),
			}},
			expectedPreemptedJobContexts: []*context.JobSchedulingContext{{
				JobId:                 "job-1",
				AssignedNode:          testfixtures.TestSimpleNode("node-3"),
				Job:                   makeJob(t, "job-1", false),
				PreemptionDescription: fmt.Sprintf(unknownPreemptionCause, testfixtures.TestSimpleNode("node-3").SummaryString()),
				PreemptionType:        context.Unknown,
			}},
		},
		"unknown cause - gang job": {
			preemptedJobContexts: []*context.JobSchedulingContext{{
				JobId:        "job-1",
				AssignedNode: testfixtures.TestSimpleNode("node-3"),
				Job:          makeJob(t, "job-1", true),
			}},
			expectedPreemptedJobContexts: []*context.JobSchedulingContext{{
				JobId:                 "job-1",
				AssignedNode:          testfixtures.TestSimpleNode("node-3"),
				Job:                   makeJob(t, "job-1", true),
				PreemptionDescription: unknownGangPreemptionCause,
				PreemptionType:        context.UnknownGangJob,
			}},
		},
		"urgency preemption - single preempting job": {
			preemptedJobContexts: []*context.JobSchedulingContext{{
				JobId:        "job-1",
				AssignedNode: testfixtures.TestSimpleNode("node-1"),
				Job:          makeJob(t, "job-1", false),
			}},
			expectedPreemptedJobContexts: []*context.JobSchedulingContext{{
				JobId:                 "job-1",
				AssignedNode:          testfixtures.TestSimpleNode("node-1"),
				Job:                   makeJob(t, "job-1", false),
				PreemptionDescription: fmt.Sprintf(urgencyPreemptionTemplate, "job-2"),
				PreemptionType:        context.PreemptedWithUrgencyPreemption,
			}},
		},
		"urgency preemption - multiple preempting jobs": {
			preemptedJobContexts: []*context.JobSchedulingContext{{
				JobId:        "job-1",
				AssignedNode: testfixtures.TestSimpleNode("node-2"),
				Job:          makeJob(t, "job-1", false),
			}},
			expectedPreemptedJobContexts: []*context.JobSchedulingContext{{
				JobId:                 "job-1",
				AssignedNode:          testfixtures.TestSimpleNode("node-2"),
				Job:                   makeJob(t, "job-1", false),
				PreemptionDescription: fmt.Sprintf(urgencyPreemptionMultiJobTemplate, "job-3,job-4"),
				PreemptionType:        context.PreemptedWithUrgencyPreemption,
			}},
		},
		"fairshare": {
			preemptedJobContexts: []*context.JobSchedulingContext{{
				JobId:         "job-1",
				AssignedNode:  testfixtures.TestSimpleNode("node-4"),
				Job:           makeJob(t, "job-1", false),
				PreemptingJob: makeJob(t, "job-7", false),
			}},
			expectedPreemptedJobContexts: []*context.JobSchedulingContext{{
				JobId:                 "job-1",
				AssignedNode:          testfixtures.TestSimpleNode("node-4"),
				Job:                   makeJob(t, "job-1", false),
				PreemptingJob:         makeJob(t, "job-7", false),
				PreemptionDescription: fmt.Sprintf(fairSharePreemptionTemplate, "job-7"),
				PreemptionType:        context.PreemptedWithFairsharePreemption,
			}},
		},
		"fairshare - gang": {
			preemptedJobContexts: []*context.JobSchedulingContext{
				{
					JobId:        "job-1",
					AssignedNode: testfixtures.TestSimpleNode("node-4"),
					Job:          makeJob(t, "job-1", true),
				},
				{
					JobId:         "job-8",
					AssignedNode:  testfixtures.TestSimpleNode("node-3"),
					Job:           makeJob(t, "job-8", true),
					PreemptingJob: makeJob(t, "job-7", false),
				},
			},
			expectedPreemptedJobContexts: []*context.JobSchedulingContext{
				{
					JobId:                 "job-1",
					AssignedNode:          testfixtures.TestSimpleNode("node-4"),
					Job:                   makeJob(t, "job-1", true),
					PreemptionDescription: fmt.Sprintf(gangSiblingFairSharePreemptionTemplate, describeGangMemberPreemptions([]preemptionInfo{{"job-8", "job-7"}})),
					PreemptionType:        context.PreemptedWithFairsharePreemption,
				},
				{
					JobId:                 "job-8",
					AssignedNode:          testfixtures.TestSimpleNode("node-3"),
					Job:                   makeJob(t, "job-8", true),
					PreemptingJob:         makeJob(t, "job-7", false),
					PreemptionDescription: fmt.Sprintf(fairSharePreemptionTemplate, "job-7"),
					PreemptionType:        context.PreemptedWithFairsharePreemption,
				},
			},
		},
		"fairshare - market based": {
			marketBased: true,
			preemptedJobContexts: []*context.JobSchedulingContext{{
				JobId:         "job-1",
				Job:           makeJobWithPrice(t, "job-1", false, 0),
				AssignedNode:  testfixtures.TestSimpleNode("node-4"),
				PreemptingJob: makeJobWithPrice(t, "job-7", false, 5),
			}},
			expectedPreemptedJobContexts: []*context.JobSchedulingContext{{
				JobId:                 "job-1",
				Job:                   makeJobWithPrice(t, "job-1", false, 0),
				AssignedNode:          testfixtures.TestSimpleNode("node-4"),
				PreemptingJob:         makeJobWithPrice(t, "job-7", false, 5),
				PreemptionDescription: fmt.Sprintf(marketBasedPreemptionTemplate, float64(0), "job-7", float64(5)),
				PreemptionType:        context.PreemptedWithFairsharePreemption,
			}},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			PopulatePreemptionDescriptions(tc.marketBased, testfixtures.TestPool, tc.preemptedJobContexts, scheduledJobContexts)
			assert.Equal(t, expectedScheduleJobContexts, scheduledJobContexts)
			assert.Equal(t, tc.expectedPreemptedJobContexts, tc.preemptedJobContexts)
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
	return makeJobWithPrice(t, jobId, isGang, 0)
}

func makeJobWithPrice(t *testing.T, jobId string, isGang bool, price float64) *jobdb.Job {
	annotations := map[string]string{}
	if isGang {
		annotations[constants.GangIdAnnotation] = "gang"
		annotations[constants.GangCardinalityAnnotation] = "2"
		annotations[constants.GangNodeUniformityLabelAnnotation] = "uniformity"
	}
	schedulingInfo := &internaltypes.JobSchedulingInfo{
		PodRequirements: &internaltypes.PodRequirements{
			Annotations: annotations,
		},
		PriorityClass: testfixtures.PriorityClass6Preemptible,
	}

	job, err := testfixtures.JobDb.NewJob(jobId, "jobset", "queue", 1, schedulingInfo, false, 1, false, false, false, 0, true, []string{}, 0)
	require.NoError(t, err)
	job = job.WithBidPrices(map[string]pricing.Bid{testfixtures.TestPool: {QueuedBid: price, RunningBid: price}})
	return job
}
