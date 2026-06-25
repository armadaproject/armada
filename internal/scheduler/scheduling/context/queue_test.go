package context

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
)

func TestQueueSchedulingContext_UnscheduleJob(t *testing.T) {
	tests := map[string]struct {
		isExistingJob    bool
		setup            func(t *testing.T, sctx *SchedulingContext, jctx *JobSchedulingContext)
		expectUnschedule bool
	}{
		"scheduled this round": {
			setup: func(t *testing.T, sctx *SchedulingContext, jctx *JobSchedulingContext) {
				_, err := sctx.AddJobSchedulingContext(jctx)
				require.NoError(t, err)
			},
			expectUnschedule: true,
		},
		"evicted job is not unscheduled": {
			isExistingJob: true,
			setup: func(t *testing.T, sctx *SchedulingContext, jctx *JobSchedulingContext) {
				_, err := sctx.EvictJob(jctx)
				require.NoError(t, err)
			},
			expectUnschedule: false,
		},
		// UnscheduleJob only reverts jobs newly scheduled this round
		"rescheduled job is not unscheduled": {
			isExistingJob: true,
			setup: func(t *testing.T, sctx *SchedulingContext, jctx *JobSchedulingContext) {
				_, err := sctx.EvictJob(jctx)
				require.NoError(t, err)
				_, err = sctx.AddJobSchedulingContext(jctx)
				require.NoError(t, err)
			},
			expectUnschedule: false,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			jctx := testSmallCpuJobSchedulingContext("A", testfixtures.TestDefaultPriorityClass)
			sctx := createSchedulingContext(t, jctx, tc.isExistingJob)
			qctx := sctx.QueueSchedulingContexts["A"]
			tc.setup(t, sctx, jctx)

			scheduledInRound := qctx.UnscheduleJob(jctx)

			assert.Equal(t, tc.expectUnschedule, scheduledInRound)
			_, stillScheduled := qctx.SuccessfulJobSchedulingContexts[jctx.JobId]
			assert.False(t, stillScheduled, "job should never remain in SuccessfulJobSchedulingContexts")
			if tc.expectUnschedule {
				assert.True(t, qctx.Allocated.AllZero())
				for _, allocated := range qctx.AllocatedByPriorityClass {
					assert.True(t, allocated.AllZero())
				}
			}
		})
	}
}

func TestQueueSchedulingContext_RemoveJob(t *testing.T) {
	tests := map[string]struct {
		isExistingJob       bool
		setup               func(t *testing.T, sctx *SchedulingContext, jctx *JobSchedulingContext)
		expectedScheduled   bool
		expectedRescheduled bool
		expectedEvicted     bool
	}{
		"scheduled this round": {
			setup: func(t *testing.T, sctx *SchedulingContext, jctx *JobSchedulingContext) {
				_, err := sctx.AddJobSchedulingContext(jctx)
				require.NoError(t, err)
			},
			expectedScheduled: true,
		},
		"rescheduled this round": {
			isExistingJob: true,
			setup: func(t *testing.T, sctx *SchedulingContext, jctx *JobSchedulingContext) {
				_, err := sctx.EvictJob(jctx)
				require.NoError(t, err)
				_, err = sctx.AddJobSchedulingContext(jctx)
				require.NoError(t, err)
			},
			expectedRescheduled: true,
		},
		"evicted this round": {
			isExistingJob: true,
			setup: func(t *testing.T, sctx *SchedulingContext, jctx *JobSchedulingContext) {
				_, err := sctx.EvictJob(jctx)
				require.NoError(t, err)
			},
			expectedEvicted: true,
		},
		"optimiser-preempted this round": {
			isExistingJob: true,
			setup: func(t *testing.T, sctx *SchedulingContext, jctx *JobSchedulingContext) {
				_, err := sctx.PreemptJob(jctx)
				require.NoError(t, err)
			},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			jctx := testSmallCpuJobSchedulingContext("A", testfixtures.TestDefaultPriorityClass)
			sctx := createSchedulingContext(t, jctx, tc.isExistingJob)
			qctx := sctx.QueueSchedulingContexts["A"]
			tc.setup(t, sctx, jctx)

			assert.False(t, qctx.Demand.AllZero())
			scheduled, rescheduled, evicted := qctx.RemoveJob(jctx)

			assert.Equal(t, tc.expectedScheduled, scheduled)
			assert.Equal(t, tc.expectedRescheduled, rescheduled)
			assert.Equal(t, tc.expectedEvicted, evicted)

			assert.Empty(t, qctx.SuccessfulJobSchedulingContexts[jctx.JobId])
			assert.Empty(t, qctx.RescheduledJobSchedulingContexts[jctx.JobId])
			assert.Empty(t, qctx.PreemptedByOptimiserJobSchedulingContexts[jctx.JobId])
			assert.Empty(t, qctx.EvictedJobsById[jctx.JobId])

			assert.True(t, qctx.Demand.AllZero())
			assert.True(t, qctx.Allocated.AllZero())
			for _, allocated := range qctx.AllocatedByPriorityClass {
				assert.True(t, allocated.AllZero())
			}
			for _, allocated := range qctx.EvictedResourcesByPriorityClass {
				assert.True(t, allocated.AllZero())
			}
			for _, allocated := range qctx.PreemptedByOptimiserResourceByPriorityClass {
				assert.True(t, allocated.AllZero())
			}
			for _, allocated := range qctx.ScheduledResourcesByPriorityClass {
				assert.True(t, allocated.AllZero())
			}
		})
	}
}
