package jobdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	protoutil "github.com/armadaproject/armada/internal/common/proto"
	"github.com/armadaproject/armada/internal/scheduler/database"
	schedulerobjects "github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

var testSchedulingInfoBytes = protoutil.MustMarshall(&schedulerobjects.JobSchedulingInfo{
	ObjectRequirements: []*schedulerobjects.ObjectRequirements{
		{
			Requirements: &schedulerobjects.ObjectRequirements_PodRequirements{
				PodRequirements: &schedulerobjects.PodRequirements{},
			},
		},
	},
})

func strPtr(s string) *string { return &s }

// TestSchedulerJobFromDatabaseJob_PreservesCancelMetadata verifies that when a job is
// first loaded from the DB (no existing in-memory job), cancel_user and cancel_reason
// are carried into the in-memory Job struct.
func TestSchedulerJobFromDatabaseJob_PreservesCancelMetadata(t *testing.T) {
	jobDb := NewTestJobDb()
	cancelUser := "alice"
	cancelReason := "no longer needed"

	dbJob := &database.Job{
		JobID:           "job-1",
		JobSet:          "set-1",
		Queue:           "queue-1",
		SchedulingInfo:  testSchedulingInfoBytes,
		CancelRequested: true,
		CancelUser:      &cancelUser,
		CancelReason:    &cancelReason,
	}

	job, err := jobDb.schedulerJobFromDatabaseJob(dbJob)
	require.NoError(t, err)

	require.NotNil(t, job.CancelUser())
	assert.Equal(t, cancelUser, *job.CancelUser())
	require.NotNil(t, job.CancelReason())
	assert.Equal(t, cancelReason, *job.CancelReason())
}

// TestSchedulerJobFromDatabaseJob_NilCancelMetadataIsPreserved verifies that nil
// cancel metadata in the DB results in nil fields on the in-memory job (no panic).
func TestSchedulerJobFromDatabaseJob_NilCancelMetadataIsPreserved(t *testing.T) {
	jobDb := NewTestJobDb()

	dbJob := &database.Job{
		JobID:          "job-1",
		JobSet:         "set-1",
		Queue:          "queue-1",
		SchedulingInfo: testSchedulingInfoBytes,
	}

	job, err := jobDb.schedulerJobFromDatabaseJob(dbJob)
	require.NoError(t, err)
	assert.Nil(t, job.CancelUser())
	assert.Nil(t, job.CancelReason())
}

// TestReconcileJobDifferences_CancelReasonValueComparison verifies that reconciliation
// compares cancel reason by value not pointer, so two *string with the same text do not
// trigger a spurious job copy every cycle.
func TestReconcileJobDifferences_CancelReasonValueComparison(t *testing.T) {
	jobDb := NewTestJobDb()

	// Build an in-memory job that already has a cancel reason set.
	reason := "test reason"
	existingJob := newJob().WithCancelReason(strPtr(reason))

	// DB row returns a different pointer with the same string value.
	dbJob := &database.Job{
		JobID:          existingJob.Id(),
		JobSet:         existingJob.Jobset(),
		Queue:          existingJob.Queue(),
		SchedulingInfo: testSchedulingInfoBytes,
		CancelReason:   strPtr(reason), // fresh pointer, same value
	}

	jst, err := jobDb.reconcileJobDifferences(existingJob, dbJob, nil)
	require.NoError(t, err)

	// The job pointer should be unchanged — no copy was made due to equal values.
	assert.Same(t, existingJob, jst.Job, "reconcile should not create a new Job when cancel reason value is unchanged")
}

// TestReconcileJobDifferences_CancelReasonUpdatesWhenChanged verifies that a changed
// cancel reason in the DB is propagated to the in-memory job.
func TestReconcileJobDifferences_CancelReasonUpdatesWhenChanged(t *testing.T) {
	jobDb := NewTestJobDb()

	existingJob := newJob().WithCancelReason(strPtr("old reason"))

	dbJob := &database.Job{
		JobID:          existingJob.Id(),
		JobSet:         existingJob.Jobset(),
		Queue:          existingJob.Queue(),
		SchedulingInfo: testSchedulingInfoBytes,
		CancelReason:   strPtr("new reason"),
	}

	jst, err := jobDb.reconcileJobDifferences(existingJob, dbJob, nil)
	require.NoError(t, err)
	require.NotNil(t, jst.Job.CancelReason())
	assert.Equal(t, "new reason", *jst.Job.CancelReason())
}

// TestReconcileJobDifferences_InitialLoadWithCancelReason verifies that when a job is
// seen for the first time (nil in-memory job) and the DB row already has a cancel reason,
// the resulting in-memory job carries that reason.
func TestReconcileJobDifferences_InitialLoadWithCancelReason(t *testing.T) {
	jobDb := NewTestJobDb()
	cancelReason := "cancelled before scheduler restart"

	dbJob := &database.Job{
		JobID:          "job-1",
		JobSet:         "set-1",
		Queue:          "queue-1",
		SchedulingInfo: testSchedulingInfoBytes,
		CancelReason:   &cancelReason,
	}

	jst, err := jobDb.reconcileJobDifferences(nil, dbJob, nil)
	require.NoError(t, err)
	require.NotNil(t, jst.Job)
	require.NotNil(t, jst.Job.CancelReason())
	assert.Equal(t, cancelReason, *jst.Job.CancelReason())
}
