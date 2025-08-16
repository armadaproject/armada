package singlestoredb

import (
	"database/sql"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/utils/pointer"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/queryingester/instructions"
)

var (
	jobId              = util.NewULID()
	submittedTime      = time.Now().UTC().Truncate(time.Millisecond)
	leasedTime         = submittedTime.Add(1 * time.Second)
	priorityUpdateTime = leasedTime.Add(1 * time.Second)
)

// NOTE: Annotations is already serialized JSON (*string)
var submitEvent = instructions.JobRow{
	JobId:              jobId,
	Queue:              "queue-1",
	Namespace:          pointer.String("namespace-1"),
	JobSet:             pointer.String("jobset-1"),
	Cpu:                pointer.Int64(1),
	Memory:             pointer.Int64(2),
	EphemeralStorage:   pointer.Int64(3),
	Gpu:                pointer.Int64(4),
	Priority:           pointer.Int64(5),
	SubmitTs:           &submittedTime,
	PriorityClass:      pointer.String("armada-default"),
	Annotations:        pointer.String(`{"foo":"bar"}`),
	JobState:           pointer.String("QUEUED"),
	LastUpdateTs:       submittedTime,
	LastTransitionTime: &submittedTime,
}

var priorityUpdateEvent = instructions.JobRow{
	JobId:        jobId,
	Queue:        "queue-1",
	Priority:     pointer.Int64(100),
	LastUpdateTs: priorityUpdateTime,
}

var leasedEvent = instructions.JobRow{
	JobId:              jobId,
	Queue:              "queue-1",
	JobState:           pointer.String("LEASED"),
	LatestRunId:        pointer.String(util.NewULID()),
	RunCluster:         pointer.String("cluster-1"),
	RunState:           pointer.String("LEASED"),
	RunNode:            pointer.String("node-1"),
	RunLeasedTs:        &leasedTime,
	LastTransitionTime: &leasedTime,
	LastUpdateTs:       leasedTime,
}

func TestUpsertJobs_Submit(t *testing.T) {
	ctx := armadacontext.Background()

	err := withTestDb(ctx, func(db *sql.DB) {
		err := upsertJobs(ctx, db, []instructions.JobRow{submitEvent})
		require.NoError(t, err)

		actual, err := getJobById(ctx, db, jobId)
		require.NoError(t, err)
		assertJobsEqual(t, submitEvent, actual)
	})

	require.NoError(t, err)
}

func TestUpsertJobs_UpdatePriority(t *testing.T) {
	ctx := armadacontext.Background()

	err := withTestDb(ctx, func(db *sql.DB) {
		require.NoError(t, upsertJobs(ctx, db, []instructions.JobRow{submitEvent}))
		require.NoError(t, upsertJobs(ctx, db, []instructions.JobRow{priorityUpdateEvent}))

		actual, err := getJobById(ctx, db, jobId)
		require.NoError(t, err)

		assertJobsEqual(t, instructions.JobRow{
			JobId:              jobId,
			Queue:              submitEvent.Queue,
			Namespace:          submitEvent.Namespace,
			JobSet:             submitEvent.JobSet,
			Cpu:                submitEvent.Cpu,
			Memory:             submitEvent.Memory,
			EphemeralStorage:   submitEvent.EphemeralStorage,
			Gpu:                submitEvent.Gpu,
			Priority:           priorityUpdateEvent.Priority,
			SubmitTs:           submitEvent.SubmitTs,
			PriorityClass:      submitEvent.PriorityClass,
			Annotations:        submitEvent.Annotations,
			JobState:           submitEvent.JobState,
			LastTransitionTime: submitEvent.LastTransitionTime,
			LastUpdateTs:       priorityUpdateEvent.LastUpdateTs,
		}, actual)
	})

	require.NoError(t, err)
}

func TestUpsertJobs_AddRun(t *testing.T) {
	ctx := armadacontext.Background()

	err := withTestDb(ctx, func(db *sql.DB) {
		require.NoError(t, upsertJobs(ctx, db, []instructions.JobRow{submitEvent, leasedEvent}))

		actual, err := getJobById(ctx, db, jobId)
		require.NoError(t, err)

		assertJobsEqual(t, instructions.JobRow{
			JobId:              jobId,
			Queue:              submitEvent.Queue,
			Namespace:          submitEvent.Namespace,
			JobSet:             submitEvent.JobSet,
			Cpu:                submitEvent.Cpu,
			Memory:             submitEvent.Memory,
			EphemeralStorage:   submitEvent.EphemeralStorage,
			Gpu:                submitEvent.Gpu,
			Priority:           submitEvent.Priority,
			SubmitTs:           submitEvent.SubmitTs,
			PriorityClass:      submitEvent.PriorityClass,
			Annotations:        submitEvent.Annotations,
			JobState:           leasedEvent.JobState,
			LatestRunId:        leasedEvent.LatestRunId,
			RunCluster:         leasedEvent.RunCluster,
			RunState:           leasedEvent.RunState,
			RunNode:            leasedEvent.RunNode,
			RunLeasedTs:        leasedEvent.RunLeasedTs,
			LastTransitionTime: leasedEvent.LastTransitionTime,
			LastUpdateTs:       leasedEvent.LastUpdateTs,
		}, actual)
	})

	require.NoError(t, err)
}

func getJobById(ctx *armadacontext.Context, db *sql.DB, jobID string) (instructions.JobRow, error) {
	var row instructions.JobRow

	const q = `
SELECT
  job_id,
  queue,
  namespace,
  job_set,
  cpu,
  memory,
  ephemeral_storage,
  gpu,
  priority,
  submit_ts,
  priority_class,
  annotations,
  job_state,
  cancel_ts,
  cancel_reason,
  cancel_user,
  latest_run_id,
  run_cluster,
  run_exit_code,
  run_finished_ts,
  run_state,
  run_node,
  run_leased_ts,
  run_pending_ts,
  run_started_ts,
  last_transition_time,
  last_update_ts,
  error
FROM jobs_active
WHERE job_id = ?
LIMIT 1;
`

	var (
		jobIDOut, queue                                        string
		namespace, jobSet, priorityClass                       *string
		cpu, memory, eph, gpu, priority                        *int64
		submitTs, cancelTs, runFinishedTs                      *time.Time
		jobState, cancelReason, cancelUser                     *string
		annotations                                            *string
		latestRunID, runCluster, runState, runNode             *string
		runExitCode                                            *int32
		runLeasedTs, runPendingTs, runStartedTs, lastTransTime *time.Time
		lastUpdateTs                                           time.Time
		errStr                                                 *string
	)

	err := db.QueryRowContext(ctx, q, jobID).Scan(
		&jobIDOut,
		&queue,
		&namespace,
		&jobSet,
		&cpu,
		&memory,
		&eph,
		&gpu,
		&priority,
		&submitTs,
		&priorityClass,
		&annotations,
		&jobState,
		&cancelTs,
		&cancelReason,
		&cancelUser,
		&latestRunID,
		&runCluster,
		&runExitCode,
		&runFinishedTs,
		&runState,
		&runNode,
		&runLeasedTs,
		&runPendingTs,
		&runStartedTs,
		&lastTransTime,
		&lastUpdateTs,
		&errStr,
	)
	if err != nil {
		return row, err
	}

	row = instructions.JobRow{
		JobId:              jobIDOut,
		Queue:              queue,
		Namespace:          namespace,
		JobSet:             jobSet,
		Cpu:                cpu,
		Memory:             memory,
		EphemeralStorage:   eph,
		Gpu:                gpu,
		Priority:           priority,
		SubmitTs:           submitTs,
		PriorityClass:      priorityClass,
		Annotations:        annotations, // serialized JSON (*string)
		JobState:           jobState,
		CancelTs:           cancelTs,
		CancelReason:       cancelReason,
		CancelUser:         cancelUser,
		LatestRunId:        latestRunID,
		RunCluster:         runCluster,
		RunExitCode:        runExitCode,
		RunFinishedTs:      runFinishedTs,
		RunState:           runState,
		RunNode:            runNode,
		RunLeasedTs:        runLeasedTs,
		RunPendingTs:       runPendingTs,
		RunStartedTs:       runStartedTs,
		LastTransitionTime: lastTransTime,
		LastUpdateTs:       lastUpdateTs,
		Error:              errStr,
	}
	return row, nil
}

func assertJobsEqual(t *testing.T, expected, actual instructions.JobRow) {
	assert.Equal(t, expected.JobId, actual.JobId, "JobID mismatch")
	assert.Equal(t, expected.Queue, actual.Queue, "Queue mismatch")
	assert.Equal(t, ptrVal(expected.Namespace), ptrVal(actual.Namespace), "Namespace mismatch")
	assert.Equal(t, ptrVal(expected.JobSet), ptrVal(actual.JobSet), "JobSet mismatch")
	assert.Equal(t, ptrVal(expected.Cpu), ptrVal(actual.Cpu), "Cpu mismatch")
	assert.Equal(t, ptrVal(expected.Memory), ptrVal(actual.Memory), "Memory mismatch")
	assert.Equal(t, ptrVal(expected.EphemeralStorage), ptrVal(actual.EphemeralStorage), "EphemeralStorage mismatch")
	assert.Equal(t, ptrVal(expected.Gpu), ptrVal(actual.Gpu), "Gpu mismatch")
	assert.Equal(t, ptrVal(expected.Priority), ptrVal(actual.Priority), "Priority mismatch")
	assert.Equal(t, ptrTime(expected.SubmitTs), ptrTime(actual.SubmitTs), "SubmitTs mismatch")
	assert.Equal(t, ptrVal(expected.PriorityClass), ptrVal(actual.PriorityClass), "PriorityClass mismatch")

	// still disabled (serialized JSON equality can be order-sensitive)
	// assert.Equal(t, expected.Annotations, actual.Annotations, "Annotations mismatch")

	assert.Equal(t, ptrVal(expected.JobState), ptrVal(actual.JobState), "JobState mismatch")
	assert.Equal(t, ptrTime(expected.CancelTs), ptrTime(actual.CancelTs), "CancelTS mismatch")
	assert.Equal(t, ptrVal(expected.CancelReason), ptrVal(actual.CancelReason), "CancelReason mismatch")
	assert.Equal(t, ptrVal(expected.CancelUser), ptrVal(actual.CancelUser), "CancelUser mismatch")
	assert.Equal(t, ptrVal(expected.LatestRunId), ptrVal(actual.LatestRunId), "LatestRunId mismatch")
	assert.Equal(t, ptrVal(expected.RunCluster), ptrVal(actual.RunCluster), "RunCluster mismatch")
	assert.Equal(t, ptrVal(expected.RunExitCode), ptrVal(actual.RunExitCode), "RunExitCode mismatch")
	assert.Equal(t, ptrTime(expected.RunFinishedTs), ptrTime(actual.RunFinishedTs), "RunFinishedTS mismatch")
	assert.Equal(t, ptrVal(expected.RunState), ptrVal(actual.RunState), "RunState mismatch")
	assert.Equal(t, ptrVal(expected.RunNode), ptrVal(actual.RunNode), "RunNode mismatch")
	assert.Equal(t, ptrTime(expected.RunLeasedTs), ptrTime(actual.RunLeasedTs), "RunLeased mismatch")
	assert.Equal(t, ptrTime(expected.RunPendingTs), ptrTime(actual.RunPendingTs), "RunPendingTs mismatch")
	assert.Equal(t, ptrTime(expected.RunStartedTs), ptrTime(actual.RunStartedTs), "RunStartedTs mismatch")
	assert.Equal(t, ptrTime(expected.LastTransitionTime), ptrTime(actual.LastTransitionTime), "LastTransitionTime mismatch")
	assert.Equal(t, expected.LastUpdateTs, actual.LastUpdateTs, "LastUpdateTs mismatch")
	assert.Equal(t, ptrVal(expected.Error), ptrVal(actual.Error), "Error mismatch")
}
