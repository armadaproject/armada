package clickhousedb

import (
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
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
	Annotations:        map[string]any{"foo": "bar"},
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

func TestInsertJobs_Submit(t *testing.T) {
	ctx := armadacontext.Background()

	err := withTestDb(ctx, func(db clickhouse.Conn) {
		err := insertJobs(ctx, db, []instructions.JobRow{submitEvent})
		require.NoError(t, err)
		actual, err := getJobById(ctx, db, jobId)
		require.NoError(t, err)
		assertJobsEqual(t, submitEvent, actual)
	})

	require.NoError(t, err)
}

func TestInsertJobs_UpdatePriority(t *testing.T) {
	ctx := armadacontext.Background()

	err := withTestDb(ctx, func(db clickhouse.Conn) {
		err := insertJobs(ctx, db, []instructions.JobRow{submitEvent})
		require.NoError(t, err)
		err = insertJobs(ctx, db, []instructions.JobRow{priorityUpdateEvent})
		require.NoError(t, err)
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

func TestInsertJobs_AddRun(t *testing.T) {
	ctx := armadacontext.Background()

	err := withTestDb(ctx, func(db clickhouse.Conn) {
		err := insertJobs(ctx, db, []instructions.JobRow{submitEvent, leasedEvent})
		require.NoError(t, err)
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

func getJobById(ctx *armadacontext.Context, conn clickhouse.Conn, jobID string) (instructions.JobRow, error) {
	var row instructions.JobRow

	query := `
        SELECT
            job_id,
            CAST(queue AS Nullable(String)) AS queue,
            CAST(namespace AS Nullable(String)) AS namespace,
            job_set, cpu, memory, ephemeral_storage, gpu,
            priority, submit_ts,
            CAST(priority_class AS Nullable(String)) AS priority_class,
            annotations,
            CAST(job_state AS Nullable(String)) AS job_state,
            cancel_ts, cancel_reason, cancel_user, latest_run_id,
            CAST(run_cluster AS Nullable(String)) AS run_cluster,
            run_exit_code, run_finished_ts,
            CAST(run_state AS Nullable(String)) AS run_state,
            CAST(run_node AS Nullable(String)) AS run_node,
            run_leased_ts, run_pending_ts, run_started_ts,
            last_transition_time, last_update_ts, error
        FROM jobs FINAL
        WHERE job_id = ?
        LIMIT 1
    `

	if err := conn.QueryRow(ctx, query, jobID).ScanStruct(&row); err != nil {
		return row, err
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

	//Disable for now
	assert.Equal(t, expected.Annotations, actual.Annotations, "Annotations mismatch")

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
