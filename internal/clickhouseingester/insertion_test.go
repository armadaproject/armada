package clickhouseingester

import (
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/utils/pointer"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/util"
)

var jobId = util.NewULID()

var submittedTime = time.Now().UTC().Truncate(time.Second)
var leasedTime = submittedTime.Add(1 * time.Second)

var submitEvent = JobEvent{
	JobID:              jobId,
	Queue:              pointer.String("queue-1"),
	Namespace:          pointer.String("namespace-1"),
	JobSet:             pointer.String("jobset-1"),
	CPU:                pointer.Int64(1),
	Memory:             pointer.Int64(2),
	EphemeralStorage:   pointer.Int64(3),
	GPU:                pointer.Int64(4),
	Priority:           pointer.Int64(5),
	SubmitTS:           &submittedTime,
	PriorityClass:      pointer.String("armada-default"),
	Annotations:        map[string]string{"foo": "bar"},
	JobState:           pointer.String("QUEUED"),
	LastUpdateTS:       submittedTime,
	LastTransitionTime: &submittedTime,
	Merged:             pointer.Bool(true),
}

var leasedEvent = JobEvent{
	JobID:              jobId,
	JobState:           pointer.String("LEASED"),
	LatestRunID:        pointer.String(util.NewULID()),
	RunCluster:         pointer.String("cluster-1"),
	RunState:           pointer.String("LEASED"),
	RunNode:            pointer.String("node-1"),
	RunLeased:          &leasedTime,
	LastTransitionTime: &leasedTime,
	LastUpdateTS:       leasedTime,
}

func TestInsertJobEvents_Submit(t *testing.T) {
	ctx := armadacontext.Background()

	err := withTestDb(ctx, func(db clickhouse.Conn) {
		err := InsertJobEvents(ctx, db, []JobEvent{submitEvent})
		require.NoError(t, err)
		actual, err := GetJobEventByID(ctx, db, jobId)
		require.NoError(t, err)
		assertJobEventEqual(t, submitEvent, actual)
	})

	require.NoError(t, err)
}

func TestInsertJobEvents_AddRun(t *testing.T) {
	ctx := armadacontext.Background()

	err := withTestDb(ctx, func(db clickhouse.Conn) {
		err := InsertJobEvents(ctx, db, []JobEvent{submitEvent, leasedEvent})
		require.NoError(t, err)
		actual, err := GetJobEventByID(ctx, db, jobId)
		require.NoError(t, err)
		assertJobEventEqual(t, JobEvent{
			JobID:              jobId,
			Queue:              submitEvent.Queue,
			Namespace:          submitEvent.Namespace,
			JobSet:             submitEvent.JobSet,
			CPU:                submitEvent.CPU,
			Memory:             submitEvent.Memory,
			EphemeralStorage:   submitEvent.EphemeralStorage,
			GPU:                submitEvent.GPU,
			Priority:           submitEvent.Priority,
			SubmitTS:           submitEvent.SubmitTS,
			PriorityClass:      submitEvent.PriorityClass,
			Annotations:        submitEvent.Annotations,
			JobState:           leasedEvent.JobState,
			LatestRunID:        leasedEvent.LatestRunID,
			RunCluster:         leasedEvent.RunCluster,
			RunState:           leasedEvent.RunState,
			RunNode:            leasedEvent.RunNode,
			RunLeased:          leasedEvent.RunLeased,
			LastTransitionTime: leasedEvent.LastTransitionTime,
			LastUpdateTS:       leasedEvent.LastUpdateTS,
			Merged:             pointer.Bool(true),
		}, actual)
	})

	require.NoError(t, err)
}

func GetJobEventByID(ctx *armadacontext.Context, conn clickhouse.Conn, jobID string) (JobEvent, error) {
	var row JobEvent

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
            run_leased, run_pending_ts, run_started_ts,
            last_transition_time, last_update_ts, merged
        FROM jobs FINAL
        WHERE job_id = ?
        LIMIT 1
    `

	if err := conn.QueryRow(ctx, query, jobID).ScanStruct(&row); err != nil {
		return row, err
	}

	return row, nil
}

func assertJobEventEqual(t *testing.T, expected, actual JobEvent) {
	assert.Equal(t, expected.JobID, actual.JobID, "JobID mismatch")
	assert.Equal(t, ptrVal(expected.Queue), ptrVal(actual.Queue), "Queue mismatch")
	assert.Equal(t, ptrVal(expected.Namespace), ptrVal(actual.Namespace), "Namespace mismatch")
	assert.Equal(t, ptrVal(expected.JobSet), ptrVal(actual.JobSet), "JobSet mismatch")
	assert.Equal(t, ptrVal(expected.CPU), ptrVal(actual.CPU), "CPU mismatch")
	assert.Equal(t, ptrVal(expected.Memory), ptrVal(actual.Memory), "Memory mismatch")
	assert.Equal(t, ptrVal(expected.EphemeralStorage), ptrVal(actual.EphemeralStorage), "EphemeralStorage mismatch")
	assert.Equal(t, ptrVal(expected.GPU), ptrVal(actual.GPU), "GPU mismatch")
	assert.Equal(t, ptrVal(expected.Priority), ptrVal(actual.Priority), "Priority mismatch")
	assert.Equal(t, ptrTime(expected.SubmitTS), ptrTime(actual.SubmitTS), "SubmitTS mismatch")
	assert.Equal(t, ptrVal(expected.PriorityClass), ptrVal(actual.PriorityClass), "PriorityClass mismatch")
	assert.Equal(t, expected.Annotations, actual.Annotations, "Annotations mismatch")
	assert.Equal(t, ptrVal(expected.JobState), ptrVal(actual.JobState), "JobState mismatch")
	assert.Equal(t, ptrTime(expected.CancelTS), ptrTime(actual.CancelTS), "CancelTS mismatch")
	assert.Equal(t, ptrVal(expected.CancelReason), ptrVal(actual.CancelReason), "CancelReason mismatch")
	assert.Equal(t, ptrVal(expected.CancelUser), ptrVal(actual.CancelUser), "CancelUser mismatch")
	assert.Equal(t, ptrVal(expected.LatestRunID), ptrVal(actual.LatestRunID), "LatestRunID mismatch")
	assert.Equal(t, ptrVal(expected.RunCluster), ptrVal(actual.RunCluster), "RunCluster mismatch")
	assert.Equal(t, ptrVal(expected.RunExitCode), ptrVal(actual.RunExitCode), "RunExitCode mismatch")
	assert.Equal(t, ptrTime(expected.RunFinishedTS), ptrTime(actual.RunFinishedTS), "RunFinishedTS mismatch")
	assert.Equal(t, ptrVal(expected.RunState), ptrVal(actual.RunState), "RunState mismatch")
	assert.Equal(t, ptrVal(expected.RunNode), ptrVal(actual.RunNode), "RunNode mismatch")
	assert.Equal(t, ptrTime(expected.RunLeased), ptrTime(actual.RunLeased), "RunLeased mismatch")
	assert.Equal(t, ptrTime(expected.RunPendingTS), ptrTime(actual.RunPendingTS), "RunPendingTS mismatch")
	assert.Equal(t, ptrTime(expected.RunStartedTS), ptrTime(actual.RunStartedTS), "RunStartedTS mismatch")
	assert.Equal(t, ptrTime(expected.LastTransitionTime), ptrTime(actual.LastTransitionTime), "LastTransitionTime mismatch")
	assert.Equal(t, expected.LastUpdateTS, actual.LastUpdateTS, "LastUpdateTS mismatch")
	assert.Equal(t, ptrBool(expected.Merged), ptrBool(actual.Merged), "Merged mismatch")
}

func ptrVal[T comparable](p *T) T {
	if p != nil {
		return *p
	}
	var zero T
	return zero
}

func ptrBool(p *bool) bool {
	if p != nil {
		return *p
	}
	return false
}

func ptrTime(p *time.Time) time.Time {
	if p != nil {
		return *p
	}
	return time.Time{}
}
