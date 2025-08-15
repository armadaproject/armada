package clickhousedb

import (
	"github.com/armadaproject/armada/internal/queryingester/instructions"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/utils/pointer"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/util"
)

var (
	runId           = util.NewULID()
	runLeasedTime   = time.Now().UTC().Truncate(time.Second)
	runPendingTime  = runLeasedTime.Add(1 * time.Second)
	runStartedTime  = runPendingTime.Add(1 * time.Second)
	runFinishedTime = runStartedTime.Add(1 * time.Second)
)

var runLeasedEvent = instructions.JobRunRow{
	JobId:    jobId,
	RunId:    runId,
	Cluster:  pointer.String("cluster-1"),
	State:    pointer.String("LEASED"),
	Node:     pointer.String("node-1"),
	LeasedTs: &runLeasedTime,
	Merged:   pointer.Bool(true),
}

var runPendingEvent = instructions.JobRunRow{
	JobId:     jobId,
	RunId:     runId,
	State:     pointer.String("PENDING"),
	PendingTs: &runPendingTime,
}

var runStartedEvent = instructions.JobRunRow{
	JobId:     jobId,
	RunId:     runId,
	State:     pointer.String("RUNNING"),
	StartedTS: &runStartedTime,
}

var runFinishedEvent = instructions.JobRunRow{
	JobId:      jobId,
	RunId:      runId,
	State:      pointer.String("SUCCEEDED"),
	ExitCode:   pointer.Int32(0),
	FinishedTS: &runFinishedTime,
}

func TestInsertJobRuns_Lease(t *testing.T) {
	ctx := armadacontext.Background()

	err := withTestDb(ctx, func(db clickhouse.Conn) {
		err := insertJobRuns(ctx, db, []instructions.JobRunRow{runLeasedEvent})
		require.NoError(t, err)
		actual, err := getJobRunById(ctx, db, runId)
		require.NoError(t, err)
		assertJobRunsEqual(t, runLeasedEvent, actual)
	})
	require.NoError(t, err)
}

func TestInsertJobRuns_UpdateStarted(t *testing.T) {
	ctx := armadacontext.Background()

	err := withTestDb(ctx, func(db clickhouse.Conn) {
		// leased -> running
		require.NoError(t, insertJobRuns(ctx, db, []instructions.JobRunRow{runLeasedEvent}))
		require.NoError(t, insertJobRuns(ctx, db, []instructions.JobRunRow{runStartedEvent}))

		actual, err := getJobRunById(ctx, db, runId)
		require.NoError(t, err)

		assertJobRunsEqual(t, instructions.JobRunRow{
			JobId:     jobId,
			RunId:     runId,
			Cluster:   runLeasedEvent.Cluster,
			State:     runStartedEvent.State,
			Node:      runLeasedEvent.Node,
			LeasedTs:  runLeasedEvent.LeasedTs,
			StartedTS: runStartedEvent.StartedTS,
			Merged:    pointer.Bool(true),
		}, actual)
	})
	require.NoError(t, err)
}

func TestInsertJobRuns_Finish(t *testing.T) {
	ctx := armadacontext.Background()

	err := withTestDb(ctx, func(db clickhouse.Conn) {
		// leased -> pending -> running -> succeeded
		require.NoError(t, insertJobRuns(ctx, db, []instructions.JobRunRow{runLeasedEvent}))
		require.NoError(t, insertJobRuns(ctx, db, []instructions.JobRunRow{runPendingEvent}))
		require.NoError(t, insertJobRuns(ctx, db, []instructions.JobRunRow{runStartedEvent}))
		require.NoError(t, insertJobRuns(ctx, db, []instructions.JobRunRow{runFinishedEvent}))

		actual, err := getJobRunById(ctx, db, runId)
		require.NoError(t, err)

		assertJobRunsEqual(t, instructions.JobRunRow{
			JobId:      jobId,
			RunId:      runId,
			Cluster:    runLeasedEvent.Cluster,
			State:      runFinishedEvent.State,
			Node:       runLeasedEvent.Node,
			LeasedTs:   runLeasedEvent.LeasedTs,
			PendingTs:  runPendingEvent.PendingTs,
			StartedTS:  runStartedEvent.StartedTS,
			FinishedTS: runFinishedEvent.FinishedTS,
			ExitCode:   runFinishedEvent.ExitCode,
			Merged:     pointer.Bool(true),
		}, actual)
	})
	require.NoError(t, err)
}

// Assert equality for JobRunRow (mirrors the JobRow comparator)
func assertJobRunsEqual(t *testing.T, expected, actual instructions.JobRunRow) {
	assert.Equal(t, expected.JobId, actual.JobId, "JobId mismatch")
	assert.Equal(t, expected.RunId, actual.RunId, "RunId mismatch")
	assert.Equal(t, ptrVal(expected.Cluster), ptrVal(actual.Cluster), "Cluster mismatch")
	assert.Equal(t, ptrVal(expected.ExitCode), ptrVal(actual.ExitCode), "ExitCode mismatch")
	assert.Equal(t, ptrVal(expected.State), ptrVal(actual.State), "State mismatch")
	assert.Equal(t, ptrVal(expected.Node), ptrVal(actual.Node), "RunNode mismatch")
	assert.Equal(t, ptrTime(expected.LeasedTs), ptrTime(actual.LeasedTs), "LeasedTs mismatch")
	assert.Equal(t, ptrTime(expected.PendingTs), ptrTime(actual.PendingTs), "PendingTs mismatch")
	assert.Equal(t, ptrTime(expected.StartedTS), ptrTime(actual.StartedTS), "StartedTs mismatch")
	assert.Equal(t, ptrTime(expected.FinishedTS), ptrTime(actual.FinishedTS), "FinishedTs mismatch")
	assert.Equal(t, ptrBool(expected.Merged), ptrBool(actual.Merged), "Merged mismatch")
}

// Helper to fetch a job run row
func getJobRunById(ctx *armadacontext.Context, conn clickhouse.Conn, runId string) (instructions.JobRunRow, error) {
	var row instructions.JobRunRow

	query := `
		SELECT
			job_id,
			run_id,
			CAST(cluster AS Nullable(String)) AS cluster,
			exit_code,
			CAST(state AS Nullable(String)) AS state,
			CAST(node AS Nullable(String)) AS node,
			leased_ts,
			pending_ts,
			started_ts,
			finished_ts,
			merged
		FROM job_runs FINAL
		WHERE run_id = ?
		LIMIT 1
	`
	if err := conn.QueryRow(ctx, query, runId).ScanStruct(&row); err != nil {
		return row, err
	}
	return row, nil
}
