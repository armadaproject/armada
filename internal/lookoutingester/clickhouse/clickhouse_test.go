package clickhouse

import (
	"context"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/armadaproject/armada/internal/common/database/lookout"
	"github.com/armadaproject/armada/internal/common/ingest/testfixtures"
	armadaslices "github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/internal/lookoutingester/model"
	"github.com/stretchr/testify/require"
	"k8s.io/utils/pointer"
	"math/rand"
	"testing"
	"time"
)

func TestInsertJob(t *testing.T) {
	ctx := context.Background()

	err := withTestDb(ctx, func(db clickhouse.Conn) {
		instructions := []*model.CreateJobInstruction{
			{
				JobId:              testfixtures.JobId,
				Queue:              testfixtures.Queue,
				Owner:              testfixtures.UserId,
				Namespace:          testfixtures.Namespace,
				JobSet:             testfixtures.JobsetName,
				Cpu:                1,
				Memory:             2,
				EphemeralStorage:   3,
				Gpu:                4,
				Priority:           5,
				Submitted:          testfixtures.BaseTime,
				State:              lookout.JobQueuedOrdinal,
				LastTransitionTime: testfixtures.BaseTime,
				PriorityClass:      pointer.String("armada-default"),
				Annotations:        map[string]string{"foo": "bar"},
			},
		}

		err := InsertJobsBatch(ctx, db, armadaslices.Map(instructions, FromCreateJob))
		require.NoError(t, err)

		result, found, err := GetJobRow(ctx, db, testfixtures.JobId)
		require.NoError(t, err)
		require.True(t, found)

		// Assert fields match inserted values
		require.Equal(t, testfixtures.JobId, result.JobId)
		require.Equal(t, testfixtures.Queue, *result.Queue)
		require.Equal(t, testfixtures.Namespace, *result.Namespace)
		require.Equal(t, int64(1), *result.Cpu)
		require.Equal(t, int64(2), *result.Memory)
		require.Equal(t, int64(3), *result.EphemeralStorage)
		require.Equal(t, int64(4), *result.Gpu)
		require.Equal(t, int64(5), *result.Priority)
		require.Equal(t, testfixtures.BaseTime, *result.Submitted)
		require.Equal(t, "armada-default", *result.PriorityClass)
		require.Equal(t, map[string]string{"foo": "bar"}, result.Annotations)
	})

	require.NoError(t, err)
}

func TestUpdateJobPriority(t *testing.T) {
	ctx := context.Background()

	err := withTestDb(ctx, func(db clickhouse.Conn) {
		instructions := []*model.CreateJobInstruction{
			{
				JobId:              testfixtures.JobId,
				Queue:              testfixtures.Queue,
				Owner:              testfixtures.UserId,
				Namespace:          testfixtures.Namespace,
				JobSet:             testfixtures.JobsetName,
				Cpu:                1,
				Memory:             2,
				EphemeralStorage:   3,
				Gpu:                4,
				Priority:           5,
				Submitted:          testfixtures.BaseTime,
				State:              lookout.JobQueuedOrdinal,
				LastTransitionTime: testfixtures.BaseTime,
				PriorityClass:      pointer.String("armada-default"),
				Annotations:        map[string]string{"foo": "bar"},
			},
		}

		err := InsertJobsBatch(ctx, db, armadaslices.Map(instructions, FromCreateJob))
		require.NoError(t, err)

		update := []*model.UpdateJobInstruction{
			{
				JobId:    testfixtures.JobId,
				Priority: pointer.Int64(10),
			},
		}

		err = InsertJobsBatch(ctx, db, armadaslices.Map(update, FromUpdateJob))
		require.NoError(t, err)

		result, found, err := GetJobRow(ctx, db, testfixtures.JobId)
		require.NoError(t, err)
		require.True(t, found)

		// Assert fields match inserted values
		require.Equal(t, testfixtures.JobId, result.JobId)
		require.Equal(t, testfixtures.Queue, *result.Queue)
		require.Equal(t, testfixtures.Namespace, *result.Namespace)
		require.Equal(t, int64(1), *result.Cpu)
		require.Equal(t, int64(2), *result.Memory)
		require.Equal(t, int64(3), *result.EphemeralStorage)
		require.Equal(t, int64(4), *result.Gpu)
		require.Equal(t, int64(10), *result.Priority)
		require.Equal(t, testfixtures.BaseTime, *result.Submitted)
		require.Equal(t, "armada-default", *result.PriorityClass)
		require.Equal(t, map[string]string{"foo": "bar"}, result.Annotations)
	})
	require.NoError(t, err)
}

func TestAddJobRun(t *testing.T) {
	ctx := context.Background()

	err := withTestDb(ctx, func(db clickhouse.Conn) {
		instructions := []*model.CreateJobInstruction{
			{
				JobId:              testfixtures.JobId,
				Queue:              testfixtures.Queue,
				Owner:              testfixtures.UserId,
				Namespace:          testfixtures.Namespace,
				JobSet:             testfixtures.JobsetName,
				Cpu:                1,
				Memory:             2,
				EphemeralStorage:   3,
				Gpu:                4,
				Priority:           5,
				Submitted:          testfixtures.BaseTime,
				State:              lookout.JobQueuedOrdinal,
				LastTransitionTime: testfixtures.BaseTime,
				PriorityClass:      pointer.String("armada-default"),
				Annotations:        map[string]string{"foo": "bar"},
			},
		}

		err := InsertJobsBatch(ctx, db, armadaslices.Map(instructions, FromCreateJob))
		require.NoError(t, err)

		updateTime := testfixtures.BaseTime.Add(time.Minute)
		update := []*model.CreateJobRunInstruction{
			{
				RunId:       testfixtures.RunId,
				JobId:       testfixtures.JobId,
				Cluster:     testfixtures.ExecutorId,
				Pool:        testfixtures.Pool,
				Node:        pointer.String(testfixtures.NodeName),
				Leased:      &updateTime,
				JobRunState: lookout.JobRunLeasedOrdinal,
			},
		}

		err = InsertJobsBatch(ctx, db, armadaslices.Map(update, FromCreateJobRun))
		require.NoError(t, err)

		result, found, err := GetJobRow(ctx, db, testfixtures.JobId)
		require.NoError(t, err)
		require.True(t, found)

		// Assert fields match inserted values
		require.Equal(t, testfixtures.JobId, result.JobId)
		require.Equal(t, updateTime, *result.LastTransitionTime)
		require.Equal(t, int32(lookout.JobLeasedOrdinal), *result.JobState)
		require.Equal(t, testfixtures.RunId, *result.LatestRunId)
		require.Equal(t, testfixtures.ExecutorId, *result.RunCluster)
		require.Equal(t, testfixtures.NodeName, *result.RunNode)
		require.Equal(t, updateTime, *result.RunLeased)
		require.Equal(t, int32(lookout.JobRunLeasedOrdinal), *result.RunState)
	})
	require.NoError(t, err)
}

func TestUpdateJobRun(t *testing.T) {
	ctx := context.Background()

	err := withTestDb(ctx, func(db clickhouse.Conn) {
		instructions := []*model.CreateJobInstruction{
			{
				JobId:              testfixtures.JobId,
				Queue:              testfixtures.Queue,
				Owner:              testfixtures.UserId,
				Namespace:          testfixtures.Namespace,
				JobSet:             testfixtures.JobsetName,
				Cpu:                1,
				Memory:             2,
				EphemeralStorage:   3,
				Gpu:                4,
				Priority:           5,
				Submitted:          testfixtures.BaseTime,
				State:              lookout.JobQueuedOrdinal,
				LastTransitionTime: testfixtures.BaseTime,
				PriorityClass:      pointer.String("armada-default"),
				Annotations:        map[string]string{"foo": "bar"},
			},
		}

		err := InsertJobsBatch(ctx, db, armadaslices.Map(instructions, FromCreateJob))
		require.NoError(t, err)

		updateTime := testfixtures.BaseTime.Add(time.Minute)
		update := []*model.CreateJobRunInstruction{
			{
				RunId:       testfixtures.RunId,
				JobId:       testfixtures.JobId,
				Cluster:     testfixtures.ExecutorId,
				Pool:        testfixtures.Pool,
				Node:        pointer.String(testfixtures.NodeName),
				Leased:      &updateTime,
				JobRunState: lookout.JobRunLeasedOrdinal,
			},
		}

		err = InsertJobsBatch(ctx, db, armadaslices.Map(update, FromCreateJobRun))
		require.NoError(t, err)

		runUpdateTime := testfixtures.BaseTime.Add(2 * time.Minute)
		runUpdate := []*model.UpdateJobRunInstruction{
			{
				JobId:              testfixtures.JobId,
				RunId:              testfixtures.RunId,
				Finished:           &runUpdateTime,
				JobRunState:        pointer.Int32(lookout.JobRunSucceededOrdinal),
				ExitCode:           pointer.Int32(0),
				LastTransitionTime: &runUpdateTime,
			},
		}

		err = InsertJobsBatch(ctx, db, armadaslices.Map(runUpdate, FromUpdateJobRun))
		require.NoError(t, err)

		result, found, err := GetJobRow(ctx, db, testfixtures.JobId)
		require.NoError(t, err)
		require.True(t, found)

		// Assert fields match inserted values
		require.Equal(t, testfixtures.JobId, result.JobId)
		require.Equal(t, runUpdateTime, *result.LastTransitionTime)
		require.Equal(t, int32(lookout.JobLeasedOrdinal), *result.JobState)
		require.Equal(t, testfixtures.RunId, *result.LatestRunId)
		require.Equal(t, testfixtures.ExecutorId, *result.RunCluster)
		require.Equal(t, testfixtures.NodeName, *result.RunNode)
		require.Equal(t, updateTime, *result.RunLeased)
		require.Equal(t, runUpdateTime, *result.RunFinished)
		require.Equal(t, int32(lookout.JobRunSucceededOrdinal), *result.RunState)
		require.Equal(t, int32(0), *result.RunExitCode)
	})
	require.NoError(t, err)
}

func TestBulkInsertUpdateAndQueryPerformance(t *testing.T) {
	ctx := context.Background()
	const totalJobs = 10_000_000
	const batchSize = 10_000
	const lookups = 100

	err := withTestDb(ctx, func(db clickhouse.Conn) {

		stopMonitor, getMaxDelay := StartMergeDelayMonitor(ctx, db)
		defer stopMonitor()

		jobIds := make([]string, totalJobs)
		now := time.Now()
		for i := 0; i < totalJobs; i++ {
			jobIds[i] = fmt.Sprintf("job-%06d", i)
		}

		// Phase 1: Insert and update in batches
		startWrite := time.Now()
		for i := 0; i < totalJobs; i += batchSize {
			end := i + batchSize
			if end > totalJobs {
				end = totalJobs
			}
			createBatch := make([]*model.CreateJobInstruction, 0, end-i)
			updateBatch := make([]*model.UpdateJobInstruction, 0, end-i)

			for j := i; j < end; j++ {
				jobId := jobIds[j]
				createBatch = append(createBatch, &model.CreateJobInstruction{
					JobId:              jobId,
					Queue:              "queue-a",
					Owner:              "user",
					Namespace:          "ns",
					JobSet:             fmt.Sprintf("set-%d", j%10),
					Cpu:                1,
					Memory:             1,
					EphemeralStorage:   1,
					Gpu:                0,
					Priority:           5,
					Submitted:          now,
					State:              lookout.JobQueuedOrdinal,
					LastTransitionTime: now,
				})
				updateBatch = append(updateBatch, &model.UpdateJobInstruction{
					JobId:              jobId,
					Priority:           pointer.Int64(10),
					LastTransitionTime: timePtr(now.Add(time.Second)),
				})
			}

			require.NoError(t, InsertJobsBatch(ctx, db, armadaslices.Map(createBatch, FromCreateJob)))
			require.NoError(t, InsertJobsBatch(ctx, db, armadaslices.Map(updateBatch, FromUpdateJob)))
		}
		writeDuration := time.Since(startWrite)
		t.Logf("Write time for %d jobs (insert + update): %v", totalJobs, writeDuration)

		// Phase 2: Benchmark queries
		rand.Seed(42)
		selectedIds := make([]string, lookups)
		for i := range selectedIds {
			selectedIds[i] = jobIds[rand.Intn(totalJobs)]
		}

		// 1. Lookup by job_id
		startLookup := time.Now()
		for _, id := range selectedIds {
			_, found, err := GetJobRow(ctx, db, id)
			require.NoError(t, err)
			require.True(t, found)
		}
		t.Logf("Lookup %d jobs: %v", lookups, time.Since(startLookup))

		// 2. Count by queue, jobset, state
		startCount := time.Now()
		rows, err := db.Query(ctx, `
			SELECT queue, job_set, job_state, count()
			FROM jobs FINAL
			GROUP BY queue, job_set, job_state
		`)
		require.NoError(t, err)
		defer rows.Close()
		for rows.Next() {
			var queue, jobset string
			var state int32
			var count uint64
			require.NoError(t, rows.Scan(&queue, &jobset, &state, &count))
		}
		t.Logf("Group count by queue, jobset, state: %v", time.Since(startCount))

		// 3. Top 100 jobs in a queue/state by last_transition_time
		startTop := time.Now()
		rows, err = db.Query(ctx, `
			SELECT
		job_id, queue, namespace, job_set, cpu, memory, ephemeral_storage, gpu, priority,
		submitted, priority_class, annotations, job_state, cancelled, cancel_reason, cancel_user,
		last_transition_time, latest_run_id, run_cluster, run_exit_code, run_finished, run_state,
		run_node, run_leased, run_pending, run_started
			FROM jobs FINAL
			WHERE queue = 'queue-a' AND job_state = ?
			ORDER BY last_transition_time DESC
			LIMIT 100
		`, lookout.JobQueuedOrdinal)
		require.NoError(t, err)
		defer rows.Close()

		for rows.Next() {
			var row JobRow
			err = rows.Scan(
				&row.JobId,
				&row.Queue,
				&row.Namespace,
				&row.JobSet,
				&row.Cpu,
				&row.Memory,
				&row.EphemeralStorage,
				&row.Gpu,
				&row.Priority,
				&row.Submitted,
				&row.PriorityClass,
				&row.Annotations,
				&row.JobState,
				&row.Cancelled,
				&row.CancelReason,
				&row.CancelUser,
				&row.LastTransitionTime,
				&row.LatestRunId,
				&row.RunCluster,
				&row.RunExitCode,
				&row.RunFinished,
				&row.RunState,
				&row.RunNode,
				&row.RunLeased,
				&row.RunPending,
				&row.RunStarted,
			)
			require.NoError(t, err)
		}
		t.Logf("Top 100 jobs in queue-a with queued state: %v", time.Since(startTop))

		time.Sleep(20 * time.Second)

		t.Logf("Final max merge delay observed: %v", getMaxDelay())
	})

	require.NoError(t, err)
}

func GetJobRow(ctx context.Context, db clickhouse.Conn, jobId string) (JobRow, bool, error) {
	var row JobRow

	query := `SELECT
		job_id, queue, namespace, job_set, cpu, memory, ephemeral_storage, gpu, priority,
		submitted, priority_class, annotations, job_state, cancelled, cancel_reason, cancel_user,
		last_transition_time, latest_run_id, run_cluster, run_exit_code, run_finished, run_state,
		run_node, run_leased, run_pending, run_started
	FROM jobs_flat final
	WHERE job_id = ?`

	rows, err := db.Query(ctx, query, jobId)
	if err != nil {
		return JobRow{}, false, err
	}
	defer rows.Close()

	found := false
	for rows.Next() {
		if found {
			return JobRow{}, false, fmt.Errorf("multiple rows found for job_id %q — expected only one", jobId)
		}
		err = rows.Scan(
			&row.JobId,
			&row.Queue,
			&row.Namespace,
			&row.JobSet,
			&row.Cpu,
			&row.Memory,
			&row.EphemeralStorage,
			&row.Gpu,
			&row.Priority,
			&row.Submitted,
			&row.PriorityClass,
			&row.Annotations,
			&row.JobState,
			&row.Cancelled,
			&row.CancelReason,
			&row.CancelUser,
			&row.LastTransitionTime,
			&row.LatestRunId,
			&row.RunCluster,
			&row.RunExitCode,
			&row.RunFinished,
			&row.RunState,
			&row.RunNode,
			&row.RunLeased,
			&row.RunPending,
			&row.RunStarted,
		)
		if err != nil {
			return JobRow{}, false, err
		}
		found = true
	}

	if err := rows.Err(); err != nil {
		return JobRow{}, false, err
	}

	return row, found, nil
}

func timePtr(t time.Time) *time.Time {
	return &t
}
