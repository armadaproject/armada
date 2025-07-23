package clickhouse

import (
	"context"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/armadaproject/armada/internal/common/ingest/testfixtures"
	armadaslices "github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/internal/lookoutingester/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/utils/pointer"
	"testing"
	"time"
)

func TestInsertJob(t *testing.T) {
	ctx := context.Background()

	err := withTestDb(ctx, func(db clickhouse.Conn) {
		instructions := []model.CreateJobInstruction{
			{
				JobId:              testfixtures.JobId,
				Queue:              testfixtures.Queue,
				Owner:              testfixtures.UserId,
				Namespace:          testfixtures.Namespace,
				JobSet:             testfixtures.JobsetName,
				Cpu:                1,
				Memory:             2,
				EphemeralStorage:   3,
				Gpu:                3,
				Priority:           5,
				Submitted:          testfixtures.BaseTime,
				State:              0,
				LastTransitionTime: testfixtures.BaseTime,
				PriorityClass:      pointer.String("armada-default"),
				Annotations:        nil,
			},
		}

		err := InsertJobsBatch(ctx, db, armadaslices.Map(instructions, FromCreateJob))
		require.NoError(t, err)

		result, found, err := GetJobRow(ctx, db, testfixtures.JobId)
		require.NoError(t, err)
		require.True(t, found)

		// Assert fields match inserted values
		require.Equal(t, testfixtures.JobId, result.JobId)
		require.Equal(t, pointer.String(testfixtures.Queue), result.Queue)
		require.Equal(t, pointer.String(testfixtures.Namespace), result.Namespace)
		require.Equal(t, pointer.Int64(1), result.Cpu)
		require.Equal(t, pointer.Int64(3), result.Gpu)
		require.Equal(t, &testfixtures.BaseTime, result.Submitted)
		require.Equal(t, pointer.String("armada-default"), result.PriorityClass)
	})

	require.NoError(t, err)
}

func TestCoalescesMultipleEvents(t *testing.T) {
	ctx := context.Background()

	err := withTestDb(ctx, func(db clickhouse.Conn) {
		instructions := []model.CreateJobInstruction{
			{
				JobId:              testfixtures.JobId,
				Queue:              testfixtures.Queue,
				Owner:              testfixtures.UserId,
				Namespace:          testfixtures.Namespace,
				JobSet:             testfixtures.JobsetName,
				Cpu:                1,
				Memory:             2,
				EphemeralStorage:   3,
				Gpu:                3,
				Priority:           5,
				Submitted:          testfixtures.BaseTime,
				State:              0,
				LastTransitionTime: testfixtures.BaseTime,
				PriorityClass:      pointer.String("armada-default"),
				Annotations:        nil,
			},
		}

		err := InsertJobsBatch(ctx, db, armadaslices.Map(instructions, FromCreateJob))
		require.NoError(t, err)

		err = InsertJobsBatch(ctx, db, armadaslices.Map(instructions, FromCreateJob))
		require.NoError(t, err)

		result, found, err := GetJobRow(ctx, db, testfixtures.JobId)
		require.NoError(t, err)
		require.True(t, found)

		// Assert fields match inserted values
		require.Equal(t, testfixtures.JobId, result.JobId)
		require.Equal(t, pointer.String(testfixtures.Queue), result.Queue)
		require.Equal(t, pointer.String(testfixtures.Namespace), result.Namespace)
		require.Equal(t, pointer.Int64(1), result.Cpu)
		require.Equal(t, pointer.Int64(3), result.Gpu)
		require.Equal(t, &testfixtures.BaseTime, result.Submitted)
		require.Equal(t, pointer.String("armada-default"), result.PriorityClass)
	})

	require.NoError(t, err)
}

func TestUpdateJob(t *testing.T) {
	ctx := context.Background()

	err := withTestDb(ctx, func(db clickhouse.Conn) {
		// Initial insert
		createInstructions := []model.CreateJobInstruction{
			{
				JobId:              testfixtures.JobId,
				Queue:              testfixtures.Queue,
				Namespace:          "A",
				LastTransitionTime: testfixtures.BaseTime,
				PriorityClass:      pointer.String("armada-default"),
			},
		}
		err := InsertJobsBatch(ctx, db, armadaslices.Map(createInstructions, FromCreateJob))
		require.NoError(t, err)

		err = db.Exec(ctx, "OPTIMIZE TABLE jobs FINAL") // flush to one part
		require.NoError(t, err)

		// Update event (sparse)
		updateInstructions := []model.CreateJobInstruction{
			{
				JobId:              testfixtures.JobId,
				Namespace:          "B",
				LastTransitionTime: testfixtures.BaseTime.Add(1 * time.Minute),
			},
		}

		err = InsertJobsBatch(ctx, db, armadaslices.Map(updateInstructions, FromCreateJob))
		require.NoError(t, err)

		// Fetch coalesced row
		row, ok, err := GetJobRow(ctx, db, testfixtures.JobId)
		require.NoError(t, err)
		require.True(t, ok)
		assert.Equal(t, testfixtures.JobId, row.JobId)
		assert.Equal(t, "B", *row.Namespace)
		assert.Equal(t, "armada-default", row.PriorityClass)
	})

	require.NoError(t, err)
}

func TestCoalescingMergeTree(t *testing.T) {
	ctx := context.Background()

	err := withTestDb(ctx, func(conn clickhouse.Conn) {

		require.NoError(t, conn.Exec(ctx, `
			CREATE TABLE vehicle_state (
				vin String,
				battery_level Nullable(UInt8),
				odometer Nullable(UInt32),
				firmware Nullable(String),
				last_update Int64
			)
			ENGINE = CoalescingMergeTree()
			ORDER BY vin
		`))

		// Step 2: Insert sparse updates with NULLs
		require.NoError(t, conn.Exec(ctx, `
			INSERT INTO vehicle_state
			VALUES ('abc123', 95, NULL, 'fish', 1)
		`))
		require.NoError(t, conn.Exec(ctx, `
			INSERT INTO vehicle_state
			VALUES ('abc123', NULL, 12000, NULL, 2)
		`))

		require.NoError(t, conn.Exec(ctx, `
			INSERT INTO vehicle_state
			VALUES ('abc123', NULL, 13000,  'chips', 3)
		`))

		// Step 3: Query the final state using FINAL
		var (
			vin          string
			batteryLevel uint8
			odometer     uint32
			firmware     string
		)

		require.NoError(t, conn.QueryRow(ctx, `
			SELECT
    vin,
    argMax(battery_level, last_update) AS battery_level,
    argMax(odometer, last_update) AS odometer,
    argMax(firmware, last_update) AS firmware
FROM vehicle_state
GROUP BY vin
		`).Scan(&vin, &batteryLevel, &odometer, &firmware))

		require.Equal(t, "abc123", vin, "vin should be 'abc123'")
		require.Equal(t, uint8(95), batteryLevel, "battery_level should be 95")
		require.Equal(t, uint32(13000), odometer, "odometer should be 13000")
		require.Equal(t, "chips", firmware, "firmware should be chips")
	})

	require.NoError(t, err)
}

func GetJobRows(ctx context.Context, db clickhouse.Conn, jobId string) ([]JobRow, error) {
	query := `
		SELECT
			job_id, queue, namespace, job_set, cpu, memory, ephemeral_storage, gpu, priority,
			submitted, priority_class, annotations, job_state, cancelled, cancel_reason, cancel_user,
			last_transition_time, latest_run_id, run_cluster, run_exit_code, run_finished, run_state,
			run_node, run_leased, run_pending, run_started
		FROM jobs
		WHERE job_id = ?
	`

	rows, err := db.Query(ctx, query, jobId)
	if err != nil {
		return nil, fmt.Errorf("query jobs: %w", err)
	}
	defer rows.Close()

	var results []JobRow
	for rows.Next() {
		var row JobRow
		if err := rows.Scan(
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
		); err != nil {
			return nil, fmt.Errorf("scan job row: %w", err)
		}
		results = append(results, row)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration error: %w", err)
	}

	return results, nil
}

func GetJobRow(ctx context.Context, db clickhouse.Conn, jobId string) (JobRow, bool, error) {
	var row JobRow

	query := `SELECT
		job_id, queue, namespace, job_set, cpu, memory, ephemeral_storage, gpu, priority,
		submitted, priority_class, annotations, job_state, cancelled, cancel_reason, cancel_user,
		last_transition_time, latest_run_id, run_cluster, run_exit_code, run_finished, run_state,
		run_node, run_leased, run_pending, run_started
	FROM jobs FINAL
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
