package schema

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/internal/common/armadacontext"
)

var testExecuteOptions = ExecuteOptions{LockTimeout: time.Second, LockAttempts: 1, ProgressInterval: time.Minute}

// insertJobsInEveryState inserts perState jobs in each of the nine job
// states, with every column populated, and returns the total.
func insertJobsInEveryState(t *testing.T, db *pgxpool.Pool, perState int) int {
	t.Helper()
	_, err := db.Exec(armadacontext.Background(), `
		INSERT INTO job (
			job_id, queue, owner, jobset, cpu, memory, ephemeral_storage, gpu, priority, submitted, cancelled,
			state, last_transition_time, last_transition_time_seconds, job_spec, duplicate, priority_class,
			latest_run_id, cancel_reason, namespace, annotations, external_job_uri, cancel_user, preempt_user,
			reprioritize_user
		)
		SELECT
			format('job-%s-%s', s, i), format('queue-%s', i % 3), 'owner', format('jobset-%s', i % 5),
			i, i * 1024, i * 2048, i % 2, i % 10, timestamp '2026-01-01' - i * interval '1 minute', timestamp '2026-01-02',
			s, timestamp '2026-01-03', 1767398400, decode(repeat('ab', 4096), 'hex'), i % 2 = 0, 'armada-default',
			format('run-%s-%s', s, i), 'reason', 'namespace', jsonb_build_object('key', i::text), 'uri', 'canceller',
			'preempter', 'reprioritiser'
		FROM generate_series(1, 9) AS s, generate_series(1, $1) AS i
	`, perState)
	require.NoError(t, err)
	return 9 * perState
}

func jobRows(t *testing.T, db *pgxpool.Pool) []string {
	t.Helper()
	rows, err := db.Query(armadacontext.Background(), `SELECT job_id || ' ' || md5(to_jsonb(j)::text) FROM job j ORDER BY job_id`)
	require.NoError(t, err)
	result, err := pgx.CollectRows(rows, pgx.RowTo[string])
	require.NoError(t, err)
	return result
}

func withConn(t *testing.T, db *pgxpool.Pool, action func(conn *pgx.Conn)) {
	t.Helper()
	conn, err := db.Acquire(armadacontext.Background())
	require.NoError(t, err)
	defer conn.Release()
	action(conn.Conn())
}

func TestExecutePartitionJobTable_PreservesDataAndIsIdempotent(t *testing.T) {
	err := withLookoutChainAppliedDb(func(db *pgxpool.Pool) error {
		ctx := armadacontext.Background()
		total := insertJobsInEveryState(t, db, 20)
		before := jobRows(t, db)

		withConn(t, db, func(conn *pgx.Conn) {
			converted, err := ExecutePartitionJobTable(ctx, conn, testExecuteOptions)
			require.NoError(t, err)
			assert.True(t, converted)

			converted, err = ExecutePartitionJobTable(ctx, conn, testExecuteOptions)
			require.NoError(t, err)
			assert.False(t, converted, "a second run should find the table already partitioned")
		})

		shape, detail, err := DetectJobTableShape(ctx, db)
		require.NoError(t, err)
		assert.Equal(t, JobTableShapePartitioned, shape, detail)
		assert.Equal(t, before, jobRows(t, db))

		var active, terminated int
		require.NoError(t, db.QueryRow(ctx, `SELECT (SELECT count(*) FROM job_active), (SELECT count(*) FROM job_terminated)`).Scan(&active, &terminated))
		assert.Equal(t, total*4/9, active)
		assert.Equal(t, total*5/9, terminated)
		return nil
	})
	require.NoError(t, err)
}

func TestConversionScript_MatchesExecutePartitionJobTable(t *testing.T) {
	ctx := armadacontext.Background()
	var expectedCatalogue, expectedRows []string
	err := withLookoutChainAppliedDb(func(db *pgxpool.Pool) error {
		insertJobsInEveryState(t, db, 5)
		withConn(t, db, func(conn *pgx.Conn) {
			_, err := ExecutePartitionJobTable(ctx, conn, testExecuteOptions)
			require.NoError(t, err)
		})
		expectedCatalogue, expectedRows = catalogue(t, db), jobRows(t, db)
		return nil
	})
	require.NoError(t, err)

	err = withLookoutChainAppliedDb(func(db *pgxpool.Pool) error {
		insertJobsInEveryState(t, db, 5)
		script, err := ConversionScript(ctx, db)
		require.NoError(t, err)
		assert.True(t, strings.HasPrefix(script, "BEGIN;\n"), script)
		assert.True(t, strings.HasSuffix(script, "COMMIT;\n\n"), script)

		_, err = db.Exec(ctx, script)
		require.NoError(t, err)
		assert.Equal(t, expectedCatalogue, catalogue(t, db))
		assert.Equal(t, expectedRows, jobRows(t, db))
		return nil
	})
	require.NoError(t, err)
}

func TestExecutePartitionJobTable_RefusesUnlessMigrationChainIsFullyApplied(t *testing.T) {
	migrations, err := LookoutMigrations()
	require.NoError(t, err)
	latest := migrations[len(migrations)-1].ID()

	for name, version := range map[string]int{"behind": latest - 1, "ahead": latest + 1} {
		t.Run(name, func(t *testing.T) {
			err := withLookoutChainAppliedDb(func(db *pgxpool.Pool) error {
				ctx := armadacontext.Background()
				_, err := db.Exec(ctx, `SELECT setval('database_version', $1)`, version)
				require.NoError(t, err)

				withConn(t, db, func(conn *pgx.Conn) {
					_, err := ExecutePartitionJobTable(ctx, conn, testExecuteOptions)
					require.Error(t, err)
					assert.Contains(t, err.Error(), fmt.Sprintf("migration %d", version))
				})

				partitioned, err := IsJobTablePartitioned(ctx, db)
				require.NoError(t, err)
				assert.False(t, partitioned)
				return nil
			})
			require.NoError(t, err)
		})
	}
}

func TestExecutePartitionJobTable_SecondConcurrentRunFailsFast(t *testing.T) {
	err := withLookoutChainAppliedDb(func(db *pgxpool.Pool) error {
		ctx := armadacontext.Background()
		withConn(t, db, func(firstRun *pgx.Conn) {
			_, err := firstRun.Exec(ctx, `SELECT pg_advisory_lock($1)`, partitionAdvisoryLockKey)
			require.NoError(t, err)

			withConn(t, db, func(secondRun *pgx.Conn) {
				start := time.Now()
				_, err := ExecutePartitionJobTable(ctx, secondRun, ExecuteOptions{LockTimeout: time.Minute, LockAttempts: 1, ProgressInterval: time.Minute})
				require.Error(t, err)
				assert.Contains(t, err.Error(), "already running")
				assert.Less(t, time.Since(start), 5*time.Second)
			})
		})

		partitioned, err := IsJobTablePartitioned(ctx, db)
		require.NoError(t, err)
		assert.False(t, partitioned)
		return nil
	})
	require.NoError(t, err)
}

func TestExecutePartitionJobTable_RetriesTheLock(t *testing.T) {
	retryOptions := func(attempts int) ExecuteOptions {
		return ExecuteOptions{LockTimeout: 100 * time.Millisecond, LockAttempts: attempts, ProgressInterval: time.Minute}
	}

	err := withLookoutChainAppliedDb(func(db *pgxpool.Pool) error {
		ctx := armadacontext.Background()
		blocker, err := db.Begin(ctx)
		require.NoError(t, err)
		defer func() { _ = blocker.Rollback(ctx) }()
		_, err = blocker.Exec(ctx, `SELECT count(*) FROM job`)
		require.NoError(t, err)

		withConn(t, db, func(conn *pgx.Conn) {
			_, err := ExecutePartitionJobTable(ctx, conn, retryOptions(3))
			require.Error(t, err)
			assert.Contains(t, err.Error(), "3 attempts")

			go func() {
				time.Sleep(500 * time.Millisecond)
				_ = blocker.Rollback(ctx)
			}()
			converted, err := ExecutePartitionJobTable(ctx, conn, retryOptions(100))
			require.NoError(t, err)
			assert.True(t, converted)
		})
		return nil
	})
	require.NoError(t, err)
}

func TestPreflight(t *testing.T) {
	err := withLookoutChainAppliedDb(func(db *pgxpool.Pool) error {
		ctx := armadacontext.Background()
		total := insertJobsInEveryState(t, db, 100)
		_, err := db.Exec(ctx, `ANALYZE job`)
		require.NoError(t, err)
		_, err = db.Exec(ctx, `DO $$ BEGIN EXECUTE format('ALTER DATABASE %I SET lock_timeout = %L', current_database(), '10s'); END $$`)
		require.NoError(t, err)

		blocker, err := db.Begin(ctx)
		require.NoError(t, err)
		defer func() { _ = blocker.Rollback(ctx) }()
		_, err = blocker.Exec(ctx, `SELECT count(*) FROM job`)
		require.NoError(t, err)

		report, err := Preflight(ctx, db)
		require.NoError(t, err)

		assert.Equal(t, JobTableShapeUnpartitioned, report.Shape)
		assert.Equal(t, report.ExpectedMigration, report.AppliedMigration)
		assert.GreaterOrEqual(t, report.ServerVersionNum, minimumPostgresVersionNum)
		assert.Positive(t, report.HeapBytes)
		assert.Positive(t, report.ToastBytes, "job_spec is large enough to be stored in TOAST")
		assert.Positive(t, report.IndexBytes)
		assert.Positive(t, report.LargestIndexBytes)
		assert.Greater(t, report.RequiredFreeBytes(), report.HeapBytes+report.ToastBytes+report.IndexBytes)
		assert.Equal(t, int64(total), report.EstimatedRows)
		require.NotNil(t, report.ActiveFraction)
		assert.InDelta(t, 4.0/9, *report.ActiveFraction, 0.01)
		require.Len(t, report.LockHolders, 1)
		assert.Equal(t, "AccessShareLock", report.LockHolders[0].Mode)
		assert.True(t, report.LockHolders[0].Granted)
		require.Len(t, report.TimeoutSettings, 1)
		assert.Contains(t, report.TimeoutSettings[0], "lock_timeout=10s (set for all roles in database test_")
		assert.Empty(t, report.Blockers())

		var out bytes.Buffer
		require.NoError(t, report.Write(&out))
		for _, expected := range []string{"Required free disk: at least", "fatalInsertionErrors", "Pulsar", "WAL", "Result: ready"} {
			assert.Contains(t, out.String(), expected)
		}
		return nil
	})
	require.NoError(t, err)
}

func TestPreflight_ReportsBlockersAndAlreadyPartitioned(t *testing.T) {
	err := withLookoutChainAppliedDb(func(db *pgxpool.Pool) error {
		ctx := armadacontext.Background()
		_, err := db.Exec(ctx, `SELECT setval('database_version', 1)`)
		require.NoError(t, err)

		report, err := Preflight(ctx, db)
		require.NoError(t, err)
		require.Len(t, report.Blockers(), 1)
		assert.Contains(t, report.Blockers()[0], "--migrateDatabase")
		var out bytes.Buffer
		require.NoError(t, report.Write(&out))
		assert.Contains(t, out.String(), "Result: NOT READY")

		require.NoError(t, PartitionJobTable(ctx, db))
		report, err = Preflight(ctx, db)
		require.NoError(t, err)
		assert.Empty(t, report.Blockers())
		out.Reset()
		require.NoError(t, report.Write(&out))
		assert.Contains(t, out.String(), "already partitioned")
		return nil
	})
	require.NoError(t, err)
}

func TestFormatBytes(t *testing.T) {
	assert.Equal(t, "512 B", formatBytes(512))
	assert.Equal(t, "1.5 KiB", formatBytes(1536))
	assert.Equal(t, "3.0 GiB", formatBytes(3<<30))
}
