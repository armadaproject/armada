package schema

import (
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/database"
)

func withLookoutChainAppliedDb(action func(db *pgxpool.Pool) error) error {
	migrations, err := LookoutMigrations()
	if err != nil {
		return err
	}
	return database.WithTestDb(migrations, action)
}

func insertUnpartitionedJob(ctx *armadacontext.Context, db database.Querier, jobID string, state int16) error {
	_, err := db.Exec(ctx, `
		INSERT INTO job (
			job_id, queue, owner, jobset, cpu, memory, ephemeral_storage, gpu,
			priority, submitted, state, last_transition_time,
			last_transition_time_seconds, annotations
		) VALUES ($1, 'q', 'o', 'js', 1, 1, 1, 0, 0, $2, $3, $2, 0, '{}'::jsonb)
	`, jobID, time.Now().UTC(), state)
	return err
}

func TestPartitionJobTable_UnpartitionedIsConverted(t *testing.T) {
	err := withLookoutChainAppliedDb(func(db *pgxpool.Pool) error {
		ctx := armadacontext.Background()

		require.NoError(t, insertUnpartitionedJob(ctx, db, "jobA", 1))
		require.NoError(t, insertUnpartitionedJob(ctx, db, "jobB", 3))
		require.NoError(t, insertUnpartitionedJob(ctx, db, "jobC", 4))
		require.NoError(t, insertUnpartitionedJob(ctx, db, "jobD", 5))

		require.NoError(t, PartitionJobTable(ctx, db))

		var partstrat byte
		require.NoError(t, db.QueryRow(ctx, `
			SELECT partstrat FROM pg_partitioned_table pt
			JOIN pg_class c ON c.oid = pt.partrelid
			WHERE c.relname = 'job'
		`).Scan(&partstrat))
		assert.Equal(t, byte('l'), partstrat)

		activeCounts := map[string]int{}
		for _, id := range []string{"jobA", "jobB"} {
			var n int
			require.NoError(t, db.QueryRow(ctx,
				`SELECT count(*) FROM job_active WHERE job_id = $1`, id).Scan(&n))
			activeCounts[id] = n
		}
		assert.Equal(t, map[string]int{"jobA": 1, "jobB": 1}, activeCounts)

		terminalCounts := map[string]int{}
		for _, id := range []string{"jobC", "jobD"} {
			var n int
			require.NoError(t, db.QueryRow(ctx,
				`SELECT count(*) FROM job_terminated WHERE job_id = $1`, id).Scan(&n))
			terminalCounts[id] = n
		}
		assert.Equal(t, map[string]int{"jobC": 1, "jobD": 1}, terminalCounts)

		return nil
	})
	require.NoError(t, err)
}

func TestPartitionJobTable_AlreadyPartitionedIsNoOp(t *testing.T) {
	err := withLookoutChainAppliedDb(func(db *pgxpool.Pool) error {
		ctx := armadacontext.Background()

		require.NoError(t, PartitionJobTable(ctx, db))

		require.NoError(t, insertUnpartitionedJob(ctx, db, "jobStable", 1))

		require.NoError(t, PartitionJobTable(ctx, db))

		var n int
		require.NoError(t, db.QueryRow(ctx,
			`SELECT count(*) FROM job_active WHERE job_id = 'jobStable'`).Scan(&n))
		assert.Equal(t, 1, n)

		return nil
	})
	require.NoError(t, err)
}

// TestPartitionJobTable_BackfillsMissingQueueJobIDIndex verifies that a job
// table partitioned before idx_job_queue_job_id existed is not rejected as
// the wrong shape, and instead gets the index backfilled in place.
func TestPartitionJobTable_BackfillsMissingQueueJobIDIndex(t *testing.T) {
	err := withLookoutChainAppliedDb(func(db *pgxpool.Pool) error {
		ctx := armadacontext.Background()

		require.NoError(t, PartitionJobTable(ctx, db))

		_, err := db.Exec(ctx, `DROP INDEX idx_job_queue_job_id`)
		require.NoError(t, err)

		require.NoError(t, PartitionJobTable(ctx, db))

		var exists bool
		require.NoError(t, db.QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM pg_indexes WHERE tablename = 'job' AND indexname = 'idx_job_queue_job_id')`,
		).Scan(&exists))
		assert.True(t, exists, "idx_job_queue_job_id should be backfilled")

		return nil
	})
	require.NoError(t, err)
}

func TestPartitionJobTable_WrongShapeRefuses(t *testing.T) {
	err := withLookoutChainAppliedDb(func(db *pgxpool.Pool) error {
		ctx := armadacontext.Background()

		require.NoError(t, PartitionJobTable(ctx, db))
		_, err := db.Exec(ctx,
			`ALTER TABLE job ADD COLUMN unexpected_column text`)
		require.NoError(t, err)

		err = PartitionJobTable(ctx, db)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "shape")
		return nil
	})
	require.NoError(t, err)
}

func TestPartitionJobTable_NoJobTableRefuses(t *testing.T) {
	err := database.WithTestDb(nil, func(db *pgxpool.Pool) error {
		ctx := armadacontext.Background()
		err := PartitionJobTable(ctx, db)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "job")
		return nil
	})
	require.NoError(t, err)
}

// TestPartitionJobTable_PKConstraintRenamedAfterConversion verifies that the
// primary key constraint on job is named job_pkey (not job_new_pkey) after
// the unpartitioned-to-partitioned conversion.
func TestPartitionJobTable_PKConstraintRenamedAfterConversion(t *testing.T) {
	err := withLookoutChainAppliedDb(func(db *pgxpool.Pool) error {
		ctx := armadacontext.Background()
		require.NoError(t, PartitionJobTable(ctx, db))

		var constraintName string
		require.NoError(t, db.QueryRow(ctx, `
			SELECT conname
			FROM pg_constraint
			JOIN pg_class ON pg_class.oid = pg_constraint.conrelid
			WHERE pg_class.relname = 'job' AND pg_constraint.contype = 'p'
		`).Scan(&constraintName))
		assert.Equal(t, "job_pkey", constraintName)
		return nil
	})
	require.NoError(t, err)
}

// TestPartitionJobTable_RowInsertedDuringConversionIsNotLost verifies that a
// concurrent INSERT into job that begins after the INSERT SELECT but before
// the DROP TABLE is not silently dropped. With LOCK TABLE held for the
// entire transaction, the concurrent INSERT blocks until after the rename
// and commits into the new partitioned table.
func TestPartitionJobTable_RowInsertedDuringConversionIsNotLost(t *testing.T) {
	err := withLookoutChainAppliedDb(func(db *pgxpool.Pool) error {
		ctx := armadacontext.Background()

		insertDone := make(chan error, 1)
		testHookBeforeStep = func(step conversionStep) {
			if step.sql != "DROP TABLE job" {
				return
			}
			ready := make(chan struct{})
			go func() {
				conn, err := db.Acquire(ctx)
				if err != nil {
					insertDone <- err
					return
				}
				defer conn.Release()
				close(ready) // signal: goroutine is at the Exec call
				_, err = conn.Exec(ctx, `
					INSERT INTO job (
						job_id, queue, owner, jobset, cpu, memory,
						ephemeral_storage, gpu, priority, submitted, state,
						last_transition_time, last_transition_time_seconds, annotations
					) VALUES ('concurrent', 'q', 'o', 'js', 1, 1, 1, 0, 0,
						NOW(), 1, NOW(), 0, '{}'::jsonb)
				`)
				insertDone <- err
			}()
			<-ready // return only after the goroutine has reached Exec
		}
		t.Cleanup(func() { testHookBeforeStep = nil })

		require.NoError(t, PartitionJobTable(ctx, db))
		require.NoError(t, <-insertDone, "concurrent insert should complete without error")

		var n int
		require.NoError(t, db.QueryRow(ctx,
			`SELECT count(*) FROM job WHERE job_id = 'concurrent'`).Scan(&n))
		assert.Equal(t, 1, n, "row inserted during conversion must not be lost")
		return nil
	})
	require.NoError(t, err)
}

func TestDetectJobTableShape(t *testing.T) {
	tests := map[string]struct {
		migrations   func() ([]database.Migration, error)
		setup        func(ctx *armadacontext.Context, db *pgxpool.Pool) error
		expected     JobTableShape
		expectDetail bool
	}{
		"missing": {
			migrations: func() ([]database.Migration, error) { return nil, nil },
			expected:   JobTableShapeMissing,
		},
		"unpartitioned": {
			migrations: LookoutMigrations,
			expected:   JobTableShapeUnpartitioned,
		},
		"partitioned": {
			migrations: LookoutMigrations,
			setup: func(ctx *armadacontext.Context, db *pgxpool.Pool) error {
				return PartitionJobTable(ctx, db)
			},
			expected: JobTableShapePartitioned,
		},
		"unexpected": {
			migrations: LookoutMigrations,
			setup: func(ctx *armadacontext.Context, db *pgxpool.Pool) error {
				if err := PartitionJobTable(ctx, db); err != nil {
					return err
				}
				_, err := db.Exec(ctx, `ALTER TABLE job ADD COLUMN unexpected_column text`)
				return err
			},
			expected:     JobTableShapeUnexpected,
			expectDetail: true,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			migrations, err := tc.migrations()
			require.NoError(t, err)
			err = database.WithTestDb(migrations, func(db *pgxpool.Pool) error {
				ctx := armadacontext.Background()
				if tc.setup != nil {
					require.NoError(t, tc.setup(ctx, db))
				}

				shape, detail, err := DetectJobTableShape(ctx, db)
				require.NoError(t, err)
				assert.Equal(t, tc.expected, shape)
				assert.Equal(t, tc.expectDetail, detail != "", "detail: %q", detail)
				return nil
			})
			require.NoError(t, err)
		})
	}
}

func TestPartitionJobTableIfEmpty_EmptyTableIsConverted(t *testing.T) {
	err := withLookoutChainAppliedDb(func(db *pgxpool.Pool) error {
		ctx := armadacontext.Background()

		shape, err := PartitionJobTableIfEmpty(ctx, db)
		require.NoError(t, err)
		assert.Equal(t, JobTableShapePartitioned, shape)

		detected, detail, err := DetectJobTableShape(ctx, db)
		require.NoError(t, err)
		assert.Equal(t, JobTableShapePartitioned, detected, detail)
		return nil
	})
	require.NoError(t, err)
}

func TestPartitionJobTableIfEmpty_NonEmptyTableIsLeftUnpartitioned(t *testing.T) {
	err := withLookoutChainAppliedDb(func(db *pgxpool.Pool) error {
		ctx := armadacontext.Background()
		require.NoError(t, insertUnpartitionedJob(ctx, db, "jobA", 4))

		shape, err := PartitionJobTableIfEmpty(ctx, db)
		require.NoError(t, err)
		assert.Equal(t, JobTableShapeUnpartitioned, shape)

		partitioned, err := IsJobTablePartitioned(ctx, db)
		require.NoError(t, err)
		assert.False(t, partitioned)

		var n int
		require.NoError(t, db.QueryRow(ctx, `SELECT count(*) FROM job WHERE job_id = 'jobA'`).Scan(&n))
		assert.Equal(t, 1, n)
		return nil
	})
	require.NoError(t, err)
}

// TestPartitionJobTableIfEmpty_AlreadyPartitionedBackfillsIndex verifies that
// installs partitioned before idx_job_queue_job_id existed still get it
// backfilled, even though their job table is not empty.
func TestPartitionJobTableIfEmpty_AlreadyPartitionedBackfillsIndex(t *testing.T) {
	err := withLookoutChainAppliedDb(func(db *pgxpool.Pool) error {
		ctx := armadacontext.Background()
		require.NoError(t, PartitionJobTable(ctx, db))
		require.NoError(t, insertUnpartitionedJob(ctx, db, "jobA", 1))
		_, err := db.Exec(ctx, `DROP INDEX idx_job_queue_job_id`)
		require.NoError(t, err)

		shape, err := PartitionJobTableIfEmpty(ctx, db)
		require.NoError(t, err)
		assert.Equal(t, JobTableShapePartitioned, shape)

		var exists bool
		require.NoError(t, db.QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM pg_indexes WHERE tablename = 'job' AND indexname = 'idx_job_queue_job_id')`,
		).Scan(&exists))
		assert.True(t, exists, "idx_job_queue_job_id should be backfilled")
		return nil
	})
	require.NoError(t, err)
}

// TestPartitionJobTableIfEmpty_RowCommittedWhileWaitingForLockPreventsConversion
// verifies that the emptiness check is repeated under the lock: a row that is
// uncommitted during the unlocked check, and committed while the partitioner
// waits for the lock, must stop the conversion.
func TestPartitionJobTableIfEmpty_RowCommittedWhileWaitingForLockPreventsConversion(t *testing.T) {
	err := withLookoutChainAppliedDb(func(db *pgxpool.Pool) error {
		ctx := armadacontext.Background()

		tx, err := db.Begin(ctx)
		require.NoError(t, err)
		defer func() { _ = tx.Rollback(ctx) }()
		require.NoError(t, insertUnpartitionedJob(ctx, tx, "jobA", 1))

		type result struct {
			shape JobTableShape
			err   error
		}
		done := make(chan result, 1)
		go func() {
			shape, err := PartitionJobTableIfEmpty(ctx, db)
			done <- result{shape, err}
		}()

		require.Eventually(t, func() bool {
			var waiting bool
			err := db.QueryRow(ctx, `
				SELECT EXISTS (
					SELECT 1 FROM pg_locks
					WHERE locktype = 'relation'
					AND database = (SELECT oid FROM pg_database WHERE datname = current_database())
					AND relation = 'job'::regclass
					AND mode = 'AccessExclusiveLock'
					AND NOT granted
				)`).Scan(&waiting)
			return err == nil && waiting
		}, 10*time.Second, 10*time.Millisecond, "partitioner should be waiting for the lock on job")
		require.NoError(t, tx.Commit(ctx))

		res := <-done
		require.NoError(t, res.err)
		assert.Equal(t, JobTableShapeUnpartitioned, res.shape)

		partitioned, err := IsJobTablePartitioned(ctx, db)
		require.NoError(t, err)
		assert.False(t, partitioned)
		return nil
	})
	require.NoError(t, err)
}
