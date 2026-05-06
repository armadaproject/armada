package schema_test

import (
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/database"
	lookoutschema "github.com/armadaproject/armada/internal/lookout/schema"
	"github.com/armadaproject/armada/internal/lookouthc/schema"
)

func withLookoutChainAppliedDb(action func(db *pgxpool.Pool) error) error {
	migrations, err := lookoutschema.LookoutMigrations()
	if err != nil {
		return err
	}
	return database.WithTestDb(migrations, action)
}

func insertUnpartitionedJob(ctx *armadacontext.Context, db *pgxpool.Pool, jobID string, state int16) error {
	_, err := db.Exec(ctx, `
		INSERT INTO job (
			job_id, queue, owner, jobset, cpu, memory, ephemeral_storage, gpu,
			priority, submitted, state, last_transition_time,
			last_transition_time_seconds, annotations
		) VALUES ($1, 'q', 'o', 'js', 1, 1, 1, 0, 0, $2, $3, $2, 0, '{}'::jsonb)
	`, jobID, time.Now().UTC(), state)
	return err
}

func TestApplyPartitioner_UnpartitionedIsConverted(t *testing.T) {
	err := withLookoutChainAppliedDb(func(db *pgxpool.Pool) error {
		ctx := armadacontext.Background()

		require.NoError(t, insertUnpartitionedJob(ctx, db, "jobA", 1))
		require.NoError(t, insertUnpartitionedJob(ctx, db, "jobB", 3))
		require.NoError(t, insertUnpartitionedJob(ctx, db, "jobC", 4))
		require.NoError(t, insertUnpartitionedJob(ctx, db, "jobD", 5))

		require.NoError(t, schema.ApplyPartitioner(ctx, db))

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

func TestApplyPartitioner_AlreadyPartitionedIsNoOp(t *testing.T) {
	err := withLookoutChainAppliedDb(func(db *pgxpool.Pool) error {
		ctx := armadacontext.Background()

		require.NoError(t, schema.ApplyPartitioner(ctx, db))

		require.NoError(t, insertUnpartitionedJob(ctx, db, "jobStable", 1))

		require.NoError(t, schema.ApplyPartitioner(ctx, db))

		var n int
		require.NoError(t, db.QueryRow(ctx,
			`SELECT count(*) FROM job_active WHERE job_id = 'jobStable'`).Scan(&n))
		assert.Equal(t, 1, n)

		return nil
	})
	require.NoError(t, err)
}

func TestApplyPartitioner_WrongShapeRefuses(t *testing.T) {
	err := withLookoutChainAppliedDb(func(db *pgxpool.Pool) error {
		ctx := armadacontext.Background()

		require.NoError(t, schema.ApplyPartitioner(ctx, db))
		_, err := db.Exec(ctx,
			`ALTER TABLE job ADD COLUMN unexpected_column text`)
		require.NoError(t, err)

		err = schema.ApplyPartitioner(ctx, db)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "shape")
		return nil
	})
	require.NoError(t, err)
}

func TestApplyPartitioner_NoJobTableRefuses(t *testing.T) {
	err := database.WithTestDb(nil, func(db *pgxpool.Pool) error {
		ctx := armadacontext.Background()
		err := schema.ApplyPartitioner(ctx, db)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "job")
		return nil
	})
	require.NoError(t, err)
}
