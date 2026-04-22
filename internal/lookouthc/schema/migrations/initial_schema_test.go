package migrations

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/internal/common/database"
	"github.com/armadaproject/armada/internal/lookouthc/schema"
)

func withLookoutHCDb(action func(db *pgxpool.Pool) error) error {
	migrations, err := schema.LookoutHCMigrations()
	if err != nil {
		return err
	}
	return database.WithTestDb(migrations, action)
}

func insertJob(ctx context.Context, db *pgxpool.Pool, jobID string, state int16) error {
	_, err := db.Exec(ctx, `
		INSERT INTO job (
			job_id, queue, owner, jobset, cpu, memory, ephemeral_storage, gpu,
			priority, submitted, state, last_transition_time,
			last_transition_time_seconds, annotations
		) VALUES ($1, 'q', 'o', 'js', 1, 1, 1, 0, 0, $2, $3, $2, 0, '{}'::jsonb)
	`, jobID, time.Now().UTC(), state)
	return err
}

func TestJobIsPartitionedWithTwoPartitions(t *testing.T) {
	err := withLookoutHCDb(func(db *pgxpool.Pool) error {
		ctx := context.Background()

		var partitionStrategy byte
		err := db.QueryRow(ctx, `
			SELECT partstrat FROM pg_partitioned_table pt
			JOIN pg_class c ON c.oid = pt.partrelid
			WHERE c.relname = 'job'
		`).Scan(&partitionStrategy)
		require.NoError(t, err)
		assert.Equal(t, byte('l'), partitionStrategy)

		rows, err := db.Query(ctx, `
			SELECT c.relname FROM pg_inherits i
			JOIN pg_class c ON c.oid = i.inhrelid
			JOIN pg_class p ON p.oid = i.inhparent
			WHERE p.relname = 'job'
			ORDER BY c.relname
		`)
		require.NoError(t, err)
		defer rows.Close()

		var partitions []string
		for rows.Next() {
			var name string
			require.NoError(t, rows.Scan(&name))
			partitions = append(partitions, name)
		}
		assert.Equal(t, []string{"job_active", "job_terminated"}, partitions)
		return nil
	})
	require.NoError(t, err)
}

func TestInsertActiveStateGoesToJobActive(t *testing.T) {
	err := withLookoutHCDb(func(db *pgxpool.Pool) error {
		ctx := context.Background()
		require.NoError(t, insertJob(ctx, db, "jobA", 1))

		var count int
		require.NoError(t, db.QueryRow(ctx,
			`SELECT count(*) FROM job_active WHERE job_id = 'jobA'`).Scan(&count))
		assert.Equal(t, 1, count)

		require.NoError(t, db.QueryRow(ctx,
			`SELECT count(*) FROM job_terminated WHERE job_id = 'jobA'`).Scan(&count))
		assert.Equal(t, 0, count)
		return nil
	})
	require.NoError(t, err)
}

func TestInsertTerminatedStateGoesToJobTerminated(t *testing.T) {
	err := withLookoutHCDb(func(db *pgxpool.Pool) error {
		ctx := context.Background()
		require.NoError(t, insertJob(ctx, db, "jobT", 4))

		var count int
		require.NoError(t, db.QueryRow(ctx,
			`SELECT count(*) FROM job_terminated WHERE job_id = 'jobT'`).Scan(&count))
		assert.Equal(t, 1, count)

		require.NoError(t, db.QueryRow(ctx,
			`SELECT count(*) FROM job_active WHERE job_id = 'jobT'`).Scan(&count))
		assert.Equal(t, 0, count)
		return nil
	})
	require.NoError(t, err)
}

func TestInsertInvalidStateFails(t *testing.T) {
	err := withLookoutHCDb(func(db *pgxpool.Pool) error {
		ctx := context.Background()
		insertErr := insertJob(ctx, db, "jobX", 99)
		require.Error(t, insertErr)
		assert.Contains(t, strings.ToLower(insertErr.Error()), "no partition")
		return nil
	})
	require.NoError(t, err)
}

func TestUpdateStateMovesRowBetweenPartitions(t *testing.T) {
	err := withLookoutHCDb(func(db *pgxpool.Pool) error {
		ctx := context.Background()
		require.NoError(t, insertJob(ctx, db, "jobM", 1))

		var count int
		require.NoError(t, db.QueryRow(ctx,
			`SELECT count(*) FROM job_active WHERE job_id = 'jobM'`).Scan(&count))
		require.Equal(t, 1, count)

		_, err := db.Exec(ctx, `UPDATE job SET state = 4 WHERE job_id = 'jobM'`)
		require.NoError(t, err)

		require.NoError(t, db.QueryRow(ctx,
			`SELECT count(*) FROM job_active WHERE job_id = 'jobM'`).Scan(&count))
		assert.Equal(t, 0, count)

		require.NoError(t, db.QueryRow(ctx,
			`SELECT count(*) FROM job_terminated WHERE job_id = 'jobM'`).Scan(&count))
		assert.Equal(t, 1, count)
		return nil
	})
	require.NoError(t, err)
}

func TestSelectFromParentReturnsAllPartitions(t *testing.T) {
	err := withLookoutHCDb(func(db *pgxpool.Pool) error {
		ctx := context.Background()
		require.NoError(t, insertJob(ctx, db, "jobActive1", 1))
		require.NoError(t, insertJob(ctx, db, "jobActive2", 3))
		require.NoError(t, insertJob(ctx, db, "jobTerm1", 4))
		require.NoError(t, insertJob(ctx, db, "jobTerm2", 6))

		rows, err := db.Query(ctx, `SELECT job_id FROM job ORDER BY job_id`)
		require.NoError(t, err)
		defer rows.Close()

		var ids []string
		for rows.Next() {
			var id string
			require.NoError(t, rows.Scan(&id))
			ids = append(ids, id)
		}
		assert.Equal(t, []string{"jobActive1", "jobActive2", "jobTerm1", "jobTerm2"}, ids)
		return nil
	})
	require.NoError(t, err)
}

func TestPartitionPruningForActiveStates(t *testing.T) {
	err := withLookoutHCDb(func(db *pgxpool.Pool) error {
		ctx := context.Background()

		rows, err := db.Query(ctx,
			`EXPLAIN (ANALYZE false) SELECT * FROM job WHERE state IN (1,2,3,8)`)
		require.NoError(t, err)
		defer rows.Close()

		var plan strings.Builder
		for rows.Next() {
			var line string
			require.NoError(t, rows.Scan(&line))
			plan.WriteString(line)
			plan.WriteString("\n")
		}
		planText := plan.String()
		assert.Contains(t, planText, "job_active")
		assert.NotContains(t, planText, "job_terminated")
		return nil
	})
	require.NoError(t, err)
}

func TestExpectedIndexesExist(t *testing.T) {
	expected := map[string][]string{
		"job": {
			"idx_job_queue_last_transition_time_seconds",
			"idx_job_queue_jobset_state",
			"idx_job_state",
			"idx_job_submitted",
			"idx_job_jobset_pattern",
			"idx_job_annotations_path",
			"idx_job_latest_run_id",
			"idx_job_queue_namespace",
			"idx_job_ltt_jobid",
		},
		"job_active": {
			"idx_job_active_queue_jobset",
		},
		"job_run": {
			"idx_job_run_job_id",
			"idx_job_run_node",
			"idx_job_run_state_pool",
			"idx_job_run_run_id_cluster_node",
			"idx_job_run_run_id_node",
			"idx_job_run_run_id_pool",
			"idx_job_run_cluster_run_id",
		},
		"job_spec":          {},
		"job_error":         {},
		"queue":             {},
		"job_deduplication": {"idx_job_deduplication_inserted"},
	}

	err := withLookoutHCDb(func(db *pgxpool.Pool) error {
		ctx := context.Background()
		for table, indexes := range expected {
			rows, err := db.Query(ctx,
				`SELECT indexname FROM pg_indexes WHERE tablename = $1`, table)
			require.NoError(t, err)

			found := map[string]bool{}
			for rows.Next() {
				var name string
				require.NoError(t, rows.Scan(&name))
				found[name] = true
			}
			rows.Close()

			for _, idx := range indexes {
				assert.Truef(t, found[idx],
					"expected index %q on table %q, found: %v", idx, table, found)
			}
		}
		return nil
	})
	require.NoError(t, err)
}

func TestAnnotationsNotNullCheckConstraintExists(t *testing.T) {
	err := withLookoutHCDb(func(db *pgxpool.Pool) error {
		ctx := context.Background()

		var count int
		err := db.QueryRow(ctx, `
			SELECT count(*) FROM pg_constraint c
			JOIN pg_class t ON t.oid = c.conrelid
			WHERE t.relname = 'job'
			  AND c.conname = 'job_annotations_not_null'
			  AND c.contype = 'c'
		`).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 1, count)
		return nil
	})
	require.NoError(t, err)
}

func TestInsertsIntoOtherTables(t *testing.T) {
	err := withLookoutHCDb(func(db *pgxpool.Pool) error {
		ctx := context.Background()

		_, err := db.Exec(ctx, `
			INSERT INTO job_run (run_id, job_id, cluster, job_run_state)
			VALUES ('run-1', 'jobR', 'cluster-a', 1)
		`)
		require.NoError(t, err)

		_, err = db.Exec(ctx,
			`INSERT INTO job_spec (job_id, job_spec) VALUES ('jobS', '\x00'::bytea)`)
		require.NoError(t, err)

		_, err = db.Exec(ctx,
			`INSERT INTO job_error (job_id, error) VALUES ('jobE', '\x00'::bytea)`)
		require.NoError(t, err)

		_, err = db.Exec(ctx,
			`INSERT INTO queue (name, definition) VALUES ('q1', '\x00'::bytea)`)
		require.NoError(t, err)

		_, err = db.Exec(ctx,
			`INSERT INTO job_deduplication (deduplication_id, job_id) VALUES ('dedup-1', 'jobD')`)
		require.NoError(t, err)

		verifyCount := func(q string, want int) {
			var got int
			require.NoError(t, db.QueryRow(ctx, q).Scan(&got))
			assert.Equal(t, want, got)
		}
		verifyCount(`SELECT count(*) FROM job_run WHERE run_id = 'run-1'`, 1)
		verifyCount(`SELECT count(*) FROM job_spec WHERE job_id = 'jobS'`, 1)
		verifyCount(`SELECT count(*) FROM job_error WHERE job_id = 'jobE'`, 1)
		verifyCount(`SELECT count(*) FROM queue WHERE name = 'q1'`, 1)
		verifyCount(`SELECT count(*) FROM job_deduplication WHERE deduplication_id = 'dedup-1'`, 1)
		return nil
	})
	require.NoError(t, err)
}
