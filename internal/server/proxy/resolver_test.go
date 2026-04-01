package proxy

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/internal/common/database/lookout"
)

// resolverTestDB is a helper that runs resolver tests against a real Postgres
// instance. If no DB is available the tests are skipped.
func resolverTestDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	cfg, err := pgxpool.ParseConfig("postgres://postgres:postgres@localhost:5432/armada?sslmode=disable")
	if err != nil {
		t.Skipf("skipping resolver test (no DB config): %v", err)
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Skipf("skipping resolver test (DB unavailable): %v", err)
	}
	if err := pool.Ping(context.Background()); err != nil {
		pool.Close()
		t.Skipf("skipping resolver test (DB not reachable): %v", err)
	}
	return pool
}

func TestResolver_RunningJobFound(t *testing.T) {
	db := resolverTestDB(t)
	defer db.Close()

	ctx := context.Background()
	r := NewJobResolver(db)

	// Seed: one job with a RUNNING run.
	jobID := "test-job-resolver-1"
	runID := "run-1"
	_, _ = db.Exec(ctx, "DELETE FROM job_run WHERE job_id = $1", jobID)
	_, _ = db.Exec(ctx, "DELETE FROM job WHERE job_id = $1", jobID)
	_, _ = db.Exec(ctx, "INSERT INTO job (job_id, queue, namespace, jobset) VALUES ($1, 'q', 'ns-1', 'js')", jobID)
	_, err := db.Exec(ctx,
		"INSERT INTO job_run (run_id, job_id, cluster, job_run_state, started) VALUES ($1, $2, 'executor-1', $3, NOW())",
		runID, jobID, lookout.JobRunRunningOrdinal)
	require.NoError(t, err)

	resolved, err := r.ResolveRunningJob(ctx, jobID)
	require.NoError(t, err)
	assert.Equal(t, "executor-1", resolved.ExecutorID)
	assert.Equal(t, "ns-1", resolved.Namespace)
	assert.Equal(t, runID, resolved.RunID)
}

func TestResolver_NonRunningJob(t *testing.T) {
	db := resolverTestDB(t)
	defer db.Close()

	ctx := context.Background()
	r := NewJobResolver(db)

	jobID := "test-job-resolver-2"
	runID := "run-2"
	_, _ = db.Exec(ctx, "DELETE FROM job_run WHERE job_id = $1", jobID)
	_, _ = db.Exec(ctx, "DELETE FROM job WHERE job_id = $1", jobID)
	_, _ = db.Exec(ctx, "INSERT INTO job (job_id, queue, namespace, jobset) VALUES ($1, 'q', 'ns-1', 'js')", jobID)
	_, _ = db.Exec(ctx,
		"INSERT INTO job_run (run_id, job_id, cluster, job_run_state, started) VALUES ($1, $2, 'executor-1', $3, NOW())",
		runID, jobID, lookout.JobRunSucceededOrdinal) // SUCCEEDED, not running

	_, err := r.ResolveRunningJob(ctx, jobID)
	assert.ErrorIs(t, err, ErrJobNotRunning)
}

func TestResolver_UnknownJob(t *testing.T) {
	db := resolverTestDB(t)
	defer db.Close()

	ctx := context.Background()
	r := NewJobResolver(db)

	_, err := r.ResolveRunningJob(ctx, "nonexistent-job-xyz")
	assert.ErrorIs(t, err, ErrJobNotFound)
}

func TestResolver_UsesConstantNotMagicNumber(t *testing.T) {
	// Verify that JobRunRunningOrdinal == 2 (the plan requirement).
	assert.Equal(t, 2, lookout.JobRunRunningOrdinal,
		"JobRunRunningOrdinal must be 2; if this changes, update the resolver query")
}
