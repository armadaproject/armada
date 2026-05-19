package db_test

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/internal/broadside/configuration"
	"github.com/armadaproject/armada/internal/broadside/db"
)

func postgresConfig(t *testing.T) map[string]string {
	t.Helper()
	host := os.Getenv("BROADSIDE_TEST_DB_HOST")
	if host == "" {
		t.Skip("BROADSIDE_TEST_DB_HOST not set; skipping PostgreSQL integration test")
	}
	return map[string]string{
		"host":     host,
		"port":     "5432",
		"user":     "postgres",
		"password": "psw",
		"dbname":   "broadside_test",
		"sslmode":  "disable",
	}
}

func TestPostgresDatabase_PopulateHistoricalJobs_JobCount(t *testing.T) {
	cfg := postgresConfig(t)
	pg := db.NewPostgresDatabase(cfg, configuration.FeatureToggles{}, nil, nil)
	ctx := context.Background()
	require.NoError(t, pg.InitialiseSchema(ctx))
	defer func() { _ = pg.TearDown(ctx) }()
	defer pg.Close()

	params := db.HistoricalJobsParams{
		QueueIdx:           0,
		JobSetIdx:          0,
		QueueName:          "test-queue",
		JobSetName:         "test-jobset",
		NJobs:              1000,
		SucceededThreshold: 800,
		ErroredThreshold:   900,
		CancelledThreshold: 950,
		JobAgeDays:         []int{0},
		JobSpecBytes:       []byte("fakejobspec"),
		ErrorBytes:         []byte("simulated error"),
		DebugBytes:         []byte("debug"),
		PreemptionBytes:    []byte("preempted"),
	}

	err := pg.PopulateHistoricalJobs(ctx, params)
	require.NoError(t, err)

	jobs, err := pg.GetJobs(&ctx, nil, false, nil, 0, 2000)
	require.NoError(t, err)
	assert.Len(t, jobs, 1000)
}

func TestPostgresDatabase_PopulateHistoricalJobs_StateDistribution(t *testing.T) {
	cfg := postgresConfig(t)
	pg := db.NewPostgresDatabase(cfg, configuration.FeatureToggles{}, nil, nil)
	ctx := context.Background()
	require.NoError(t, pg.InitialiseSchema(ctx))
	defer func() { _ = pg.TearDown(ctx) }()
	defer pg.Close()

	params := db.HistoricalJobsParams{
		QueueIdx:           0,
		JobSetIdx:          0,
		QueueName:          "test-queue",
		JobSetName:         "test-jobset",
		NJobs:              1000,
		SucceededThreshold: 700,
		ErroredThreshold:   800,
		CancelledThreshold: 900,
		JobAgeDays:         []int{0},
		JobSpecBytes:       []byte("fakejobspec"),
		ErrorBytes:         []byte("simulated error"),
		DebugBytes:         []byte("debug"),
		PreemptionBytes:    []byte("preempted"),
	}

	err := pg.PopulateHistoricalJobs(ctx, params)
	require.NoError(t, err)

	jobs, err := pg.GetJobs(&ctx, nil, false, nil, 0, 2000)
	require.NoError(t, err)

	states := make(map[string]int)
	for _, j := range jobs {
		states[j.State]++
	}

	assert.Equal(t, 700, states["SUCCEEDED"])
	assert.Equal(t, 100, states["FAILED"])
	assert.Equal(t, 100, states["CANCELLED"])
	assert.Equal(t, 100, states["PREEMPTED"])
}

func TestPostgresDatabase_PopulateHistoricalJobs_Chunked(t *testing.T) {
	cfg := postgresConfig(t)
	pg := db.NewPostgresDatabase(cfg, configuration.FeatureToggles{}, nil, nil)
	ctx := context.Background()
	require.NoError(t, pg.InitialiseSchema(ctx))
	defer func() { _ = pg.TearDown(ctx) }()
	defer pg.Close()

	params := db.HistoricalJobsParams{
		QueueIdx:           0,
		JobSetIdx:          0,
		QueueName:          "test-queue",
		JobSetName:         "test-jobset",
		NJobs:              500,
		ChunkSize:          200,
		SucceededThreshold: 700,
		ErroredThreshold:   800,
		CancelledThreshold: 900,
		JobAgeDays:         []int{0},
		JobSpecBytes:       []byte("fakejobspec"),
		ErrorBytes:         []byte("simulated error"),
		DebugBytes:         []byte("debug"),
		PreemptionBytes:    []byte("preempted"),
	}

	err := pg.PopulateHistoricalJobs(ctx, params)
	require.NoError(t, err)

	jobs, err := pg.GetJobs(&ctx, nil, false, nil, 0, 1000)
	require.NoError(t, err)
	assert.Len(t, jobs, 500)

	// All 500 jobs have i < 700, so all should be SUCCEEDED
	states := make(map[string]int)
	for _, j := range jobs {
		states[j.State]++
	}
	assert.Equal(t, 500, states["SUCCEEDED"])
}

func TestPostgresDatabase_PopulateHistoricalJobs_Resume(t *testing.T) {
	cfg := postgresConfig(t)
	pg := db.NewPostgresDatabase(cfg, configuration.FeatureToggles{}, nil, nil)
	ctx := context.Background()
	require.NoError(t, pg.InitialiseSchema(ctx))
	defer func() { _ = pg.TearDown(ctx) }()
	defer pg.Close()

	params := db.HistoricalJobsParams{
		QueueIdx:           0,
		JobSetIdx:          0,
		QueueName:          "test-queue",
		JobSetName:         "test-jobset",
		NJobs:              1000,
		ChunkSize:          300,
		SucceededThreshold: 800,
		ErroredThreshold:   900,
		CancelledThreshold: 950,
		JobAgeDays:         []int{0},
		JobSpecBytes:       []byte("fakejobspec"),
		ErrorBytes:         []byte("simulated error"),
		DebugBytes:         []byte("debug"),
		PreemptionBytes:    []byte("preempted"),
	}

	// First run: insert all 1000 jobs in chunks of 300
	err := pg.PopulateHistoricalJobs(ctx, params)
	require.NoError(t, err)

	jobs, err := pg.GetJobs(&ctx, nil, false, nil, 0, 2000)
	require.NoError(t, err)
	assert.Len(t, jobs, 1000)

	// Second run: should detect all jobs already exist and be a no-op
	err = pg.PopulateHistoricalJobs(ctx, params)
	require.NoError(t, err)

	jobs, err = pg.GetJobs(&ctx, nil, false, nil, 0, 2000)
	require.NoError(t, err)
	assert.Len(t, jobs, 1000, "re-running should not create duplicates")
}

func TestPostgresDatabase_InitialiseSchema_ExecutesTuningSQLWithoutError(t *testing.T) {
	cfg := postgresConfig(t)
	tuningSQLStatements := []string{
		"ALTER TABLE job SET (autovacuum_vacuum_scale_factor = 0.01)",
	}
	pg := db.NewPostgresDatabase(cfg, configuration.FeatureToggles{}, tuningSQLStatements, nil)
	ctx := context.Background()
	require.NoError(t, pg.InitialiseSchema(ctx))
	defer func() { _ = pg.TearDown(ctx) }()
	defer pg.Close()

	// Once tuningSQLStatements is populated, InitialiseSchema applies
	// each statement without error.
}

func TestPostgresDatabase_TearDown_ExecutesTuningRevertSQLWithoutError(t *testing.T) {
	cfg := postgresConfig(t)
	revertSQLStatements := []string{
		"ALTER TABLE job RESET (autovacuum_vacuum_scale_factor)",
	}
	pg := db.NewPostgresDatabase(cfg, configuration.FeatureToggles{}, nil, revertSQLStatements)
	ctx := context.Background()
	require.NoError(t, pg.InitialiseSchema(ctx))
	defer pg.Close()

	require.NoError(t, pg.TearDown(ctx))
	// TearDown should have executed the revert SQL statement without error
	// and truncated all tables.
}
