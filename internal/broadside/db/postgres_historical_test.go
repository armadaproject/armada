package db_test

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
	pg := db.NewPostgresDatabase(cfg)
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
	pg := db.NewPostgresDatabase(cfg)
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
