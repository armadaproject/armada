package database

import (
	"context"
	"github.com/G-Research/armada/internal/common/database"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

const defaultBatchSize = 1

func TestFetchJobUpdates(t *testing.T) {

	testjobs := make([]*Job, 10)

	tests := map[string]struct {
		dbJobs       []*Job
		dbRuns       []*Run
		jobsSerial   int64
		runsSerial   int64
		expectedJobs []*Job
		expectedRuns []*Run
	}{
		"jobsOnly": {
			dbJobs: []*Job{},
		},
	}
	for name, tc := range tests {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		t.Run(name, func(t *testing.T) {
			withJobRepository(func(repo *PostgresJobRepository) {

				// Set up db
				err := database.Upsert(ctx, repo.db, "jobs", tc.dbJobs)
				require.NoError(t, err)
				err = database.Upsert(ctx, repo.db, "runs", tc.dbRuns)
				require.NoError(t, err)

				jobs, runs, err := repo.FetchJobUpdates(ctx, tc.jobsSerial, tc.runsSerial)
				require.NoError(t, err)

				assert.Equal(t, jobs, tc.expectedJobs)
				assert.Equal(t, runs, tc.expectedRuns)
			})
		})
	}
}

func withJobRepository(action func(repository *PostgresJobRepository)) {
	WithTestDb(func(_ *Queries, db *pgxpool.Pool) error {
		repo := NewPostgresJobRepository(db, defaultBatchSize)
		action(repo)
		return nil
	})
}
