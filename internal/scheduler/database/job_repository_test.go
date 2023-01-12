package database

import (
	"context"
	"github.com/G-Research/armada/internal/common/database"
	"github.com/G-Research/armada/internal/common/util"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

const defaultBatchSize = 1

func TestFetchJobUpdates(t *testing.T) {

	dbJobs, expectedJobs := createTestJobs(10)
	dbRuns, expectedRuns := createTestRuns(10)

	tests := map[string]struct {
		dbJobs       []Job
		dbRuns       []Run
		jobsSerial   int64
		runsSerial   int64
		expectedJobs []Job
		expectedRuns []Run
	}{
		"all jobs": {
			dbJobs:       dbJobs,
			expectedJobs: expectedJobs,
			expectedRuns: []Run{},
			jobsSerial:   0,
		},
		"some jobs": {
			dbJobs:       dbJobs,
			expectedJobs: expectedJobs[5:],
			expectedRuns: []Run{},
			jobsSerial:   5,
		},
		"no jobs": {
			dbJobs:       dbJobs,
			expectedJobs: []Job{},
			expectedRuns: []Run{},
			jobsSerial:   10,
		},
		"all runs": {
			dbRuns:       dbRuns,
			expectedRuns: expectedRuns,
			expectedJobs: []Job{},
			jobsSerial:   0,
		},
		"some runs": {
			dbRuns:       dbRuns,
			expectedRuns: expectedRuns[5:],
			expectedJobs: []Job{},
			runsSerial:   5,
		},
		"no runs": {
			dbRuns:       dbRuns,
			expectedJobs: []Job{},
			expectedRuns: []Run{},
			runsSerial:   10,
		},
		"both jobs and runs": {
			dbJobs:       dbJobs,
			dbRuns:       dbRuns,
			expectedJobs: expectedJobs,
			expectedRuns: expectedRuns,
		},
		"empty db": {
			expectedJobs: []Job{},
			expectedRuns: []Run{},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			err := withJobRepository(func(repo *PostgresJobRepository) error {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

				// Set up db
				err := database.Upsert(ctx, repo.db, "jobs", tc.dbJobs)
				require.NoError(t, err)
				err = database.Upsert(ctx, repo.db, "runs", tc.dbRuns)
				require.NoError(t, err)

				// Fetch updates
				jobs, runs, err := repo.FetchJobUpdates(ctx, tc.jobsSerial, tc.runsSerial)
				require.NoError(t, err)

				// Runs will have LastModified filled in- we don't want to compare this
				for i, _ := range runs {
					runs[i].LastModified = time.Time{}
				}

				// Assert results
				assert.Equal(t, tc.expectedJobs, jobs)
				assert.Equal(t, tc.expectedRuns, runs)
				cancel()
				return nil
			})
			require.NoError(t, err)
		})

	}
}

func createTestJobs(numJobs int) ([]Job, []Job) {
	dbJobs := make([]Job, numJobs)
	expectedJobs := make([]Job, numJobs)

	for i := 0; i < numJobs; i++ {
		dbJobs[i] = Job{
			JobID:           util.NewULID(),
			JobSet:          "test-jobset",
			Queue:           "test-queue",
			Submitted:       int64(i),
			Priority:        int64(i),
			CancelRequested: true,
			Cancelled:       true,
			Succeeded:       true,
			Failed:          true,
			SchedulingInfo:  []byte{byte(i)},
			SubmitMessage:   []byte{},
		}
	}

	for i, job := range dbJobs {
		expectedJobs[i] = Job{
			JobID:           job.JobID,
			JobSet:          job.JobSet,
			Queue:           job.Queue,
			Submitted:       job.Submitted,
			Priority:        job.Priority,
			CancelRequested: job.CancelRequested,
			Cancelled:       job.Cancelled,
			Succeeded:       job.Succeeded,
			Failed:          job.Failed,
			SchedulingInfo:  job.SchedulingInfo,
			Serial:          int64(i + 1),
		}
	}
	return dbJobs, expectedJobs
}

func createTestRuns(numRuns int) ([]Run, []Run) {
	dbRuns := make([]Run, numRuns)
	expectedRuns := make([]Run, numRuns)

	for i := 0; i < numRuns; i++ {
		dbRuns[i] = Run{
			RunID:          uuid.New(),
			JobID:          util.NewULID(),
			JobSet:         "test-jobset",
			Executor:       "test-executor",
			SentToExecutor: true,
			Cancelled:      true,
			Running:        true,
			Succeeded:      true,
			Failed:         true,
			Returned:       true,
		}
	}

	for i, run := range dbRuns {
		expectedRuns[i] = Run{
			RunID:          run.RunID,
			JobID:          run.JobID,
			JobSet:         run.JobSet,
			Executor:       run.Executor,
			SentToExecutor: run.SentToExecutor,
			Cancelled:      run.Cancelled,
			Running:        run.Running,
			Succeeded:      run.Succeeded,
			Failed:         run.Failed,
			Returned:       run.Returned,
			Serial:         int64(i + 1),
		}
	}
	return dbRuns, expectedRuns
}

func withJobRepository(action func(repository *PostgresJobRepository) error) error {
	return WithTestDb(func(_ *Queries, db *pgxpool.Pool) error {
		repo := NewPostgresJobRepository(db, defaultBatchSize)
		return action(repo)
	})
}
