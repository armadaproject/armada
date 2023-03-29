package database

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"

	"github.com/armadaproject/armada/internal/common/compress"
	"github.com/armadaproject/armada/internal/common/database"
	protoutil "github.com/armadaproject/armada/internal/common/proto"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/pkg/armadaevents"
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
				for i := range runs {
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

func TestFetchJobRunErrors(t *testing.T) {
	const numErrors = 10

	dbErrors := make([]JobRunError, numErrors)
	expectedErrors := make([]*armadaevents.Error, numErrors)

	for i := 0; i < numErrors; i++ {
		runError := &armadaevents.Error{
			Terminal: true,
			Reason: &armadaevents.Error_PodError{
				PodError: &armadaevents.PodError{
					PodNumber: int32(i), // makes each error unique
				},
			},
		}
		expectedErrors[i] = runError
		dbErrors[i] = JobRunError{
			RunID: uuid.New(),
			JobID: util.NewULID(),
			Error: protoutil.MustMarshallAndCompress(runError, compress.NewThreadSafeZlibCompressor(1024)),
		}
	}

	tests := map[string]struct {
		errorsInDb  []JobRunError
		idsToLookup []uuid.UUID
		expected    map[uuid.UUID]*armadaevents.Error
		expectError bool
	}{
		"single error": {
			errorsInDb:  dbErrors,
			idsToLookup: []uuid.UUID{dbErrors[1].RunID},
			expected:    map[uuid.UUID]*armadaevents.Error{dbErrors[1].RunID: expectedErrors[1]},
		},
		"multiple errors": {
			errorsInDb:  dbErrors,
			idsToLookup: []uuid.UUID{dbErrors[1].RunID, dbErrors[4].RunID, dbErrors[5].RunID, dbErrors[7].RunID},
			expected: map[uuid.UUID]*armadaevents.Error{
				dbErrors[1].RunID: expectedErrors[1],
				dbErrors[4].RunID: expectedErrors[4],
				dbErrors[5].RunID: expectedErrors[5],
				dbErrors[7].RunID: expectedErrors[7],
			},
		},
		"some errors missing": {
			errorsInDb:  dbErrors,
			idsToLookup: []uuid.UUID{dbErrors[1].RunID, uuid.New(), uuid.New(), dbErrors[7].RunID},
			expected: map[uuid.UUID]*armadaevents.Error{
				dbErrors[1].RunID: expectedErrors[1],
				dbErrors[7].RunID: expectedErrors[7],
			},
		},
		"all errors missing": {
			errorsInDb:  dbErrors,
			idsToLookup: []uuid.UUID{uuid.New(), uuid.New(), uuid.New(), uuid.New()},
			expected:    map[uuid.UUID]*armadaevents.Error{},
		},
		"emptyDb": {
			errorsInDb:  []JobRunError{},
			idsToLookup: []uuid.UUID{uuid.New(), uuid.New(), uuid.New(), uuid.New()},
			expected:    map[uuid.UUID]*armadaevents.Error{},
		},
		"invalid data": {
			errorsInDb: []JobRunError{{
				RunID: dbErrors[0].RunID,
				JobID: dbErrors[0].JobID,
				Error: []byte{0x1, 0x4, 0x5}, // not a valid compressed proto
			}},
			idsToLookup: []uuid.UUID{dbErrors[0].RunID},
			expectError: true,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			err := withJobRepository(func(repo *PostgresJobRepository) error {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				// Set up db
				err := database.Upsert(ctx, repo.db, "job_run_errors", tc.errorsInDb)
				require.NoError(t, err)

				// Fetch updates
				received, err := repo.FetchJobRunErrors(ctx, tc.idsToLookup)
				if tc.expectError {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
					assert.Equal(t, tc.expected, received)
				}
				cancel()
				return nil
			})
			require.NoError(t, err)
		})
	}
}

func TestCountReceivedPartitions(t *testing.T) {
	tests := map[string]struct {
		numPartitions int
	}{
		"100 partitions": {
			numPartitions: 100,
		},
		"0 partitions": {
			numPartitions: 0,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			err := withJobRepository(func(repo *PostgresJobRepository) error {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

				markers := make([]Marker, tc.numPartitions)
				groupId := uuid.New()
				for i := 0; i < tc.numPartitions; i++ {
					markers[i] = Marker{
						GroupID:     groupId,
						PartitionID: int32(i),
					}
				}

				// Set up db
				err := insertMarkers(ctx, markers, repo.db)
				require.NoError(t, err)

				// Fetch updates
				received, err := repo.CountReceivedPartitions(ctx, groupId)
				require.NoError(t, err)

				assert.Equal(t, uint32(tc.numPartitions), received)

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
			JobID:                   util.NewULID(),
			JobSet:                  "test-jobset",
			Queue:                   "test-queue",
			Submitted:               int64(i),
			Priority:                int64(i),
			CancelRequested:         true,
			Cancelled:               true,
			CancelByJobsetRequested: true,
			Succeeded:               true,
			Failed:                  true,
			SchedulingInfo:          []byte{byte(i)},
			SubmitMessage:           []byte{},
		}
	}

	for i, job := range dbJobs {
		expectedJobs[i] = Job{
			JobID:                   job.JobID,
			JobSet:                  job.JobSet,
			Queue:                   job.Queue,
			Submitted:               job.Submitted,
			Priority:                job.Priority,
			CancelRequested:         job.CancelRequested,
			CancelByJobsetRequested: true,
			Cancelled:               job.Cancelled,
			Succeeded:               job.Succeeded,
			Failed:                  job.Failed,
			SchedulingInfo:          job.SchedulingInfo,
			Serial:                  int64(i + 1),
		}
	}
	return dbJobs, expectedJobs
}

func TestFindInactiveRuns(t *testing.T) {
	uuids := make([]uuid.UUID, 3)
	for i := 0; i < len(uuids); i++ {
		uuids[i] = uuid.New()
	}
	tests := map[string]struct {
		dbRuns           []Run
		runsToCheck      []uuid.UUID
		expectedInactive []uuid.UUID
	}{
		"empty database": {
			runsToCheck:      uuids,
			expectedInactive: uuids,
		},
		"no inactive": {
			runsToCheck: uuids,
			dbRuns: []Run{
				{RunID: uuids[0]},
				{RunID: uuids[1]},
				{RunID: uuids[2]},
			},
			expectedInactive: nil,
		},
		"run succeeded": {
			runsToCheck: uuids,
			dbRuns: []Run{
				{RunID: uuids[0]},
				{RunID: uuids[1], Succeeded: true},
				{RunID: uuids[2]},
			},
			expectedInactive: []uuid.UUID{uuids[1]},
		},
		"run failed": {
			runsToCheck: uuids,
			dbRuns: []Run{
				{RunID: uuids[0]},
				{RunID: uuids[1], Failed: true},
				{RunID: uuids[2]},
			},
			expectedInactive: []uuid.UUID{uuids[1]},
		},
		"run cancelled": {
			runsToCheck: uuids,
			dbRuns: []Run{
				{RunID: uuids[0]},
				{RunID: uuids[1], Cancelled: true},
				{RunID: uuids[2]},
			},
			expectedInactive: []uuid.UUID{uuids[1]},
		},
		"run missing": {
			runsToCheck: uuids,
			dbRuns: []Run{
				{RunID: uuids[0]},
				{RunID: uuids[2]},
			},
			expectedInactive: []uuid.UUID{uuids[1]},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			err := withJobRepository(func(repo *PostgresJobRepository) error {
				ctx, cancel := context.WithTimeout(context.Background(), 500*time.Second)

				// Set up db
				err := database.Upsert(ctx, repo.db, "runs", tc.dbRuns)
				require.NoError(t, err)

				inactive, err := repo.FindInactiveRuns(ctx, tc.runsToCheck)
				require.NoError(t, err)
				uuidSort := func(a uuid.UUID, b uuid.UUID) bool { return a.String() > b.String() }
				slices.SortFunc(inactive, uuidSort)
				slices.SortFunc(tc.expectedInactive, uuidSort)
				assert.Equal(t, tc.expectedInactive, inactive)
				cancel()
				return nil
			})
			require.NoError(t, err)
		})
	}
}

func TestFetchJobRunLeases(t *testing.T) {
	const executorName = "testExecutor"
	dbJobs, _ := createTestJobs(5)

	// first three runs can be picked up by executor
	// last three runs are not available
	dbRuns := []Run{
		{
			RunID:    uuid.New(),
			JobID:    dbJobs[0].JobID,
			JobSet:   "test-jobset",
			Executor: executorName,
		},
		{
			RunID:    uuid.New(),
			JobID:    dbJobs[1].JobID,
			JobSet:   "test-jobset",
			Executor: executorName,
		},
		{
			RunID:    uuid.New(),
			JobID:    dbJobs[2].JobID,
			JobSet:   "test-jobset",
			Executor: executorName,
		},
		{
			RunID:    uuid.New(),
			JobID:    dbJobs[0].JobID,
			JobSet:   "test-jobset",
			Executor: executorName,
			Failed:   true, // should be ignored as terminal
		},
		{
			RunID:     uuid.New(),
			JobID:     dbJobs[0].JobID,
			JobSet:    "test-jobset",
			Executor:  executorName,
			Cancelled: true, // should be ignored as terminal
		},
		{
			RunID:     uuid.New(),
			JobID:     dbJobs[3].JobID,
			JobSet:    "test-jobset",
			Executor:  executorName,
			Succeeded: true, // should be ignored as terminal
		},
	}
	expectedLeases := make([]*JobRunLease, 3)
	for i := range expectedLeases {
		expectedLeases[i] = &JobRunLease{
			RunID:         dbRuns[i].RunID,
			Queue:         dbJobs[i].Queue,
			JobSet:        dbJobs[i].JobSet,
			UserID:        dbJobs[i].UserID,
			Groups:        dbJobs[i].Groups,
			SubmitMessage: dbJobs[i].SubmitMessage,
		}
	}
	tests := map[string]struct {
		dbRuns         []Run
		dbJobs         []Job
		excludedRuns   []uuid.UUID
		maxRowsToFetch uint
		executor       string
		expectedLeases []*JobRunLease
	}{
		"all runs": {
			dbJobs:         dbJobs,
			dbRuns:         dbRuns,
			excludedRuns:   nil,
			maxRowsToFetch: 100,
			executor:       executorName,
			expectedLeases: expectedLeases,
		},
		"limit rows": {
			dbJobs:         dbJobs,
			dbRuns:         dbRuns,
			excludedRuns:   nil,
			maxRowsToFetch: 2,
			executor:       executorName,
			expectedLeases: []*JobRunLease{expectedLeases[0], expectedLeases[1]},
		},
		"exclude one run": {
			dbJobs:         dbJobs,
			dbRuns:         dbRuns,
			excludedRuns:   []uuid.UUID{dbRuns[1].RunID},
			maxRowsToFetch: 100,
			executor:       executorName,
			expectedLeases: []*JobRunLease{expectedLeases[0], expectedLeases[2]},
		},
		"exclude everything": {
			dbJobs:         dbJobs,
			dbRuns:         dbRuns,
			excludedRuns:   []uuid.UUID{dbRuns[0].RunID, dbRuns[1].RunID, dbRuns[2].RunID},
			maxRowsToFetch: 100,
			executor:       executorName,
			expectedLeases: nil,
		},
		"another executor": {
			dbJobs:         dbJobs,
			dbRuns:         dbRuns,
			excludedRuns:   nil,
			maxRowsToFetch: 100,
			executor:       "some other executor",
			expectedLeases: nil,
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

				leases, err := repo.FetchJobRunLeases(ctx, tc.executor, tc.maxRowsToFetch, tc.excludedRuns)
				require.NoError(t, err)
				leaseSort := func(a *JobRunLease, b *JobRunLease) bool { return a.RunID.String() > b.RunID.String() }
				slices.SortFunc(leases, leaseSort)
				slices.SortFunc(tc.expectedLeases, leaseSort)
				assert.Equal(t, tc.expectedLeases, leases)
				cancel()
				return nil
			})
			require.NoError(t, err)
		})
	}
}

func createTestRuns(numRuns int) ([]Run, []Run) {
	dbRuns := make([]Run, numRuns)
	expectedRuns := make([]Run, numRuns)

	for i := 0; i < numRuns; i++ {
		dbRuns[i] = Run{
			RunID:     uuid.New(),
			JobID:     util.NewULID(),
			JobSet:    "test-jobset",
			Executor:  "test-executor",
			Node:      fmt.Sprintf("test-node-%d", i),
			Cancelled: true,
			Running:   true,
			Succeeded: true,
			Failed:    true,
			Returned:  true,
		}
	}

	for i, run := range dbRuns {
		expectedRuns[i] = Run{
			RunID:     run.RunID,
			JobID:     run.JobID,
			JobSet:    run.JobSet,
			Executor:  run.Executor,
			Node:      fmt.Sprintf("test-node-%d", i),
			Cancelled: run.Cancelled,
			Running:   run.Running,
			Succeeded: run.Succeeded,
			Failed:    run.Failed,
			Returned:  run.Returned,
			Serial:    int64(i + 1),
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

func insertMarkers(ctx context.Context, markers []Marker, db *pgxpool.Pool) error {
	for _, marker := range markers {
		_, err := db.Exec(ctx, "INSERT INTO markers VALUES ($1, $2)", marker.GroupID, marker.PartitionID)
		if err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}
