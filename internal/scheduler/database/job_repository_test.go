package database

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"
	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/compress"
	"github.com/armadaproject/armada/internal/common/database"
	protoutil "github.com/armadaproject/armada/internal/common/proto"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

const defaultBatchSize = 1

func TestFetchInitialJobs(t *testing.T) {
	leasedJob := Job{
		JobID:          util.NewULID(),
		JobSet:         "test-jobset",
		Queue:          "test-queue",
		QueuedVersion:  1,
		SchedulingInfo: []byte{byte(0)},
		SubmitMessage:  []byte{},
	}

	expectedLeasedJob := Job{
		JobID:          leasedJob.JobID,
		JobSet:         "test-jobset",
		Queue:          "test-queue",
		QueuedVersion:  1,
		SchedulingInfo: []byte{byte(0)},
		Serial:         1,
	}

	leasedJobRun := Run{
		RunID:    util.NewULID(),
		JobID:    leasedJob.JobID,
		JobSet:   "test-jobset",
		Executor: "test-executor",
		Node:     fmt.Sprintf("test-node-%d", 0),
		Running:  true,
	}

	expectedLeasedJobRun := Run{
		RunID:    leasedJobRun.RunID,
		JobID:    leasedJob.JobID,
		JobSet:   "test-jobset",
		Executor: "test-executor",
		Node:     fmt.Sprintf("test-node-%d", 0),
		Running:  true,
		Serial:   1,
	}

	queuedJob := Job{
		JobID:          util.NewULID(),
		JobSet:         "test-jobset",
		Queue:          "test-queue",
		Queued:         true,
		QueuedVersion:  1,
		SchedulingInfo: []byte{byte(0)},
		SubmitMessage:  []byte{},
	}

	expectedQueuedJob := Job{
		JobID:          queuedJob.JobID,
		JobSet:         "test-jobset",
		Queue:          "test-queue",
		Queued:         true,
		QueuedVersion:  1,
		SchedulingInfo: []byte{byte(0)},
		Serial:         2,
	}

	cancelledJob := Job{
		JobID:          util.NewULID(),
		JobSet:         "test-jobset",
		Queue:          "test-queue",
		QueuedVersion:  1,
		Cancelled:      true,
		SchedulingInfo: []byte{byte(0)},
		SubmitMessage:  []byte{},
	}

	cancelledJobRun := Run{
		RunID:     util.NewULID(),
		JobID:     cancelledJob.JobID,
		JobSet:    "test-jobset",
		Executor:  "test-executor",
		Node:      fmt.Sprintf("test-node-%d", 0),
		Cancelled: true,
	}

	failedJob := Job{
		JobID:          util.NewULID(),
		JobSet:         "test-jobset",
		Queue:          "test-queue",
		QueuedVersion:  1,
		Failed:         true,
		SchedulingInfo: []byte{byte(0)},
		SubmitMessage:  []byte{},
	}

	failedJobRun := Run{
		RunID:    util.NewULID(),
		JobID:    failedJob.JobID,
		JobSet:   "test-jobset",
		Executor: "test-executor",
		Node:     fmt.Sprintf("test-node-%d", 0),
		Failed:   true,
	}

	succeededJob := Job{
		JobID:          util.NewULID(),
		JobSet:         "test-jobset",
		Queue:          "test-queue",
		Queued:         false,
		QueuedVersion:  1,
		Succeeded:      true,
		SchedulingInfo: []byte{byte(0)},
		SubmitMessage:  []byte{},
	}

	succeededJobRun := Run{
		RunID:     util.NewULID(),
		JobID:     succeededJob.JobID,
		JobSet:    "test-jobset",
		Executor:  "test-executor",
		Node:      fmt.Sprintf("test-node-%d", 0),
		Succeeded: true,
	}

	tests := map[string]struct {
		dbJobs       []Job
		dbRuns       []Run
		expectedJobs []Job
		expectedRuns []Run
	}{
		"all jobs and runs": {
			dbJobs:       []Job{leasedJob, queuedJob, cancelledJob, failedJob, succeededJob},
			expectedJobs: []Job{expectedLeasedJob, expectedQueuedJob},
			dbRuns:       []Run{leasedJobRun, cancelledJobRun, failedJobRun, succeededJobRun},
			expectedRuns: []Run{expectedLeasedJobRun},
		},
		"only jobs": {
			dbJobs:       []Job{leasedJob, queuedJob, cancelledJob, failedJob, succeededJob},
			expectedJobs: []Job{expectedLeasedJob, expectedQueuedJob},
			expectedRuns: []Run{},
		},
		"only runs": {
			dbRuns:       []Run{leasedJobRun, cancelledJobRun, failedJobRun, succeededJobRun},
			expectedJobs: []Job{},
			expectedRuns: []Run{},
		},
		"empty db": {
			expectedJobs: []Job{},
			expectedRuns: []Run{},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			err := withJobRepository(func(repo *PostgresJobRepository) error {
				ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 5*time.Second)

				// Set up db
				err := database.UpsertWithTransaction(ctx, repo.db, "jobs", tc.dbJobs)
				require.NoError(t, err)
				err = database.UpsertWithTransaction(ctx, repo.db, "runs", tc.dbRuns)
				require.NoError(t, err)

				// Fetch updates
				jobs, runs, err := repo.FetchInitialJobs(ctx)
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
				ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 5*time.Second)

				// Set up db
				err := database.UpsertWithTransaction(ctx, repo.db, "jobs", tc.dbJobs)
				require.NoError(t, err)
				err = database.UpsertWithTransaction(ctx, repo.db, "runs", tc.dbRuns)
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
			RunID: uuid.NewString(),
			JobID: util.NewULID(),
			Error: protoutil.MustMarshallAndCompress(runError, compress.NewThreadSafeZlibCompressor(1024)),
		}
	}

	tests := map[string]struct {
		errorsInDb  []JobRunError
		idsToLookup []string
		expected    map[string]*armadaevents.Error
		expectError bool
	}{
		"single error": {
			errorsInDb:  dbErrors,
			idsToLookup: []string{dbErrors[1].RunID},
			expected:    map[string]*armadaevents.Error{dbErrors[1].RunID: expectedErrors[1]},
		},
		"multiple errors": {
			errorsInDb:  dbErrors,
			idsToLookup: []string{dbErrors[1].RunID, dbErrors[4].RunID, dbErrors[5].RunID, dbErrors[7].RunID},
			expected: map[string]*armadaevents.Error{
				dbErrors[1].RunID: expectedErrors[1],
				dbErrors[4].RunID: expectedErrors[4],
				dbErrors[5].RunID: expectedErrors[5],
				dbErrors[7].RunID: expectedErrors[7],
			},
		},
		"some errors missing": {
			errorsInDb:  dbErrors,
			idsToLookup: []string{dbErrors[1].RunID, uuid.NewString(), uuid.NewString(), dbErrors[7].RunID},
			expected: map[string]*armadaevents.Error{
				dbErrors[1].RunID: expectedErrors[1],
				dbErrors[7].RunID: expectedErrors[7],
			},
		},
		"all errors missing": {
			errorsInDb:  dbErrors,
			idsToLookup: []string{uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString()},
			expected:    map[string]*armadaevents.Error{},
		},
		"emptyDb": {
			errorsInDb:  []JobRunError{},
			idsToLookup: []string{uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString()},
			expected:    map[string]*armadaevents.Error{},
		},
		"invalid data": {
			errorsInDb: []JobRunError{{
				RunID: dbErrors[0].RunID,
				JobID: dbErrors[0].JobID,
				Error: []byte{0x1, 0x4, 0x5}, // not a valid compressed proto
			}},
			idsToLookup: []string{dbErrors[0].RunID},
			expectError: true,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			err := withJobRepository(func(repo *PostgresJobRepository) error {
				ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 5*time.Second)
				// Set up db
				err := database.UpsertWithTransaction(ctx, repo.db, "job_run_errors", tc.errorsInDb)
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
				ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 5*time.Second)

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

	testCancelUser := "test-cancelling-user"

	for i := 0; i < numJobs; i++ {
		dbJobs[i] = Job{
			JobID:                   util.NewULID(),
			JobSet:                  "test-jobset",
			Queue:                   "test-queue",
			Submitted:               int64(i),
			Priority:                int64(i),
			Queued:                  false,
			QueuedVersion:           1,
			CancelRequested:         true,
			CancelUser:              &testCancelUser,
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
			CancelUser:              job.CancelUser,
			Queued:                  job.Queued,
			QueuedVersion:           job.QueuedVersion,
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
	runIds := make([]string, 3)
	for i := 0; i < len(runIds); i++ {
		runIds[i] = uuid.New().String()
	}

	tests := map[string]struct {
		dbRuns           []Run
		runsToCheck      []string
		expectedInactive []string
	}{
		"empty database": {
			runsToCheck:      runIds,
			expectedInactive: runIds,
		},
		"no inactive": {
			runsToCheck: runIds,
			dbRuns: []Run{
				{RunID: runIds[0]},
				{RunID: runIds[1]},
				{RunID: runIds[2]},
			},
			expectedInactive: nil,
		},
		"run succeeded": {
			runsToCheck: runIds,
			dbRuns: []Run{
				{RunID: runIds[0]},
				{RunID: runIds[1], Succeeded: true},
				{RunID: runIds[2]},
			},
			expectedInactive: []string{runIds[1]},
		},
		"run failed": {
			runsToCheck: runIds,
			dbRuns: []Run{
				{RunID: runIds[0]},
				{RunID: runIds[1], Failed: true},
				{RunID: runIds[2]},
			},
			expectedInactive: []string{runIds[1]},
		},
		"run cancelled": {
			runsToCheck: runIds,
			dbRuns: []Run{
				{RunID: runIds[0]},
				{RunID: runIds[1], Cancelled: true},
				{RunID: runIds[2]},
			},
			expectedInactive: []string{runIds[1]},
		},
		"run missing": {
			runsToCheck: runIds,
			dbRuns: []Run{
				{RunID: runIds[0]},
				{RunID: runIds[2]},
			},
			expectedInactive: []string{runIds[1]},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			err := withJobRepository(func(repo *PostgresJobRepository) error {
				ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 500*time.Second)

				// Set up db
				err := database.UpsertWithTransaction(ctx, repo.db, "runs", tc.dbRuns)
				require.NoError(t, err)

				inactive, err := repo.FindInactiveRuns(ctx, tc.runsToCheck)
				require.NoError(t, err)
				slices.Sort(inactive)
				slices.Sort(tc.expectedInactive)
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
			RunID:    uuid.NewString(),
			JobID:    dbJobs[0].JobID,
			JobSet:   "test-jobset",
			Executor: executorName,
			Pool:     "test-pool",
		},
		{
			RunID:    uuid.NewString(),
			JobID:    dbJobs[1].JobID,
			JobSet:   "test-jobset",
			Executor: executorName,
			Pool:     "test-pool-away",
		},
		{
			RunID:    uuid.NewString(),
			JobID:    dbJobs[2].JobID,
			JobSet:   "test-jobset",
			Executor: executorName,
			Pool:     "test-pool",
		},
		{
			RunID:    uuid.NewString(),
			JobID:    dbJobs[2].JobID,
			JobSet:   "test-jobset",
			Executor: executorName,
			Pool:     "test-pool",
			PodRequirementsOverlay: protoutil.MustMarshall(
				&schedulerobjects.PodRequirements{
					Tolerations: []*v1.Toleration{
						{
							Key:    "whale",
							Value:  "true",
							Effect: v1.TaintEffectNoSchedule,
						},
					},
				},
			),
		},
		{
			RunID:    uuid.NewString(),
			JobID:    dbJobs[0].JobID,
			JobSet:   "test-jobset",
			Executor: executorName,
			Pool:     "test-pool",
			Failed:   true, // should be ignored as terminal
		},
		{
			RunID:     uuid.NewString(),
			JobID:     dbJobs[0].JobID,
			JobSet:    "test-jobset",
			Executor:  executorName,
			Pool:      "test-pool",
			Cancelled: true, // should be ignored as terminal
		},
		{
			RunID:     uuid.NewString(),
			JobID:     dbJobs[3].JobID,
			JobSet:    "test-jobset",
			Executor:  executorName,
			Pool:      "test-pool",
			Succeeded: true, // should be ignored as terminal
		},
	}
	expectedLeases := make([]*JobRunLease, 4)
	for i := range expectedLeases {
		expectedLeases[i] = &JobRunLease{
			RunID:                  dbRuns[i].RunID,
			Queue:                  dbJobs[i].Queue,
			Pool:                   dbRuns[i].Pool,
			JobSet:                 dbJobs[i].JobSet,
			UserID:                 dbJobs[i].UserID,
			Groups:                 dbJobs[i].Groups,
			SubmitMessage:          dbJobs[i].SubmitMessage,
			PodRequirementsOverlay: dbRuns[i].PodRequirementsOverlay,
		}
	}
	tests := map[string]struct {
		dbRuns         []Run
		dbJobs         []Job
		excludedRuns   []string
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
			excludedRuns:   []string{dbRuns[1].RunID},
			maxRowsToFetch: 100,
			executor:       executorName,
			expectedLeases: []*JobRunLease{expectedLeases[0], expectedLeases[2], expectedLeases[3]},
		},
		"exclude everything": {
			dbJobs:         dbJobs,
			dbRuns:         dbRuns,
			excludedRuns:   []string{dbRuns[0].RunID, dbRuns[1].RunID, dbRuns[2].RunID, dbRuns[3].RunID},
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
				ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 5*time.Second)

				// Set up db
				err := database.UpsertWithTransaction(ctx, repo.db, "jobs", tc.dbJobs)
				require.NoError(t, err)
				err = database.UpsertWithTransaction(ctx, repo.db, "runs", tc.dbRuns)
				require.NoError(t, err)

				leases, err := repo.FetchJobRunLeases(ctx, tc.executor, tc.maxRowsToFetch, tc.excludedRuns)
				require.NoError(t, err)
				leaseSort := func(a *JobRunLease, b *JobRunLease) int {
					if a.RunID > b.RunID {
						return -1
					} else if a.RunID < b.RunID {
						return 1
					} else {
						return 0
					}
				}
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
			RunID:     uuid.NewString(),
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

func insertMarkers(ctx *armadacontext.Context, markers []Marker, db *pgxpool.Pool) error {
	for _, marker := range markers {
		_, err := db.Exec(ctx, "INSERT INTO markers VALUES ($1, $2)", marker.GroupID, marker.PartitionID)
		if err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}
