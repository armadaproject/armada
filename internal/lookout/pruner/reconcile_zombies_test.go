package pruner

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clock "k8s.io/utils/clock/testing"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/compress"
	"github.com/armadaproject/armada/internal/common/database/lookout"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/lookout/repository"
	"github.com/armadaproject/armada/internal/lookoutingester/instructions"
	"github.com/armadaproject/armada/internal/lookoutingester/lookoutdb"
	"github.com/armadaproject/armada/internal/lookoutingester/metrics"
)

// zombieScenario describes a job that should already exist in a terminal
// state in the database, and the non-terminal state we will then forcibly
// rewind job.state to in order to simulate the historical ingester bug.
type zombieScenario struct {
	jobId            string
	runFinishedAt    time.Time
	terminalJobState lookout.JobState
	rewindToJobState lookout.JobState
	expectRepair     bool
	expectedJobState lookout.JobState // only checked when expectRepair is true
}

func TestReconcileZombieJobs(t *testing.T) {
	type testCase struct {
		name                  string
		zombieRepairThreshold time.Duration
		batchSize             int
		scenarios             []zombieScenario
	}

	id := func() string { return util.NewULID() }

	testCases := []testCase{
		{
			name:                  "succeeded run zombie is repaired",
			zombieRepairThreshold: 1 * time.Hour,
			batchSize:             10,
			scenarios: []zombieScenario{
				{
					jobId:            id(),
					runFinishedAt:    baseTime.Add(-2 * time.Hour),
					terminalJobState: lookout.JobSucceeded,
					rewindToJobState: lookout.JobRunning,
					expectRepair:     true,
					expectedJobState: lookout.JobSucceeded,
				},
			},
		},
		{
			name:                  "failed run zombie is repaired",
			zombieRepairThreshold: 1 * time.Hour,
			batchSize:             10,
			scenarios: []zombieScenario{
				{
					jobId:            id(),
					runFinishedAt:    baseTime.Add(-2 * time.Hour),
					terminalJobState: lookout.JobFailed,
					rewindToJobState: lookout.JobPending,
					expectRepair:     true,
					expectedJobState: lookout.JobFailed,
				},
			},
		},
		{
			name:                  "cancelled run zombie is repaired",
			zombieRepairThreshold: 1 * time.Hour,
			batchSize:             10,
			scenarios: []zombieScenario{
				{
					jobId:            id(),
					runFinishedAt:    baseTime.Add(-2 * time.Hour),
					terminalJobState: lookout.JobCancelled,
					rewindToJobState: lookout.JobQueued,
					expectRepair:     true,
					expectedJobState: lookout.JobCancelled,
				},
			},
		},
		{
			name:                  "preempted run zombie is repaired",
			zombieRepairThreshold: 1 * time.Hour,
			batchSize:             10,
			scenarios: []zombieScenario{
				{
					jobId:            id(),
					runFinishedAt:    baseTime.Add(-2 * time.Hour),
					terminalJobState: lookout.JobPreempted,
					rewindToJobState: lookout.JobLeased,
					expectRepair:     true,
					expectedJobState: lookout.JobPreempted,
				},
			},
		},
		{
			name:                  "zombie within grace period is left alone",
			zombieRepairThreshold: 1 * time.Hour,
			batchSize:             10,
			scenarios: []zombieScenario{
				{
					jobId:            id(),
					runFinishedAt:    baseTime.Add(-30 * time.Minute),
					terminalJobState: lookout.JobSucceeded,
					rewindToJobState: lookout.JobRunning,
					expectRepair:     false,
				},
			},
		},
		{
			name:                  "many zombies repaired across multiple batches",
			zombieRepairThreshold: 1 * time.Hour,
			batchSize:             3,
			scenarios: func() []zombieScenario {
				ss := make([]zombieScenario, 7)
				for i := range ss {
					ss[i] = zombieScenario{
						jobId:            id(),
						runFinishedAt:    baseTime.Add(-2 * time.Hour),
						terminalJobState: lookout.JobSucceeded,
						rewindToJobState: lookout.JobRunning,
						expectRepair:     true,
						expectedJobState: lookout.JobSucceeded,
					}
				}
				return ss
			}(),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := lookout.WithLookoutDb(func(db *pgxpool.Pool) error {
				converter := instructions.NewInstructionConverter(metrics.Get().Metrics, "armadaproject.io/", []string{}, &compress.NoOpCompressor{})
				store := lookoutdb.NewLookoutDb(db, nil, metrics.Get(), 10, 10)

				ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 5*time.Minute)
				defer cancel()

				for _, s := range tc.scenarios {
					seedTerminalJob(t, ctx, db, store, converter, s)
					rewindJobState(t, ctx, db, s.jobId, s.rewindToJobState)
				}

				dbConn, err := db.Acquire(ctx)
				require.NoError(t, err)

				repaired, err := ReconcileZombieJobs(ctx, dbConn.Conn(), tc.zombieRepairThreshold, tc.batchSize, clock.NewFakeClock(baseTime))
				require.NoError(t, err)

				expectedRepairs := 0
				for _, s := range tc.scenarios {
					actual := readJobState(t, ctx, db, s.jobId)
					if s.expectRepair {
						expectedRepairs++
						assert.Equal(t, s.expectedJobState, actual, "job %s should have been repaired to %s but is %s", s.jobId, s.expectedJobState, actual)
					} else {
						assert.Equal(t, s.rewindToJobState, actual, "job %s should NOT have been repaired but state is %s", s.jobId, actual)
					}
				}
				assert.Equal(t, expectedRepairs, repaired)

				return nil
			})
			assert.NoError(t, err)
		})
	}
}

// TestReconcileZombieJobsLeavesNonZombiesAlone verifies the reconciler does
// not touch jobs that are legitimately in a non-terminal state with a still-
// running latest run, nor jobs already in their proper terminal state.
func TestReconcileZombieJobsLeavesNonZombiesAlone(t *testing.T) {
	err := lookout.WithLookoutDb(func(db *pgxpool.Pool) error {
		converter := instructions.NewInstructionConverter(metrics.Get().Metrics, "armadaproject.io/", []string{}, &compress.NoOpCompressor{})
		store := lookoutdb.NewLookoutDb(db, nil, metrics.Get(), 10, 10)

		ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 5*time.Minute)
		defer cancel()

		// Healthy running job (no terminal run yet).
		runningJobId := util.NewULID()
		runningRunId := uuid.NewString()
		repository.NewJobSimulator(converter, store).
			Submit("queue", "jobSet", "owner", "namespace", baseTime.Add(-3*time.Hour), &repository.JobOptions{JobId: runningJobId}).
			Lease(runningRunId, "cluster", "node", "pool", baseTime.Add(-3*time.Hour)).
			Pending(runningRunId, "cluster", baseTime.Add(-3*time.Hour)).
			Running(runningRunId, "node", baseTime.Add(-3*time.Hour)).
			Build()

		// Healthy terminal job (state and latest run already in agreement).
		healthyTerminalJobId := util.NewULID()
		healthyTerminalRunId := uuid.NewString()
		repository.NewJobSimulator(converter, store).
			Submit("queue", "jobSet", "owner", "namespace", baseTime.Add(-3*time.Hour), &repository.JobOptions{JobId: healthyTerminalJobId}).
			Lease(healthyTerminalRunId, "cluster", "node", "pool", baseTime.Add(-3*time.Hour)).
			Pending(healthyTerminalRunId, "cluster", baseTime.Add(-3*time.Hour)).
			Running(healthyTerminalRunId, "node", baseTime.Add(-3*time.Hour)).
			RunSucceeded(healthyTerminalRunId, baseTime.Add(-2*time.Hour)).
			Succeeded(baseTime.Add(-2 * time.Hour)).
			Build()

		dbConn, err := db.Acquire(ctx)
		require.NoError(t, err)

		repaired, err := ReconcileZombieJobs(ctx, dbConn.Conn(), 1*time.Hour, 10, clock.NewFakeClock(baseTime))
		require.NoError(t, err)
		assert.Equal(t, 0, repaired)

		assert.Equal(t, lookout.JobRunning, readJobState(t, ctx, db, runningJobId))
		assert.Equal(t, lookout.JobSucceeded, readJobState(t, ctx, db, healthyTerminalJobId))

		return nil
	})
	assert.NoError(t, err)
}

// TestReconcileZombieJobsCountsNullFinishedZombies verifies that zombie jobs
// whose latest run has no finished timestamp are observed via the
// zombiesSkippedNullFinished metric (and are NOT silently repaired with bogus
// data, since the reconciler relies on finished to populate
// last_transition_time).
func TestReconcileZombieJobsCountsNullFinishedZombies(t *testing.T) {
	err := lookout.WithLookoutDb(func(db *pgxpool.Pool) error {
		converter := instructions.NewInstructionConverter(metrics.Get().Metrics, "armadaproject.io/", []string{}, &compress.NoOpCompressor{})
		store := lookoutdb.NewLookoutDb(db, nil, metrics.Get(), 10, 10)

		ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 5*time.Minute)
		defer cancel()

		// Build a normal terminal job, then NULL out the run's finished
		// timestamp and rewind the job state so it looks like a zombie whose
		// run never recorded a finish time.
		jobId := util.NewULID()
		seedTerminalJob(t, ctx, db, store, converter, zombieScenario{
			jobId:            jobId,
			runFinishedAt:    baseTime.Add(-2 * time.Hour),
			terminalJobState: lookout.JobSucceeded,
		})
		_, err := db.Exec(ctx, `UPDATE job_run SET finished = NULL WHERE job_id = $1`, jobId)
		require.NoError(t, err)
		rewindJobState(t, ctx, db, jobId, lookout.JobRunning)

		dbConn, err := db.Acquire(ctx)
		require.NoError(t, err)

		before := testutil.ToFloat64(zombiesSkippedNullFinished)
		repaired, err := ReconcileZombieJobs(ctx, dbConn.Conn(), 1*time.Hour, 10, clock.NewFakeClock(baseTime))
		require.NoError(t, err)
		after := testutil.ToFloat64(zombiesSkippedNullFinished)

		// Not repaired: the reconciler refuses to touch a row with NULL finished.
		assert.Equal(t, 0, repaired)
		assert.Equal(t, lookout.JobRunning, readJobState(t, ctx, db, jobId))

		// But the diagnostic counter incremented, proving the gap is observable.
		assert.GreaterOrEqual(t, after-before, 1.0)
		return nil
	})
	assert.NoError(t, err)
}

// TestPruneDbRunsZombieReconciliation is a thin smoke test that
// PruneDb invokes ReconcileZombieJobs when the threshold is non-zero.
func TestPruneDbRunsZombieReconciliation(t *testing.T) {
	err := lookout.WithLookoutDb(func(db *pgxpool.Pool) error {
		converter := instructions.NewInstructionConverter(metrics.Get().Metrics, "armadaproject.io/", []string{}, &compress.NoOpCompressor{})
		store := lookoutdb.NewLookoutDb(db, nil, metrics.Get(), 10, 10)

		ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 5*time.Minute)
		defer cancel()

		zombie := zombieScenario{
			jobId:            util.NewULID(),
			runFinishedAt:    baseTime.Add(-2 * time.Hour),
			terminalJobState: lookout.JobSucceeded,
			rewindToJobState: lookout.JobRunning,
			expectRepair:     true,
			expectedJobState: lookout.JobSucceeded,
		}
		seedTerminalJob(t, ctx, db, store, converter, zombie)
		rewindJobState(t, ctx, db, zombie.jobId, zombie.rewindToJobState)

		dbConn, err := db.Acquire(ctx)
		require.NoError(t, err)

		isHC := isHotColdSchema(ctx, db)
		err = PruneDb(ctx, dbConn.Conn(), 100*time.Hour, 100*time.Hour, 1*time.Hour, 10, clock.NewFakeClock(baseTime), isHC)
		require.NoError(t, err)

		assert.Equal(t, lookout.JobSucceeded, readJobState(t, ctx, db, zombie.jobId))
		return nil
	})
	assert.NoError(t, err)
}

func seedTerminalJob(
	t *testing.T,
	ctx *armadacontext.Context,
	db *pgxpool.Pool,
	store *lookoutdb.LookoutDb,
	converter *instructions.InstructionConverter,
	s zombieScenario,
) {
	t.Helper()
	runId := uuid.NewString()
	sim := repository.NewJobSimulator(converter, store).
		Submit("queue", "jobSet", "owner", "namespace", s.runFinishedAt.Add(-1*time.Hour), &repository.JobOptions{JobId: s.jobId}).
		Lease(runId, "cluster", "node", "pool", s.runFinishedAt.Add(-1*time.Hour)).
		Pending(runId, "cluster", s.runFinishedAt.Add(-1*time.Hour)).
		Running(runId, "node", s.runFinishedAt.Add(-1*time.Hour))

	switch s.terminalJobState {
	case lookout.JobSucceeded:
		sim.RunSucceeded(runId, s.runFinishedAt).Succeeded(s.runFinishedAt).Build()
	case lookout.JobFailed:
		sim.RunFailed(runId, "node", 1, "", "", s.runFinishedAt).Failed("node", 1, "", s.runFinishedAt).Build()
	case lookout.JobCancelled:
		sim.Cancelled(s.runFinishedAt, "canceluser").Build()
	case lookout.JobPreempted:
		sim.Preempted(s.runFinishedAt).Build()
	default:
		t.Fatalf("unsupported terminal state %s", s.terminalJobState)
	}

	// Cancelled and Preempted state transitions in the simulator do not
	// always set finished on the run row, but the reconciler relies on it.
	// Force it here so the test's assumptions match the production scenarios
	// the reconciler is designed to repair.
	forceRunFinished(t, ctx, db, s.jobId, s.runFinishedAt, s.terminalJobState)
}

func forceRunFinished(t *testing.T, ctx *armadacontext.Context, db *pgxpool.Pool, jobId string, finished time.Time, terminalJobState lookout.JobState) {
	t.Helper()
	runStateOrdinal := terminalRunStateForJobState(t, terminalJobState)
	_, err := db.Exec(ctx, `
		UPDATE job_run
		SET finished      = $1,
		    job_run_state = $2
		WHERE job_id = $3
		  AND run_id = (SELECT latest_run_id FROM job WHERE job_id = $3)`,
		finished, runStateOrdinal, jobId)
	require.NoError(t, err)
}

func terminalRunStateForJobState(t *testing.T, s lookout.JobState) int {
	t.Helper()
	switch s {
	case lookout.JobSucceeded:
		return lookout.JobRunSucceededOrdinal
	case lookout.JobFailed:
		return lookout.JobRunFailedOrdinal
	case lookout.JobCancelled:
		return lookout.JobRunCancelledOrdinal
	case lookout.JobPreempted:
		return lookout.JobRunPreemptedOrdinal
	}
	t.Fatalf("no terminal job_run_state for %s", s)
	return 0
}

func rewindJobState(t *testing.T, ctx *armadacontext.Context, db *pgxpool.Pool, jobId string, state lookout.JobState) {
	t.Helper()
	ordinal := lookout.JobStateOrdinalMap[state]
	_, err := db.Exec(ctx, `UPDATE job SET state = $1 WHERE job_id = $2`, ordinal, jobId)
	require.NoError(t, err)
}

func readJobState(t *testing.T, ctx *armadacontext.Context, db *pgxpool.Pool, jobId string) lookout.JobState {
	t.Helper()
	var ordinal int
	err := db.QueryRow(ctx, `SELECT state FROM job WHERE job_id = $1`, jobId).Scan(&ordinal)
	require.NoError(t, err)
	return lookout.JobStateMap[ordinal]
}
