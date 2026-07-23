package database

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/constants"
	protoutil "github.com/armadaproject/armada/internal/common/proto"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

func TestExecutorRepository_LoadAndSave(t *testing.T) {
	t1 := time.Now().UTC().Round(1 * time.Microsecond) // postgres only stores times with micro precision
	t1Proto := protoutil.ToTimestamp(t1)
	tests := map[string]struct {
		executors []*schedulerobjects.Executor
	}{
		"not empty": {
			executors: []*schedulerobjects.Executor{
				{
					Id:   "test-executor-1",
					Pool: "test-pool-1",
					Nodes: []*schedulerobjects.Node{
						{
							Id:       "test-node-1",
							LastSeen: t1Proto,
						},
					},
					LastUpdateTime:    t1Proto,
					UnassignedJobRuns: []string{"run1", "run2"},
				},
				{
					Id:   "test-executor-2",
					Pool: "test-pool-2",
					Nodes: []*schedulerobjects.Node{
						{
							Id:       "test-node-2",
							LastSeen: t1Proto,
						},
					},
					LastUpdateTime:    t1Proto,
					UnassignedJobRuns: []string{"run3", "run4"},
				},
			},
		},
		"empty": {
			executors: []*schedulerobjects.Executor{},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			err := withExecutorRepository(func(repo *PostgresExecutorRepository) error {
				ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 5*time.Second)
				defer cancel()
				for _, executor := range tc.executors {
					err := repo.StoreExecutor(ctx, executor)
					require.NoError(t, err)
				}
				retrievedExecutors, err := repo.GetExecutors(ctx)
				require.NoError(t, err)
				executorSort := func(a *schedulerobjects.Executor, b *schedulerobjects.Executor) int {
					if a.Id > b.Id {
						return -1
					} else if a.Id < b.Id {
						return 1
					} else {
						return 0
					}
				}
				slices.SortFunc(retrievedExecutors, executorSort)
				slices.SortFunc(tc.executors, executorSort)
				require.Equal(t, len(tc.executors), len(retrievedExecutors))
				for i, expected := range tc.executors {
					assert.Equal(t, expected, retrievedExecutors[i])
				}

				return nil
			})
			require.NoError(t, err)
		})
	}
}

func TestExecutorRepository_GetLastUpdateTimes(t *testing.T) {
	t1 := time.Now().UTC().Round(1 * time.Microsecond) // postgres only stores times with micro precision
	t1Proto := protoutil.ToTimestamp(t1)
	t2 := t1.Add(-1 * time.Second)
	t2Proto := protoutil.ToTimestamp(t2)
	tests := map[string]struct {
		executors           []*schedulerobjects.Executor
		expectedUpdateTimes map[string]time.Time
	}{
		"not empty": {
			executors: []*schedulerobjects.Executor{
				{
					Id:             "test-executor-1",
					LastUpdateTime: t1Proto,
				},
				{
					Id:             "test-executor-2",
					LastUpdateTime: t2Proto,
				},
			},
			expectedUpdateTimes: map[string]time.Time{"test-executor-1": t1, "test-executor-2": t2},
		},
		"empty": {
			executors:           []*schedulerobjects.Executor{},
			expectedUpdateTimes: map[string]time.Time{},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			err := withExecutorRepository(func(repo *PostgresExecutorRepository) error {
				ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 5*time.Second)
				defer cancel()
				for _, executor := range tc.executors {
					err := repo.StoreExecutor(ctx, executor)
					require.NoError(t, err)
				}
				retrievedUpdateTimes, err := repo.GetLastUpdateTimes(ctx)
				require.NoError(t, err)
				assert.Equal(t, tc.expectedUpdateTimes, retrievedUpdateTimes)
				return nil
			})
			require.NoError(t, err)
		})
	}
}

func TestExecutorRepository_StoreExecutorRejectsDeletedExecutor(t *testing.T) {
	setAtTime := time.Now().UTC().Round(1 * time.Microsecond)
	lastUpdateTime := protoutil.ToTimestamp(setAtTime)
	err := withExecutorRepository(func(repo *PostgresExecutorRepository) error {
		ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 5*time.Second)
		defer cancel()
		queries := New(repo.db)
		err := queries.UpsertExecutorSettings(ctx, UpsertExecutorSettingsParams{
			ExecutorID:            "deleted-executor",
			Cordoned:              true,
			CordonReason:          constants.ExecutorDeletedCordonReason,
			SetByUser:             "alice",
			SetAtTime:             setAtTime,
			TombstoneCordonReason: constants.ExecutorDeletedCordonReason,
		})
		require.NoError(t, err)

		err = repo.StoreExecutor(ctx, &schedulerobjects.Executor{
			Id:             "deleted-executor",
			LastUpdateTime: lastUpdateTime,
		})
		require.ErrorIs(t, err, ErrExecutorDeleted)

		executors, err := repo.GetExecutors(ctx)
		require.NoError(t, err)
		for _, executor := range executors {
			assert.NotEqual(t, "deleted-executor", executor.Id)
		}
		return nil
	})
	require.NoError(t, err)
}

func TestExecutorRepository_DeleteExecutorDefersWhileRunsRemain(t *testing.T) {
	err := withExecutorRepository(func(repo *PostgresExecutorRepository) error {
		ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 5*time.Second)
		defer cancel()

		executorID := "active-executor"
		require.NoError(t, repo.StoreExecutor(ctx, &schedulerobjects.Executor{
			Id:             executorID,
			LastUpdateTime: protoutil.ToTimestamp(time.Now()),
		}))

		queries := New(repo.db)
		insertRunAssignedToExecutor(t, ctx, repo.db, executorID, false)

		err := repo.DeleteExecutor(ctx, executorID)
		require.ErrorIs(t, err, ErrExecutorNotDrained)

		_, err = queries.SelectExecutorById(ctx, executorID)
		require.NoError(t, err)

		err = repo.DeleteExecutor(ctx, executorID)
		require.ErrorIs(t, err, ErrExecutorNotDrained)
		return nil
	})
	require.NoError(t, err)
}

func TestExecutorRepository_DeleteExecutorDeletesWhenDrained(t *testing.T) {
	err := withExecutorRepository(func(repo *PostgresExecutorRepository) error {
		ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 5*time.Second)
		defer cancel()

		executorID := "drained-executor"
		require.NoError(t, repo.StoreExecutor(ctx, &schedulerobjects.Executor{
			Id:             executorID,
			LastUpdateTime: protoutil.ToTimestamp(time.Now()),
		}))

		queries := New(repo.db)
		insertRunAssignedToExecutor(t, ctx, repo.db, executorID, true)

		require.NoError(t, repo.DeleteExecutor(ctx, executorID))
		_, err := queries.SelectExecutorById(ctx, executorID)
		require.ErrorIs(t, err, pgx.ErrNoRows)

		require.NoError(t, repo.DeleteExecutor(ctx, executorID))
		return nil
	})
	require.NoError(t, err)
}

func withExecutorRepository(action func(repository *PostgresExecutorRepository) error) error {
	return WithTestDb(func(_ *Queries, db *pgxpool.Pool) error {
		repo := NewPostgresExecutorRepository(db)
		return action(repo)
	})
}

func insertRunAssignedToExecutor(t *testing.T, ctx *armadacontext.Context, db *pgxpool.Pool, executorID string, terminated bool) {
	t.Helper()
	jobID := uuid.NewString()
	runID := uuid.NewString()
	_, err := db.Exec(ctx, `
		INSERT INTO jobs (job_id, queue, job_set, user_id, submitted, priority, queued, queued_version, validated, cancel_requested, cancelled, succeeded, failed, scheduling_info, pools, last_modified)
		VALUES ($1, 'queue-a', 'job-set-a', 'user-a', 0, 0, false, 0, false, false, false, false, false, '{}', '{}', now())`, jobID)
	require.NoError(t, err)
	_, err = db.Exec(ctx, `
		INSERT INTO job_metadata (job_id, submit_message, groups)
		VALUES ($1, '{}', '{}')`, jobID)
	require.NoError(t, err)
	_, err = db.Exec(ctx, `
		INSERT INTO runs (run_id, job_id, created, job_set, executor, node, queue, failed, last_modified)
		VALUES ($1, $2, 0, 'job-set-a', $3, 'node-a', 'queue-a', $4, now())`, runID, jobID, executorID, terminated)
	require.NoError(t, err)
}
