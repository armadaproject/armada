package database

import (
	"context"
	"github.com/armadaproject/armada/internal/common/compress"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/stretchr/testify/assert"
	"golang.org/x/exp/slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestExecutorRepository_LoadAndSave(t *testing.T) {
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
							LastSeen: time.Now().UTC(),
						},
					},
					LastUpdateTime:    time.Now().UTC(),
					UnassignedJobRuns: []string{"run1", "run2"},
				},
				{
					Id:   "test-executor-2",
					Pool: "test-pool-2",
					Nodes: []*schedulerobjects.Node{
						{
							Id:       "test-node-2",
							LastSeen: time.Now().UTC(),
						},
					},
					LastUpdateTime:    time.Now().UTC(),
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
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				for _, executor := range tc.executors {
					err := repo.StoreExecutor(ctx, executor)
					require.NoError(t, err)
				}
				retrievedExecutors, err := repo.GetExecutors(ctx)
				require.NoError(t, err)
				executorSort := func(a *schedulerobjects.Executor, b *schedulerobjects.Executor) bool {
					return a.Id > b.Id
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
	t1 := time.Now().UTC()
	t2 := t1.Add(-1 * time.Second)
	tests := map[string]struct {
		executors           []*schedulerobjects.Executor
		expectedUpdateTimes map[string]time.Time
	}{
		"not empty": {
			executors: []*schedulerobjects.Executor{
				{
					Id:             "test-executor-1",
					LastUpdateTime: t1,
				},
				{
					Id:             "test-executor-2",
					LastUpdateTime: t2,
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
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
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

func withExecutorRepository(action func(repository *PostgresExecutorRepository) error) error {
	return WithTestDb(func(_ *Queries, db *pgxpool.Pool) error {
		repo := NewPostgresExecutorRepository(
			db,
			compress.NewThreadSafeZlibCompressor(1024),
			compress.NewThreadSafeZlibDecompressor())
		return action(repo)
	})
}
