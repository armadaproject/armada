package database

import (
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"

	"github.com/armadaproject/armada/internal/common/armadacontext"
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

func TestExecutorRepository_GetExecutor(t *testing.T) {
	now := time.Now().UTC().Round(1 * time.Microsecond) // postgres only stores times with micro precision
	stored := &schedulerobjects.Executor{
		Id:   "test-executor-1",
		Pool: "test-pool-1",
		Nodes: []*schedulerobjects.Node{
			{Id: "test-node-1", LastSeen: protoutil.ToTimestamp(now)},
		},
		LastUpdateTime: protoutil.ToTimestamp(now),
	}
	tests := map[string]struct {
		id       string
		expected *schedulerobjects.Executor
	}{
		"found":     {id: "test-executor-1", expected: stored},
		"not found": {id: "missing-executor", expected: nil},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			err := withExecutorRepository(func(repo *PostgresExecutorRepository) error {
				ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 5*time.Second)
				defer cancel()
				require.NoError(t, repo.StoreExecutor(ctx, stored))
				require.NoError(t, repo.StoreExecutor(ctx, &schedulerobjects.Executor{Id: "test-executor-2", LastUpdateTime: protoutil.ToTimestamp(now)}))

				executor, err := repo.GetExecutor(ctx, tc.id)
				require.NoError(t, err)
				assert.Equal(t, tc.expected, executor)
				return nil
			})
			require.NoError(t, err)
		})
	}
}

func TestExecutorRepository_GetExecutorSettingsByID(t *testing.T) {
	now := time.Now().UTC().Round(1 * time.Microsecond) // postgres only stores times with micro precision
	stored := &schedulerobjects.ExecutorSettings{
		ExecutorId:   "test-executor-1",
		Cordoned:     true,
		CordonReason: "maintenance",
		SetByUser:    "alice",
		SetAtTime:    protoutil.ToTimestamp(now),
	}
	tests := map[string]struct {
		id       string
		expected *schedulerobjects.ExecutorSettings
	}{
		"found":     {id: "test-executor-1", expected: stored},
		"not found": {id: "missing-executor", expected: nil},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			err := withExecutorRepository(func(repo *PostgresExecutorRepository) error {
				ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 5*time.Second)
				defer cancel()
				queries := New(repo.db)
				for _, settings := range []*schedulerobjects.ExecutorSettings{
					stored,
					{ExecutorId: "test-executor-2", SetByUser: "bob", SetAtTime: protoutil.ToTimestamp(now)},
				} {
					require.NoError(t, queries.UpsertExecutorSettings(ctx, UpsertExecutorSettingsParams{
						ExecutorID:   settings.ExecutorId,
						Cordoned:     settings.Cordoned,
						CordonReason: settings.CordonReason,
						SetByUser:    settings.SetByUser,
						SetAtTime:    now,
					}))
				}

				settings, err := repo.GetExecutorSettingsByID(ctx, tc.id)
				require.NoError(t, err)
				assert.Equal(t, tc.expected, settings)
				return nil
			})
			require.NoError(t, err)
		})
	}
}

func withExecutorRepository(action func(repository *PostgresExecutorRepository) error) error {
	return WithTestDb(func(_ *Queries, db *pgxpool.Pool) error {
		repo := NewPostgresExecutorRepository(db)
		return action(repo)
	})
}
