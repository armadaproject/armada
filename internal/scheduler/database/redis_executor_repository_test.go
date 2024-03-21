package database

import (
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

func TestRedisExecutorRepository_LoadAndSave(t *testing.T) {
	t1 := time.Now().UTC().Round(1 * time.Microsecond)
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
							LastSeen: t1,
						},
					},
					LastUpdateTime:    t1,
					UnassignedJobRuns: []string{"run1", "run2"},
				},
				{
					Id:   "test-executor-2",
					Pool: "test-pool-2",
					Nodes: []*schedulerobjects.Node{
						{
							Id:       "test-node-2",
							LastSeen: t1,
						},
					},
					LastUpdateTime:    t1,
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
			ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 5*time.Second)
			defer cancel()
			withRedisExecutorRepository(ctx, func(repo *RedisExecutorRepository) {
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
			})
		})
	}
}

func withRedisExecutorRepository(ctx *armadacontext.Context, action func(repository *RedisExecutorRepository)) {
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379", DB: 10})
	defer client.FlushDB(ctx)
	defer client.Close()

	client.FlushDB(ctx)
	repo := NewRedisExecutorRepository(client, "pulsar")
	action(repo)
}
