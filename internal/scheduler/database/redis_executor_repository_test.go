package database

import (
	"testing"
	"time"

	"github.com/go-redis/redis"
	log "github.com/sirupsen/logrus"
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
			withRedisExecutorRepository(func(repo *RedisExecutorRepository) {
				ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 5*time.Second)
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
			})
		})
	}
}

func withRedisExecutorRepository(action func(repository *RedisExecutorRepository)) {
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379", DB: 10})
	defer client.FlushDB()
	defer func() {
		err := client.Close()
		log.WithError(err).Warn("Error closing redis client")
	}()
	repo := NewRedisExecutorRepository(client, "pulsar")
	action(repo)
}
