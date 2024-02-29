package database

import (
	"testing"

	"github.com/go-redis/redis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"

	clientQueue "github.com/armadaproject/armada/pkg/client/queue"
)

func TestLegacyQueueRepository_GetAllQueues(t *testing.T) {
	tests := map[string]struct {
		queues         []clientQueue.Queue
		expectedQueues []*Queue
	}{
		"Not empty": {
			queues: []clientQueue.Queue{
				{
					Name:           "test-queue-1",
					PriorityFactor: 10,
				},
				{
					Name:           "test-queue-2",
					PriorityFactor: 20,
				},
			},
			expectedQueues: []*Queue{
				{
					Name:   "test-queue-1",
					Weight: 10,
				},
				{
					Name:   "test-queue-2",
					Weight: 20,
				},
			},
		},
		"Empty": {
			queues:         []clientQueue.Queue{},
			expectedQueues: []*Queue{},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			rc := redis.NewClient(&redis.Options{Addr: "localhost:6379", DB: 10})
			rc.FlushDB()
			defer func() {
				rc.FlushDB()
				_ = rc.Close()
			}()
			repo := NewLegacyQueueRepository(rc)
			for _, queue := range tc.queues {
				err := repo.backingRepo.CreateQueue(queue)
				require.NoError(t, err)
			}
			retrievedQueues, err := repo.GetAllQueues()
			require.NoError(t, err)
			sortFunc := func(a, b *Queue) int {
				if a.Name > b.Name {
					return -1
				} else if a.Name > b.Name {
					return 1
				} else {
					return 0
				}
			}
			slices.SortFunc(tc.expectedQueues, sortFunc)
			slices.SortFunc(retrievedQueues, sortFunc)
			assert.Equal(t, tc.expectedQueues, retrievedQueues)
		})
	}
}
