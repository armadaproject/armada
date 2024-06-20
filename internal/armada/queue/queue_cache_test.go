package queue

import (
	"sort"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/internal/armada/mocks"
	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/pkg/client/queue"
)

func TestFetch(t *testing.T) {
	tests := map[string]struct {
		queues []queue.Queue
	}{
		"No Queues": {
			queues: []queue.Queue{},
		},
		"One Queue": {
			queues: []queue.Queue{{Name: "testQueue1"}},
		},
		"Many Queues": {
			queues: []queue.Queue{
				{Name: "testQueue1", PriorityFactor: 1},
				{Name: "testQueue2", PriorityFactor: 2},
				{Name: "testQueue3", PriorityFactor: 3},
				{Name: "testQueue4", PriorityFactor: 4},
				{Name: "testQueue5", PriorityFactor: 5},
			},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 5*time.Second)
			ctrl := gomock.NewController(t)
			mockRepo := mocks.NewMockQueueRepository(ctrl)
			mockRepo.EXPECT().GetAllQueues(ctx).Return(tc.queues, nil).Times(1)

			cache := NewCachedQueueRepository(mockRepo, 1*time.Millisecond)
			fetchErr := cache.fetchQueues(ctx)

			// Assert that getting all queues worked as expected
			queues, getErr := cache.GetAllQueues(ctx)
			require.NoError(t, fetchErr)
			sort.Slice(queues, func(i, j int) bool {
				return queues[i].Name < queues[j].Name
			})
			assert.NoError(t, getErr)
			assert.Equal(t, tc.queues, queues)

			// Assert that all queues can be fetched individually
			for _, expectedQueue := range tc.queues {
				actualQueue, err := cache.GetQueue(ctx, expectedQueue.Name)
				require.NoError(t, err)
				assert.Equal(t, expectedQueue, actualQueue)
			}
			ctrl.Finish()
			cancel()
		})
	}
}
