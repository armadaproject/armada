package queue

import (
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/database/lookout"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/client/queue"
)

var (
	queueA = queue.Queue{
		Name:                              "queueA",
		PriorityFactor:                    1000,
		Permissions:                       []queue.Permissions{},
		ResourceLimitsByPriorityClassName: map[string]api.PriorityClassResourceLimits{},
	}
	queueB = queue.Queue{
		Name:                              "queueB",
		PriorityFactor:                    2000,
		Permissions:                       []queue.Permissions{},
		ResourceLimitsByPriorityClassName: map[string]api.PriorityClassResourceLimits{},
	}
	twoQueues = []queue.Queue{queueA, queueB}
)

func TestGetAllQueues(t *testing.T) {
	tests := map[string]struct {
		queues []queue.Queue
	}{
		"Empty Database": {
			queues: []queue.Queue{},
		},
		"One Queue": {
			queues: []queue.Queue{queueA},
		},
		"Two Queues": {
			queues: []queue.Queue{queueA, queueB},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 5*time.Second)
			err := lookout.WithLookoutDb(func(db *pgxpool.Pool) error {
				repo := NewPostgresQueueRepository(db)
				for _, q := range tc.queues {
					err := repo.CreateQueue(ctx, q)
					require.NoError(t, err)
				}
				fetched, err := repo.GetAllQueues(ctx)
				assert.NoError(t, err)
				assert.Equal(t, tc.queues, fetched)
				return nil
			})
			assert.NoError(t, err)
			cancel()
		})
	}
}

func TestDeleteQueue(t *testing.T) {
	tests := map[string]struct {
		intialQueues  []queue.Queue
		queueToDelete string
	}{
		"Empty Database": {
			queueToDelete: "queueA",
		},
		"QueueNot present": {
			intialQueues:  twoQueues,
			queueToDelete: "queueC",
		},
		"Delete Queue": {
			intialQueues:  twoQueues,
			queueToDelete: "queueA",
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 5*time.Second)
			err := lookout.WithLookoutDb(func(db *pgxpool.Pool) error {
				repo := NewPostgresQueueRepository(db)
				for _, q := range tc.intialQueues {
					err := repo.CreateQueue(ctx, q)
					require.NoError(t, err)
				}
				err := repo.DeleteQueue(ctx, tc.queueToDelete)
				require.NoError(t, err)

				_, err = repo.GetQueue(ctx, tc.queueToDelete)
				assert.Equal(t, &ErrQueueNotFound{QueueName: tc.queueToDelete}, err)
				return nil
			})
			assert.NoError(t, err)
			cancel()
		})
	}
}

func TestGetAndUpdateQueue(t *testing.T) {
	tests := map[string]struct {
		intialQueues  []queue.Queue
		queueToUpdate queue.Queue
	}{
		"Empty Database": {
			queueToUpdate: queueA,
		},
		"Queue Doesn't Exist": {
			intialQueues: twoQueues,
			queueToUpdate: queue.Queue{
				Name:                              "queueC",
				Permissions:                       []queue.Permissions{},
				PriorityFactor:                    1,
				ResourceLimitsByPriorityClassName: map[string]api.PriorityClassResourceLimits{},
			},
		},
		"Queue Does Exist": {
			intialQueues: twoQueues,
			queueToUpdate: queue.Queue{
				Name:                              "queueA",
				PriorityFactor:                    queueA.PriorityFactor + 100,
				Permissions:                       []queue.Permissions{},
				ResourceLimitsByPriorityClassName: map[string]api.PriorityClassResourceLimits{},
			},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 5*time.Second)
			err := lookout.WithLookoutDb(func(db *pgxpool.Pool) error {
				repo := NewPostgresQueueRepository(db)
				for _, q := range tc.intialQueues {
					err := repo.CreateQueue(ctx, q)
					require.NoError(t, err)
				}
				err := repo.UpdateQueue(ctx, tc.queueToUpdate)
				assert.NoError(t, err)
				fetched, err := repo.GetQueue(ctx, tc.queueToUpdate.Name)
				require.NoError(t, err)
				assert.Equal(t, tc.queueToUpdate, fetched)
				return nil
			})
			assert.NoError(t, err)
			cancel()
		})
	}
}
