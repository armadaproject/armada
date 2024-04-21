package repository

import (
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/database/lookout"
	"github.com/armadaproject/armada/pkg/client/queue"
)

var (
	queueA = queue.Queue{
		Name:           "queueA",
		PriorityFactor: 1000,
	}
	queueB = queue.Queue{
		Name:           "queueB",
		PriorityFactor: 2000,
	}
	twoQueues = []queue.Queue{queueA, queueB}
)

func TestGetQueue(t *testing.T) {
	tests := map[string]struct {
		intialQueues  []queue.Queue
		queueToFetch  string
		expectSuccess bool
		desiredQueue  queue.Queue
	}{
		"Empty Database": {
			queueToFetch:  "queueA",
			expectSuccess: false,
		},
		"Queue Found": {
			intialQueues:  twoQueues,
			queueToFetch:  "queueA",
			expectSuccess: true,
			desiredQueue:  queueA,
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
				fetched, err := repo.GetQueue(ctx, tc.queueToFetch)
				if tc.expectSuccess {
					assert.NoError(t, err)
				} else {
					assert.Error(t, err)
					assert.Equal(t, tc.desiredQueue, fetched)
				}
				return nil
			})
			assert.NoError(t, err)
			cancel()
		})
	}
}
