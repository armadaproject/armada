package submit

import (
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/database/lookout"
	"github.com/armadaproject/armada/pkg/api"
)

type deduplicationIdsWithQueue struct {
	queue string
	kvs   map[string]string
}

func TestDeduplicator(t *testing.T) {
	tests := map[string]struct {
		initialKeys  []deduplicationIdsWithQueue
		queueToFetch string
		jobsToFetch  []*api.JobSubmitRequestItem
		expectedKeys map[string]string
	}{
		"empty": {
			initialKeys:  []deduplicationIdsWithQueue{},
			queueToFetch: "testQueue",
			jobsToFetch: []*api.JobSubmitRequestItem{
				{ClientId: "a"},
			},
			expectedKeys: map[string]string{},
		},
		"all keys exist": {
			initialKeys: []deduplicationIdsWithQueue{
				{
					queue: "testQueue",
					kvs:   map[string]string{"foo": "bar", "fish": "chips"},
				},
			},
			queueToFetch: "testQueue",
			jobsToFetch: []*api.JobSubmitRequestItem{
				{ClientId: "foo"},
				{ClientId: "fish"},
			},
			expectedKeys: map[string]string{"foo": "bar", "fish": "chips"},
		},
		"some keys exist": {
			initialKeys: []deduplicationIdsWithQueue{
				{
					queue: "testQueue",
					kvs:   map[string]string{"fish": "chips"},
				},
			},
			queueToFetch: "testQueue",
			jobsToFetch: []*api.JobSubmitRequestItem{
				{ClientId: "foo"},
				{ClientId: "fish"},
			},
			expectedKeys: map[string]string{"fish": "chips"},
		},
		"consider queue to be part of  the key": {
			initialKeys: []deduplicationIdsWithQueue{
				{
					queue: "testQueue",
					kvs:   map[string]string{"foo": "bar"},
				},
			},
			queueToFetch: "anotherTestQueue",
			jobsToFetch: []*api.JobSubmitRequestItem{
				{ClientId: "foo"},
			},
			expectedKeys: map[string]string{},
		},
		"Don't fetch when clienmt id empty": {
			initialKeys: []deduplicationIdsWithQueue{
				{
					queue: "testQueue",
					kvs:   map[string]string{"": "bar"},
				},
			},
			queueToFetch: "testQueue",
			jobsToFetch: []*api.JobSubmitRequestItem{
				{ClientId: ""},
			},
			expectedKeys: map[string]string{},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 5*time.Second)
			err := lookout.WithLookoutDb(func(db *pgxpool.Pool) error {
				deduplicator := NewDeduplicator(db)

				// Store
				for _, keys := range tc.initialKeys {
					err := deduplicator.StoreOriginalJobIds(ctx, keys.queue, keys.kvs)
					require.NoError(t, err)
				}

				// Fetch
				keys, err := deduplicator.GetOriginalJobIds(ctx, tc.queueToFetch, tc.jobsToFetch)
				require.NoError(t, err)

				assert.Equal(t, tc.expectedKeys, keys)

				return nil
			})
			assert.NoError(t, err)
			cancel()
		})
	}
}
