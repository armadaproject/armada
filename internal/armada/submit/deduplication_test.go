package submit

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/maps"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/pkg/api"
)

type deduplicationIdsWithQueue struct {
	queue string
	kvs   map[string]string
}

type InMemoryKeyValueStore struct {
	kvs map[string][]byte
}

func (m *InMemoryKeyValueStore) Store(_ *armadacontext.Context, kvs map[string][]byte) error {
	maps.Copy(m.kvs, kvs)
	return nil
}

func (m *InMemoryKeyValueStore) Load(_ *armadacontext.Context, keys []string) (map[string][]byte, error) {
	result := make(map[string][]byte, len(keys))
	for _, k := range keys {
		v, ok := m.kvs[k]
		if ok {
			result[k] = v
		}
	}
	return result, nil
}

func TestDeduplicator(t *testing.T) {
	tests := map[string]struct {
		intitialKeys []deduplicationIdsWithQueue
		queueToFetch string
		jobsToFetch  []*api.JobSubmitRequestItem
		expectedKeys map[string]string
	}{
		"empty": {
			intitialKeys: []deduplicationIdsWithQueue{},
			queueToFetch: "testQueue",
			jobsToFetch: []*api.JobSubmitRequestItem{
				{ClientId: "a"},
			},
			expectedKeys: map[string]string{},
		},
		"all keys exist": {
			intitialKeys: []deduplicationIdsWithQueue{
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
			intitialKeys: []deduplicationIdsWithQueue{
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
			intitialKeys: []deduplicationIdsWithQueue{
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
			intitialKeys: []deduplicationIdsWithQueue{
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
			deduplicator := NewDeduplicator(&InMemoryKeyValueStore{kvs: map[string][]byte{}})

			// Store
			for _, keys := range tc.intitialKeys {
				err := deduplicator.StoreOriginalJobIds(ctx, keys.queue, keys.kvs)
				require.NoError(t, err)
			}

			// Fetch
			keys, err := deduplicator.GetOriginalJobIds(ctx, tc.queueToFetch, tc.jobsToFetch)
			require.NoError(t, err)

			assert.Equal(t, tc.expectedKeys, keys)
			cancel()
		})
	}
}
