package submit

import (
	"crypto/sha1"
	"fmt"

	"golang.org/x/exp/maps"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/pgkeyvalue"
	"github.com/armadaproject/armada/pkg/api"
)

// Deduplicator deduplicates jobs submitted ot armada in order to prevent double submission
type Deduplicator interface {
	GetOriginalJobIds(ctx *armadacontext.Context, queue string, jobRequests []*api.JobSubmitRequestItem) (map[string]string, error)
	StoreOriginalJobIds(ctx *armadacontext.Context, queue string, mappings map[string]string) error
}

// PostgresDeduplicator is an implementation of a Deduplicator that uses a pgkeyvalue.KeyValueStore as its state store
type PostgresDeduplicator struct {
	kvStore pgkeyvalue.KeyValueStore
}

func NewDeduplicator(kvStore pgkeyvalue.KeyValueStore) *PostgresDeduplicator {
	return &PostgresDeduplicator{kvStore: kvStore}
}

func (s *PostgresDeduplicator) GetOriginalJobIds(ctx *armadacontext.Context, queue string, jobRequests []*api.JobSubmitRequestItem) (map[string]string, error) {
	// Armada checks for duplicate job submissions if a ClientId (i.e. a deduplication id) is provided.
	// Deduplication is based on storing the combined hash of the ClientId and queue. For storage efficiency,
	// we store hashes instead of user-provided strings.
	kvs := make(map[string][]byte, len(jobRequests))
	for _, req := range jobRequests {
		if req.ClientId != "" {
			kvs[s.jobKey(queue, req.ClientId)] = []byte(req.ClientId)
		}
	}

	duplicates := make(map[string]string)
	// If we have any client Ids, retrieve their job ids
	if len(kvs) > 0 {
		keys := maps.Keys(kvs)
		existingKvs, err := s.kvStore.Load(ctx, keys)
		if err != nil {
			return nil, err
		}
		for k, v := range kvs {
			originalJobId, ok := existingKvs[k]
			if ok {
				duplicates[string(v)] = string(originalJobId)
			}
		}
	}
	return duplicates, nil
}

func (s *PostgresDeduplicator) StoreOriginalJobIds(ctx *armadacontext.Context, queue string, mappings map[string]string) error {
	if s.kvStore == nil || len(mappings) == 0 {
		return nil
	}
	kvs := make(map[string][]byte, len(mappings))
	for k, v := range mappings {
		kvs[s.jobKey(queue, k)] = []byte(v)
	}
	return s.kvStore.Store(ctx, kvs)
}

func (s *PostgresDeduplicator) jobKey(queue, clientId string) string {
	combined := fmt.Sprintf("%s:%s", queue, clientId)
	h := sha1.Sum([]byte(combined))
	return fmt.Sprintf("%x", h)
}
