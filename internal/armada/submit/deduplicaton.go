package submit

import (
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/exp/maps"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/pkg/api"
)

// Deduplicator deduplicates jobs submitted ot armada in order to prevent double submission
type Deduplicator interface {
	GetOriginalJobIds(ctx *armadacontext.Context, queue string, jobRequests []*api.JobSubmitRequestItem) (map[string]string, error)
	StoreOriginalJobIds(ctx *armadacontext.Context, queue string, mappings map[string]string) error
}

// PostgresDeduplicator is an implementation of a Deduplicator that uses a pgkeyvalue.KeyValueStore as its state store
type PostgresDeduplicator struct {
	db *pgxpool.Pool
}

func NewDeduplicator(db *pgxpool.Pool) *PostgresDeduplicator {
	return &PostgresDeduplicator{db: db}
}

func (s *PostgresDeduplicator) GetOriginalJobIds(ctx *armadacontext.Context, queue string, jobRequests []*api.JobSubmitRequestItem) (map[string]string, error) {
	// Armada checks for duplicate job submissions if a ClientId (i.e. a deduplication id) is provided.
	kvs := make(map[string]string, len(jobRequests))
	for _, req := range jobRequests {
		if req.ClientId != "" {
			kvs[s.jobKey(queue, req.ClientId)] = req.ClientId
		}
	}

	duplicates := make(map[string]string)
	// If we have any client Ids, retrieve their job ids
	if len(kvs) > 0 {
		keys := maps.Keys(kvs)
		existingKvs, err := s.loadMappings(ctx, keys)
		if err != nil {
			return nil, err
		}
		for k, v := range kvs {
			originalJobId, ok := existingKvs[k]
			if ok {
				duplicates[v] = originalJobId
			}
		}
	}
	return duplicates, nil
}

func (s *PostgresDeduplicator) StoreOriginalJobIds(ctx *armadacontext.Context, queue string, mappings map[string]string) error {
	if len(mappings) == 0 {
		return nil
	}
	kvs := make(map[string]string, len(mappings))
	for k, v := range mappings {
		kvs[s.jobKey(queue, k)] = v
	}
	return s.storeMappings(ctx, kvs)
}

func (s *PostgresDeduplicator) jobKey(queue, clientId string) string {
	return fmt.Sprintf("%s:%s", queue, clientId)
}

func (s *PostgresDeduplicator) storeMappings(ctx *armadacontext.Context, mappings map[string]string) error {
	deduplicationIDs := make([]string, 0, len(mappings))
	jobIDs := make([]string, 0, len(mappings))

	for deduplicationID, jobID := range mappings {
		deduplicationIDs = append(deduplicationIDs, deduplicationID)
		jobIDs = append(jobIDs, jobID)
	}

	sql := `
        INSERT INTO job_deduplication (deduplication_id, job_id)
        SELECT unnest($1::text[]), unnest($2::text[])
        ON CONFLICT (deduplication_id) DO NOTHING
    `
	_, err := s.db.Exec(ctx, sql, deduplicationIDs, jobIDs)
	if err != nil {
		return err
	}

	return nil
}

func (s *PostgresDeduplicator) loadMappings(ctx *armadacontext.Context, keys []string) (map[string]string, error) {
	// Prepare the output map
	result := make(map[string]string)

	sql := `
        SELECT deduplication_id, job_id
        FROM job_deduplication
        WHERE deduplication_id = ANY($1)
    `

	rows, err := s.db.Query(ctx, sql, keys)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Iterate through the result rows
	for rows.Next() {
		var deduplicationID, jobID string
		if err := rows.Scan(&deduplicationID, &jobID); err != nil {
			return nil, err
		}
		result[deduplicationID] = jobID
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return result, nil
}
