package eventapi

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"

	lru "github.com/hashicorp/golang-lru"
	log "github.com/sirupsen/logrus"

	"github.com/G-Research/armada/internal/eventapi/eventdb"
)

// JobsetMapper Allows us to map between external jobset identifiers (i.e (queue, jobset)) and our internal
// int64 representation
type JobsetMapper interface {
	// Get returns the int64 mapping, or an error if no mapping can be determined
	Get(ctx context.Context, queue string, jobset string) (int64, error)
}

// StaticJobsetMapper has a set of jobsets backed by a simple map. It's mainly intended for test purposes
type StaticJobsetMapper struct {
	JobsetIds map[string]int64
}

func (j *StaticJobsetMapper) Get(ctx context.Context, queue string, jobset string) (int64, error) {
	key := key(queue, jobset)
	id, ok := j.JobsetIds[key]
	if !ok {
		return -1, errors.WithStack(fmt.Errorf("no mapping exists for queue %sand jobset %s", queue, jobset))
	}
	return id, nil
}

// PostgresJobsetMapper uses Postgres to store mappings.
// New Mappings will be created automatically and stored in a local LRU cache for fast access
type PostgresJobsetMapper struct {
	jobsetIds *lru.Cache
	eventDb   *eventdb.EventDb
}

func NewJobsetMapper(eventDb *eventdb.EventDb, cachesize int, initialiseSince time.Duration) (*PostgresJobsetMapper, error) {
	initialJobsets, err := eventDb.LoadJobsetsAfter(context.Background(), time.Now().UTC().Add(-initialiseSince))
	log.Infof("Loaded %d jobset mapping from the db", len(initialJobsets))
	if err != nil {
		return nil, err
	}
	jobsetIds, err := lru.New(cachesize)
	if err != nil {
		return nil, err
	}
	for _, js := range initialJobsets {
		key := key(js.Queue, js.Jobset)
		jobsetIds.Add(key, js.JobSetId)
	}
	return &PostgresJobsetMapper{
		jobsetIds: jobsetIds,
		eventDb:   eventDb,
	}, nil
}

func (j *PostgresJobsetMapper) Get(ctx context.Context, queue string, jobset string) (int64, error) {
	key := key(queue, jobset)
	id, ok := j.jobsetIds.Get(key)
	if ok {
		return id.(int64), nil
	}

	// get from db
	start := time.Now()
	id, err := j.eventDb.GetOrCreateJobsetId(ctx, queue, jobset)
	taken := time.Now().Sub(start).Milliseconds()
	if err != nil {
		return 0, err
	}
	log.Debugf("Retrieved jobset mapping in %dms", taken)
	j.jobsetIds.Add(key, id)
	return id.(int64), nil
}

func key(queue string, jobset string) string {
	return fmt.Sprintf("%s:%s", queue, jobset)
}
