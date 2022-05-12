package eventapi

import (
	"context"
	"fmt"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru"

	"github.com/G-Research/armada/internal/eventapi/eventdb"
)

type JobsetMapper interface {
	Get(ctx context.Context, queue string, jobset string) (int64, error)
}

type StaticJobsetMapper struct {
	JobsetIds map[string]int64
}

func (j *StaticJobsetMapper) Get(ctx context.Context, queue string, jobset string) (int64, error) {
	key := key(queue, jobset)
	id, ok := j.JobsetIds[key]
	if !ok {
		return -1, fmt.Errorf("no mapping exists for queue %sand jobset %s", queue, jobset)
	}
	return id, nil
}

type PostgresJobsetMapper struct {
	jobsetIds *lru.Cache
	eventDb   *eventdb.EventDb
	mutex     sync.Mutex
}

func NewJobsetMapper(eventDb *eventdb.EventDb, cachesize int, initialiseSince time.Duration) (*PostgresJobsetMapper, error) {
	initialJobsets, err := eventDb.LoadJobsetsAfter(context.Background(), time.Now().UTC().Add(-initialiseSince))
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
	id, err := j.eventDb.GetOrCreateJobsetId(ctx, queue, jobset)
	if err != nil {
		return 0, err
	}
	j.jobsetIds.Add(key, id)
	return id.(int64), nil
}

func key(queue string, jobset string) string {
	return fmt.Sprintf("%s:%s", queue, jobset)
}
