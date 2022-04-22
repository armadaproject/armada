package eventapi

import (
	"context"
	"fmt"
	"sync"

	"github.com/G-Research/armada/internal/eventapi/postgres"
)

type JobsetMapper struct {
	jobsetIds map[string]int64
	eventDb   *postgres.EventDb
	mutex     sync.Mutex
}

func NewJobsetMapper(eventDb *postgres.EventDb) (*JobsetMapper, error) {
	initialJobsets, err := eventDb.GetJobsets(context.Background())
	if err != nil {
		return nil, err
	}
	jobsetIds := make(map[string]int64, len(initialJobsets))
	for _, js := range initialJobsets {
		key := key(js.Queue, js.Jobset)
		jobsetIds[key] = js.Id
	}
	return &JobsetMapper{
		jobsetIds: jobsetIds,
		eventDb:   eventDb,
	}, nil
}

func (j *JobsetMapper) get(ctx context.Context, queue string, jobset string) (int64, error) {
	j.mutex.Lock()
	defer j.mutex.Unlock()
	key := key(queue, jobset)
	id, ok := j.jobsetIds[key]
	if !ok {
		// get from db
		id, err := j.eventDb.GetOrCreateJobsetId(ctx, queue, jobset)
		if err != nil {
			return 0, err
		}
		j.jobsetIds[key] = id
	}
	return id, nil
}
func key(queue string, jobset string) string {
	return fmt.Sprintf("%s:%s", queue, jobset)
}
