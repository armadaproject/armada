package database

import (
	"fmt"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/redis/go-redis/v9"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	protoutil "github.com/armadaproject/armada/internal/common/proto"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

const (
	executorsPrefix = "executors"
)

type RedisExecutorRepository struct {
	db           redis.UniversalClient
	executorsKey string
}

func NewRedisExecutorRepository(db redis.UniversalClient, schedulerName string) *RedisExecutorRepository {
	return &RedisExecutorRepository{
		db:           db,
		executorsKey: fmt.Sprintf("%s_%s", executorsPrefix, schedulerName),
	}
}

func (r *RedisExecutorRepository) GetExecutors(ctx *armadacontext.Context) ([]*schedulerobjects.Executor, error) {
	result, err := r.db.HGetAll(ctx, r.executorsKey).Result()
	if err != nil {
		return nil, errors.Wrap(err, "Error retrieving executors from redis")
	}
	executors := make([]*schedulerobjects.Executor, len(result))
	i := 0
	for _, v := range result {
		executor, err := protoutil.Unmarshall([]byte(v), &schedulerobjects.Executor{})
		if err != nil {
			return nil, err
		}
		executors[i] = executor
		i++
	}
	return executors, nil
}

func (r *RedisExecutorRepository) GetLastUpdateTimes(_ *armadacontext.Context) (map[string]time.Time, error) {
	// We could implement this in a very inefficient way, but I don't believe it's needed so panic for now
	panic("GetLastUpdateTimes is not implemented")
}

func (r *RedisExecutorRepository) StoreExecutor(ctx *armadacontext.Context, executor *schedulerobjects.Executor) error {
	data, err := proto.Marshal(executor)
	if err != nil {
		return errors.Wrap(err, "Error marshalling executor proto")
	}

	pipe := r.db.TxPipeline()
	pipe.HSet(ctx, r.executorsKey, executor.Id, data)
	_, err = pipe.Exec(ctx)
	if err != nil {
		return errors.Wrap(err, "Error storing executor in redis")
	}
	return nil
}
