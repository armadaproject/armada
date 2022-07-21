package repository

import (
	"fmt"
	"time"
	
	"github.com/go-redis/redis"
	"github.com/pkg/errors"
	"github.com/gogo/protobuf/proto"

	js "github.com/G-Research/armada/pkg/api/jobservice"
)
type RedisJobServiceRepository struct {
	db  redis.UniversalClient
	ttl time.Duration
}

func NewRedisJobServiceRepository(db redis.UniversalClient, ttl time.Duration) *RedisJobServiceRepository {
	return &RedisJobServiceRepository{db: db, ttl: ttl}
}

func (jsr *RedisJobServiceRepository) HealthCheck() bool {
	err := HealthCheck(jsr.db)
	if err != nil {
		panic(err)
	}
	return true
}
func (jsr *RedisJobServiceRepository) GetJobStatus(jobId string) (*js.JobServiceResponse, error) {
	val, err := jsr.db.Get(jobId).Result()
	if err == redis.Nil {
		return &js.JobServiceResponse{State: js.JobServiceResponse_JOB_ID_NOT_FOUND}, nil
	} else if err != nil {
		return nil, errors.WithStack(err)
	}
	jobResponse := &js.JobServiceResponse{}
	e := proto.Unmarshal([]byte(val), jobResponse)
	if e != nil {
		return &js.JobServiceResponse{}, fmt.Errorf("[RedisJobServiceRepository.GetJobStatus] error unmarshalling JobResponse: %s", err)
	}

	return jobResponse, nil
}
func (jsr *RedisJobServiceRepository) UpdateJobServiceDb(jobId string, jobResponse *js.JobServiceResponse) error {
	data, err := proto.Marshal(jobResponse)
	if err != nil {
		return errors.WithStack(err)
	}

	if err := jsr.db.Set(jobId, data, jsr.ttl).Err(); err != nil {
		return errors.WithStack(err)
	}
	return nil
}
