package repository

import (
	"fmt"

	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/go-redis/redis"

	"github.com/G-Research/armada/pkg/api/jobservice"
)

type JobServiceRepository interface {
	GetJobStatus(jobId string) (*jobservice.JobServiceResponse, error)
	UpdateJobServiceDb(jobId string, jobResponse *jobservice.JobServiceResponse) error
}
type RedisJobServiceRepository struct {
	db redis.UniversalClient
}

func NewRedisJobServiceRepository(db redis.UniversalClient) *RedisJobServiceRepository {
	return &RedisJobServiceRepository{db: db}
}

func (jsr *RedisJobServiceRepository) HealthCheck() bool {
	err := HealthCheck(jsr.db)
	if err != nil {
		panic(err)
	}
	return true
}
func (jsr *RedisJobServiceRepository) GetJobStatus(jobId string) (*jobservice.JobServiceResponse, error) {
	val, err := jsr.db.Get(jobId).Result()
	log.Infof("GetJobStatus jobId: %s value: %s err: %s", jobId, val, err)
	if err == redis.Nil {
		return &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_JOB_ID_NOT_FOUND}, nil
	} else if err != nil {
		return nil, err
	}
	jobResponse := &jobservice.JobServiceResponse{}
	e := proto.Unmarshal([]byte(val), jobResponse)
	if e != nil {
		return &jobservice.JobServiceResponse{}, fmt.Errorf("[RedisJobServiceRepository.GetJobStatus] error unmarshalling JobResponse: %s", err)
	}

	return jobResponse, nil
}
func (jsr *RedisJobServiceRepository) UpdateJobServiceDb(jobId string, jobResponse *jobservice.JobServiceResponse) error {
	data, err := proto.Marshal(jobResponse)
	if err != nil {
		return errors.WithStack(err)
	}

	if err := jsr.db.Set(jobId, data, 0).Err(); err != nil {
		panic(err)
	}
	log.Infof("UpdateJobServiceDb jobId: %s jobState: %s", jobId, jobResponse.State)
	return nil
}
