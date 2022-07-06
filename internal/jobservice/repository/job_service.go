package repository

import (
	"fmt"

	"github.com/gogo/protobuf/proto"
	log "github.com/sirupsen/logrus"

	"github.com/G-Research/armada/pkg/api/jobservice"
	"github.com/go-redis/redis"
)

type JobServiceRepository interface {
	GetJobStatus(jobId string) (*jobservice.JobServiceResponse, error)
	UpdateJobServiceDb(jobSetId string, jobResponse *jobservice.JobServiceResponse) (error)
}
type RedisJobServiceRepository struct {
	db redis.UniversalClient
}
func NewRedisJobServiceRepository(db redis.UniversalClient) *RedisJobServiceRepository {
	_, err := db.Ping().Result()
	if err != nil {
		log.Fatal(err)
	}
	return &RedisJobServiceRepository{db: db}
}

func (jsr *RedisJobServiceRepository) GetJobStatus(jobId string) (*jobservice.JobServiceResponse, error) {
	val, err := jsr.db.Get(jobId).Result()
	if err != nil {
		return nil, err
	}
	jobResponse := &jobservice.JobServiceResponse{}
	e := proto.Unmarshal([]byte(val), jobResponse)
	if e != nil {
		return &jobservice.JobServiceResponse{}, fmt.Errorf("[RedisJobServiceRepository.GetJobStatus] error unmarshalling JobResponse: %s", err)
	}

	return jobResponse, nil
}
func (jsr *RedisJobServiceRepository) UpdateJobServiceDb(jobSetId string, jobResponse *jobservice.JobServiceResponse) (error) {
	if err := jsr.db.Publish(jobSetId, jobResponse).Err(); err != nil {
        panic(err)
    }
	return nil
}
