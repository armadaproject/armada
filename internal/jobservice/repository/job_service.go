package repository

import (
	"fmt"

	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"

	"github.com/G-Research/armada/pkg/api/jobservice"
	"github.com/go-redis/redis"
)

type JobServiceRepository interface {
	GetJobStatus(jobId string) (*jobservice.JobServiceResponse, error)
	UpdateJobStatus(jobSetId string) ([]string, error)
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
		return &jobservice.JobServiceResponse{}, fmt.Errorf("[RedisQueueRepository.GetQueue] error unmarshalling queue: %s", err)
	}

	return jobservice.NewJobServiceResponse(apiQueue)
}
