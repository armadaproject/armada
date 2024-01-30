package repository

import (
	"fmt"
	"time"

	protoutil "github.com/armadaproject/armada/internal/common/proto"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/go-redis/redis"
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
)

const (
	pulsarJobPrefix = "PulsarJob:" // {jobId}            - pulsarjob protobuf object
)

type JobRepository interface {
	StorePulsarSchedulerJobDetails(jobDetails []*schedulerobjects.PulsarSchedulerJobDetails) error
	GetPulsarSchedulerJobDetails(jobIds string) (*schedulerobjects.PulsarSchedulerJobDetails, error)
	ExpirePulsarSchedulerJobDetails(jobId []string) error
}

type RedisJobRepository struct {
	db redis.UniversalClient
}

func NewRedisJobRepository(
	db redis.UniversalClient,
) *RedisJobRepository {
	return &RedisJobRepository{db: db}
}

func (repo *RedisJobRepository) StorePulsarSchedulerJobDetails(jobDetails []*schedulerobjects.PulsarSchedulerJobDetails) error {
	pipe := repo.db.Pipeline()
	for _, job := range jobDetails {
		key := fmt.Sprintf("%s%s", pulsarJobPrefix, job.JobId)
		jobData, err := proto.Marshal(job)
		if err != nil {
			return errors.WithStack(err)
		}
		pipe.Set(key, jobData, 375*24*time.Hour) // expire after a year
	}
	_, err := pipe.Exec()
	if err != nil {
		return errors.Wrapf(err, "error storing pulsar job details in redis")
	}
	return nil
}

func (repo *RedisJobRepository) GetPulsarSchedulerJobDetails(jobId string) (*schedulerobjects.PulsarSchedulerJobDetails, error) {
	cmd := repo.db.Get(pulsarJobPrefix + jobId)

	bytes, err := cmd.Bytes()
	if err != nil && err != redis.Nil {
		return nil, errors.Wrapf(err, "Errror retrieving job details for %s in redis", jobId)
	}
	if err == redis.Nil {
		return nil, nil
	}
	details, err := protoutil.Unmarshall(bytes, &schedulerobjects.PulsarSchedulerJobDetails{})
	if err != nil {
		return nil, errors.Wrapf(err, "Errror unmarshalling job details for %s in redis", jobId)
	}

	return details, nil
}

func (repo *RedisJobRepository) ExpirePulsarSchedulerJobDetails(jobIds []string) error {
	if len(jobIds) == 0 {
		return nil
	}
	pipe := repo.db.Pipeline()
	for _, jobId := range jobIds {
		key := fmt.Sprintf("%s%s", pulsarJobPrefix, jobId)
		// Expire as opposed to delete so that we are permissive of race conditions.
		pipe.Expire(key, 1*time.Hour)
	}
	if _, err := pipe.Exec(); err != nil {
		return errors.Wrap(err, "failed to delete pulsar job details in Redis")
	}
	return nil
}
