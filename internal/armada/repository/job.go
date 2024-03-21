package repository

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
	pulsarJobPrefix = "PulsarJob:" // {jobId}            - pulsarjob protobuf object
)

type JobRepository interface {
	StorePulsarSchedulerJobDetails(ctx *armadacontext.Context, jobDetails []*schedulerobjects.PulsarSchedulerJobDetails) error
	GetPulsarSchedulerJobDetails(ctx *armadacontext.Context, jobIds string) (*schedulerobjects.PulsarSchedulerJobDetails, error)
	ExpirePulsarSchedulerJobDetails(ctx *armadacontext.Context, jobId []string) error
}

type RedisJobRepository struct {
	db redis.UniversalClient
}

func NewRedisJobRepository(
	db redis.UniversalClient,
) *RedisJobRepository {
	return &RedisJobRepository{db: db}
}

func (repo *RedisJobRepository) StorePulsarSchedulerJobDetails(ctx *armadacontext.Context, jobDetails []*schedulerobjects.PulsarSchedulerJobDetails) error {
	pipe := repo.db.Pipeline()
	for _, job := range jobDetails {
		key := fmt.Sprintf("%s%s", pulsarJobPrefix, job.JobId)
		jobData, err := proto.Marshal(job)
		if err != nil {
			return errors.WithStack(err)
		}
		pipe.Set(ctx, key, jobData, 375*24*time.Hour) // expire after a year
	}
	_, err := pipe.Exec(ctx)
	if err != nil {
		return errors.Wrapf(err, "error storing pulsar job details in redis")
	}
	return nil
}

func (repo *RedisJobRepository) GetPulsarSchedulerJobDetails(ctx *armadacontext.Context, jobId string) (*schedulerobjects.PulsarSchedulerJobDetails, error) {
	cmd := repo.db.Get(ctx, pulsarJobPrefix+jobId)

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

func (repo *RedisJobRepository) ExpirePulsarSchedulerJobDetails(ctx *armadacontext.Context, jobIds []string) error {
	if len(jobIds) == 0 {
		return nil
	}
	pipe := repo.db.Pipeline()
	for _, jobId := range jobIds {
		key := fmt.Sprintf("%s%s", pulsarJobPrefix, jobId)
		// Expire as opposed to delete so that we are permissive of race conditions.
		pipe.Expire(ctx, key, 1*time.Hour)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return errors.Wrap(err, "failed to delete pulsar job details in Redis")
	}
	return nil
}
