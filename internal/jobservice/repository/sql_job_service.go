package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/internal/common/slices"
	jsconfig "github.com/armadaproject/armada/internal/jobservice/configuration"
	js "github.com/armadaproject/armada/pkg/api/jobservice"
)

//go:generate moq -out job_table_updater_moq.go . JobTableUpdater
type JobTableUpdater interface {
	SubscribeJobSet(ctx context.Context, queue string, jobSet string, fromMessageId string) error
	IsJobSetSubscribed(ctx context.Context, queue string, jobSet string) (bool, string, error)
	UpdateJobServiceDb(ctx context.Context, jobTable *JobStatus) error
	UpdateJobSetDb(ctx context.Context, queue string, jobSet string, fromMessageId string) error
	SetSubscriptionError(ctx context.Context, queue string, jobSet string, err string, fromMessageId string) error
	GetSubscriptionError(ctx context.Context, queue string, jobSet string) (string, error)
	AddMessageIdAndClearSubscriptionError(ctx context.Context, queue string, jobSet string, messageId string) error
	UnsubscribeJobSet(ctx context.Context, queue string, jobSet string) (int64, error)
	GetSubscribedJobSets(ctx context.Context) ([]SubscribedTuple, error)
}

// SQLJobService for persisting to DB.
//
//go:generate moq -out sql_job_service_moq.go . SQLJobService
type SQLJobService interface {
	AddMessageIdAndClearSubscriptionError(ctx context.Context, queue string, jobSet string, fromMessageId string) error
	CheckToUnSubscribe(ctx context.Context, queue string, jobSet string, configTimeWithoutUpdates time.Duration) (bool, error)
	DeleteJobsInJobSet(ctx context.Context, queue string, jobSet string) (int64, error)
	GetJobStatus(ctx context.Context, jobId string) (*js.JobServiceResponse, error)
	GetSubscribedJobSets(ctx context.Context) ([]SubscribedTuple, error)
	GetSubscriptionError(ctx context.Context, queue string, jobSet string) (string, error)
	HealthCheck(ctx context.Context) (bool, error)
	IsJobSetSubscribed(ctx context.Context, queue string, jobSet string) (bool, string, error)
	SetSubscriptionError(ctx context.Context, queue string, jobSet string, connErr string, fromMessageId string) error
	Setup(ctx context.Context)
	SubscribeJobSet(ctx context.Context, queue string, jobSet string, fromMessageId string) error
	UnsubscribeJobSet(ctx context.Context, queue, jobSet string) (int64, error)
	UpdateJobServiceDb(ctx context.Context, jobTable *JobStatus) error
	UpdateJobSetDb(ctx context.Context, queue string, jobSet string, fromMessageId string) error
	PurgeExpiredJobSets(ctx context.Context)
}

var (
	supportedDBDialects = []configuration.DatabaseDialect{
		configuration.PostgresDialect,
		configuration.SqliteDialect,
	}
)

func NewSQLJobService(cfg *jsconfig.JobServiceConfiguration, log *log.Entry) (error, SQLJobService, func()) {
	if slices.Contains[configuration.DatabaseDialect](supportedDBDialects, cfg.DatabaseConfig.Dialect) {
		return errors.New("database type must be either 'postgres' or 'sqlite'"), nil, func() {}
	}

	if cfg.DatabaseConfig.Dialect == configuration.SqliteDialect {
		return NewJSRepoSQLite(cfg, log)
	}

	return NewJSRepository(cfg, log)
}

type JobSetKey struct {
	Queue    string
	JobSetId string
}

type SubscribedTuple struct {
	JobSetKey

	FromMessageId string
}

func JobStateStrToJSRState(jobState string) (js.JobServiceResponse_State, error) {
	switch jobState {
	case "SUBMITTED":
		return js.JobServiceResponse_SUBMITTED, nil
	case "DUPLICATE_FOUND":
		return js.JobServiceResponse_DUPLICATE_FOUND, nil
	case "RUNNING":
		return js.JobServiceResponse_RUNNING, nil
	case "FAILED":
		return js.JobServiceResponse_FAILED, nil
	case "SUCCEEDED":
		return js.JobServiceResponse_SUCCEEDED, nil
	case "CANCELLED":
		return js.JobServiceResponse_CANCELLED, nil
	case "JOB_ID_NOT_FOUND":
		return js.JobServiceResponse_JOB_ID_NOT_FOUND, nil
	}

	return js.JobServiceResponse_JOB_ID_NOT_FOUND,
		fmt.Errorf("JobStateStrToJSRState: invalid job state string '%s'", jobState)
}
