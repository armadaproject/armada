package repository

import (
	"context"
	"fmt"
	"time"

	_ "modernc.org/sqlite"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/armadaproject/armada/internal/common/database"
	"github.com/armadaproject/armada/internal/jobservice/configuration"
	js "github.com/armadaproject/armada/pkg/api/jobservice"
)

type JSRepoPostgres struct {
	jobServiceConfig *configuration.JobServiceConfiguration
	dbpool           *pgxpool.Pool
}

func NewJSRepoPostgres(cfg *configuration.JobServiceConfiguration, log *log.Entry) (error, *JSRepoPostgres, func()) {
	poolCfg, err := pgxpool.ParseConfig(database.CreateConnectionString(cfg.PostgresConfig.Connection))
	if err != nil {
		return errors.Wrap(err, "cannot parse Postgres connection config"), nil, func() {}
	}

	pool, err := pgxpool.NewWithConfig(context.Background(), poolCfg)
	if err != nil {
		return errors.Wrap(err, "cannot create Postgres connection pool"), nil, func() {}
	}

	return nil, &JSRepoPostgres{jobServiceConfig: cfg, dbpool: pool}, func() {}
}

// Set up the DB for use, create tables
func (s *JSRepoPostgres) Setup(ctx context.Context) {
	setupStmts := []string{
		`DROP TABLE IF EXISTS jobs`,
		`DROP INDEX IF EXISTS idx_job_set_queue`,
		`DROP TABLE IF EXISTS jobsets`,
		`CREATE TABLE jobsets (
			Queue TEXT,
			Id TEXT,
			Timestamp INTEGER,
			ConnectionError TEXT,
			FromMessageId TEXT,
			UNIQUE (Id),
			PRIMARY KEY (Queue, Id))`,
		`CREATE INDEX idx_jobsets_timestamp ON jobsets (Timestamp)`,
		`CREATE TABLE jobs (
			Queue TEXT,
			JobSetId TEXT,
			Id TEXT,
			JobResponseState TEXT,
			JobResponseError TEXT,
			Timestamp INTEGER,
			PRIMARY KEY(Id))`,
		`CREATE INDEX idx_job_set_queue ON jobs (Queue, JobSetId)`,
		`CREATE INDEX idx_jobs_timestamp ON jobs (Timestamp)`,
		`DROP TRIGGER IF EXISTS trigger_delete_expired_jobsets ON jobsets`,
		`DROP FUNCTION IF EXISTS delete_expired_jobsets`,
	}

	for _, stmt := range setupStmts {
		_, err := s.dbpool.Exec(ctx, stmt)
		if err != nil {
			panic(err)
		}
	}
}

// Get the JobStatus given the jodId
func (s *JSRepoPostgres) GetJobStatus(ctx context.Context, jobId string) (*js.JobServiceResponse, error) {
	sqlStmt := "SELECT Queue, JobSetId, JobResponseState, JobResponseError FROM jobs WHERE Id = $1"

	row := s.dbpool.QueryRow(ctx, sqlStmt, jobId)
	var queue, jobSetId, jobState, jobError string

	err := row.Scan(&queue, &jobSetId, &jobState, &jobError)

	if err == pgx.ErrNoRows {
		return &js.JobServiceResponse{State: js.JobServiceResponse_JOB_ID_NOT_FOUND}, nil
	} else if err != nil {
		return nil, err
	}

	// indicate connnection error for jobset/queue subscription where present
	connErr, err := s.GetSubscriptionError(ctx, queue, jobSetId)
	if err != nil {
		return nil, err
	}
	if connErr != "" {
		return &js.JobServiceResponse{
			Error: connErr,
			State: js.JobServiceResponse_CONNECTION_ERR,
		}, nil
	}

	jobJSRState, err := JobStateStrToJSRState(jobState)
	if err != nil {
		return nil, err
	}

	return &js.JobServiceResponse{
		Error: jobError,
		State: jobJSRState,
	}, nil
}

// Update database with JobTable.
func (s *JSRepoPostgres) UpdateJobServiceDb(ctx context.Context, jobTable *JobStatus) error {
	sqlStmt := `INSERT INTO jobs (Queue, JobSetId, Id, JobResponseState, JobResponseError, Timestamp)
		VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (Id) DO UPDATE SET
		(Queue, JobSetId, JobResponseState, JobResponseError, Timestamp) =
		(excluded.Queue, excluded.JobSetId, excluded.JobResponseState, excluded.JobResponseError, excluded.Timestamp)`

	_, errExec := s.dbpool.Exec(ctx, sqlStmt, jobTable.queue, jobTable.jobSetId, jobTable.jobId,
		jobTable.jobResponse.State.String(), jobTable.jobResponse.Error, jobTable.timeStamp)
	return errExec
}

// We should check if a JobSet exists first before updating the database and return an error if it doesn't exist
// However, The only caller of this function, in jobservice/server/server.go, does this check before calling.
// Adding the check here will be redundant and a performance botteneck.
// TODO: We should descend the check here and adjust the JobSet subscription logic in jobservice/server/server.go
func (s *JSRepoPostgres) UpdateJobSetDb(ctx context.Context, queue string, jobSet string, fromMessageId string) error {
	sqlStmt := `INSERT INTO jobsets (Queue, Id, Timestamp, ConnectionError, FromMessageId)
			VALUES ($1, $2, $3, $4, $5) ON CONFLICT (Queue, Id) DO UPDATE SET
			(Timestamp, ConnectionError, FromMessageId) =
			(excluded.Timestamp, excluded.ConnectionError, excluded.FromMessageId)`

	_, jobSetErr := s.dbpool.Exec(ctx, sqlStmt, queue, jobSet, time.Now().Unix(), "", &fromMessageId)
	if jobSetErr != nil {
		return jobSetErr
	}
	return nil
}

func (s *JSRepoPostgres) HealthCheck(ctx context.Context) (bool, error) {
	row := s.dbpool.QueryRow(ctx, "SELECT 1")
	var col int
	err := row.Scan(&col)
	if err == nil {
		return true, nil
	} else {
		return false, fmt.Errorf("database health check failed: %v", err)
	}
}

// Check if JobSet is in our map.
func (s *JSRepoPostgres) IsJobSetSubscribed(ctx context.Context, queue string, jobSet string) (bool, string, error) {
	sqlStmt := "SELECT Queue, Id, FromMessageId FROM jobsets WHERE Queue = $1 AND Id = $2"
	row := s.dbpool.QueryRow(ctx, sqlStmt, queue, jobSet)
	var queueScan, jobSetIdScan, fromMessageId string

	err := row.Scan(&queueScan, &jobSetIdScan, &fromMessageId)

	if err == pgx.ErrNoRows {
		return false, "", nil
	} else if err != nil {
		return false, "", err
	}
	return true, fromMessageId, nil
}

// Clear subscription error if present
func (s *JSRepoPostgres) AddMessageIdAndClearSubscriptionError(ctx context.Context, queue string,
	jobSet string, fromMessageId string,
) error {
	return s.SetSubscriptionError(ctx, queue, jobSet, "", fromMessageId)
}

// Set subscription error if present
func (s *JSRepoPostgres) SetSubscriptionError(ctx context.Context, queue string, jobSet string,
	connErr string, fromMessageId string,
) error {
	sqlStmt := `INSERT INTO jobsets (Queue, Id, Timestamp, ConnectionError, FromMessageId)
			VALUES ($1, $2, $3, $4, $5) ON CONFLICT (Queue, Id) DO UPDATE SET
			(Timestamp, ConnectionError, FromMessageId) =
			(excluded.Timestamp, excluded.ConnectionError, excluded.FromMessageId)`

	subscribeTable := NewSubscribeTable(queue, jobSet)
	_, jobSetErr := s.dbpool.Exec(ctx, sqlStmt, subscribeTable.queue, jobSet, subscribeTable.lastRequestTimeStamp,
		connErr, fromMessageId)
	if jobSetErr != nil {
		return jobSetErr
	}
	return jobSetErr
}

// Get subscription error if present
func (s *JSRepoPostgres) GetSubscriptionError(ctx context.Context, queue string, jobSet string) (string, error) {
	sqlStmt := "SELECT ConnectionError FROM jobsets WHERE Queue = $1 AND Id = $2"
	row := s.dbpool.QueryRow(ctx, sqlStmt, queue, jobSet)
	var connError string

	err := row.Scan(&connError)

	if err == pgx.ErrNoRows {
		return "", nil
	} else if err != nil {
		return "", err
	}
	return connError, nil
}

// Mark our JobSet as being subscribed
// SubscribeTable contains Queue, JobSet and time when it was created.
func (s *JSRepoPostgres) SubscribeJobSet(ctx context.Context, queue string, jobSet string,
	fromMessageId string,
) error {
	sqlStmt := `INSERT INTO jobsets (Queue, Id, Timestamp, ConnectionError, FromMessageId)
			VALUES ($1, $2, $3, $4, $5) ON CONFLICT (Queue, Id) DO UPDATE SET
			(Timestamp, ConnectionError, FromMessageId) =
			(excluded.Timestamp, excluded.ConnectionError, excluded.FromMessageId)`

	subscribeTable := NewSubscribeTable(queue, jobSet)
	_, jobSetErr := s.dbpool.Exec(ctx, sqlStmt, subscribeTable.queue, subscribeTable.jobSet,
		subscribeTable.lastRequestTimeStamp, "", fromMessageId)
	return jobSetErr
}

// Checks JobSet table to make determine if we should unsubscribe from JobSet
// configTimeWithoutUpdates is a configurable value that is read from the config
// We allow unsubscribing if the jobset hasn't been updated in configTime
// TODO implement this
func (s *JSRepoPostgres) CheckToUnSubscribe(ctx context.Context, queue string, jobSet string,
	configTimeWithoutUpdates time.Duration,
) (bool, error) {
	jobSetFound, _, err := s.IsJobSetSubscribed(ctx, queue, jobSet)
	if err != nil {
		return false, nil
	}
	if !jobSetFound {
		return false, nil
	}

	sqlStmt := "SELECT Timestamp FROM jobsets WHERE Queue = $1 AND Id = $2"

	row := s.dbpool.QueryRow(ctx, sqlStmt, queue, jobSet)
	var timeStamp int64

	timeErr := row.Scan(&timeStamp)

	if timeErr == pgx.ErrNoRows {
		return false, nil
	} else if err != nil {
		return false, err
	}

	currentTime := time.Now()
	lastUpdate := time.Unix(timeStamp, 0)
	if currentTime.After(lastUpdate.Add(configTimeWithoutUpdates)) {
		return true, nil
	}
	return false, nil
}

// Deletes the corresponding jobset along with it's associated jobs due to
// the CASCADE DELETE constraint on the foreign-key relationship.
func (s *JSRepoPostgres) UnsubscribeJobSet(ctx context.Context, queue, jobSet string) (int64, error) {
	sqlStmt := "DELETE FROM jobsets WHERE Queue = $1 AND Id = $2"

	result, err := s.dbpool.Exec(ctx, sqlStmt, queue, jobSet)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected(), nil
}

// Delete Jobs in the database
func (s *JSRepoPostgres) DeleteJobsInJobSet(ctx context.Context, queue string, jobSet string) (int64, error) {
	sqlStmt := "DELETE FROM jobs WHERE Queue = $1 AND JobSetId = $2"

	result, err := s.dbpool.Exec(ctx, sqlStmt, queue, jobSet)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected(), nil
}

func (s *JSRepoPostgres) GetSubscribedJobSets(ctx context.Context) ([]SubscribedTuple, error) {
	rows, err := s.dbpool.Query(ctx, "SELECT Queue, Id, FromMessageId FROM jobsets")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tuples []SubscribedTuple

	// Loop through rows, using Scan to assign column data to struct fields.
	for rows.Next() {
		var st SubscribedTuple
		if err := rows.Scan(&st.Queue, &st.JobSetId, &st.FromMessageId); err != nil {
			return tuples, err
		}
		tuples = append(tuples, st)
	}
	if err = rows.Err(); err != nil {
		return tuples, err
	}
	return tuples, nil
}

// PurgeExpiredJobSets purges all expired Jobs/JobSets from the database
// An expired Job/JobSet is a Job/JobSet that has not been updated within the specified PurgeJobSetTime period.
func (s *JSRepoPostgres) PurgeExpiredJobSets(ctx context.Context) {
	jobSetStmt := fmt.Sprintf(`DELETE FROM jobsets WHERE Timestamp < (extract(epoch from now()) - %d);`, s.jobServiceConfig.PurgeJobSetTime)
	jobStmt := fmt.Sprintf(`DELETE FROM jobs WHERE Timestamp < (extract(epoch from now()) - %d);`, s.jobServiceConfig.PurgeJobSetTime)
	ticker := time.NewTicker(time.Duration(s.jobServiceConfig.PurgeJobSetTime) * time.Second)
	log := log.WithField("JobService", "ExpiredJobSetsPurge")

	log.Info("Starting purge of expired jobsets")
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			result, err := s.dbpool.Exec(ctx, jobSetStmt)
			if err != nil {
				log.Error("error deleting expired jobsets: ", err)
			} else {
				log.Debugf("Deleted %d expired jobsets", result.RowsAffected())
			}
			result, err = s.dbpool.Exec(ctx, jobStmt)
			if err != nil {
				log.Error("error deleting expired jobs: ", err)
			} else {
				log.Debugf("Deleted %d expired jobs", result.RowsAffected())
			}
		}
	}
}
