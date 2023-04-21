//go:generate moq -out sql_job_service_moq.go . JobTableUpdater
package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	_ "modernc.org/sqlite"

	log "github.com/sirupsen/logrus"

	"github.com/armadaproject/armada/internal/jobservice/configuration"
	js "github.com/armadaproject/armada/pkg/api/jobservice"
)

// RepositorySQLite for persisting to DB.
type RepositorySQLite struct {
	cfg  *configuration.JobServiceConfiguration
	db   *sql.DB
	lock sync.RWMutex
	tbls map[string]string
}

func NewRepoSQLite(config *configuration.JobServiceConfiguration, log *log.Entry) (error, *RepositorySQLite, func()) {
	var err error

	dbDir := filepath.Dir(config.DatabasePath)
	if _, err := os.Stat(dbDir); os.IsNotExist(err) {
		if errMkDir := os.Mkdir(dbDir, 0o755); errMkDir != nil {
			errMsg := fmt.Sprintf("error: could not make directory at %s for sqlite db: %v", dbDir, errMkDir)
			return errors.New(errMsg), nil, func() {}
		}
	}

	sqliteDb, err := sql.Open("sqlite", config.DatabasePath)
	if err != nil {
		errMsg := fmt.Sprintf("error opening sqlite DB from %s %v", config.DatabasePath, err)
		return errors.New(errMsg), nil, func() {}
	}

	repo := RepositorySQLite{
		cfg: config,
		db:  sqliteDb,
		tbls: map[string]string{
			"jobservice": config.TablesPrefix + "jobservice",
			"jobsets":    config.TablesPrefix + "jobsets",
		},
	}

	return nil, &repo, func() {
		if err := sqliteDb.Close(); err != nil {
			log.Warnf("error closing database: %v", err)
		}
	}
}

// Set up the DB for use, create tables
func (s *RepositorySQLite) Setup(ctx context.Context) {
	s.lock.Lock()
	defer s.lock.Unlock()

	_, err := s.db.Exec("PRAGMA journal_mode=WAL")
	if err != nil {
		panic(err)
	}

	_, err = s.db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", s.tbls["jobservice"]))
	if err != nil {
		panic(err)
	}

	_, err = s.db.Exec(fmt.Sprintf(`
		CREATE TABLE %s (
		Queue TEXT,
		JobSetId TEXT,
		JobId TEXT,
		JobResponseState TEXT,
		JobResponseError TEXT,
		Timestamp INT,
		PRIMARY KEY(JobId))`, s.tbls["jobservice"]))

	if err != nil {
		panic(err)
	}

	_, errIndex := s.db.Exec(fmt.Sprintf(`CREATE INDEX idx_job_set_queue ON %s (Queue, JobSetId)`, s.tbls["jobservice"]))
	if errIndex != nil {
		panic(errIndex)
	}

	_, err = s.db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", s.tbls["jobsets"]))
	if err != nil {
		panic(err)
	}

	_, err = s.db.Exec(fmt.Sprintf(`
		CREATE TABLE %s (
			Queue TEXT,
			JobSetId TEXT,
			Timestamp INT,
			ConnectionError TEXT,
			FromMessageId TEXT,
			UNIQUE(Queue,JobSetId))`, s.tbls["jobsets"]))
	if err != nil {
		panic(err)
	}
}

// Get the JobStatus given the jodId
func (s *RepositorySQLite) GetJobStatus(ctx context.Context, jobId string) (*js.JobServiceResponse, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	var queue, jobSetId, jobState, jobError string
	sqlStmt := fmt.Sprintf(`SELECT Queue, JobSetId, JobResponseState, JobResponseError
		FROM %s WHERE JobId = ?`, s.tbls["jobservice"])

	row := s.db.QueryRow(sqlStmt, jobId)
	err := row.Scan(&queue, &jobSetId, &jobState, &jobError)

	if err == sql.ErrNoRows {
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

	return &js.JobServiceResponse{Error: jobError, State: jobJSRState}, nil
}

// Update database with JobTable.
func (s *RepositorySQLite) UpdateJobServiceDb(ctx context.Context, jobTable *JobStatus) error {
	// SQLite only allows one write at a time. Therefore we must serialize
	// writes in order to avoid SQL_BUSY errors.
	s.lock.Lock()
	defer s.lock.Unlock()

	sqlStmt := fmt.Sprintf("INSERT OR REPLACE INTO %s VALUES (?, ?, ?, ?, ?, ?)", s.tbls["jobservice"])
	stmt, err := s.db.Prepare(sqlStmt)
	if err != nil {
		return err
	}
	defer stmt.Close()
	_, errExec := stmt.Exec(jobTable.queue, jobTable.jobSetId, jobTable.jobId,
		jobTable.jobResponse.State.String(), jobTable.jobResponse.Error, jobTable.timeStamp)
	return errExec
}

func (s *RepositorySQLite) UpdateJobSetDb(ctx context.Context, queue string, jobSet string, fromMessageId string) error {
	subscribe, _, err := s.IsJobSetSubscribed(ctx, queue, jobSet)
	if err != nil {
		return err
	}
	if !subscribe {
		return fmt.Errorf("queue %s jobSet %s is already unsubscribed", queue, jobSet)
	}
	s.lock.Lock()
	defer s.lock.Unlock()

	sqlStmt := fmt.Sprintf("INSERT OR REPLACE INTO %s VALUES(?, ?, ?, ?, ?)", s.tbls["jobsets"])

	jobSetState, err := s.db.Prepare(sqlStmt)
	if err != nil {
		return err
	}
	defer jobSetState.Close()
	_, jobSetErr := jobSetState.Exec(queue, jobSet, time.Now().Unix(), "", &fromMessageId)
	if jobSetErr != nil {
		return jobSetErr
	}
	return nil
}

func (s *RepositorySQLite) HealthCheck(ctx context.Context) (bool, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	row := s.db.QueryRow("SELECT 1")
	var col int
	err := row.Scan(&col)
	if err == nil {
		return true, nil
	} else {
		return false, fmt.Errorf("SQL health check failed: %v", err)
	}
}

// Check if JobSet is in our map.
func (s *RepositorySQLite) IsJobSetSubscribed(ctx context.Context, queue string, jobSet string) (bool, string, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	sqlStmt := fmt.Sprintf(`SELECT Queue, JobSetId, FromMessageId FROM %s WHERE Queue = ? AND JobSetId = ?`, s.tbls["jobsets"])
	row := s.db.QueryRow(sqlStmt, queue, jobSet)
	var queueScan, jobSetIdScan, fromMessageId string

	err := row.Scan(&queueScan, &jobSetIdScan, &fromMessageId)

	if err == sql.ErrNoRows {
		return false, "", nil
	} else if err != nil {
		return false, "", err
	}
	return true, fromMessageId, nil
}

// Clear subscription error if present
func (s *RepositorySQLite) AddMessageIdAndClearSubscriptionError(ctx context.Context, queue string,
	jobSet string, fromMessageId string,
) error {
	return s.SetSubscriptionError(ctx, queue, jobSet, "", fromMessageId)
}

// Set subscription error if present
func (s *RepositorySQLite) SetSubscriptionError(ctx context.Context, queue string, jobSet string,
	connErr string, fromMessageId string,
) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	sqlStmt := fmt.Sprintf("INSERT OR REPLACE INTO %s VALUES(?, ?, ?, ?, ?)", s.tbls["jobsets"])
	jobSetState, err := s.db.Prepare(sqlStmt)
	if err != nil {
		return err
	}
	defer jobSetState.Close()

	subscribeTable := NewSubscription(queue, jobSet)
	_, jobSetErr := jobSetState.Exec(subscribeTable.queue, jobSet, subscribeTable.lastRequestTimeStamp,
		connErr, fromMessageId)
	if jobSetErr != nil {
		return jobSetErr
	}
	return jobSetErr
}

// Get subscription error if present
func (s *RepositorySQLite) GetSubscriptionError(ctx context.Context, queue string, jobSet string) (string, error) {
	sqlStmt := fmt.Sprintf("SELECT ConnectionError FROM %s WHERE Queue = ? AND JobSetId = ?", s.tbls["jobsets"])
	var connError string

	row := s.db.QueryRow(sqlStmt, queue, jobSet)
	err := row.Scan(&connError)

	if err == sql.ErrNoRows {
		return "", nil
	} else if err != nil {
		return "", err
	}
	return connError, nil
}

// Mark our JobSet as being subscribed
// SubscribeTable contains Queue, JobSet and time when it was created.
func (s *RepositorySQLite) SubscribeJobSet(ctx context.Context, queue string, jobSet string, fromMessageId string) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	sqlStmt := fmt.Sprintf(`INSERT OR REPLACE INTO %s VALUES(?, ?, ?, ?, ?)`, s.tbls["jobsets"])

	jobSetState, err := s.db.Prepare(sqlStmt)
	if err != nil {
		return err
	}
	defer jobSetState.Close()
	subscribeTable := NewSubscription(queue, jobSet)
	_, jobSetErr := jobSetState.Exec(subscribeTable.queue, subscribeTable.jobSet,
		subscribeTable.lastRequestTimeStamp, "", fromMessageId)
	return jobSetErr
}

// UnSubscribe to JobSet and delete all the jobs in the database
func (s *RepositorySQLite) CleanupJobSetAndJobs(ctx context.Context, queue string, jobSet string) (int64, error) {
	_, errUnsubscribe := s.UnsubscribeJobSet(ctx, queue, jobSet)
	if errUnsubscribe != nil {
		return 0, errUnsubscribe
	}
	return s.DeleteJobsInJobSet(ctx, queue, jobSet)
}

// Checks JobSet table to make determine if we should unsubscribe from JobSet
// configTimeWithoutUpdates is a configurable value that is read from the config
// We allow unsubscribing if the jobset hasn't been updated in configTime
// TODO implement this
func (s *RepositorySQLite) CheckToUnSubscribe(ctx context.Context, queue string, jobSet string,
	configTimeWithoutUpdates int64,
) (bool, error) {
	jobSetFound, _, err := s.IsJobSetSubscribed(ctx, queue, jobSet)
	if err != nil {
		return false, nil
	}
	if !jobSetFound {
		return false, nil
	}

	s.lock.Lock()
	defer s.lock.Unlock()

	sqlStmt := fmt.Sprintf(`SELECT Timestamp FROM %s WHERE Queue = ? AND JobSetId = ?`, s.tbls["jobsets"])
	row := s.db.QueryRow(sqlStmt, queue, jobSet)
	var timeStamp int

	timeErr := row.Scan(&timeStamp)

	if timeErr == sql.ErrNoRows {
		return false, nil
	} else if err != nil {
		return false, err
	}

	currentTime := time.Now().Unix()
	if (currentTime - configTimeWithoutUpdates) > int64(timeStamp) {
		return true, nil
	}
	return false, nil
}

func (s *RepositorySQLite) UnsubscribeJobSet(ctx context.Context, queue, jobSet string) (int64, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	sqlStmt := fmt.Sprintf("DELETE FROM %s WHERE Queue = ? AND JobSetId = ?", s.tbls["jobsets"])

	result, err := s.db.Exec(sqlStmt, queue, jobSet)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// Delete Jobs in the database
func (s *RepositorySQLite) DeleteJobsInJobSet(ctx context.Context, queue string, jobSet string) (int64, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	sqlStmt := fmt.Sprintf("DELETE FROM %s WHERE Queue = ? AND JobSetId = ?", s.tbls["jobservice"])

	result, err := s.db.Exec(sqlStmt, queue, jobSet)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func (s *RepositorySQLite) GetSubscribedJobSets(ctx context.Context) ([]SubscribedTuple, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	rows, err := s.db.Query(fmt.Sprintf("SELECT Queue, JobSetId, FromMessageId FROM %s", s.tbls["jobsets"]))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tuples []SubscribedTuple

	// Loop through rows, using Scan to assign column data to struct fields.
	for rows.Next() {
		var st SubscribedTuple
		if err := rows.Scan(&st.Queue, &st.JobSet, &st.FromMessageId); err != nil {
			return tuples, err
		}
		tuples = append(tuples, st)
	}
	if err = rows.Err(); err != nil {
		return tuples, err
	}
	return tuples, nil
}
