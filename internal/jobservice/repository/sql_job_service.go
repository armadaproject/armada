//go:generate moq -out sql_job_service_moq.go . JobTableUpdater
package repository

import (
	"database/sql"
	"fmt"
	"sync"
	"time"

	_ "modernc.org/sqlite"

	"github.com/armadaproject/armada/internal/jobservice/configuration"
	js "github.com/armadaproject/armada/pkg/api/jobservice"
)

type JobTableUpdater interface {
	SubscribeJobSet(queue string, jobSet string, fromMessageId string) error
	IsJobSetSubscribed(queue string, jobSet string) (bool, string, error)
	UpdateJobServiceDb(jobTable *JobStatus) error
	UpdateJobSetDb(queue string, jobSet string, fromMessageId string) error
	SetSubscriptionError(queue string, jobSet string, err string, fromMessageId string) error
	GetSubscriptionError(queue string, jobSet string) (string, error)
	AddMessageIdAndClearSubscriptionError(queue string, jobSet string, messageId string) error
	UnsubscribeJobSet(queue string, jobSet string) (int64, error)
}

// SQLJobService for persisting to DB.
type SQLJobService struct {
	jobServiceConfig *configuration.JobServiceConfiguration
	db               *sql.DB

	lock sync.RWMutex
}

func NewSQLJobService(config *configuration.JobServiceConfiguration, db *sql.DB) *SQLJobService {
	return &SQLJobService{jobServiceConfig: config, db: db}
}

// Call on a newly created SQLJobService object to setup the DB for use.
func (s *SQLJobService) Setup() {
	if s.jobServiceConfig.DatabaseType == "sqlite" {
		s.useWAL()
	}

	s.CreateTable()
}

func (s *SQLJobService) useWAL() {
	s.lock.Lock()
	defer s.lock.Unlock()

	_, err := s.db.Exec("PRAGMA journal_mode=WAL")
	if err != nil {
		panic(err)
	}
}

type SubscribedTuple struct {
	Queue         string
	JobSet        string
	FromMessageId string
}

// Create a Table from a hard-coded schema.
func (s *SQLJobService) CreateTable() {
	var integerType string
	if s.jobServiceConfig.DatabaseType == "sqlite" {
		integerType = "INT"
	} else if s.jobServiceConfig.DatabaseType == "postgres" {
		integerType = "INTEGER"
	}

	s.lock.Lock()
	defer s.lock.Unlock()

	_, err := s.db.Exec("DROP TABLE IF EXISTS jobservice")
	if err != nil {
		panic(err)
	}
	_, err = s.db.Exec(fmt.Sprintf(`
		CREATE TABLE jobservice (
		Queue TEXT,
		JobSetId TEXT,
		JobId TEXT,
		JobResponseState TEXT,
		JobResponseError TEXT,
		Timestamp %s,
		PRIMARY KEY(JobId))`, integerType))

	if err != nil {
		panic(err)
	}
	_, errIndex := s.db.Exec(`CREATE INDEX idx_job_set_queue ON jobservice (Queue, JobSetId)`)
	if errIndex != nil {
		panic(errIndex)
	}
	_, err = s.db.Exec("DROP TABLE IF EXISTS jobsets")
	if err != nil {
		panic(err)
	}

	_, err = s.db.Exec(fmt.Sprintf(`
		CREATE TABLE jobsets (
			Queue TEXT,
			JobSetId TEXT,
			Timestamp %s,
			ConnectionError TEXT,
			FromMessageId TEXT,
			UNIQUE(Queue,JobSetId))`, integerType))
	if err != nil {
		panic(err)
	}
}

// Get the JobStatus given the jodId
func (s *SQLJobService) GetJobStatus(jobId string) (*js.JobServiceResponse, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	var sqlStmt string
	if s.jobServiceConfig.DatabaseType == "sqlite" {
		sqlStmt = "SELECT Queue, JobSetId, JobResponseState, JobResponseError FROM jobservice WHERE JobId = ?"
	} else if s.jobServiceConfig.DatabaseType == "postgres" {
		sqlStmt = "SELECT Queue, JobSetId, JobResponseState, JobResponseError FROM jobservice WHERE JobId = $1"
	}

	row := s.db.QueryRow(sqlStmt, jobId)
	var queue, jobSetId, jobState, jobError string

	err := row.Scan(&queue, &jobSetId, &jobState, &jobError)

	if err == sql.ErrNoRows {
		return &js.JobServiceResponse{State: js.JobServiceResponse_JOB_ID_NOT_FOUND}, nil
	} else if err != nil {
		return nil, err
	}

	// indicate connnection error for jobset/queue subscription where present
	connErr, err := s.GetSubscriptionError(queue, jobSetId)
	if err != nil {
		return nil, err
	}
	if connErr != "" {
		return &js.JobServiceResponse{
			Error: connErr,
			State: js.JobServiceResponse_CONNECTION_ERR,
		}, nil
	}

	jobJSRState, err := jobStateStrToJSRState(jobState)
	if err != nil {
		return nil, err
	}

	return &js.JobServiceResponse{
		Error: jobError,
		State: jobJSRState,
	}, nil
}

func jobStateStrToJSRState(jobState string) (js.JobServiceResponse_State, error) {
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
		fmt.Errorf("jobStateStrToJSRState: invalid job state string '%s'", jobState)
}

// Update database with JobTable.
func (s *SQLJobService) UpdateJobServiceDb(jobTable *JobStatus) error {
	// SQLite only allows one write at a time. Therefore we must serialize
	// writes in order to avoid SQL_BUSY errors.
	s.lock.Lock()
	defer s.lock.Unlock()

	var sqlStmt string

	if s.jobServiceConfig.DatabaseType == "sqlite" {
		sqlStmt = "INSERT OR REPLACE INTO jobservice VALUES (?, ?, ?, ?, ?, ?)"
	} else if s.jobServiceConfig.DatabaseType == "postgres" {
		sqlStmt = `INSERT INTO jobservice (Queue, JobSetId, JobId, JobResponseState, JobResponseError, Timestamp)
		VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (JobId) DO UPDATE SET
		(Queue, JobSetId, JobResponseState, JobResponseError, Timestamp) =
		(excluded.Queue, excluded.JobSetId, excluded.JobResponseState, excluded.JobResponseError, excluded.Timestamp)`
	}

	stmt, err := s.db.Prepare(sqlStmt)
	if err != nil {
		return err
	}
	defer stmt.Close()
	_, errExec := stmt.Exec(jobTable.queue, jobTable.jobSetId, jobTable.jobId,
		jobTable.jobResponse.State.String(), jobTable.jobResponse.Error, jobTable.timeStamp)
	return errExec
}

func (s *SQLJobService) UpdateJobSetDb(queue string, jobSet string, fromMessageId string) error {
	subscribe, _, err := s.IsJobSetSubscribed(queue, jobSet)
	if err != nil {
		return err
	}
	if !subscribe {
		return fmt.Errorf("queue %s jobSet %s is already unsubscribed", queue, jobSet)
	}
	s.lock.Lock()
	defer s.lock.Unlock()

	var sqlStmt string
	if s.jobServiceConfig.DatabaseType == "sqlite" {
		sqlStmt = "INSERT OR REPLACE INTO jobsets VALUES(?, ?, ?, ?, ?)"
	} else if s.jobServiceConfig.DatabaseType == "postgres" {
		sqlStmt = `INSERT INTO jobsets (Queue, JobSetId, Timestamp, ConnectionError, FromMessageId)
			VALUES ($1, $2, $3, $4, $5) ON CONFLICT (Queue, JobSetId) DO UPDATE SET
			(Timestamp, ConnectionError, FromMessageId) =
			(excluded.Timestamp, excluded.ConnectionError, excluded.FromMessageId)`
	}

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

// Simple Health Check to Verify if SQLite is working.
func (s *SQLJobService) HealthCheck() (bool, error) {
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
func (s *SQLJobService) IsJobSetSubscribed(queue string, jobSet string) (bool, string, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	var sqlStmt string

	if s.jobServiceConfig.DatabaseType == "sqlite" {
		sqlStmt = "SELECT Queue, JobSetId, FromMessageId FROM jobsets WHERE Queue = ? AND JobSetId = ?"
	} else if s.jobServiceConfig.DatabaseType == "postgres" {
		sqlStmt = "SELECT Queue, JobSetId, FromMessageId FROM jobsets WHERE Queue = $1 AND JobSetId = $2"
	}
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
func (s *SQLJobService) AddMessageIdAndClearSubscriptionError(queue string, jobSet string, fromMessageId string) error {
	return s.SetSubscriptionError(queue, jobSet, "", fromMessageId)
}

// Set subscription error if present
func (s *SQLJobService) SetSubscriptionError(queue string, jobSet string, connErr string, fromMessageId string) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	var sqlStmt string
	if s.jobServiceConfig.DatabaseType == "sqlite" {
		sqlStmt = "INSERT OR REPLACE INTO jobsets VALUES(?, ?, ?, ?, ?)"
	} else if s.jobServiceConfig.DatabaseType == "postgres" {
		sqlStmt = `INSERT INTO jobsets (Queue, JobSetId, Timestamp, ConnectionError, FromMessageId)
			VALUES ($1, $2, $3, $4, $5) ON CONFLICT (Queue, JobSetId) DO UPDATE SET
			(Timestamp, ConnectionError, FromMessageId) =
			(excluded.Timestamp, excluded.ConnectionError, excluded.FromMessageId)`
	}

	jobSetState, err := s.db.Prepare(sqlStmt)
	if err != nil {
		return err
	}
	defer jobSetState.Close()
	subscribeTable := NewSubscribeTable(queue, jobSet)
	_, jobSetErr := jobSetState.Exec(subscribeTable.queue, jobSet, subscribeTable.lastRequestTimeStamp,
		connErr, fromMessageId)
	if jobSetErr != nil {
		return jobSetErr
	}
	return jobSetErr
}

// Get subscription error if present
func (s *SQLJobService) GetSubscriptionError(queue string, jobSet string) (string, error) {
	var sqlStmt string
	if s.jobServiceConfig.DatabaseType == "sqlite" {
		sqlStmt = "SELECT ConnectionError FROM jobsets WHERE Queue = ? AND JobSetId = ?"
	} else if s.jobServiceConfig.DatabaseType == "postgres" {
		sqlStmt = "SELECT ConnectionError FROM jobsets WHERE Queue = $1 AND JobSetId = $2"
	}
	row := s.db.QueryRow(sqlStmt, queue, jobSet)
	var connError string

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

func (s *SQLJobService) SubscribeJobSet(queue string, jobSet string, fromMessageId string) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	var sqlStmt string
	if s.jobServiceConfig.DatabaseType == "sqlite" {
		sqlStmt = "INSERT OR REPLACE INTO jobsets VALUES(?, ?, ?, ?, ?)"
	} else if s.jobServiceConfig.DatabaseType == "postgres" {
		sqlStmt = `INSERT INTO jobsets (Queue, JobSetId, Timestamp, ConnectionError, FromMessageId)
			VALUES ($1, $2, $3, $4, $5) ON CONFLICT (Queue, JobSetId) DO UPDATE SET
			(Timestamp, ConnectionError, FromMessageId) =
			(excluded.Timestamp, excluded.ConnectionError, excluded.FromMessageId)`
	}

	jobSetState, err := s.db.Prepare(sqlStmt)
	if err != nil {
		return err
	}
	defer jobSetState.Close()
	subscribeTable := NewSubscribeTable(queue, jobSet)
	_, jobSetErr := jobSetState.Exec(subscribeTable.queue, subscribeTable.jobSet,
		subscribeTable.lastRequestTimeStamp, "", fromMessageId)
	return jobSetErr
}

// UnSubscribe to JobSet and delete all the jobs in the database
func (s *SQLJobService) CleanupJobSetAndJobs(queue string, jobSet string) (int64, error) {
	_, errUnsubscribe := s.UnsubscribeJobSet(queue, jobSet)
	if errUnsubscribe != nil {
		return 0, errUnsubscribe
	}
	return s.DeleteJobsInJobSet(queue, jobSet)
}

// Checks JobSet table to make determine if we should unsubscribe from JobSet
// configTimeWithoutUpdates is a configurable value that is read from the config
// We allow unsubscribing if the jobset hasn't been updated in configTime
// TODO implement this
func (s *SQLJobService) CheckToUnSubscribe(queue string, jobSet string, configTimeWithoutUpdates int64) (bool, error) {
	jobSetFound, _, err := s.IsJobSetSubscribed(queue, jobSet)
	if err != nil {
		return false, nil
	}
	if !jobSetFound {
		return false, nil
	}
	s.lock.Lock()
	defer s.lock.Unlock()

	var sqlStmt string
	if s.jobServiceConfig.DatabaseType == "sqlite" {
		sqlStmt = "SELECT Timestamp FROM jobsets WHERE Queue = ? AND JobSetId = ?"
	} else if s.jobServiceConfig.DatabaseType == "postgres" {
		sqlStmt = "SELECT Timestamp FROM jobsets WHERE Queue = $1 AND JobSetId = $2"
	}

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

func (s *SQLJobService) UnsubscribeJobSet(queue, jobSet string) (int64, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	var sqlStmt string
	if s.jobServiceConfig.DatabaseType == "sqlite" {
		sqlStmt = "DELETE FROM jobsets WHERE Queue = ? AND JobSetId = ?"
	} else if s.jobServiceConfig.DatabaseType == "postgres" {
		sqlStmt = "DELETE FROM jobsets WHERE Queue = $1 AND JobSetId = $2"
	}

	result, err := s.db.Exec(sqlStmt, queue, jobSet)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// Delete Jobs in the database
func (s *SQLJobService) DeleteJobsInJobSet(queue string, jobSet string) (int64, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	var sqlStmt string
	if s.jobServiceConfig.DatabaseType == "sqlite" {
		sqlStmt = "DELETE FROM jobservice WHERE Queue = ? AND JobSetId = ?"
	} else if s.jobServiceConfig.DatabaseType == "postgres" {
		sqlStmt = "DELETE FROM jobservice WHERE Queue = $1 AND JobSetId = $2"
	}

	result, err := s.db.Exec(sqlStmt, queue, jobSet)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func (s *SQLJobService) GetSubscribedJobSets() ([]SubscribedTuple, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	rows, err := s.db.Query("SELECT Queue, JobSetId, FromMessageId FROM jobsets")
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
