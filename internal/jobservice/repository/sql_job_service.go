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
	SubscribeJobSet(queue string, jobSet string) error
	IsJobSetSubscribed(queue string, jobSet string) (bool, error)
	UpdateJobServiceDb(jobTable *JobStatus) error
	UpdateJobSetDb(queue string, jobSet string) error
	SetSubscriptionError(queue string, jobSet string, err string) error
	GetSubscriptionError(queue string, jobSet string) (string, error)
	ClearSubscriptionError(queue string, jobSet string) error
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
	s.useWAL()
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
	Queue  string
	JobSet string
}

// Create a Table from a hard-coded schema.
func (s *SQLJobService) CreateTable() {
	s.lock.Lock()
	defer s.lock.Unlock()

	_, err := s.db.Exec("DROP TABLE IF EXISTS jobservice")
	if err != nil {
		panic(err)
	}
	_, err = s.db.Exec(`
CREATE TABLE jobservice (
Queue TEXT,
JobSetId TEXT,
JobId TEXT,
JobResponseState TEXT,
JobResponseError TEXT,
Timestamp INT,
PRIMARY KEY(JobId)
)`)
	if err != nil {
		panic(err)
	}
	_, errIndex := s.db.Exec(`CREATE INDEX idx_job_set_queue 
ON jobservice (Queue, JobSetId)`)
	if errIndex != nil {
		panic(errIndex)
	}
	_, err = s.db.Exec("DROP TABLE IF EXISTS jobsets")
	if err != nil {
		panic(err)
	}

	_, err = s.db.Exec(`
	CREATE TABLE jobsets (
		Queue TEXT,
		JobSetId TEXT,
		Timestamp INT,
		ConnectionError TEXT,
		UNIQUE(Queue,JobSetId))`)
	if err != nil {
		panic(err)
	}
}

// Get the JobStatus given the jodId
func (s *SQLJobService) GetJobStatus(jobId string) (*js.JobServiceResponse, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	row := s.db.QueryRow("SELECT Queue, JobSetId, JobResponseState, JobResponseError FROM jobservice WHERE JobId=?", jobId)
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

	stmt, err := s.db.Prepare("INSERT OR REPLACE INTO jobservice VALUES (?, ?, ?, ?, ?, ?)")
	if err != nil {
		return err
	}
	defer stmt.Close()
	jobState := jobTable.jobResponse.State.String()

	_, errExec := stmt.Exec(jobTable.queue, jobTable.jobSetId, jobTable.jobId, jobState, jobTable.jobResponse.Error, jobTable.timeStamp)
	return errExec
}

func (s *SQLJobService) UpdateJobSetDb(queue string, jobSet string) error {
	subscribe, err := s.IsJobSetSubscribed(queue, jobSet)
	if err != nil {
		return err
	}
	if !subscribe {
		return fmt.Errorf("queue %s jobSet %s is already unsubscribed", queue, jobSet)
	}
	s.lock.Lock()
	defer s.lock.Unlock()
	jobSetState, err := s.db.Prepare("INSERT OR REPLACE INTO jobsets VALUES(?, ?, ?, ?)")
	if err != nil {
		return err
	}
	defer jobSetState.Close()
	_, jobSetErr := jobSetState.Exec(queue, jobSet, time.Now().Unix(), "")
	if jobSetErr != nil {
		return jobSetErr
	}
	return nil
}

// Simple Health Check to Verify if SqlLite is working.
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
func (s *SQLJobService) IsJobSetSubscribed(queue string, jobSet string) (bool, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	row := s.db.QueryRow("SELECT Queue, JobSetId FROM jobsets WHERE Queue=? AND JobSetId=?", queue, jobSet)
	var queueScan, jobSetIdScan string

	err := row.Scan(&queueScan, &jobSetIdScan)

	if err == sql.ErrNoRows {
		return false, nil
	} else if err != nil {
		return false, err
	}
	return true, nil
}

// Clear subscription error if present
func (s *SQLJobService) ClearSubscriptionError(queue string, jobSet string) error {
	return s.SetSubscriptionError(queue, jobSet, "")
}

// Set subscription error if present
func (s *SQLJobService) SetSubscriptionError(queue string, jobSet string, connErr string) error {
	jobSetState, err := s.db.Prepare("INSERT OR REPLACE INTO jobsets VALUES(?, ?, ?, ?)")
	if err != nil {
		return err
	}
	defer jobSetState.Close()
	subscribeTable := NewSubscribeTable(queue, jobSet)
	_, jobSetErr := jobSetState.Exec(subscribeTable.queue, jobSet, subscribeTable.lastRequestTimeStamp, connErr)
	if jobSetErr != nil {
		return jobSetErr
	}
	return jobSetErr
}

// Get subscription error if present
func (s *SQLJobService) GetSubscriptionError(queue string, jobSet string) (string, error) {
	row := s.db.QueryRow("SELECT ConnectionError FROM jobsets WHERE Queue=? AND JobSetId=?", queue, jobSet)
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

func (s *SQLJobService) SubscribeJobSet(queue string, jobSet string) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	jobSetState, err := s.db.Prepare("INSERT OR REPLACE INTO jobsets VALUES(?, ?, ?, ?)")
	if err != nil {
		return err
	}
	defer jobSetState.Close()
	subscribeTable := NewSubscribeTable(queue, jobSet)
	_, jobSetErr := jobSetState.Exec(subscribeTable.queue, subscribeTable.jobSet, subscribeTable.lastRequestTimeStamp, "")
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
	jobSetFound, err := s.IsJobSetSubscribed(queue, jobSet)
	if err != nil {
		return false, nil
	}
	if !jobSetFound {
		return false, nil
	}
	row := s.db.QueryRow("SELECT Timestamp FROM jobsets WHERE Queue=? AND JobSetId=?", queue, jobSet)
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

	result, err := s.db.Exec("DELETE FROM jobsets WHERE Queue=? AND JobSetId=?", queue, jobSet)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// Delete Jobs in the database
func (s *SQLJobService) DeleteJobsInJobSet(queue string, jobSet string) (int64, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	result, err := s.db.Exec("DELETE FROM jobservice WHERE Queue=? AND JobSetId=?", queue, jobSet)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func (s *SQLJobService) GetSubscribedJobSets() ([]SubscribedTuple, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	rows, err := s.db.Query("SELECT Queue, JobSetId FROM jobsets")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tuples []SubscribedTuple

	// Loop through rows, using Scan to assign column data to struct fields.
	for rows.Next() {
		var st SubscribedTuple
		if err := rows.Scan(&st.Queue, &st.JobSet); err != nil {
			return tuples, err
		}
		tuples = append(tuples, st)
	}
	if err = rows.Err(); err != nil {
		return tuples, err
	}
	return tuples, nil
}
