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

	log "github.com/sirupsen/logrus"
)

type JobTableUpdater interface {
	SubscribeJobSet(queue string, jobSet string)
	IsJobSetSubscribed(queue string, jobSet string) bool
	UpdateJobServiceDb(*JobStatus) error
	SetSubscriptionError(queue string, jobSet string, err string)
	GetSubscriptionError(queue string, jobSet string) string
	ClearSubscriptionError(queue string, jobSet string)
	UnsubscribeJobSet(queue string, jobSet string)
}

// Internal structure for storing in memory JobTables and Subscription JobSets
// Locks are used for concurrent access of map
type JobSetSubscriptions struct {
	subscribeMap  map[string]*SubscribeTable
	subscribeLock sync.RWMutex
}

func NewJobSetSubscriptions(subscribeMap map[string]*SubscribeTable) *JobSetSubscriptions {
	return &JobSetSubscriptions{subscribeMap: subscribeMap}
}

// SQLJobService for persisting to DB.
type SQLJobService struct {
	jobSetSubscribe  *JobSetSubscriptions
	jobServiceConfig *configuration.JobServiceConfiguration
	db               *sql.DB

	writeLock sync.Mutex
}

func NewSQLJobService(jobSetSubscribe *JobSetSubscriptions, config *configuration.JobServiceConfiguration, db *sql.DB) *SQLJobService {
	return &SQLJobService{jobSetSubscribe: jobSetSubscribe, jobServiceConfig: config, db: db}
}

// Call on a newly created SQLJobService object to setup the DB for use.
func (s *SQLJobService) Setup() {
	s.useWAL()
	s.CreateTable()
}

func (s *SQLJobService) useWAL() {
	s.writeLock.Lock()
	defer s.writeLock.Unlock()

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
	s.writeLock.Lock()
	defer s.writeLock.Unlock()

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
}

// Get the JobStatus given the jodId
func (s *SQLJobService) GetJobStatus(jobId string) (*js.JobServiceResponse, error) {
	row := s.db.QueryRow("SELECT Queue, JobSetId, JobResponseState, JobResponseError FROM jobservice WHERE JobId=?", jobId)
	var queue, jobSetId, jobState, jobError string

	err := row.Scan(&queue, &jobSetId, &jobState, &jobError)

	if err == sql.ErrNoRows {
		return &js.JobServiceResponse{State: js.JobServiceResponse_JOB_ID_NOT_FOUND}, nil
	} else if err != nil {
		return nil, err
	}

	// indicate connnection error for jobset/queue subscription where present
	connErr := s.GetSubscriptionError(queue, jobSetId)
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
	s.writeLock.Lock()
	defer s.writeLock.Unlock()

	stmt, err := s.db.Prepare("INSERT OR REPLACE INTO jobservice VALUES (?, ?, ?, ?, ?, ?)")
	if err != nil {
		return err
	}
	defer stmt.Close()
	jobState := jobTable.jobResponse.State.String()

	_, errExec := stmt.Exec(jobTable.queue, jobTable.jobSetId, jobTable.jobId, jobState, jobTable.jobResponse.Error, jobTable.timeStamp)

	return errExec
}

// Simple Health Check to Verify if SqlLite is working.
func (s *SQLJobService) HealthCheck() (bool, error) {
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
func (s *SQLJobService) IsJobSetSubscribed(queue string, jobSet string) bool {
	s.jobSetSubscribe.subscribeLock.Lock()
	defer s.jobSetSubscribe.subscribeLock.Unlock()
	primaryKey := queue + jobSet
	_, ok := s.jobSetSubscribe.subscribeMap[primaryKey]
	return ok
}

// Clear subscription error if present
func (s *SQLJobService) ClearSubscriptionError(queue string, jobSet string) {
	s.SetSubscriptionError(queue, jobSet, "")
}

// Set subscription error if present
func (s *SQLJobService) SetSubscriptionError(queue string, jobSet string, err string) {
	s.jobSetSubscribe.subscribeLock.Lock()
	defer s.jobSetSubscribe.subscribeLock.Unlock()
	primaryKey := queue + jobSet
	subscription, ok := s.jobSetSubscribe.subscribeMap[primaryKey]
	if ok {
		subscription.err = err
	}
}

// Get subscription error if present
func (s *SQLJobService) GetSubscriptionError(queue string, jobSet string) string {
	s.jobSetSubscribe.subscribeLock.Lock()
	defer s.jobSetSubscribe.subscribeLock.Unlock()
	primaryKey := queue + jobSet
	subscription, ok := s.jobSetSubscribe.subscribeMap[primaryKey]
	if ok {
		return subscription.err
	}
	return ""
}

// Mark our JobSet as being subscribed
// SubscribeTable contains Queue, JobSet and time when it was created.
func (s *SQLJobService) SubscribeJobSet(queue string, jobSet string) {
	s.jobSetSubscribe.subscribeLock.Lock()
	defer s.jobSetSubscribe.subscribeLock.Unlock()
	primaryKey := queue + jobSet
	_, ok := s.jobSetSubscribe.subscribeMap[primaryKey]
	if !ok {
		s.jobSetSubscribe.subscribeMap[primaryKey] = NewSubscribeTable(queue, jobSet)
	}
}

// UnSubscribe to JobSet and delete all the jobs in the database
func (s *SQLJobService) CleanupJobSetAndJobs(queue string, jobSet string) (int64, error) {
	s.jobSetSubscribe.subscribeLock.Lock()
	defer s.jobSetSubscribe.subscribeLock.Unlock()
	primaryKey := queue + jobSet
	_, ok := s.jobSetSubscribe.subscribeMap[primaryKey]
	if !ok {
		log.Infof("JobSetId %s already unsubscribed", primaryKey)
		return int64(0), nil
	}
	delete(s.jobSetSubscribe.subscribeMap, primaryKey)
	log.Infof("Queue %s JobSetId %s unsubscribed", queue, jobSet)
	rowsAffected, errDelete := s.DeleteJobsInJobSet(queue, jobSet)
	return rowsAffected, errDelete
}

// Checks JobSet table to make determine if we should unsubscribe from JobSet
// configTimeWithoutUpdates is a configurable value that is read from the config
// We allow unsubscribing if the jobset hasn't been updated in configTime
func (s *SQLJobService) CheckToUnSubscribe(queue string, jobSet string, configTimeWithoutUpdates int64) bool {
	if !s.IsJobSetSubscribed(queue, jobSet) {
		return false
	}
	currentTime := time.Now().Unix()
	s.jobSetSubscribe.subscribeLock.RLock()
	defer s.jobSetSubscribe.subscribeLock.RUnlock()
	for _, val := range s.jobSetSubscribe.subscribeMap {
		if val.queue == queue && val.jobSet == jobSet {
			if (currentTime - val.lastRequestTimeStamp) > configTimeWithoutUpdates {
				return true
			}
		}
	}
	return false
}

func (s *SQLJobService) UnsubscribeJobSet(queue, jobSet string) {
	s.jobSetSubscribe.subscribeLock.Lock()
	defer s.jobSetSubscribe.subscribeLock.Unlock()
	primaryKey := queue + jobSet
	delete(s.jobSetSubscribe.subscribeMap, primaryKey)
}

// Update JobSet Map with time that a Job in that JobSet was requested
func (s *SQLJobService) UpdateJobSetTime(queue string, jobSet string) error {
	s.jobSetSubscribe.subscribeLock.Lock()
	defer s.jobSetSubscribe.subscribeLock.Unlock()
	primaryKey := queue + jobSet
	_, ok := s.jobSetSubscribe.subscribeMap[primaryKey]
	if ok {
		s.jobSetSubscribe.subscribeMap[primaryKey] = NewSubscribeTable(queue, jobSet)
		return nil
	} else {
		return fmt.Errorf("queue %s jobSet %s is already unsubscribed", queue, jobSet)
	}
}

// Delete Jobs in the database
func (s *SQLJobService) DeleteJobsInJobSet(queue string, jobSet string) (int64, error) {
	s.writeLock.Lock()
	defer s.writeLock.Unlock()

	result, err := s.db.Exec("DELETE FROM jobservice WHERE Queue=? AND JobSetId=?", queue, jobSet)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// Get a list of SubscribedJobSets (Queue JobSet)
func (s *SQLJobService) GetSubscribedJobSets() []SubscribedTuple {
	var returnJobSets []SubscribedTuple
	s.jobSetSubscribe.subscribeLock.RLock()
	defer s.jobSetSubscribe.subscribeLock.RUnlock()
	for _, value := range s.jobSetSubscribe.subscribeMap {
		tuple := &SubscribedTuple{Queue: value.queue, JobSet: value.jobSet}
		returnJobSets = append(returnJobSets, *tuple)
	}
	return returnJobSets
}
