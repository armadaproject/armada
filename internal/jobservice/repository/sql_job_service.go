package repository

import (
	"database/sql"
	"fmt"
	"sync"
	"time"

	_ "modernc.org/sqlite"

	"github.com/G-Research/armada/internal/jobservice/configuration"
	js "github.com/G-Research/armada/pkg/api/jobservice"

	log "github.com/sirupsen/logrus"
)

// Internal structure for storing in memory JobTables and Subscription JobSets
// Locks are used for concurrent access of map
type JobStatus struct {
	jobMap        map[string]*JobTable
	jobLock       sync.RWMutex
	subscribeMap  map[string]*SubscribeTable
	subscribeLock sync.RWMutex
}

func NewJobStatus(jobMap map[string]*JobTable, subscribeMap map[string]*SubscribeTable) *JobStatus {
	return &JobStatus{jobMap: jobMap, subscribeMap: subscribeMap}
}

// SQLJobService for persisting to DB.
type SQLJobService struct {
	jobStatus        *JobStatus
	jobServiceConfig *configuration.JobServiceConfiguration
	db               *sql.DB
}

func NewSQLJobService(jobMap *JobStatus, config *configuration.JobServiceConfiguration, db *sql.DB) *SQLJobService {
	return &SQLJobService{jobStatus: jobMap, jobServiceConfig: config, db: db}
}

type SubscribedTuple struct {
	Queue  string
	JobSet string
}

// Create a Table from a hard-coded schema.
// Open to suggestions on how to make this better
func (s *SQLJobService) CreateTable() {
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
Timestamp INT
)`)
	if err != nil {
		panic(err)
	}
}

// Get the JobStatus given the jodId
// If a job is not in the map, we return JOB_ID_NOT_FOUND
// This should not be an error.
func (s *SQLJobService) GetJobStatus(jobId string) (*js.JobServiceResponse, error) {
	s.jobStatus.jobLock.RLock()
	jobResponse, ok := s.jobStatus.jobMap[jobId]
	s.jobStatus.jobLock.RUnlock()
	if !ok {
		sqlResponse, errSql := s.GetJobStatusSQL(jobId)
		if sqlResponse != nil {
			return sqlResponse, errSql
		}
	}

	return &jobResponse.jobResponse, nil
}

// If our in memory map no longer contains the status, we should query the row to get it
// If that doesn't exist then we assume the job doesn't exist yet.
func (s *SQLJobService) GetJobStatusSQL(jobId string) (*js.JobServiceResponse, error) {
	row := s.db.QueryRow("SELECT JobResponseState, JobResponseError FROM jobservice WHERE JobId=?", jobId)
	var jobState string
	var jobError string
	err := row.Scan(&jobState, &jobError)
	if err == sql.ErrNoRows {
		return &js.JobServiceResponse{State: js.JobServiceResponse_JOB_ID_NOT_FOUND}, nil
	} else if err != nil {
		return nil, err
	}
	jobProtoResponse := &js.JobServiceResponse{Error: jobError}
	switch jobState {
	case "SUBMITTED":
		jobProtoResponse.State = js.JobServiceResponse_SUBMITTED
	case "DUPLICATE_FOUND":
		jobProtoResponse.State = js.JobServiceResponse_DUPLICATE_FOUND
	case "RUNNING":
		jobProtoResponse.State = js.JobServiceResponse_RUNNING
	case "FAILED":
		jobProtoResponse.State = js.JobServiceResponse_FAILED
	case "SUCCEEDED":
		jobProtoResponse.State = js.JobServiceResponse_SUCCEEDED
	case "CANCELLED":
		jobProtoResponse.State = js.JobServiceResponse_CANCELLED
	case "JOB_ID_NOT_FOUND":
		jobProtoResponse.State = js.JobServiceResponse_JOB_ID_NOT_FOUND
	}
	return jobProtoResponse, nil
}

// Update in memory JobStatus Map with jobId and our JobTable
func (s *SQLJobService) UpdateJobServiceDb(jobTable *JobTable) {
	s.jobStatus.jobLock.Lock()
	defer s.jobStatus.jobLock.Unlock()
	s.jobStatus.jobMap[jobTable.jobId] = jobTable
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
// Note: The key should be queuejobset.
func (s *SQLJobService) IsJobSetSubscribed(queue string, jobSet string) bool {
	s.jobStatus.subscribeLock.Lock()
	primaryKey := queue + jobSet
	defer s.jobStatus.subscribeLock.Unlock()
	_, ok := s.jobStatus.subscribeMap[primaryKey]
	return ok
}

// Mark our JobSet as being subscribed
// SubscribeTable contains Queue, JobSet and time when it was created.
func (s *SQLJobService) SubscribeJobSet(queue string, jobSet string) {
	s.jobStatus.subscribeLock.Lock()
	defer s.jobStatus.subscribeLock.Unlock()
	primaryKey := queue + jobSet
	_, ok := s.jobStatus.subscribeMap[primaryKey]
	if !ok {
		s.jobStatus.subscribeMap[primaryKey] = NewSubscribeTable(queue, jobSet)
	}

}

// UnSubscribe to JobSet and delete all the jobs in the in memory map
func (s *SQLJobService) UnSubscribeJobSet(queue string, jobSet string) (int64, error) {
	s.jobStatus.subscribeLock.RLock()
	defer s.jobStatus.subscribeLock.RUnlock()
	primaryKey := queue + jobSet
	_, ok := s.jobStatus.subscribeMap[primaryKey]
	if !ok {
		log.Infof("JobSetId %s already unsubscribed", primaryKey)
		return int64(0), nil
	}
	delete(s.jobStatus.subscribeMap, primaryKey)
	log.Infof("Queue %s JobSetId %s unsubscribed", queue, jobSet)
	rowsAffected, errDelete := s.DeleteJobsInJobSet(queue, jobSet)
	if errDelete != nil {
		return rowsAffected, errDelete
	}
	// If jobs are subscribed but aren't in DB than we should delete from local map
	if rowsAffected == 0 {
		return s.deleteJobsFromInMemoryMap(queue, jobSet)
	}
	// If we were able to delete from DB than we assume all is good
	return rowsAffected, errDelete
}

func (s *SQLJobService) deleteJobsFromInMemoryMap(queue string, jobSet string) (int64, error) {
	var rowsAffected int64 = 0
	for _, val := range s.jobStatus.jobMap {
		if val.queue == queue && val.jobSetId == jobSet {
			rowsAffected++
			delete(s.jobStatus.jobMap, val.jobId)
		}
	}
	return rowsAffected, nil
}

// Checks JobSet table to make determine if we should unsubscribe from JobSet
// configTimeWithoutUpdates is a configurable value that is read from the config
// We allow unsubscribing if the jobset hasn't been updated in configTime
func (s *SQLJobService) CheckToUnSubscribe(queue string, jobSet string, configTimeWithoutUpdates int64) bool {
	if !s.IsJobSetSubscribed(queue, jobSet) {
		return false
	}
	currentTime := time.Now().Unix()
	for _, val := range s.jobStatus.subscribeMap {
		if val.queue == queue && val.jobSet == jobSet {
			if (currentTime - val.lastRequestTimeStamp) > configTimeWithoutUpdates {
				return true
			}
		}
	}
	return false
}

// Update JobSet Map with time that a Job in that JobSet was requested
func (s *SQLJobService) UpdateJobSetTime(queue string, jobSet string) error {
	s.jobStatus.subscribeLock.Lock()
	defer s.jobStatus.subscribeLock.Unlock()
	primaryKey := queue + jobSet
	_, ok := s.jobStatus.subscribeMap[primaryKey]
	if ok {
		s.jobStatus.subscribeMap[primaryKey] = NewSubscribeTable(queue, jobSet)
		return nil
	} else {
		return fmt.Errorf("queue %s jobSet %s is already unsubscribed", queue, jobSet)
	}
}

// Delete Jobs in the map.
func (s *SQLJobService) DeleteJobsInJobSet(queue string, jobSet string) (int64, error) {
	s.jobStatus.jobLock.RLock()
	defer s.jobStatus.jobLock.RUnlock()
	result, err := s.db.Exec("DELETE FROM jobservice WHERE Queue=? AND JobSetId=?", queue, jobSet)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// Get a list of SubscribedJobSets (Queue JobSet)
func (s *SQLJobService) GetSubscribedJobSets() []SubscribedTuple {
	var returnJobSets []SubscribedTuple
	s.jobStatus.jobLock.RLock()
	defer s.jobStatus.jobLock.RUnlock()
	for _, value := range s.jobStatus.subscribeMap {
		tuple := &SubscribedTuple{Queue: value.queue, JobSet: value.jobSet}
		returnJobSets = append(returnJobSets, *tuple)
	}
	return returnJobSets
}

// Save our in memory map to Database and delete from in memory map.
// Is failing to persist a fatal error?  I'd think so..
func (s *SQLJobService) PersistDataToDatabase() error {
	log.Info("Saving Data to Database")
	s.jobStatus.jobLock.RLock()
	defer s.jobStatus.jobLock.RUnlock()
	for key, value := range s.jobStatus.jobMap {
		stmt, err := s.db.Prepare("INSERT INTO jobservice VALUES (?, ?, ?, ?, ?, ?)")
		if err != nil {
			panic(err)
		}
		jobState := value.jobResponse.State.String()
		_, execErr := stmt.Exec(value.queue, value.jobSetId, value.jobId, jobState, value.jobResponse.Error, value.timeStamp)
		if execErr != nil {
			panic(execErr)
		}
		delete(s.jobStatus.jobMap, key)
	}
	return nil
}
