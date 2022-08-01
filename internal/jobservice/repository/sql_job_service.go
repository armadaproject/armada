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

// Create a Table from a hard-coded schema.
// Open to suggestions on how to make this better
func (s *SQLJobService) CreateTable() {
	_, err := s.db.Exec("DROP TABLE IF EXISTS jobservice")
	if err != nil {
		panic(err)
	}
	_, err = s.db.Exec(`
CREATE TABLE jobservice (
queueJobSetId TEXT,
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
		return &js.JobServiceResponse{State: js.JobServiceResponse_JOB_ID_NOT_FOUND}, nil
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
	if err != nil {
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
	log.Infof("Updating JobId %s with State %s", jobTable.jobId, jobTable.jobResponse.State)
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
func (s *SQLJobService) IsJobSetSubscribed(jobSetQueue string) bool {
	s.jobStatus.subscribeLock.Lock()
	defer s.jobStatus.subscribeLock.Unlock()
	_, ok := s.jobStatus.subscribeMap[jobSetQueue]
	return ok
}

// Mark our JobSet as being subscribed
// SubscribeTable contains JobSet and time when it was created.
func (s *SQLJobService) SubscribeJobSet(jobSetQueue string) {
	s.jobStatus.subscribeLock.Lock()
	defer s.jobStatus.subscribeLock.Unlock()
	_, ok := s.jobStatus.subscribeMap[jobSetQueue]
	if !ok {
		s.jobStatus.subscribeMap[jobSetQueue] = NewSubscribeTable(jobSetQueue)
	}

}

// UnSubscribe to JobSet and delete all the jobs in the in memory map
func (s *SQLJobService) UnSubscribeJobSet(jobSetQueue string) {
	s.jobStatus.subscribeLock.RLock()
	defer s.jobStatus.subscribeLock.RUnlock()
	_, ok := s.jobStatus.subscribeMap[jobSetQueue]
	if !ok {
		log.Infof("JobSetId %s already unsubscribed", jobSetQueue)
		return
	}
	delete(s.jobStatus.subscribeMap, jobSetQueue)
	log.Infof("QueueJobSetId %s unsubscribed", jobSetQueue)
	s.DeleteJobsInJobSet(jobSetQueue)
}

// Checks JobSet table to make determine if we should unsubscribe from JobSet
// configTimeWithoutUpdates is a configurable value that is read from the config
// We allow unsubscribing if the jobset hasn't been updated in configTime
func (s *SQLJobService) CheckToUnSubscribe(queueJobSet string, configTimeWithoutUpdates int64) bool {
	if !s.IsJobSetSubscribed(queueJobSet) {
		return false
	}
	currentTime := time.Now().Unix()
	for _, val := range s.jobStatus.subscribeMap {
		if val.subscribedJobSet == queueJobSet {
			if (currentTime - val.lastRequestTimeStamp) > configTimeWithoutUpdates {
				return true
			}
		}
	}
	return false
}

// Update JobSet Map with time that a Job in that JobSet was requested
func (s *SQLJobService) UpdateJobSetTime(jobSetId string) error {
	s.jobStatus.subscribeLock.Lock()
	defer s.jobStatus.subscribeLock.Unlock()

	_, ok := s.jobStatus.subscribeMap[jobSetId]
	if ok {
		s.jobStatus.subscribeMap[jobSetId] = NewSubscribeTable(jobSetId)
		return nil
	} else {
		return fmt.Errorf("JobSet %s is already unsubscribed", jobSetId)
	}
}

// Delete Jobs in the map.
// This could be a race condition if the in memory map contains the jobs and they haven't been
// persisted to the DB yet.
func (s *SQLJobService) DeleteJobsInJobSet(jobSetId string) error {
	s.jobStatus.jobLock.RLock()
	defer s.jobStatus.jobLock.RUnlock()
	// TODO Handle race condition
	// Maybe we should persist this JobSet to DB if it doesn't exist
	_, err := s.db.Exec("DELETE FROM jobservice WHERE JobSetId=?", jobSetId)
	if err != nil {
		return err
	}
	return nil
}

// Get a list of SubscribedJobSets (QueueJobSetId)
func (s *SQLJobService) GetSubscribedJobSets() []string {
	var returnJobSets []string
	s.jobStatus.jobLock.RLock()
	defer s.jobStatus.jobLock.RUnlock()
	for _, value := range s.jobStatus.subscribeMap {
		returnJobSets = append(returnJobSets, value.subscribedJobSet)
	}
	return returnJobSets
}

// Save our in memory map to Database and delete from in memory map.
func (s *SQLJobService) PersistDataToDatabase() error {
	log.Info("Saving Data to Database")
	s.jobStatus.jobLock.RLock()
	defer s.jobStatus.jobLock.RUnlock()
	for key, value := range s.jobStatus.jobMap {
		stmt, err := s.db.Prepare("INSERT INTO jobservice VALUES (?, ?, ?, ?, ?)")
		if err != nil {
			panic(err)
		}
		jobState := value.jobResponse.State.String()
		log.Infof("State: %s", jobState)
		_, execErr := stmt.Exec(value.queueJobSetId, value.jobId, jobState, value.jobResponse.Error, value.timeStamp)
		if execErr != nil {
			panic(execErr)
		}
		delete(s.jobStatus.jobMap, key)
	}
	return nil
}
