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

type JobStatus struct {
	jobMap        map[string]*JobTable
	jobLock       sync.RWMutex
	subscribeMap  map[string]*SubscribeTable
	subscribeLock sync.RWMutex
}

func NewJobStatus(jobMap map[string]*JobTable, subscribeMap map[string]*SubscribeTable) *JobStatus {
	return &JobStatus{jobMap: jobMap, subscribeMap: subscribeMap}
}

type SQLJobServiceRepository struct {
	jobStatus        *JobStatus
	jobServiceConfig *configuration.JobServiceConfiguration
	db               *sql.DB
}

func NewSQLJobServiceRepository(jobMap *JobStatus, config *configuration.JobServiceConfiguration, db *sql.DB) *SQLJobServiceRepository {
	return &SQLJobServiceRepository{jobStatus: jobMap, jobServiceConfig: config, db: db}
}

func (s *SQLJobServiceRepository) CreateTable() {
	_, err := s.db.Exec("DROP TABLE IF EXISTS jobservice")
	if err != nil {
		panic(err)
	}
	_, err = s.db.Exec(`
CREATE TABLE jobservice (
queue TEXT,
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

func (s *SQLJobServiceRepository) GetJobStatus(jobId string) (*js.JobServiceResponse, error) {
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

func (s *SQLJobServiceRepository) GetJobStatusSQL(jobId string) (*js.JobServiceResponse, error) {
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
func (s *SQLJobServiceRepository) UpdateJobServiceDb(jobTable *JobTable) {
	log.Infof("Updating JobId %s with State %s", jobTable.jobId, jobTable.jobResponse.State)
	s.jobStatus.jobLock.Lock()
	defer s.jobStatus.jobLock.Unlock()
	s.jobStatus.jobMap[jobTable.jobId] = jobTable
}
func (s *SQLJobServiceRepository) HealthCheck() (bool, error) {
	row := s.db.QueryRow("SELECT 1")
	var col int
	err := row.Scan(&col)
	if err == nil {
		return true, nil
	} else {
		return false, fmt.Errorf("SQL health check failed: %v", err)
	}
}

func (s *SQLJobServiceRepository) IsJobSetSubscribed(jobSetId string) bool {
	s.jobStatus.subscribeLock.Lock()
	defer s.jobStatus.subscribeLock.Unlock()
	_, ok := s.jobStatus.subscribeMap[jobSetId]
	return ok
}

func (s *SQLJobServiceRepository) SubscribeJobSet(jobSetId string) {
	s.jobStatus.subscribeLock.Lock()
	defer s.jobStatus.subscribeLock.Unlock()
	_, ok := s.jobStatus.subscribeMap[jobSetId]
	if ok {
		return
	} else {
		s.jobStatus.subscribeMap[jobSetId] = NewSubscribeTable(jobSetId)
	}

}

func (s *SQLJobServiceRepository) UnSubscribeJobSet(jobSetId string) {
	s.jobStatus.subscribeLock.RLock()
	defer s.jobStatus.subscribeLock.RUnlock()
	_, ok := s.jobStatus.subscribeMap[jobSetId]
	if !ok {
		log.Infof("JobSetId %s already unsubscribed", jobSetId)
		return
	}
	delete(s.jobStatus.subscribeMap, jobSetId)
	log.Infof("JobSetId %s unsubscribed", jobSetId)
	s.DeleteJobsInJobSet(jobSetId)
}

func (s *SQLJobServiceRepository) CheckToUnSubscribe(jobSetId string, configTimeWithoutUpdates int64) bool {
	if !s.IsJobSetSubscribed(jobSetId) {
		return false
	}
	currentTime := time.Now().Unix()
	for _, val := range s.jobStatus.subscribeMap {
		if val.subscribedJobSet == jobSetId {
			if (currentTime - val.lastRequestTimeStamp) > configTimeWithoutUpdates {
				return true
			}
		}
	}
	return false
}

func (s *SQLJobServiceRepository) UpdateJobSetTime(jobSetId string) error {
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

// This is a very slow function until we get a database.
// We will loop over keys in map and delete ones that have a matching jobSetId.
// Painfully slow!
func (s *SQLJobServiceRepository) DeleteJobsInJobSet(jobSetId string) error {
	s.jobStatus.jobLock.RLock()
	defer s.jobStatus.jobLock.RUnlock()
	_, err := s.db.Exec("DELETE FROM jobservice WHERE JobSetId=?", jobSetId)
	if err != nil {
		return err
	}
	return nil
}

func (s *SQLJobServiceRepository) GetSubscribedJobSets() []string {
	var returnJobSets []string
	for _, value := range s.jobStatus.subscribeMap {
		returnJobSets = append(returnJobSets, value.subscribedJobSet)
	}
	return returnJobSets
}

// Once we add database, we should use this to persist.
func (s *SQLJobServiceRepository) PersistDataToDatabase() error {
	log.Info("Saving Data to Database")
	s.jobStatus.jobLock.RLock()
	defer s.jobStatus.jobLock.RUnlock()
	for key, value := range s.jobStatus.jobMap {
		stmt, err := s.db.Prepare("INSERT INTO jobservice VALUES (?, ?, ?, ?, ?, ?)")
		if err != nil {
			panic(err)
		}
		jobState := value.jobResponse.State.String()
		log.Infof("State: %s", jobState)
		_, execErr := stmt.Exec(value.queue, value.jobSetId, value.jobId, jobState, value.jobResponse.Error, value.timeStamp)
		if execErr != nil {
			panic(execErr)
		}
		delete(s.jobStatus.jobMap, key)
	}
	return nil
}
