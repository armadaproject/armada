package repository

import (
	"fmt"
	"sync"
	"time"

	"github.com/G-Research/armada/internal/jobservice/configuration"
	js "github.com/G-Research/armada/pkg/api/jobservice"

	log "github.com/sirupsen/logrus"
)

// Internal structure for storing in memory JobTables and Subscription JobSets
// Locks are used for concurrent access of map
type jobStatus struct {
	jobMap        map[string]*JobTable
	jobLock       sync.RWMutex
	subscribeMap  map[string]*SubscribeTable
	subscribeLock sync.RWMutex
}

func NewJobStatus(jobMap map[string]*JobTable, subscribeMap map[string]*SubscribeTable) *jobStatus {
	return &jobStatus{jobMap: jobMap, subscribeMap: subscribeMap}
}

// Implementation of JobService
type InMemoryJobServiceRepository struct {
	jobStatus        *jobStatus
	jobServiceConfig *configuration.JobServiceConfiguration
}

func NewInMemoryJobServiceRepository(jobMap *jobStatus, config *configuration.JobServiceConfiguration) *InMemoryJobServiceRepository {
	return &InMemoryJobServiceRepository{jobStatus: jobMap, jobServiceConfig: config}
}

// Get the JobStatus given the jodId
// If a job is not in the map, we return JOB_ID_NOT_FOUND
// This should not be an error.
func (inMem *InMemoryJobServiceRepository) GetJobStatus(jobId string) (*js.JobServiceResponse, error) {
	inMem.jobStatus.jobLock.RLock()
	jobResponse, ok := inMem.jobStatus.jobMap[jobId]
	inMem.jobStatus.jobLock.RUnlock()
	if !ok {
		return &js.JobServiceResponse{State: js.JobServiceResponse_JOB_ID_NOT_FOUND}, nil
	}

	return &jobResponse.jobResponse, nil
}

// Update in memory JobStatus Map with jobId and our JobTable
func (inMem *InMemoryJobServiceRepository) UpdateJobServiceDb(jobId string, jobTable *JobTable) error {
	log.Infof("Updating JobId %s with State %s", jobId, jobTable.jobResponse.State)
	inMem.jobStatus.jobLock.Lock()
	defer inMem.jobStatus.jobLock.Unlock()
	jobResponse, ok := inMem.jobStatus.jobMap[jobId]
	if ok && jobResponse.jobResponse.State == jobTable.jobResponse.State {
	} else {

		inMem.jobStatus.jobMap[jobId] = jobTable
	}
	return nil
}

// Health Check for this service
// True always in the in memory case
func (inMem *InMemoryJobServiceRepository) HealthCheck() bool {
	return true
}

// Check if JobSet is in our JobSetMap
func (inMem *InMemoryJobServiceRepository) IsJobSetSubscribed(jobSetId string) bool {
	inMem.jobStatus.subscribeLock.Lock()
	defer inMem.jobStatus.subscribeLock.Unlock()
	_, ok := inMem.jobStatus.subscribeMap[jobSetId]
	return ok
}

// Mark our JobSet as being subscribed
// SubscribeTable contains JobSet and time when it was created.
func (inMem *InMemoryJobServiceRepository) SubscribeJobSet(jobSetId string) {
	inMem.jobStatus.subscribeLock.Lock()
	defer inMem.jobStatus.subscribeLock.Unlock()
	_, ok := inMem.jobStatus.subscribeMap[jobSetId]
	if ok {
		return
	} else {
		inMem.jobStatus.subscribeMap[jobSetId] = NewSubscribeTable(jobSetId)
	}

}

// UnSubscribe to JobSet and delete all the jobs in the in memory map
func (inMem *InMemoryJobServiceRepository) UnSubscribeJobSet(jobSetId string) {
	inMem.jobStatus.subscribeLock.RLock()
	defer inMem.jobStatus.subscribeLock.RUnlock()
	_, ok := inMem.jobStatus.subscribeMap[jobSetId]
	if !ok {
		log.Infof("JobSetId %s already unsubscribed", jobSetId)
		return
	}
	delete(inMem.jobStatus.subscribeMap, jobSetId)
	log.Infof("JobSetId %s unsubscribed", jobSetId)
	inMem.DeleteJobsInJobSet(jobSetId)
}

// Checks JobSet table to make determine if we should unsubscribe from JobSet
// configTimeWithoutUpdates is a configurable value that is read from the config
// We allow unsubscribing if the jobset hasn't been updated in configTime
func (inMem *InMemoryJobServiceRepository) CheckToUnSubscribe(jobSetId string, configTime int64) bool {
	if !inMem.IsJobSetSubscribed(jobSetId) {
		return false
	}
	currentTime := time.Now().Unix()
	for _, val := range inMem.jobStatus.subscribeMap {
		if val.subscribedJobSet == jobSetId {
			if (currentTime - val.lastRequestTimeStamp) > configTime {
				return true
			}
		}
	}
	return false
}

// Update JobSet Map with time that a Job in that JobSet was requested
func (inMem *InMemoryJobServiceRepository) UpdateJobSetTime(jobSetId string) error {
	inMem.jobStatus.subscribeLock.Lock()
	defer inMem.jobStatus.subscribeLock.Unlock()

	_, ok := inMem.jobStatus.subscribeMap[jobSetId]
	if ok {
		inMem.jobStatus.subscribeMap[jobSetId] = NewSubscribeTable(jobSetId)
		return nil
	} else {
		return fmt.Errorf("JobSet %s is already unsubscribed", jobSetId)
	}
}

// Delete Jobs in JobSet
func (inMem *InMemoryJobServiceRepository) DeleteJobsInJobSet(jobSetId string) error {
	inMem.jobStatus.jobLock.RLock()
	defer inMem.jobStatus.jobLock.RUnlock()
	log.Infof("Deleting jobs in job-set %s", jobSetId)
	for key, val := range inMem.jobStatus.jobMap {
		if val.jobSetId == jobSetId {
			delete(inMem.jobStatus.jobMap, key)
		}
	}
	return nil
}

// private function for debugging purposes.
// Once we have a DB we should remove this.
func (inMem *InMemoryJobServiceRepository) printAllItems() {
	log.Info("Printing All Items")
	inMem.jobStatus.jobLock.RLock()
	defer inMem.jobStatus.jobLock.RUnlock()
	for key, value := range inMem.jobStatus.jobMap {
		log.Infof("JobKey: %s Queue %s JobId %s JobSet %s State %s TimeStamp %d", key, value.queue, value.jobId, value.jobSetId, value.jobResponse.State, value.timeStamp)
	}
}

// Get a list of SubscribedJobSets
func (inMem *InMemoryJobServiceRepository) GetSubscribedJobSets() []string {
	var returnJobSets []string
	for _, value := range inMem.jobStatus.subscribeMap {
		returnJobSets = append(returnJobSets, value.subscribedJobSet)
	}
	return returnJobSets
}

// Once we add database, we should use this to persist.
func (inMem *InMemoryJobServiceRepository) PersistDataToDatabase() error {
	inMem.printAllItems()
	return nil
}
