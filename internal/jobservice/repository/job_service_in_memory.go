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
	_, ok := inMem.jobStatus.jobMap[jobId]
	if !ok {
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
func (inMem *InMemoryJobServiceRepository) IsJobSetSubscribed(jobSetQueue string) bool {
	inMem.jobStatus.subscribeLock.Lock()
	defer inMem.jobStatus.subscribeLock.Unlock()
	_, ok := inMem.jobStatus.subscribeMap[jobSetQueue]
	return ok
}

// Mark our JobSet as being subscribed
// SubscribeTable contains JobSet and time when it was created.
func (inMem *InMemoryJobServiceRepository) SubscribeJobSet(jobSetQueue string) {
	inMem.jobStatus.subscribeLock.Lock()
	defer inMem.jobStatus.subscribeLock.Unlock()

	_, ok := inMem.jobStatus.subscribeMap[jobSetQueue]
	if !ok {
		inMem.jobStatus.subscribeMap[jobSetQueue] = NewSubscribeTable(jobSetQueue)
	}
}

// UnSubscribe to JobSet and delete all the jobs in the in memory map
func (inMem *InMemoryJobServiceRepository) UnSubscribeJobSet(queueJobSet string) {
	inMem.jobStatus.subscribeLock.RLock()
	defer inMem.jobStatus.subscribeLock.RUnlock()
	_, ok := inMem.jobStatus.subscribeMap[queueJobSet]
	if !ok {
		log.Infof("JobSetId %s already unsubscribed", queueJobSet)
		return
	}
	delete(inMem.jobStatus.subscribeMap, queueJobSet)
	log.Infof("JobSetId %s unsubscribed", queueJobSet)
	inMem.DeleteJobsInJobSet(queueJobSet)
}

// Checks JobSet table to make determine if we should unsubscribe from JobSet
// configTimeWithoutUpdates is a configurable value that is read from the config
// We allow unsubscribing if the jobset hasn't been updated in configTime
func (inMem *InMemoryJobServiceRepository) CheckToUnSubscribe(queueJobSet string, configTime int64) bool {
	if !inMem.IsJobSetSubscribed(queueJobSet) {
		return false
	}
	currentTime := time.Now().Unix()
	inMem.jobStatus.subscribeLock.RLock()
	defer inMem.jobStatus.subscribeLock.RUnlock()
	for _, val := range inMem.jobStatus.subscribeMap {
		if val.subscribedJobSet == queueJobSet {
			if (currentTime - val.lastRequestTimeStamp) > configTime {
				return true
			}
		}
	}
	return false
}

// Update JobSet Map with time that a Job in that JobSet was requested
func (inMem *InMemoryJobServiceRepository) UpdateJobSetTime(queueJobSet string) error {
	inMem.jobStatus.subscribeLock.Lock()
	defer inMem.jobStatus.subscribeLock.Unlock()

	_, ok := inMem.jobStatus.subscribeMap[queueJobSet]
	if ok {
		inMem.jobStatus.subscribeMap[queueJobSet] = NewSubscribeTable(queueJobSet)
		return nil
	} else {
		return fmt.Errorf("JobSet %s is already unsubscribed", queueJobSet)
	}
}

// Delete Jobs in JobSet
func (inMem *InMemoryJobServiceRepository) DeleteJobsInJobSet(queueJobSet string) error {
	inMem.jobStatus.jobLock.RLock()
	defer inMem.jobStatus.jobLock.RUnlock()

	for key, val := range inMem.jobStatus.jobMap {
		if val.queueJobSetId == queueJobSet {
			delete(inMem.jobStatus.jobMap, key)
		}
	}
	log.Infof("Deleted jobs in job-set %s", queueJobSet)
	return nil
}

// private function for debugging purposes.
// Once we have a DB we should remove this.
func (inMem *InMemoryJobServiceRepository) printAllItems() {
	log.Info("Printing All Items")
	inMem.jobStatus.jobLock.RLock()
	defer inMem.jobStatus.jobLock.RUnlock()
	for key, value := range inMem.jobStatus.jobMap {
		log.Infof("JobKey: %s QueueJobSet %s JobId %s State %s TimeStamp %d", key, value.queueJobSetId, value.jobId, value.jobResponse.State, value.timeStamp)
	}
}

// Get a list of SubscribedJobSets
func (inMem *InMemoryJobServiceRepository) GetSubscribedJobSets() []string {
	var returnJobSets []string
	inMem.jobStatus.jobLock.RLock()
	defer inMem.jobStatus.jobLock.RUnlock()
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
