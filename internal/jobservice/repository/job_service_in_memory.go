package repository

import (
	"fmt"
	"sync"
	"time"

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

type InMemoryJobServiceRepository struct {
	jobStatus        *JobStatus
	jobServiceConfig *configuration.JobServiceConfiguration
}

func NewInMemoryJobServiceRepository(jobMap *JobStatus, config *configuration.JobServiceConfiguration) *InMemoryJobServiceRepository {
	return &InMemoryJobServiceRepository{jobStatus: jobMap, jobServiceConfig: config}
}

func (inMem *InMemoryJobServiceRepository) GetJobStatus(jobId string) (*js.JobServiceResponse, error) {
	inMem.jobStatus.jobLock.RLock()
	jobResponse, ok := inMem.jobStatus.jobMap[jobId]
	inMem.jobStatus.jobLock.RUnlock()
	if !ok {
		return &js.JobServiceResponse{State: js.JobServiceResponse_JOB_ID_NOT_FOUND}, nil
	}

	return &jobResponse.jobResponse, nil
}
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
func (inMem *InMemoryJobServiceRepository) HealthCheck() bool {
	return true
}

func (inMem *InMemoryJobServiceRepository) IsJobSetSubscribed(jobSetId string) bool {
	inMem.jobStatus.subscribeLock.Lock()
	defer inMem.jobStatus.subscribeLock.Unlock()
	_, ok := inMem.jobStatus.subscribeMap[jobSetId]
	return ok
}

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

func (inMem *InMemoryJobServiceRepository) CheckToUnSubscribe(jobSetId string, configTimeWithoutUpdates int64) bool {
	if !inMem.IsJobSetSubscribed(jobSetId) {
		return false
	}
	currentTime := time.Now().Unix()
	for _, val := range inMem.jobStatus.subscribeMap {
		if val.subscribedJobSet == jobSetId {
			if (currentTime - val.lastRequestTimeStamp) > configTimeWithoutUpdates {
				return true
			}
		}
	}
	return false
}

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

// This is a very slow function until we get a database.
// We will loop over keys in map and delete ones that have a matching jobSetId.
// Painfully slow!
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

func (inMem *InMemoryJobServiceRepository) PrintAllItems() {
	log.Info("Printing All Items")
	inMem.jobStatus.jobLock.RLock()
	defer inMem.jobStatus.jobLock.RUnlock()
	for key, value := range inMem.jobStatus.jobMap {
		log.Infof("JobKey: %s Queue %s JobId %s JobSet %s State %s TimeStamp %d", key, value.queue, value.jobId, value.jobSetId, value.jobResponse.State, value.timeStamp)
	}
}

func (inMem *InMemoryJobServiceRepository) GetSubscribedJobSets() []string {
	var returnJobSets []string
	for _, value := range inMem.jobStatus.subscribeMap {
		returnJobSets = append(returnJobSets, value.subscribedJobSet)
	}
	return returnJobSets
}

// Once we add database, we should use this to persist.
func (inMem *InMemoryJobServiceRepository) PersistDataToDatabase() error {
	inMem.PrintAllItems()
	return nil
}
