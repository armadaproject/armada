package repository

import (
	"fmt"
	"time"

	"github.com/G-Research/armada/internal/jobservice/configuration"
	js "github.com/G-Research/armada/pkg/api/jobservice"

	log "github.com/sirupsen/logrus"
)

type InMemoryJobServiceRepository struct {
	jobMap            map[string]JobTable
	subscribedJobSets map[string]string
	jobServiceConfig  *configuration.JobServiceConfiguration
}

func NewInMemoryJobServiceRepository(jobMap map[string]JobTable, subscribedJobSets map[string]string, config *configuration.JobServiceConfiguration) *InMemoryJobServiceRepository {
	return &InMemoryJobServiceRepository{jobMap: jobMap, subscribedJobSets: subscribedJobSets, jobServiceConfig: config}
}

func (inMem *InMemoryJobServiceRepository) GetJobStatus(jobId string) (*js.JobServiceResponse, error) {
	jobResponse, ok := inMem.jobMap[jobId]
	if !ok {
		return &js.JobServiceResponse{State: js.JobServiceResponse_JOB_ID_NOT_FOUND}, nil
	}

	return &jobResponse.jobResponse, nil
}
func (inMem *InMemoryJobServiceRepository) UpdateJobServiceDb(jobId string, jobTable *JobTable) error {
	log.Infof("Updating JobId %s with State %s", jobId, jobTable.jobResponse.State)
	inMem.jobMap[jobId] = *jobTable
	return nil
}
func (inMem *InMemoryJobServiceRepository) HealthCheck() bool {
	return true
}

func (inMem *InMemoryJobServiceRepository) IsJobSetAlreadySubscribed(jobSetId string) bool {
	_, ok := inMem.subscribedJobSets[jobSetId]
	if ok {
		return true
	}
	inMem.subscribedJobSets[jobSetId] = jobSetId
	return false
}

func (inMem *InMemoryJobServiceRepository) UnSubscribeJobSet(jobSetId string) error {
	_, ok := inMem.subscribedJobSets[jobSetId]
	if !ok {
		return fmt.Errorf("JobSetId %s already unsubscribed", jobSetId)
	}
	delete(inMem.subscribedJobSets, jobSetId)
	return nil
}

// This is a very slow function until we get a database.
// We will loop over keys in map and delete ones that have a matching jobSetId.
// Painfully slow!
func (inMem *InMemoryJobServiceRepository) DeleteJobsInJobSet(jobSetId string) error {
	for key, val := range inMem.jobMap {
		if val.jobSetId == jobSetId {
			delete(inMem.jobMap, key)
		}
	}
	return nil
}

func (inMem *InMemoryJobServiceRepository) PrintAllItems() {
	log.Info("Printing All Items")
	for key, value := range inMem.jobMap {
		log.Infof("JobKey: %s Queue %s JobId %s JobSet %s State %s TimeStamp %s", key, value.queue, value.jobId, value.jobSetId, value.jobResponse.State, value.timeStamp)
	}
}

// Once we add database, we should use this to persist.
func (inMem *InMemoryJobServiceRepository) PersistDataToDatabase() error {
	ticker := time.NewTicker(time.Duration(inMem.jobServiceConfig.PersistenceInterval) * time.Second)
	for range ticker.C {
		inMem.PrintAllItems()
	}
	return nil
}

func (inMem *InMemoryJobServiceRepository) DeleteAllJobsTTL() error {
	currentTime := time.Now().Unix()
	for key, value := range inMem.jobMap {
		if (currentTime - value.timeStamp) > inMem.jobServiceConfig.TimeToLiveCache {
			delete(inMem.jobMap, key)
		}
	}
	return nil
}
