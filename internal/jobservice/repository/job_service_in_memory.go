package repository

import (
	js "github.com/G-Research/armada/pkg/api/jobservice"
	log "github.com/sirupsen/logrus"
)

type InMemoryJobServiceRepository struct {
	jobMap            map[string]*js.JobServiceResponse
	subscribedJobSets map[string]string
}

func NewInMemoryJobServiceRepository(jobMap map[string]*js.JobServiceResponse, subscribedJobSets map[string]string) *InMemoryJobServiceRepository {
	return &InMemoryJobServiceRepository{jobMap: jobMap, subscribedJobSets : subscribedJobSets}
}

func (inMem *InMemoryJobServiceRepository) GetJobStatus(jobId string) (*js.JobServiceResponse, error) {
	jobResponse, ok := inMem.jobMap[jobId]
	if !ok {
		return &js.JobServiceResponse{State: js.JobServiceResponse_JOB_ID_NOT_FOUND}, nil
	}

	return jobResponse, nil
}
func (inMem *InMemoryJobServiceRepository) UpdateJobServiceDb(jobId string, jobResponse *js.JobServiceResponse) error {
	inMem.jobMap[jobId] = jobResponse
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

func (inMem *InMemoryJobServiceRepository) PrintAllItems() {
	for key, value := range inMem.jobMap {
		log.Infof("JobKey: %s JobMap: %s", key, value.State)
	}
}
