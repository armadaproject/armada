package repository

import (
	js "github.com/G-Research/armada/pkg/api/jobservice"
)

type JobServiceRepository interface {
	GetJobStatus(jobId string) (*js.JobServiceResponse, error)
	UpdateJobServiceDb(jobId string, jobResponse *JobTable) error
	HealthCheck() bool
	IsJobSetAlreadySubscribed(jobSetId string) bool
	UnSubscribeJobSet(jobSetId string) error
	DeleteJobsInJobSet(jobSetId string) error
	PersistDataToDatabase() error
}
