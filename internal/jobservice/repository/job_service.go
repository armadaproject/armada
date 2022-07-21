package repository

import (
	js "github.com/G-Research/armada/pkg/api/jobservice"
)

type JobServiceRepository interface {
	GetJobStatus(jobId string) (*js.JobServiceResponse, error)
	UpdateJobServiceDb(jobId string, jobResponse *js.JobServiceResponse) error
	HealthCheck() bool
	IsJobSetAlreadySubscribed(jobSetId string) bool
}
