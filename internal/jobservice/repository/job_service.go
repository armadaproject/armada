package repository

import (
	js "github.com/G-Research/armada/pkg/api/jobservice"
)

type JobServiceRepository interface {
	GetJobStatus(jobId string) (*js.JobServiceResponse, error)
	UpdateJobServiceDb(jobId string, jobResponse *JobTable) error
	HealthCheck() bool
	GetSubscribedJobSets() []string
	IsJobSetSubscribed(jobSetId string) bool
	SubscribeJobSet(jobSetId string)
	UpdateJobSetTime(jobSetId string) error
	UnSubscribeJobSet(jobSetId string)
	CheckToUnSubscribe(jobSetId string, configTimeWithoutUpdates int64) bool
	DeleteJobsInJobSet(jobSetId string) error
	PersistDataToDatabase() error
}
