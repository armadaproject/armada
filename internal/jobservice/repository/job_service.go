package repository

import (
	js "github.com/G-Research/armada/pkg/api/jobservice"
)

type JobServiceRepository interface {
	CreateTable()
	GetJobStatus(jobId string) (*js.JobServiceResponse, error)
	UpdateJobServiceDb(jobResponse *JobTable)
	HealthCheck() (bool, error)
	GetSubscribedJobSets() []string
	IsJobSetSubscribed(jobSetId string) bool
	SubscribeJobSet(jobSetId string)
	UpdateJobSetTime(jobSetId string) error
	UnSubscribeJobSet(jobSetId string)
	CheckToUnSubscribe(jobSetId string, configTimeWithoutUpdates int64) bool
	DeleteJobsInJobSet(jobSetId string) error
	PersistDataToDatabase() error
}
