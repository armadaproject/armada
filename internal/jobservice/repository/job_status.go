package repository

import (
	"time"

	js "github.com/armadaproject/armada/pkg/api/jobservice"
)

// JobStatus represents a job status
type JobStatus struct {
	queue       string
	jobSetId    string
	jobId       string
	jobResponse js.JobServiceResponse
	timeStamp   int64
}

// NewJobStatus combines params into struct JobStatus and adds a timestamp
func NewJobStatus(queue string, jobSetId string, jobId string, jobResponse js.JobServiceResponse) *JobStatus {
	return &JobStatus{queue: queue, jobSetId: jobSetId, jobId: jobId, jobResponse: jobResponse, timeStamp: time.Now().Unix()}
}
