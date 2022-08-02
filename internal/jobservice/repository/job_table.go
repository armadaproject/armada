package repository

import (
	"time"

	js "github.com/G-Research/armada/pkg/api/jobservice"
)

// Our representation for a JobStatus
type JobTable struct {
	queueJobSetId string
	jobId         string
	jobResponse   js.JobServiceResponse
	timeStamp     int64
}

// Construct a JobTable and adds a timestamp when it was created.
func NewJobTable(queue string, jobSetId string, jobId string, jobResponse js.JobServiceResponse) *JobTable {
	// Construct a key of (queue, jobSetId) since that uniqutely determines jobset.
	primaryKey := queue + jobSetId
	return &JobTable{queueJobSetId: primaryKey, jobId: jobId, jobResponse: jobResponse, timeStamp: time.Now().Unix()}
}
