package repository

import (
	"time"

	js "github.com/G-Research/armada/pkg/api/jobservice"
)

type JobTable struct {
	queue       string
	jobSetId    string
	jobId       string
	jobResponse js.JobServiceResponse
	timeStamp   int64
}

func NewJobTable(queue string, jobSetId string, jobId string, jobResponse js.JobServiceResponse) *JobTable {
	return &JobTable{queue: queue, jobSetId: jobSetId, jobId: jobId, jobResponse: jobResponse, timeStamp: time.Now().Unix()}
}
