package client

import (
	"github.com/G-Research/armada/internal/armada/api"
	"github.com/G-Research/armada/internal/common"
)

const MaxJobsPerRequest = 200

func CreateQueue(submitClient api.SubmitClient, queue *api.Queue) error {
	ctx, cancel := common.ContextWithDefaultTimeout()
	defer cancel()
	_, e := submitClient.CreateQueue(ctx, queue)

	return e
}

func SubmitJobs(submitClient api.SubmitClient, request *api.JobSubmitRequest) (*api.JobSubmitResponse, error) {
	ctx, cancel := common.ContextWithDefaultTimeout()
	defer cancel()
	return submitClient.SubmitJobs(ctx, request)
}

func CreateChunkedSubmitRequests(queue string, jobSetId string, jobs []*api.JobSubmitRequestItem) []*api.JobSubmitRequest {
	requests := make([]*api.JobSubmitRequest, 0, 10)

	for i := 0; i < len(jobs); i += MaxJobsPerRequest {
		jobsForRequest := jobs[i:min(i+MaxJobsPerRequest, len(jobs))]

		request := &api.JobSubmitRequest{
			Queue:           queue,
			JobSetId:        jobSetId,
			JobRequestItems: jobsForRequest,
		}

		requests = append(requests, request)
	}

	return requests
}

func min(a, b int) int {
	if a <= b {
		return a
	}
	return b
}
