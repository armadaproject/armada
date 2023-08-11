package client

import (
	"github.com/armadaproject/armada/internal/common"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/pkg/api"
)

const MaxJobsPerRequest = 200

func CreateQueue(submitClient api.SubmitClient, queue *api.Queue) error {
	ctx, cancel := common.ContextWithDefaultTimeout()
	defer cancel()
	_, e := submitClient.CreateQueue(ctx, queue)

	return e
}

func UpdateQueue(submitClient api.SubmitClient, queue *api.Queue) error {
	ctx, cancel := common.ContextWithDefaultTimeout()
	defer cancel()
	_, e := submitClient.UpdateQueue(ctx, queue)

	return e
}

func DeleteQueue(submitClient api.SubmitClient, name string) error {
	ctx, cancel := common.ContextWithDefaultTimeout()
	defer cancel()
	_, e := submitClient.DeleteQueue(ctx, &api.QueueDeleteRequest{name})
	return e
}

func SubmitJobs(submitClient api.CustomSubmitClient, request *api.JobSubmitRequest) (*api.JobSubmitResponse, error) {
	AddClientIds(request.JobRequestItems)
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

func AddClientIds(jobs []*api.JobSubmitRequestItem) {
	for _, j := range jobs {
		if j.ClientId == "" {
			j.ClientId = util.NewULID()
		}
	}
}

func min(a, b int) int {
	if a <= b {
		return a
	}
	return b
}
