package service

import (
	"github.com/G-Research/armada/internal/armada/api"
	"github.com/G-Research/armada/internal/common"
)

func CreateQueue(submitClient api.SubmitClient, queue *api.Queue) error {
	ctx, cancel := common.ContextWithDefaultTimeout()
	defer cancel()
	_, e := submitClient.CreateQueue(ctx, queue)

	return e
}

func SubmitJob(submitClient api.SubmitClient, job *api.JobRequest) (*api.JobSubmitResponse, error) {
	ctx, cancel := common.ContextWithDefaultTimeout()
	defer cancel()
	return submitClient.SubmitJob(ctx, job)
}
