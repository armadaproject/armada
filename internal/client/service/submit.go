package service

import (
	"github.com/G-Research/k8s-batch/internal/armada/api"
	"github.com/G-Research/k8s-batch/internal/common"
)

type JobSubmissionService interface {
	CreateQueue(*api.Queue) error
	SubmitJob(*api.JobRequest) (*api.JobSubmitResponse, error)
}

type ArmadaApiJobSubmissionService struct {
	submitClient api.SubmitClient
}

func NewArmadaApiJobSubmissionService(client api.SubmitClient) *ArmadaApiJobSubmissionService {
	return &ArmadaApiJobSubmissionService{
		submitClient: client,
	}
}

func (submissionService ArmadaApiJobSubmissionService) CreateQueue(queue *api.Queue) error {
	ctx, cancel := common.ContextWithDefaultTimeout()
	defer cancel()
	_, e := submissionService.submitClient.CreateQueue(ctx, queue)

	return e
}

func (submissionService ArmadaApiJobSubmissionService) SubmitJob(job *api.JobRequest) (*api.JobSubmitResponse, error) {
	ctx, cancel := common.ContextWithDefaultTimeout()
	defer cancel()
	return submissionService.submitClient.SubmitJob(ctx, job)
}
