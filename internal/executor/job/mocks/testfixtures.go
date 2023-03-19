package mocks

import (
	"github.com/armadaproject/armada/internal/executor/job"
	"github.com/armadaproject/armada/pkg/api"
)

type FakeSubmitter struct {
	FailedSubmissionDetails []*job.FailedSubmissionDetails
	ReceivedSubmitJobs      []*job.SubmitJob
}

func (f *FakeSubmitter) SubmitApiJobs(jobsToSubmit []*api.Job) []*job.FailedSubmissionDetails {
	return f.FailedSubmissionDetails
}

func (f *FakeSubmitter) SubmitJobs(jobsToSubmit []*job.SubmitJob) []*job.FailedSubmissionDetails {
	f.ReceivedSubmitJobs = append(f.ReceivedSubmitJobs, jobsToSubmit...)
	return f.FailedSubmissionDetails
}
