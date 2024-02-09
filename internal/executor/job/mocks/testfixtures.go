package mocks

import (
	"github.com/armadaproject/armada/internal/executor/job"
)

type FakeSubmitter struct {
	FailedSubmissionDetails []*job.FailedSubmissionDetails
	ReceivedSubmitJobs      []*job.SubmitJob
}

func (f *FakeSubmitter) SubmitJobs(jobsToSubmit []*job.SubmitJob) []*job.FailedSubmissionDetails {
	f.ReceivedSubmitJobs = append(f.ReceivedSubmitJobs, jobsToSubmit...)
	return f.FailedSubmissionDetails
}
