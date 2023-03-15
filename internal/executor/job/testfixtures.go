package job

import (
	"github.com/armadaproject/armada/pkg/api"
)

type FakeSubmitter struct {
	FailedSubmissionDetails []*FailedSubmissionDetails
	ReceivedSubmitJobs      []*SubmitJob
}

func (f *FakeSubmitter) SubmitApiJobs(jobsToSubmit []*api.Job) []*FailedSubmissionDetails {
	return f.FailedSubmissionDetails
}

func (f *FakeSubmitter) SubmitJobs(jobsToSubmit []*SubmitJob) []*FailedSubmissionDetails {
	f.ReceivedSubmitJobs = append(f.ReceivedSubmitJobs, jobsToSubmit...)
	return f.FailedSubmissionDetails
}
