package domain

import (
	"time"

	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/pkg/api"
)

type LoadTestSpecification struct {
	Submissions []*SubmissionDescription
}

type SubmissionDescription struct {
	Queue               string
	QueuePrefix         string
	JobSetPrefix        string
	Count               int
	QueuePriorityFactor float64
	Jobs                []*JobSubmissionDescription
}

type JobSubmissionDescription struct {
	Name               string
	Count              int
	Namespace          string
	Annotations        map[string]string
	Labels             map[string]string
	RequiredNodeLabels map[string]string
	DelaySubmit        time.Duration
	Priority           float64
	Spec               *v1.PodSpec
}

type JobSubmitFile struct {
	Queue    string
	JobSetId string
	Jobs     []*api.JobSubmitRequestItem `json:"jobs"`
}

type LoadTestSummary struct {
	SubmittedJobs []string
	CurrentState  *WatchContext
}

func (loadTest LoadTestSpecification) NumberOfJobsInSpecification() int {
	numberOfJobs := 0
	for _, submission := range loadTest.Submissions {
		for _, jobDescription := range submission.Jobs {
			numberOfJobs += jobDescription.Count * submission.Count
		}
	}
	return numberOfJobs
}
