package domain

import (
	"time"

	v1 "k8s.io/api/core/v1"

	"github.com/G-Research/armada/pkg/api"
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
