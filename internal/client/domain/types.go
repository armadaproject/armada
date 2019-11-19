package domain

import (
	v1 "k8s.io/api/core/v1"
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
	Name  string
	Count int
	Spec  *v1.PodSpec
}
