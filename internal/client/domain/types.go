package domain

import v1 "k8s.io/api/core/v1"

type LoadTestSpecification struct {
	Submission []*SubmissionDescription
}

type SubmissionDescription struct {
	UserNamePrefix string `json:"name"`
	Count          int
	Queue          string
	Jobs           []*JobSubmissionDescription
}

type JobSubmissionDescription struct {
	Name  string
	Count int
	Spec  *v1.PodSpec
}
