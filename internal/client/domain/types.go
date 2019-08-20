package domain

import (
	"github.com/G-Research/k8s-batch/internal/common"
	v1 "k8s.io/api/core/v1"
)

type LoadTestSpecification struct {
	Submissions []*SubmissionDescription
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

type ArmadaApiConnectionDetails struct {
	Url         string
	Credentials common.LoginCredentials
}
