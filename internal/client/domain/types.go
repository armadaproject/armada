package domain

import (
	v1 "k8s.io/api/core/v1"

	"github.com/G-Research/armada/internal/common"
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

type OpenIdConnectClientDetails struct {
	ProviderUrl string
	ClientId    string
	LocalPort   uint16
	Scopes      []string

	Username string
	Password string
}

type ArmadaApiConnectionDetails struct {
	ArmadaUrl     string
	Credentials   common.LoginCredentials
	OpenIdConnect OpenIdConnectClientDetails
}
