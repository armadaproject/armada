package job

import (
	v1 "k8s.io/api/core/v1"
	networking "k8s.io/api/networking/v1"
)

type SubmitJobMetaInfo struct {
	JobId           string
	Owner           string
	OwnershipGroups []string
}

type SubmitJob struct {
	Meta      SubmitJobMetaInfo
	Pod       *v1.Pod
	Ingresses []*networking.Ingress
	Services  []*v1.Service
}
