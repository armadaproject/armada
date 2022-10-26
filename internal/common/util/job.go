package util

import (
	"github.com/G-Research/armada/pkg/api"
	"k8s.io/api/core/v1"
)

func PodSpecFromJob(job *api.Job) *v1.PodSpec {
	if job.PodSpec != nil {
		return job.PodSpec
	}
	for _, podSpec := range job.PodSpecs {
		if podSpec != nil {
			return podSpec
		}
	}
	return nil
}
