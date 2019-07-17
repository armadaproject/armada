package task

import (
	"github.com/G-Research/k8s-batch/internal/executor/service"
	"time"
)

type RequestJobsTask struct {
	AllocationService service.KubernetesAllocationService
	Interval          time.Duration
}

func (requestJobs RequestJobsTask) Execute() {
	requestJobs.AllocationService.FillInSpareClusterCapacity()
}

func (requestJobs RequestJobsTask) GetInterval() time.Duration {
	return requestJobs.Interval
}
