package task

import "github.com/G-Research/k8s-batch/internal/executor/service"

type RequestJobsTask struct {
	AllocationService service.KubernetesAllocationService
}

func (requestJobs RequestJobsTask) Run() {
	requestJobs.AllocationService.FillInSpareClusterCapacity()
}
