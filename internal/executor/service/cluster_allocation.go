package service

import (
	"fmt"
	"github.com/G-Research/k8s-batch/internal/executor/submitter"
)

type ClusterAllocationService struct {
	LeaseService       JobLeaseService
	UtilisationService ClusterUtilisationService
	JobSubmitter       submitter.JobSubmitter
}

func (allocationService ClusterAllocationService) AllocateSpareClusterCapacity() {
	availableResource := allocationService.UtilisationService.GetAvailableClusterCapacity()

	newJobs, err := allocationService.LeaseService.RequestJobLeases(availableResource)

	if err != nil {
		fmt.Printf("Failed to lease new jobs because %s \n", err)
	} else {
		for _, job := range newJobs {
			_, err = allocationService.JobSubmitter.SubmitJob(job)
			if err != nil {
				fmt.Printf("Failed to submit job %s because %s \n", job.Id, err)
			}
		}
	}
}
