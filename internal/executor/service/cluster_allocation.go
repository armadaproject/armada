package service

import (
	"github.com/G-Research/k8s-batch/internal/executor/submitter"
	log "github.com/sirupsen/logrus"
)

type ClusterAllocationService struct {
	LeaseService       JobLeaseService
	UtilisationService ClusterUtilisationService
	JobSubmitter       submitter.JobSubmitter
}

func (allocationService ClusterAllocationService) AllocateSpareClusterCapacity() {
	availableResource, err := allocationService.UtilisationService.GetAvailableClusterCapacity()
	if err != nil {
		log.Errorf("Failed to allocate spare cluster capacity because %s", err)
		return
	}

	newJobs, err := allocationService.LeaseService.RequestJobLeases(availableResource)

	if err != nil {
		log.Errorf("Failed to lease new jobs because %s", err)
		return
	} else {
		for _, job := range newJobs {
			_, err = allocationService.JobSubmitter.SubmitJob(job)
			if err != nil {
				log.Errorf("Failed to submit job %s because %s", job.Id, err)
			}
		}
	}
}
