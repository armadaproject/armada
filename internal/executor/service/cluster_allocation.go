package service

import (
	log "github.com/sirupsen/logrus"

	"github.com/G-Research/k8s-batch/internal/executor/submitter"
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

	cpu := (*availableResource)["cpu"]
	memory := (*availableResource)["memory"]
	log.Infof("Requesting new jobs with free resource cpu: %d, memory %d. Received %d new jobs. ", cpu.AsDec(), memory.Value(), len(newJobs))

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
