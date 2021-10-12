package server

import (
	"fmt"

	"github.com/G-Research/armada/internal/armada/repository"
	"github.com/G-Research/armada/internal/armada/scheduling"
	"github.com/G-Research/armada/pkg/api"
)

func validateJobsCanBeScheduled(jobs []*api.Job, schedulingInfoRepository repository.SchedulingInfoRepository) error {
	allClusterSchedulingInfo, e := schedulingInfoRepository.GetClusterSchedulingInfo()
	if e != nil {
		return e
	}

	activeClusterSchedulingInfo := scheduling.FilterActiveClusterSchedulingInfoReports(allClusterSchedulingInfo)
	for i, job := range jobs {
		if !scheduling.MatchSchedulingRequirementsOnAnyCluster(job, activeClusterSchedulingInfo) {
			return fmt.Errorf("job with index %d is not schedulable on any cluster", i)
		}
	}

	return nil
}
