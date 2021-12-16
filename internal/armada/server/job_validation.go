package server

import (
	"fmt"

	"github.com/G-Research/armada/internal/armada/scheduling"
	"github.com/G-Research/armada/pkg/api"
)

func validateJobsCanBeScheduled(jobs []*api.Job, allClusterSchedulingInfo map[string]*api.ClusterSchedulingInfoReport) error {
	activeClusterSchedulingInfo := scheduling.FilterActiveClusterSchedulingInfoReports(allClusterSchedulingInfo)
	for i, job := range jobs {
		if !scheduling.MatchSchedulingRequirementsOnAnyCluster(job, activeClusterSchedulingInfo) {
			return fmt.Errorf("[validateJobsCanBeScheduled] the %d-th job (job ID %s) can't be scheduled on any cluster", i, job.Id)
		}
	}

	return nil
}
