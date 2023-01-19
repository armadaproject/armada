package server

import (
	"github.com/pkg/errors"

	"github.com/armadaproject/armada/internal/armada/scheduling"
	"github.com/armadaproject/armada/pkg/api"
)

// validateJobsCanBeScheduled returns a boolean indicating if all pods that make up the provided jobs
// can be scheduled. If it returns false, it also returns an error with information about which job
// can't be scheduled and why.
func validateJobsCanBeScheduled(
	jobs []*api.Job,
	allClusterSchedulingInfo map[string]*api.ClusterSchedulingInfoReport,
) (bool, error) {
	activeClusterSchedulingInfo := scheduling.FilterActiveClusterSchedulingInfoReports(allClusterSchedulingInfo)
	for i, job := range jobs {
		if ok, err := scheduling.MatchSchedulingRequirementsOnAnyCluster(job, activeClusterSchedulingInfo); !ok {
			if err != nil {
				return false, errors.WithMessagef(err, "%d-th job can't be scheduled", i)
			} else {
				return false, errors.Errorf("%d-th job can't be scheduled", i)
			}
		}
	}
	return true, nil
}
