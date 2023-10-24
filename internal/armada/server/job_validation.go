package server

import (
	"fmt"

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
) (bool, []*api.JobSubmitResponseItem, error) {
	activeClusterSchedulingInfo := scheduling.FilterActiveClusterSchedulingInfoReports(allClusterSchedulingInfo)
	responseItems := make([]*api.JobSubmitResponseItem, 0, len(jobs))
	for i, job := range jobs {
		if ok, err := scheduling.MatchSchedulingRequirementsOnAnyCluster(job, activeClusterSchedulingInfo); !ok {
			if err != nil {
				response := &api.JobSubmitResponseItem{
					JobId: job.Id,
					Error: fmt.Sprintf("%d-th job can't be scheduled: %v", i, err),
				}
				responseItems = append(responseItems, response)
			} else {
				response := &api.JobSubmitResponseItem{
					JobId: job.Id,
					Error: fmt.Sprintf("%d-th job can't be scheduled", i),
				}
				responseItems = append(responseItems, response)
			}
		}
	}

	if len(responseItems) > 0 {
		return false, responseItems, errors.New("[createJobs] Failed to validate jobs can be scheduled")
	}

	return true, nil, nil
}
