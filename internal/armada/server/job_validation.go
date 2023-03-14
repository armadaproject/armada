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

// validateAndFilterJobs returns a list of jobs that can be scheduled and a map of jobs that can't be scheduled.
// The map key is the job that can't be scheduled and the value is the reason why it can't be scheduled.
func validateAndFilterJobs(
	jobs []*api.Job,
	allClusterSchedulingInfo map[string]*api.ClusterSchedulingInfoReport,
) ([]*api.Job, map[*api.Job]error) {
	failedToSchedule := map[*api.Job]error{}
	canBeScheduled := []*api.Job{}
	activeClusterSchedulingInfo := scheduling.FilterActiveClusterSchedulingInfoReports(allClusterSchedulingInfo)
	for i, job := range jobs {
		if ok, err := scheduling.MatchSchedulingRequirementsOnAnyCluster(job, activeClusterSchedulingInfo); !ok {
			if err != nil {
				failedToSchedule[job] = errors.WithMessagef(err, "%d-th job can't be scheduled", i)
			} else {
				failedToSchedule[job] = errors.Errorf("%d-th job can't be scheduled", i)
			}
		} else {
			canBeScheduled = append(canBeScheduled, job)
		}
	}
	return canBeScheduled, failedToSchedule
}
