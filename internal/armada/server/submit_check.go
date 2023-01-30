package server

import (
	"fmt"
	"github.com/armadaproject/armada/internal/armada/repository"
	"github.com/armadaproject/armada/internal/armada/scheduling"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/pkg/errors"
)

type SubmitCheckResult struct {
	JobId                        string
	SchedulableOnLegacyScheduler bool
	SchedulableOnPulsarScheduler bool
	Error                        error // error that will be filled in if scheduleable on neither
}

type SubmitChecker interface {
	CheckApiJobs(jobs []*api.Job, considerPulsarScheduler bool) ([]SubmitCheckResult, error)
}

type LegacySchedulerSubmitChecker struct {
	schedulingInfoRepository repository.SchedulingInfoRepository
}

func (s *LegacySchedulerSubmitChecker) CheckApiJobs(jobs []*api.Job, considerPulsarScheduler bool) ([]SubmitCheckResult, error) {
	allClusterSchedulingInfo, err := s.schedulingInfoRepository.GetClusterSchedulingInfo()
	if err != nil {
		err = errors.WithMessage(err, "error getting scheduling info")
		return nil, err
	}
	activeClusterSchedulingInfo := scheduling.FilterActiveClusterSchedulingInfoReports(allClusterSchedulingInfo)
	results := make([]SubmitCheckResult, len(jobs))
	for i, job := range jobs {
		ok, err := scheduling.MatchSchedulingRequirementsOnAnyCluster(job, activeClusterSchedulingInfo)
		results[i] = SubmitCheckResult{
			SchedulableOnLegacyScheduler: ok,
			Error:                        err,
		}
	}
	return results, nil
}

type PulsarSchedulerSubmitChecker struct {
}

func (s *PulsarSchedulerSubmitChecker) CheckApiJobs(jobs []*api.Job) ([]SubmitCheckResult, error) {

	// First, check if all jobs can be scheduled individually.
	for i, job := range jobs {
		reqs := PodRequirementsFromJob(srv.priorityClasses, job)
		canSchedule, reason := srv.Check(reqs)
		if !canSchedule {
			return canSchedule, fmt.Sprintf("%d-th job unschedulable:\n%s", i, reason)
		}
	}
	// Then, check if all gangs can be scheduled.
	for gangId, jobs := range groupJobsByAnnotation(srv.gangIdAnnotation, jobs) {
		if gangId == "" {
			continue
		}
		reqs := PodRequirementsFromLegacySchedulerJobs(jobs, srv.priorityClasses)
		canSchedule, reason := srv.Check(reqs)
		if !canSchedule {
			return canSchedule, fmt.Sprintf("gang %s is unschedulable:\n%s", gangId, reason)
		}
	}
	return true, ""
}
