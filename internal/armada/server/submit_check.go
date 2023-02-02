package server

import (
	"github.com/armadaproject/armada/internal/armada/repository"
	"github.com/armadaproject/armada/internal/armada/scheduling"
	"github.com/armadaproject/armada/internal/scheduler"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/pkg/errors"
)

type SubmitCheckResult struct {
	JobId                        string
	SchedulableOnLegacyScheduler bool
	SchedulableOnPulsarScheduler bool
	LegacySchedulerError         error
	PulsarSchedulerError         error
}

type SubmitChecker interface {
	CheckApiJobs(jobs []*api.Job, considerPulsarScheduler bool) ([]*SubmitCheckResult, error)
}

type DualSchedulerSubmitChecker struct {
	schedulingInfoRepository     repository.SchedulingInfoRepository
	pulsarSchedulerSubmitChecker scheduler.SubmitChecker
}

func (s *DualSchedulerSubmitChecker) CheckApiJobs(jobs []*api.Job, considerPulsarScheduler bool) ([]*SubmitCheckResult, error) {

	results, err := s.checkLegacyScheduling(jobs)
	if err != nil {
		return nil, err
	}

	if considerPulsarScheduler {
		err := s.checkPulsarScheduling(jobs, results)
		if err != nil {
			return nil, err
		}
	}

	return results, nil
}

func (s *DualSchedulerSubmitChecker) checkLegacyScheduling(jobs []*api.Job) ([]*SubmitCheckResult, error) {
	results := make([]SubmitCheckResult, len(jobs))
	allClusterSchedulingInfo, err := s.schedulingInfoRepository.GetClusterSchedulingInfo()
	if err != nil {
		return nil, errors.WithMessage(err, "error getting scheduling info")
	}
	activeClusterSchedulingInfo := scheduling.FilterActiveClusterSchedulingInfoReports(allClusterSchedulingInfo)

	for i, job := range jobs {
		ok, err := scheduling.MatchSchedulingRequirementsOnAnyCluster(job, activeClusterSchedulingInfo)
		results[i] = SubmitCheckResult{
			SchedulableOnLegacyScheduler: ok,
			LegacySchedulerError:         err,
		}
	}
	return results
}

func (s *DualSchedulerSubmitChecker) checkPulsarScheduling(jobs []*api.Job, results []*SubmitCheckResult) error {

}
