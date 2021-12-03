package server

import (
	"context"
	"fmt"
	"time"

	"github.com/G-Research/armada/internal/armada/repository"
	"github.com/G-Research/armada/pkg/api"
)

type newJobsFn func(ctx context.Context, req *api.JobSubmitRequest) ([]*api.Job, error)

type getClusterSchedulingInfo func() (map[string]*api.ClusterSchedulingInfoReport, error)
type validateJobs func(jobs []*api.Job, allClusterSchedulingInfo map[string]*api.ClusterSchedulingInfoReport) error

func (new newJobsFn) Validate(getSchedulingInfo getClusterSchedulingInfo, validateJobs validateJobs) newJobsFn {
	return func(ctx context.Context, req *api.JobSubmitRequest) ([]*api.Job, error) {
		jobs, err := new(ctx, req)
		if err != nil {
			return nil, err
		}

		schedulingInfo, err := getSchedulingInfo()
		if err != nil {
			return nil, fmt.Errorf("failed to retrieve cluster scheduling info: %w", err)
		}

		if err := validateJobs(jobs, schedulingInfo); err != nil {
			return nil, fmt.Errorf("failed to validate jobs scheduling: %w", err)
		}

		return jobs, nil
	}
}

func NewJobs(owner string, getQueueOwnership GetQueueOwnershipFn, fromRequest api.JobsFromSubmitRequestFn) newJobsFn {
	return func(ctx context.Context, req *api.JobSubmitRequest) ([]*api.Job, error) {
		ownership, err := getQueueOwnership(ctx, req.Queue)
		if err != nil {
			return nil, fmt.Errorf("failed to retrieve queue ownership. %w", err)
		}

		return fromRequest(req, owner, ownership), nil
	}
}

type reportEvents func([]*api.EventMessage) error
type addJobsFn func([]*api.Job) (repository.SubmitJobResults, error)

func (add addJobsFn) ReportQueued(report reportEvents) addJobsFn {
	return func(jobs []*api.Job) (repository.SubmitJobResults, error) {
		results, err := add(jobs)
		if err != nil {
			return nil, err
		}

		filter := func(result *repository.SubmitJobResult) bool {
			return result.Error == nil && !result.DuplicateDetected
		}

		now := time.Now()
		events := make([]*api.EventMessage, 0, len(jobs))
		for _, result := range results.Filter(filter) {
			events = append(events, &api.EventMessage{
				Events: &api.EventMessage_Queued{
					Queued: &api.JobQueuedEvent{
						JobId:    result.JobId,
						Queue:    result.SubmittedJob.Queue,
						JobSetId: result.SubmittedJob.JobSetId,
						Created:  now,
					},
				},
			})
		}

		if err := report(events); err != nil {
			return nil, fmt.Errorf("failed to report queued jobs")
		}

		return results, nil
	}
}

func (add addJobsFn) ReportDuplicate(report reportEvents) addJobsFn {
	return func(jobs []*api.Job) (repository.SubmitJobResults, error) {
		results, err := add(jobs)
		if err != nil {
			return nil, err
		}

		filter := func(result *repository.SubmitJobResult) bool {
			return result.Error == nil && result.DuplicateDetected
		}
		now := time.Now()
		events := make([]*api.EventMessage, 0, len(jobs))
		for _, result := range results.Filter(filter) {
			events = append(events, &api.EventMessage{
				Events: &api.EventMessage_DuplicateFound{
					DuplicateFound: &api.JobDuplicateFoundEvent{
						JobId:         result.JobId,
						Queue:         result.SubmittedJob.Queue,
						JobSetId:      result.SubmittedJob.JobSetId,
						Created:       now,
						OriginalJobId: result.JobId,
					},
				},
			})
		}

		if err := report(events); err != nil {
			return nil, fmt.Errorf("failed to report duplicate jobs")
		}

		return results, nil
	}
}

func (add addJobsFn) ReportSubmitted(report reportEvents) addJobsFn {
	return func(jobs []*api.Job) (repository.SubmitJobResults, error) {
		results, err := add(jobs)
		if err != nil {
			return nil, err
		}

		now := time.Now()
		events := make([]*api.EventMessage, 0, len(jobs))
		for _, result := range results {
			events = append(events, &api.EventMessage{
				Events: &api.EventMessage_Submitted{
					Submitted: &api.JobSubmittedEvent{
						JobId:    result.JobId,
						Queue:    result.SubmittedJob.Queue,
						JobSetId: result.SubmittedJob.JobSetId,
						Created:  now,
						Job:      *result.SubmittedJob,
					},
				},
			})
		}

		if err := report(events); err != nil {
			return nil, fmt.Errorf("failed to report queued jobs")
		}

		return results, nil
	}
}
