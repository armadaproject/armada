package conversions

import (
	"github.com/G-Research/armada/internal/lookoutv2"
	"github.com/G-Research/armada/internal/lookoutv2/gen/models"
	"github.com/go-openapi/strfmt"
	"time"
)

func ToSwaggerJob(job *lookoutv2.Job) *models.Job {
	runs := make([]*models.Run, len(job.Runs))
	for i := 0; i < len(job.Runs); i++ {
		runs[i] = ToSwaggerRun(job.Runs[i])
	}
	return &models.Job{
		Annotations:        job.Annotations,
		Cancelled:          toSwaggerTimePtr(job.Cancelled),
		CPU:                job.Cpu,
		Duplicate:          job.Duplicate,
		EphemeralStorage:   job.EphemeralStorage,
		Gpu:                job.Gpu,
		JobID:              job.JobId,
		JobSet:             job.JobSet,
		LastActiveRunID:    job.LastActiveRunId,
		LastTransitionTime: strfmt.DateTime(job.LastTransitionTime),
		Memory:             job.Memory,
		Owner:              job.Owner,
		Priority:           job.Priority,
		PriorityClass:      job.PriorityClass,
		Queue:              job.Queue,
		Runs:               runs,
		State:              job.State,
		Submitted:          strfmt.DateTime(job.Submitted),
	}
}

func ToSwaggerRun(run *lookoutv2.Run) *models.Run {
	return &models.Run{
		Cluster:     run.Cluster,
		ExitCode:    run.ExitCode,
		Finished:    toSwaggerTimePtr(run.Finished),
		JobRunState: run.JobRunState,
		Node:        run.Node,
		Pending:     strfmt.DateTime(run.Pending),
		RunID:       run.RunId,
		Started:     toSwaggerTimePtr(run.Started),
	}
}

func ToSwaggerGroup(group *lookoutv2.JobGroup) *models.Group {
	return &models.Group{
		Aggregates: group.Aggregates,
		Count:      group.Count,
		Name:       group.Name,
	}
}

func ToSwaggerError(err string) *models.Error {
	return &models.Error{
		Error: err,
	}
}

func FromSwaggerFilter(filter *models.Filter) *lookoutv2.Filter {
	return &lookoutv2.Filter{
		Field:        filter.Field,
		Match:        filter.Match,
		Value:        filter.Value,
		IsAnnotation: filter.IsAnnotation,
	}
}

func FromSwaggerOrder(order *models.Order) *lookoutv2.Order {
	return &lookoutv2.Order{
		Direction: order.Direction,
		Field:     order.Field,
	}
}

func toSwaggerTimePtr(swaggerTs *time.Time) *strfmt.DateTime {
	if swaggerTs == nil {
		return nil
	}
	ts := strfmt.DateTime(*swaggerTs)
	return &ts
}
