package conversions

import (
	"time"

	"github.com/go-openapi/strfmt"

	"github.com/armadaproject/armada/internal/common/database/lookout"
	"github.com/armadaproject/armada/internal/lookoutv2/gen/models"
	"github.com/armadaproject/armada/internal/lookoutv2/gen/restapi/operations"
	"github.com/armadaproject/armada/internal/lookoutv2/model"
)

func ToSwaggerJob(job *model.Job) *models.Job {
	runs := make([]*models.Run, len(job.Runs))
	for i := 0; i < len(job.Runs); i++ {
		runs[i] = ToSwaggerRun(job.Runs[i])
	}
	return &models.Job{
		Annotations:        job.Annotations,
		Cancelled:          ToSwaggerTime(job.Cancelled),
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
		Namespace:          job.Namespace,
		Priority:           job.Priority,
		PriorityClass:      job.PriorityClass,
		Queue:              job.Queue,
		Runs:               runs,
		State:              job.State,
		Submitted:          strfmt.DateTime(job.Submitted),
		CancelReason:       job.CancelReason,
		Node:               job.Node,
		Cluster:            job.Cluster,
		ExitCode:           job.ExitCode,
	}
}

func ToSwaggerRun(run *model.Run) *models.Run {
	return &models.Run{
		Cluster:     run.Cluster,
		ExitCode:    run.ExitCode,
		Finished:    PostgreSQLTimeToSwaggerTime(run.Finished),
		JobRunState: string(lookout.JobRunStateMap[run.JobRunState]),
		Node:        run.Node,
		Leased:      PostgreSQLTimeToSwaggerTime(run.Leased),
		Pending:     PostgreSQLTimeToSwaggerTime(run.Pending),
		RunID:       run.RunId,
		Started:     PostgreSQLTimeToSwaggerTime(run.Started),
	}
}

func ToSwaggerGroup(group *model.JobGroup) *models.Group {
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

func FromSwaggerFilter(filter *models.Filter) *model.Filter {
	return &model.Filter{
		Field:        filter.Field,
		Match:        filter.Match,
		Value:        filter.Value,
		IsAnnotation: filter.IsAnnotation,
	}
}

func FromSwaggerOrder(order *models.Order) *model.Order {
	return &model.Order{
		Direction: order.Direction,
		Field:     order.Field,
	}
}

func FromSwaggerGroupedField(groupedField *operations.GroupJobsParamsBodyGroupedField) *model.GroupedField {
	return &model.GroupedField{
		Field:        groupedField.Field,
		IsAnnotation: groupedField.IsAnnotation,
	}
}

func ToSwaggerTime(t *time.Time) *strfmt.DateTime {
	if t == nil {
		return nil
	}
	s := strfmt.DateTime(*t)
	return &s
}

func PostgreSQLTimeToSwaggerTime(t *model.PostgreSQLTime) *strfmt.DateTime {
	if t == nil {
		return nil
	}
	s := strfmt.DateTime(t.Time)
	return &s
}
