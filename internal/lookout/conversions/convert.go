package conversions

import (
	"strconv"
	"time"

	"github.com/go-openapi/strfmt"

	"github.com/armadaproject/armada/internal/common/database/lookout"
	"github.com/armadaproject/armada/internal/lookout/gen/models"
	"github.com/armadaproject/armada/internal/lookout/gen/restapi/operations"
	"github.com/armadaproject/armada/internal/lookout/model"
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
		CancelUser:         job.CancelUser,
		Node:               job.Node,
		Cluster:            job.Cluster,
		Pool:               job.Pool,
		ExitCode:           job.ExitCode,
		RuntimeSeconds:     job.RuntimeSeconds,
	}
}

func ToSwaggerRun(run *model.Run) *models.Run {
	return &models.Run{
		Cluster:          run.Cluster,
		ExitCode:         run.ExitCode,
		Finished:         PostgreSQLTimeToSwaggerTime(run.Finished),
		JobRunState:      string(lookout.JobRunStateMap[run.JobRunState]),
		Node:             run.Node,
		Leased:           PostgreSQLTimeToSwaggerTime(run.Leased),
		Pending:          PostgreSQLTimeToSwaggerTime(run.Pending),
		Pool:             run.Pool,
		RunID:            run.RunId,
		Started:          PostgreSQLTimeToSwaggerTime(run.Started),
		IngressAddresses: ingressAddressesToSwagger(run.IngressAddresses),
		FailureInfo:      failureInfoToSwagger(run.FailureInfo),
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
	lastTransitionTimeAggregate := ""
	if groupedField.LastTransitionTimeAggregate != nil {
		lastTransitionTimeAggregate = *groupedField.LastTransitionTimeAggregate
	}

	return &model.GroupedField{
		Field:                       groupedField.Field,
		IsAnnotation:                groupedField.IsAnnotation,
		LastTransitionTimeAggregate: lastTransitionTimeAggregate,
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

func failureInfoToSwagger(failureInfo map[string]any) *models.RunFailureInfo {
	if len(failureInfo) == 0 {
		return nil
	}

	result := &models.RunFailureInfo{}
	populated := false
	// After JSON round-trip through PostgreSQL's json_agg, Go's json.Unmarshal
	// produces float64 for numbers and []interface{} for arrays in map[string]any.
	if v, ok := failureInfo["exitCode"].(float64); ok {
		result.ExitCode = int32(v)
		populated = true
	}
	if msg, ok := failureInfo["terminationMessage"].(string); ok {
		result.TerminationMessage = msg
		populated = true
	}
	if cats, ok := failureInfo["categories"].([]interface{}); ok {
		result.Categories = make([]string, 0, len(cats))
		populated = true
		for _, c := range cats {
			if s, ok := c.(string); ok {
				result.Categories = append(result.Categories, s)
			}
		}
	}
	if name, ok := failureInfo["containerName"].(string); ok {
		result.ContainerName = name
		populated = true
	}
	if !populated {
		return nil
	}
	return result
}

func ingressAddressesToSwagger(addresses map[int32]string) map[string]string {
	if len(addresses) == 0 {
		return nil
	}

	swaggerAddresses := make(map[string]string, len(addresses))
	for k, v := range addresses {
		swaggerAddresses[strconv.Itoa(int(k))] = v
	}
	return swaggerAddresses
}
