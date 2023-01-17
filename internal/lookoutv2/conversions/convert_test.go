package conversions

import (
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"k8s.io/utils/pointer"

	"github.com/armadaproject/armada/internal/common/database/lookout"
	"github.com/armadaproject/armada/internal/lookoutv2/gen/models"
	"github.com/armadaproject/armada/internal/lookoutv2/model"
)

var (
	baseTime, _     = time.Parse("2006-01-02T15:04:05.000Z", "2022-03-01T15:04:05.000Z")
	baseTimeSwagger = strfmt.DateTime(baseTime)

	swaggerJob = &models.Job{
		Annotations: map[string]string{
			"key1": "value1",
		},
		Cancelled:          &baseTimeSwagger,
		CPU:                15000,
		Duplicate:          false,
		EphemeralStorage:   2000,
		Gpu:                8,
		JobID:              "job-id",
		JobSet:             "job-set",
		LastActiveRunID:    pointer.String("run-id"),
		LastTransitionTime: baseTimeSwagger,
		Memory:             3000,
		Owner:              "user-id",
		Priority:           10,
		PriorityClass:      pointer.String("default"),
		Queue:              "queue",
		Runs: []*models.Run{
			{
				Cluster:     "cluster",
				ExitCode:    pointer.Int32(322),
				Finished:    &baseTimeSwagger,
				JobRunState: string(lookout.JobRunLeaseReturned),
				Node:        pointer.String("node"),
				Pending:     baseTimeSwagger,
				RunID:       "run-id",
				Started:     &baseTimeSwagger,
			},
		},
		State:     string(lookout.JobFailed),
		Submitted: baseTimeSwagger,
	}

	job = &model.Job{
		Annotations: map[string]string{
			"key1": "value1",
		},
		Cancelled:          &baseTime,
		Cpu:                15000,
		Duplicate:          false,
		EphemeralStorage:   2000,
		Gpu:                8,
		JobId:              "job-id",
		JobSet:             "job-set",
		LastActiveRunId:    pointer.String("run-id"),
		LastTransitionTime: baseTime,
		Memory:             3000,
		Owner:              "user-id",
		Priority:           10,
		PriorityClass:      pointer.String("default"),
		Queue:              "queue",
		Runs: []*model.Run{
			{
				Cluster:     "cluster",
				ExitCode:    pointer.Int32(322),
				Finished:    &baseTime,
				JobRunState: string(lookout.JobRunLeaseReturned),
				Node:        pointer.String("node"),
				Pending:     baseTime,
				RunId:       "run-id",
				Started:     &baseTime,
			},
		},
		State:     string(lookout.JobFailed),
		Submitted: baseTime,
	}

	swaggerGroup = &models.Group{
		Aggregates: map[string]string{
			"averageTimeInState": "3d",
		},
		Count: 1000,
		Name:  "queue-1",
	}

	group = &model.JobGroup{
		Aggregates: map[string]string{
			"averageTimeInState": "3d",
		},
		Count: 1000,
		Name:  "queue-1",
	}

	swaggerFilter = &models.Filter{
		Field:        "jobSet",
		Match:        "exact",
		Value:        "something",
		IsAnnotation: true,
	}

	filter = &model.Filter{
		Field:        "jobSet",
		Match:        "exact",
		Value:        "something",
		IsAnnotation: true,
	}

	swaggerOrder = &models.Order{
		Direction: "ASC",
		Field:     "lastTransitionTime",
	}

	order = &model.Order{
		Direction: "ASC",
		Field:     "lastTransitionTime",
	}
)

func TestToSwaggerJob(t *testing.T) {
	actual := ToSwaggerJob(job)
	assert.Equal(t, swaggerJob, actual)
}

func TestToSwaggerGroup(t *testing.T) {
	actual := ToSwaggerGroup(group)
	assert.Equal(t, swaggerGroup, actual)
}

func TestToSwaggerError(t *testing.T) {
	errMsg := "some error message"
	actual := ToSwaggerError("some error message")
	assert.Equal(t, errMsg, actual.Error)
}

func TestFromSwaggerFilter(t *testing.T) {
	actual := FromSwaggerFilter(swaggerFilter)
	assert.Equal(t, filter, actual)
}

func TestFromSwaggerOrder(t *testing.T) {
	actual := FromSwaggerOrder(swaggerOrder)
	assert.Equal(t, order, actual)
}
