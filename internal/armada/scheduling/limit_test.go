package scheduling

import (
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/G-Research/armada/pkg/api"
)

func TestSchedulingLimit_RemoveFromRemainingLimit(t *testing.T) {
	job := createJob()
	jobSize := job.Size()

	limit := NewLeasePayloadLimit(10, 1000000, 1000)
	limit.RemoveFromRemainingLimit(job)
	assert.Equal(t, limit.remainingJobCount, 9)
	assert.Equal(t, limit.remainingPayloadSizeLimitBytes, 1000000-jobSize)
}

func TestAtLimit(t *testing.T) {
	limit := NewLeasePayloadLimit(1, 11, 10)
	assert.False(t, limit.AtLimit())
}

func TestAtLimit_WhenJobCountLimitExceeded(t *testing.T) {
	// Is at limit when job count <= 0
	limit := NewLeasePayloadLimit(0, 100, 10)
	assert.True(t, limit.AtLimit())
	limit = NewLeasePayloadLimit(-1, 100, 10)
	assert.True(t, limit.AtLimit())
}

func TestAtLimit_WhenJobSizeLimitExceeded(t *testing.T) {
	// At limit when remainingPayloadSizeLimitBytes <= maxExpectedJobSizeBytes
	limit := NewLeasePayloadLimit(1, 10, 10)
	assert.True(t, limit.AtLimit())
	limit = NewLeasePayloadLimit(-1, -1, 10)
	assert.True(t, limit.AtLimit())
}

func TestSchedulingLimit_IsWithinLimit(t *testing.T) {
	job := createJob()
	jobSize := job.Size()

	limit := NewLeasePayloadLimit(10, jobSize*2, 1)
	assert.True(t, limit.IsWithinLimit(job))

	limit = NewLeasePayloadLimit(10, jobSize-1, 1)
	assert.False(t, limit.IsWithinLimit(job))
}

func createJob() *api.Job {
	podSpec := &v1.PodSpec{
		Containers: []v1.Container{{
			Name:  "Container1",
			Image: "index.docker.io/library/ubuntu:latest",
			Args:  []string{"sleep", "10s"},
			Resources: v1.ResourceRequirements{
				Requests: v1.ResourceList{"cpu": resource.MustParse("1"), "memory": resource.MustParse("1Mi")},
				Limits:   v1.ResourceList{"cpu": resource.MustParse("1"), "memory": resource.MustParse("1Mi")},
			},
		}},
	}
	return &api.Job{
		Id:        "test",
		JobSetId:  "jobSet",
		Queue:     "test",
		Namespace: "test",
		Priority:  1,
		PodSpec:   podSpec,
	}
}
