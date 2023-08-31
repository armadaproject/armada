package repository

import (
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"

	"github.com/armadaproject/armada/internal/common/compress"
	"github.com/armadaproject/armada/internal/common/database/lookout"
	"github.com/armadaproject/armada/internal/lookoutingesterv2/instructions"
	"github.com/armadaproject/armada/internal/lookoutingesterv2/lookoutdb"
	"github.com/armadaproject/armada/internal/lookoutingesterv2/metrics"
	"github.com/armadaproject/armada/pkg/api"
)

func TestGetJobSpec(t *testing.T) {
	err := lookout.WithLookoutDb(func(db *pgxpool.Pool) error {
		converter := instructions.NewInstructionConverter(metrics.Get(), userAnnotationPrefix, &compress.NoOpCompressor{}, true)
		store := lookoutdb.NewLookoutDb(db, metrics.Get(), 3, 10)

		job := NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, baseTime, &JobOptions{
				JobId:            jobId,
				Priority:         priority,
				PriorityClass:    "other-default",
				Cpu:              cpu,
				Memory:           memory,
				EphemeralStorage: ephemeralStorage,
				Gpu:              gpu,
				Annotations: map[string]string{
					"step_path": "/1/2/3",
					"hello":     "world",
				},
			}).
			Pending(runId, cluster, baseTime).
			Running(runId, node, baseTime).
			RunSucceeded(runId, baseTime).
			Succeeded(baseTime).
			Build().
			ApiJob()

		repo := NewSqlGetJobSpecRepository(db, &compress.NoOpDecompressor{})
		result, err := repo.GetJobSpec(context.TODO(), jobId)
		assert.NoError(t, err)
		assertApiJobsEquivalent(t, job, result)
		return nil
	})
	assert.NoError(t, err)
}

func TestGetJobSpecError(t *testing.T) {
	err := lookout.WithLookoutDb(func(db *pgxpool.Pool) error {
		repo := NewSqlGetJobSpecRepository(db, &compress.NoOpDecompressor{})
		_, err := repo.GetJobSpec(context.TODO(), jobId)
		assert.Error(t, err)
		return nil
	})
	assert.NoError(t, err)
}

func assertApiJobsEquivalent(t *testing.T, expected, actual *api.Job) {
	assert.Equal(t, expected.Id, actual.Id)
	assert.Equal(t, expected.ClientId, actual.ClientId)
	assert.Equal(t, expected.JobSetId, actual.JobSetId)
	assert.Equal(t, expected.Queue, actual.Queue)
	assert.Equal(t, expected.Namespace, actual.Namespace)
	assert.Equal(t, expected.Labels, actual.Labels)
	assert.Equal(t, expected.Annotations, actual.Annotations)
	assert.Equal(t, expected.Owner, actual.Owner)
	assertSlicesEquivalent(t, expected.QueueOwnershipUserGroups, actual.QueueOwnershipUserGroups)
	assertSlicesEquivalent(t, expected.CompressedQueueOwnershipUserGroups, actual.CompressedQueueOwnershipUserGroups)
	assert.Equal(t, expected.Priority, actual.Priority)
	assert.Equal(t, expected.PodSpec, actual.PodSpec)
	assertSlicesEquivalent(t, expected.PodSpecs, actual.PodSpecs)
	assert.Equal(t, expected.Created, actual.Created)
	assertSlicesEquivalent(t, expected.Ingress, actual.Ingress)
	assertSlicesEquivalent(t, expected.Services, actual.Services)
	assertSlicesEquivalent(t, expected.K8SIngress, actual.K8SIngress)
	assertSlicesEquivalent(t, expected.K8SService, actual.K8SService)
	assert.Equal(t, expected.Scheduler, actual.Scheduler)
}

func assertSlicesEquivalent[T any](t *testing.T, expected, actual []T) {
	if actual == nil || expected == nil {
		assert.Equal(t, len(expected), len(actual))
		return
	}
	assert.Equal(t, expected, actual)
}
