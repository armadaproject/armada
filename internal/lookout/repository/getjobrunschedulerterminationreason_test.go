package repository

import (
	"encoding/json"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/compress"
	"github.com/armadaproject/armada/internal/common/database/lookout"
	"github.com/armadaproject/armada/internal/lookoutingester/instructions"
	"github.com/armadaproject/armada/internal/lookoutingester/lookoutdb"
	"github.com/armadaproject/armada/internal/lookoutingester/metrics"
)

func TestGetJobRunSchedulerTerminationReason(t *testing.T) {
	err := lookout.WithLookoutDb(func(db *pgxpool.Pool) error {
		converter := instructions.NewInstructionConverter(metrics.Get().Metrics, userAnnotationPrefix, []string{}, &compress.NoOpCompressor{})
		store := lookoutdb.NewLookoutDb(db, nil, metrics.Get(), 10, 10)

		_ = NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, namespace, baseTime, basicJobOpts).
			Lease(runId, cluster, node, pool, baseTime).
			Pending(runId, cluster, baseTime).
			Running(runId, node, baseTime).
			PreemptedWithRunId(runId, "job preempted by fair-share", "preempting-job-id-123", baseTime).
			Build().
			ApiJob()

		repo := NewSqlGetJobRunSchedulerTerminationReasonRepository(db)
		result, err := repo.GetJobRunSchedulerTerminationReason(armadacontext.TODO(), runId)
		require.NoError(t, err)

		var parsed map[string]any
		require.NoError(t, json.Unmarshal([]byte(result), &parsed))
		assert.Equal(t, "job preempted by fair-share", parsed["reason"])
		args, ok := parsed["args"].(map[string]any)
		require.True(t, ok, "expected args field in scheduler termination reason")
		assert.Equal(t, "preempting-job-id-123", args["preemptingJobId"])

		return nil
	})
	assert.NoError(t, err)
}

func TestGetJobRunSchedulerTerminationReasonNoPreemptingJob(t *testing.T) {
	err := lookout.WithLookoutDb(func(db *pgxpool.Pool) error {
		converter := instructions.NewInstructionConverter(metrics.Get().Metrics, userAnnotationPrefix, []string{}, &compress.NoOpCompressor{})
		store := lookoutdb.NewLookoutDb(db, nil, metrics.Get(), 10, 10)

		_ = NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, namespace, baseTime, basicJobOpts).
			Lease(runId, cluster, node, pool, baseTime).
			Pending(runId, cluster, baseTime).
			Running(runId, node, baseTime).
			PreemptedWithRunId(runId, "job preempted", "", baseTime).
			Build().
			ApiJob()

		repo := NewSqlGetJobRunSchedulerTerminationReasonRepository(db)
		result, err := repo.GetJobRunSchedulerTerminationReason(armadacontext.TODO(), runId)
		require.NoError(t, err)

		var parsed map[string]any
		require.NoError(t, json.Unmarshal([]byte(result), &parsed))
		assert.Equal(t, "job preempted", parsed["reason"])
		assert.Nil(t, parsed["args"])

		return nil
	})
	assert.NoError(t, err)
}

func TestGetJobRunSchedulerTerminationReasonNotFound(t *testing.T) {
	err := lookout.WithLookoutDb(func(db *pgxpool.Pool) error {
		repo := NewSqlGetJobRunSchedulerTerminationReasonRepository(db)
		_, err := repo.GetJobRunSchedulerTerminationReason(armadacontext.TODO(), runId)
		assert.ErrorIs(t, err, ErrNotFound)
		return nil
	})
	assert.NoError(t, err)
}

func TestGetJobRunSchedulerTerminationReasonNullForNonPreemptedRun(t *testing.T) {
	err := lookout.WithLookoutDb(func(db *pgxpool.Pool) error {
		converter := instructions.NewInstructionConverter(metrics.Get().Metrics, userAnnotationPrefix, []string{}, &compress.NoOpCompressor{})
		store := lookoutdb.NewLookoutDb(db, nil, metrics.Get(), 10, 10)

		_ = NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, namespace, baseTime, basicJobOpts).
			Lease(runId, cluster, node, pool, baseTime).
			Pending(runId, cluster, baseTime).
			Running(runId, node, baseTime).
			RunFailed(runId, node, 137, "oom killed", "", baseTime).
			Failed(node, 137, "", baseTime).
			Build().
			ApiJob()

		repo := NewSqlGetJobRunSchedulerTerminationReasonRepository(db)
		_, err := repo.GetJobRunSchedulerTerminationReason(armadacontext.TODO(), runId)
		assert.ErrorIs(t, err, ErrNotFound)
		return nil
	})
	assert.NoError(t, err)
}
