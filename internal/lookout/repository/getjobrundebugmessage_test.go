package repository

import (
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/compress"
	"github.com/armadaproject/armada/internal/common/database/lookout"
	"github.com/armadaproject/armada/internal/lookoutingester/instructions"
	"github.com/armadaproject/armada/internal/lookoutingester/lookoutdb"
	"github.com/armadaproject/armada/internal/lookoutingester/metrics"
)

func TestGetJobRunDebugMessage(t *testing.T) {
	err := lookout.WithLookoutDb(func(db *pgxpool.Pool) error {
		converter := instructions.NewInstructionConverter(metrics.Get().Metrics, userAnnotationPrefix, &compress.NoOpCompressor{})
		store := lookoutdb.NewLookoutDb(db, nil, metrics.Get(), 10)

		debugMessageStrings := []string{
			"some bad error happened!",
			"",
		}
		for _, expected := range debugMessageStrings {
			_ = NewJobSimulator(converter, store).
				Submit(queue, jobSet, owner, namespace, baseTime, basicJobOpts).
				Lease(runId, cluster, node, baseTime).
				Pending(runId, cluster, baseTime).
				Running(runId, node, baseTime).
				RunFailed(runId, node, 137, "", expected, baseTime).
				Failed(node, 137, "", baseTime).
				Build().
				ApiJob()

			repo := NewSqlGetJobRunDebugMessageRepository(db, &compress.NoOpDecompressor{})
			result, err := repo.GetJobRunDebugMessage(armadacontext.TODO(), runId)
			assert.NoError(t, err)
			assert.Equal(t, expected, result)
		}
		return nil
	})
	assert.NoError(t, err)
}

func TestGetJobRunDebugMessageNotFound(t *testing.T) {
	err := lookout.WithLookoutDb(func(db *pgxpool.Pool) error {
		repo := NewSqlGetJobRunDebugMessageRepository(db, &compress.NoOpDecompressor{})
		_, err := repo.GetJobRunDebugMessage(armadacontext.TODO(), runId)
		assert.Error(t, err)
		return nil
	})
	assert.NoError(t, err)
}
