package repository

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/stretchr/testify/assert"

	"github.com/armadaproject/armada/internal/common/compress"
	"github.com/armadaproject/armada/internal/common/database/lookout"
	"github.com/armadaproject/armada/internal/lookoutingesterv2/instructions"
	"github.com/armadaproject/armada/internal/lookoutingesterv2/lookoutdb"
	"github.com/armadaproject/armada/internal/lookoutingesterv2/metrics"
)

func TestGetJobRunError(t *testing.T) {
	err := lookout.WithLookoutDb(func(db *pgxpool.Pool) error {
		converter := instructions.NewInstructionConverter(metrics.Get(), userAnnotationPrefix, &compress.NoOpCompressor{}, true)
		store := lookoutdb.NewLookoutDb(db, metrics.Get(), 3, 10)

		errorStrings := []string{
			"some bad error happened!",
			"",
		}
		for _, expected := range errorStrings {
			_ = NewJobSimulator(converter, store).
				Submit(queue, jobSet, owner, baseTime, basicJobOpts).
				Pending(runId, cluster, baseTime).
				Running(runId, node, baseTime).
				RunFailed(runId, node, 137, expected, baseTime).
				Failed(node, 137, "", baseTime).
				Build().
				ApiJob()

			repo := NewSqlGetJobRunErrorRepository(db, &compress.NoOpDecompressor{})
			result, err := repo.GetJobRunError(context.TODO(), runId)
			assert.NoError(t, err)
			assert.Equal(t, expected, result)
		}
		return nil
	})
	assert.NoError(t, err)
}

func TestGetJobRunErrorNotFound(t *testing.T) {
	err := lookout.WithLookoutDb(func(db *pgxpool.Pool) error {
		repo := NewSqlGetJobRunErrorRepository(db, &compress.NoOpDecompressor{})
		_, err := repo.GetJobRunError(context.TODO(), runId)
		assert.Error(t, err)
		return nil
	})
	assert.NoError(t, err)
}
