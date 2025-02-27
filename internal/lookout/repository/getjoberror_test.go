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

func TestGetJobError(t *testing.T) {
	err := lookout.WithLookoutDb(func(db *pgxpool.Pool) error {
		converter := instructions.NewInstructionConverter(metrics.Get().Metrics, userAnnotationPrefix, &compress.NoOpCompressor{})
		store := lookoutdb.NewLookoutDb(db, nil, metrics.Get(), 10)
		errMsg := "some bad error happened!"
		_ = NewJobSimulator(converter, store).
			Submit(queue, jobSet, owner, namespace, baseTime, &JobOptions{
				JobId:            jobId,
				Priority:         priority,
				PriorityClass:    priorityClass,
				Cpu:              cpu,
				Memory:           memory,
				EphemeralStorage: ephemeralStorage,
				Gpu:              gpu,
			}).
			Rejected(errMsg, baseTime).
			Build().
			ApiJob()

		repo := NewSqlGetJobErrorRepository(db, &compress.NoOpDecompressor{})
		result, err := repo.GetJobErrorMessage(armadacontext.TODO(), jobId)
		assert.NoError(t, err)
		assert.Equal(t, errMsg, result)
		return nil
	})
	assert.NoError(t, err)
}

func TestGetJobErrorNotFound(t *testing.T) {
	err := lookout.WithLookoutDb(func(db *pgxpool.Pool) error {
		repo := NewSqlGetJobErrorRepository(db, &compress.NoOpDecompressor{})
		_, err := repo.GetJobErrorMessage(armadacontext.TODO(), jobId)
		assert.Error(t, err)
		return nil
	})
	assert.NoError(t, err)
}
