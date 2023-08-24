package repository

import (
	"context"

	"github.com/gogo/protobuf/proto"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/armadaproject/armada/internal/common/compress"
	"github.com/armadaproject/armada/pkg/api"
)

type GetJobSpecRepository interface {
	GetJobSpec(ctx context.Context, jobId string) (*api.Job, error)
}

type SqlGetJobSpecRepository struct {
	db           *pgxpool.Pool
	decompressor compress.Decompressor
}

func NewSqlGetJobSpecRepository(db *pgxpool.Pool, decompressor compress.Decompressor) *SqlGetJobSpecRepository {
	return &SqlGetJobSpecRepository{
		db:           db,
		decompressor: decompressor,
	}
}

func (r *SqlGetJobSpecRepository) GetJobSpec(ctx context.Context, jobId string) (*api.Job, error) {
	var rawBytes []byte
	err := r.db.QueryRow(ctx, "SELECT job_spec FROM job WHERE job_id = $1", jobId).Scan(&rawBytes)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, errors.Errorf("job with id %s not found", jobId)
		}
		return nil, err
	}
	decompressed, err := r.decompressor.Decompress(rawBytes)
	if err != nil {
		log.WithError(err).Error("failed to decompress")
		return nil, err
	}
	var job api.Job
	err = proto.Unmarshal(decompressed, &job)
	if err != nil {
		log.WithError(err).Error("failed to unmarshal bytes")
		return nil, err
	}
	return &job, nil
}
