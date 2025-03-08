package repository

import (
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/compress"
	log "github.com/armadaproject/armada/internal/common/logging"
	"github.com/armadaproject/armada/pkg/api"
)

type GetJobSpecRepository interface {
	GetJobSpec(ctx *armadacontext.Context, jobId string) (*api.Job, error)
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

func (r *SqlGetJobSpecRepository) GetJobSpec(ctx *armadacontext.Context, jobId string) (*api.Job, error) {
	var rawBytes []byte

	err := r.db.QueryRow(
		ctx, `
			SELECT
				COALESCE(job_spec.job_spec, job.job_spec)
			FROM job LEFT JOIN job_spec
				ON job.job_id = job_spec.job_id
			WHERE job.job_id = $1`, jobId).Scan(&rawBytes)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, errors.Errorf("job_spec with job id %s not found", jobId)
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
