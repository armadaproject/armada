package repository

import (
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/compress"
)

type GetJobErrorRepository interface {
	GetJobErrorMessage(ctx *armadacontext.Context, jobId string) (string, error)
}

type SqlGetJobErrorMessageRepository struct {
	db           *pgxpool.Pool
	decompressor compress.Decompressor
}

func NewSqlGetJobErrorRepository(db *pgxpool.Pool, decompressor compress.Decompressor) *SqlGetJobErrorMessageRepository {
	return &SqlGetJobErrorMessageRepository{
		db:           db,
		decompressor: decompressor,
	}
}

func (r *SqlGetJobErrorMessageRepository) GetJobErrorMessage(ctx *armadacontext.Context, jobId string) (string, error) {
	var rawBytes []byte
	err := r.db.QueryRow(ctx, "SELECT error FROM job_error WHERE job_id = $1", jobId).Scan(&rawBytes)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", errors.Errorf("no error found for job with id %s", jobId)
		}
		return "", err
	}
	decompressed, err := r.decompressor.Decompress(rawBytes)
	if err != nil {
		log.WithError(err).Error("failed to decompress")
		return "", err
	}
	return string(decompressed), nil
}
