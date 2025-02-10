package repository

import (
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pkg/errors"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/compress"
	log "github.com/armadaproject/armada/internal/common/logging"
)

type GetJobRunDebugMessageRepository interface {
	GetJobRunDebugMessage(ctx *armadacontext.Context, runId string) (string, error)
}

type SqlGetJobRunDebugMessageRepository struct {
	db           *pgxpool.Pool
	decompressor compress.Decompressor
}

func NewSqlGetJobRunDebugMessageRepository(db *pgxpool.Pool, decompressor compress.Decompressor) *SqlGetJobRunDebugMessageRepository {
	return &SqlGetJobRunDebugMessageRepository{
		db:           db,
		decompressor: decompressor,
	}
}

func (r *SqlGetJobRunDebugMessageRepository) GetJobRunDebugMessage(ctx *armadacontext.Context, runId string) (string, error) {
	var rawBytes []byte
	err := r.db.QueryRow(ctx, "SELECT debug FROM job_run WHERE run_id = $1 AND error IS NOT NULL", runId).Scan(&rawBytes)
	if err != nil {
		if err == pgx.ErrNoRows {
			return "", errors.Errorf("no debug message found for run with id %s", runId)
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
