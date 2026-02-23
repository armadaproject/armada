package repository

import (
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/compress"
	log "github.com/armadaproject/armada/internal/common/logging"
)

type GetJobRunErrorRepository interface {
	GetJobRunError(ctx *armadacontext.Context, runId string) (string, error)
}

type SqlGetJobRunErrorRepository struct {
	db           *pgxpool.Pool
	decompressor compress.Decompressor
}

func NewSqlGetJobRunErrorRepository(db *pgxpool.Pool, decompressor compress.Decompressor) *SqlGetJobRunErrorRepository {
	return &SqlGetJobRunErrorRepository{
		db:           db,
		decompressor: decompressor,
	}
}

func (r *SqlGetJobRunErrorRepository) GetJobRunError(ctx *armadacontext.Context, runId string) (string, error) {
	var rawBytes []byte
	err := r.db.QueryRow(ctx, "SELECT error FROM job_run WHERE run_id = $1 AND error IS NOT NULL", runId).Scan(&rawBytes)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", fmt.Errorf("no error found for run with id %s: %w", runId, ErrNotFound)
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
