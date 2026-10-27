package repository

import (
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/armadaproject/armada/internal/common/armadacontext"
)

type GetJobRunSchedulerTerminationReasonRepository interface {
	GetJobRunSchedulerTerminationReason(ctx *armadacontext.Context, runId string) (string, error)
}

type SqlGetJobRunSchedulerTerminationReasonRepository struct {
	db *pgxpool.Pool
}

func NewSqlGetJobRunSchedulerTerminationReasonRepository(db *pgxpool.Pool) *SqlGetJobRunSchedulerTerminationReasonRepository {
	return &SqlGetJobRunSchedulerTerminationReasonRepository{db: db}
}

func (r *SqlGetJobRunSchedulerTerminationReasonRepository) GetJobRunSchedulerTerminationReason(ctx *armadacontext.Context, runId string) (string, error) {
	var result string
	err := r.db.QueryRow(ctx, "SELECT scheduler_termination_reason FROM job_run WHERE run_id = $1 AND scheduler_termination_reason IS NOT NULL",
		runId).Scan(&result)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", fmt.Errorf("no scheduler termination reason found for run with id %s: %w", runId, ErrNotFound)
		}
		return "", err
	}
	return result, nil
}
