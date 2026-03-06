package db

import (
	"context"
	_ "embed"
	"fmt"

	"github.com/armadaproject/armada/internal/common/database/lookout"
)

//go:embed sql/hotcold_up.sql
var hotColdMigrationSQL string

//go:embed sql/hotcold_down.sql
var hotColdRevertSQL string

//go:embed sql/move_terminal_jobs.sql
var moveTerminalJobsSQL string

// isTerminalState reports whether the given job state ordinal is terminal
// (i.e. the job will never receive further updates).
func isTerminalState(state int32) bool {
	switch state {
	case int32(lookout.JobSucceededOrdinal),
		int32(lookout.JobFailedOrdinal),
		int32(lookout.JobCancelledOrdinal),
		int32(lookout.JobPreemptedOrdinal),
		int32(lookout.JobRejectedOrdinal):
		return true
	}
	return false
}

// moveTerminalJobs atomically moves the given job IDs from job to
// job_historical. Jobs that no longer exist in job (e.g. already moved) are
// silently skipped by the DELETE RETURNING pattern.
func (p *PostgresDatabase) moveTerminalJobs(ctx context.Context, jobIDs []string) error {
	if len(jobIDs) == 0 {
		return nil
	}
	_, err := p.pool.Exec(ctx, moveTerminalJobsSQL, jobIDs)
	if err != nil {
		return fmt.Errorf("moving terminal jobs to job_historical: %w", err)
	}
	return nil
}
