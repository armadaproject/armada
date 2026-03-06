package db

import (
	"context"
	_ "embed"
	"fmt"
	"time"

	lookoutmodel "github.com/armadaproject/armada/internal/lookoutingester/model"

	"github.com/armadaproject/armada/internal/common/database/lookout"
)

//go:embed sql/hotcold_up.sql
var hotColdMigrationSQL string

//go:embed sql/hotcold_down.sql
var hotColdRevertSQL string

//go:embed sql/update_and_move_terminal_jobs.sql
var updateAndMoveTerminalJobsSQL string

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

// updateAndMoveTerminalJobs atomically applies a terminal state update to each
// job and moves it from job to job_historical in a single statement. This avoids
// ever writing a terminal state to job, which would violate chk_job_active_state.
// Jobs not found in job (e.g. already moved by a concurrent call) are silently skipped.
func (p *PostgresDatabase) updateAndMoveTerminalJobs(ctx context.Context, updates []*lookoutmodel.UpdateJobInstruction) error {
	if len(updates) == 0 {
		return nil
	}

	jobIDs := make([]string, len(updates))
	states := make([]int16, len(updates))
	ltts := make([]time.Time, len(updates))
	lttSeconds := make([]int64, len(updates))
	cancelled := make([]*time.Time, len(updates))
	cancelReasons := make([]*string, len(updates))
	cancelUsers := make([]*string, len(updates))

	for i, u := range updates {
		jobIDs[i] = u.JobId
		states[i] = int16(*u.State)
		if u.LastTransitionTime != nil {
			ltts[i] = *u.LastTransitionTime
		}
		if u.LastTransitionTimeSeconds != nil {
			lttSeconds[i] = *u.LastTransitionTimeSeconds
		}
		cancelled[i] = u.Cancelled
		cancelReasons[i] = u.CancelReason
		cancelUsers[i] = u.CancelUser
	}

	_, err := p.pool.Exec(ctx, updateAndMoveTerminalJobsSQL,
		jobIDs, states, ltts, lttSeconds, cancelled, cancelReasons, cancelUsers,
	)
	if err != nil {
		return fmt.Errorf("updating and moving terminal jobs to job_historical: %w", err)
	}
	return nil
}
