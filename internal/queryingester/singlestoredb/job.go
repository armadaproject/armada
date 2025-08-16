package singlestoredb

import (
	"database/sql"
	"fmt"
	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/queryingester/instructions"
	"golang.org/x/exp/slices"
	"strings"
)

func upsertJobs(ctx *armadacontext.Context, db *sql.DB, events []instructions.JobRow) error {
	events = slices.Clone(events)

	// Freeze the view of the slice to avoid races with a mutating batcher.
	n := len(events)
	if n == 0 {
		return nil
	}

	cols := []string{
		"job_id",
		"queue",
		"namespace",
		"job_set",
		"cpu",
		"memory",
		"ephemeral_storage",
		"gpu",
		"priority",
		"submit_ts",
		"priority_class",
		"annotations",
		"job_state",
		"cancel_ts",
		"cancel_reason",
		"cancel_user",
		"latest_run_id",
		"run_cluster",
		"run_exit_code",
		"run_finished_ts",
		"run_state",
		"run_node",
		"run_leased_ts",
		"run_pending_ts",
		"run_started_ts",
		"last_transition_time",
		"last_update_ts",
		"error",
	}

	// Build update clause once
	updateClauses := make([]string, 0, len(cols)-1)
	for _, col := range cols {
		if col == "job_id" {
			continue // primary key
		}
		if col == "last_update_ts" {
			updateClauses = append(updateClauses, "last_update_ts = VALUES(last_update_ts)")
			continue
		}
		updateClauses = append(updateClauses,
			fmt.Sprintf("%s = IF(VALUES(%s) IS NOT NULL, VALUES(%s), %s)", col, col, col, col))
	}

	// Pre-size args: rows * cols
	valueArgs := make([]any, 0, n*len(cols))

	// Build the SQL using a builder; emit placeholders and args in the same loop
	var b strings.Builder
	b.Grow(256 + n*len(cols)*2) // rough hint

	b.WriteString("INSERT INTO jobs_active (")
	b.WriteString(strings.Join(cols, ", "))
	b.WriteString(") VALUES ")

	placeForRow := "(" + strings.TrimRight(strings.Repeat("?,", len(cols)), ",") + ")"

	for i := 0; i < n; i++ {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString(placeForRow)

		e := events[i]
		// Append args for this row in the same order as cols
		valueArgs = append(valueArgs,
			e.JobId,
			e.Queue,
			orNil(e.Namespace),
			orNil(e.JobSet),
			orNil(e.Cpu),
			orNil(e.Memory),
			orNil(e.EphemeralStorage),
			orNil(e.Gpu),
			orNil(e.Priority),
			orNil(e.SubmitTs),
			orNil(e.PriorityClass),
			orNil(e.Annotations), // *string with serialized JSON
			orNil(e.JobState),
			orNil(e.CancelTs),
			orNil(e.CancelReason),
			orNil(e.CancelUser),
			orNil(e.LatestRunId),
			orNil(e.RunCluster),
			orNil(e.RunExitCode),
			orNil(e.RunFinishedTs),
			orNil(e.RunState),
			orNil(e.RunNode),
			orNil(e.RunLeasedTs),
			orNil(e.RunPendingTs),
			orNil(e.RunStartedTs),
			orNil(e.LastTransitionTime),
			e.LastUpdateTs, // always non-nil
			orNil(e.Error),
		)
	}

	b.WriteString(" ON DUPLICATE KEY UPDATE ")
	b.WriteString(strings.Join(updateClauses, ", "))

	query := b.String()

	_, err := db.ExecContext(ctx, query, valueArgs...)
	if err != nil {
		return fmt.Errorf("bulk upsert: %w", err)
	}
	return nil
}

// helpers
func orNil[T any](p *T) any {
	if p == nil {
		return nil
	}
	return *p
}
