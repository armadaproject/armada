package clickhousedb

import (
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/queryingester/instructions"
	"strings"
	"time"
)

func insertJobs(ctx *armadacontext.Context, conn clickhouse.Conn, events []instructions.JobRow) error {

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
		"merged",
	}

	query := fmt.Sprintf("INSERT INTO jobs (%s) VALUES", strings.Join(cols, ", "))

	batch, err := conn.PrepareBatch(ctx, query)
	if err != nil {
		return err
	}

	now := time.Now()

	for _, e := range events {
		if err := batch.Append(
			e.JobId,
			orNil(e.Queue),
			orNil(e.Namespace),
			orNil(e.JobSet),
			orNil(e.Cpu),
			orNil(e.Memory),
			orNil(e.EphemeralStorage),
			orNil(e.Gpu),
			orNil(e.Priority),
			orNil(e.SubmitTs),
			orNil(e.PriorityClass),
			nilIfEmptyMap(e.Annotations),
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
			now,
			orNil(e.Error),
			orNil(e.Merged),
		); err != nil {
			_ = batch.Abort()
			return fmt.Errorf("append job_id=%s: %w", e.JobId, err)
		}
	}

	if err := batch.Send(); err != nil {
		_ = batch.Abort()
		return fmt.Errorf("send batch: %w", err)
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

func nilIfEmptyMap(m map[string]string) any {
	if len(m) == 0 {
		return map[string]string{}
	}
	return m
}
