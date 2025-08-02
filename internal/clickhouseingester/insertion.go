package clickhouseingester

import (
	"fmt"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/armadaproject/armada/internal/common/armadacontext"
)

type ClickhouseDb struct {
	db clickhouse.Conn
}

func NewClickhouseDb(db clickhouse.Conn) *ClickhouseDb {
	return &ClickhouseDb{
		db: db,
	}
}

// Store updates clickhouse with the supplied rows
func (l *ClickhouseDb) Store(ctx *armadacontext.Context, instructions *Instructions) error {
	return InsertJobEvents(ctx, l.db, instructions.Rows)
}

func InsertJobEvents(ctx *armadacontext.Context, conn clickhouse.Conn, events []JobEvent) error {
	for _, e := range events {
		cols := []string{"job_id"}
		vals := []interface{}{e.JobID}

		if e.Queue != nil {
			cols = append(cols, "queue")
			vals = append(vals, *e.Queue)
		}
		if e.Namespace != nil {
			cols = append(cols, "namespace")
			vals = append(vals, *e.Namespace)
		}
		if e.JobSet != nil {
			cols = append(cols, "job_set")
			vals = append(vals, *e.JobSet)
		}
		if e.CPU != nil {
			cols = append(cols, "cpu")
			vals = append(vals, *e.CPU)
		}
		if e.Memory != nil {
			cols = append(cols, "memory")
			vals = append(vals, *e.Memory)
		}
		if e.EphemeralStorage != nil {
			cols = append(cols, "ephemeral_storage")
			vals = append(vals, *e.EphemeralStorage)
		}
		if e.GPU != nil {
			cols = append(cols, "gpu")
			vals = append(vals, *e.GPU)
		}
		if e.Priority != nil {
			cols = append(cols, "priority")
			vals = append(vals, *e.Priority)
		}
		if e.SubmitTS != nil {
			cols = append(cols, "submit_ts")
			vals = append(vals, *e.SubmitTS)
		}
		if e.PriorityClass != nil {
			cols = append(cols, "priority_class")
			vals = append(vals, *e.PriorityClass)
		}
		if len(e.Annotations) > 0 {
			cols = append(cols, "annotations")
			vals = append(vals, e.Annotations)
		}
		if e.JobState != nil {
			cols = append(cols, "job_state")
			vals = append(vals, *e.JobState)
		}
		if e.CancelTS != nil {
			cols = append(cols, "cancel_ts")
			vals = append(vals, *e.CancelTS)
		}
		if e.CancelReason != nil {
			cols = append(cols, "cancel_reason")
			vals = append(vals, *e.CancelReason)
		}
		if e.CancelUser != nil {
			cols = append(cols, "cancel_user")
			vals = append(vals, *e.CancelUser)
		}
		if e.LatestRunID != nil {
			cols = append(cols, "latest_run_id")
			vals = append(vals, *e.LatestRunID)
		}
		if e.RunCluster != nil {
			cols = append(cols, "run_cluster")
			vals = append(vals, *e.RunCluster)
		}
		if e.RunExitCode != nil {
			cols = append(cols, "run_exit_code")
			vals = append(vals, *e.RunExitCode)
		}
		if e.RunFinishedTS != nil {
			cols = append(cols, "run_finished_ts")
			vals = append(vals, *e.RunFinishedTS)
		}
		if e.RunState != nil {
			cols = append(cols, "run_state")
			vals = append(vals, *e.RunState)
		}
		if e.RunNode != nil {
			cols = append(cols, "run_node")
			vals = append(vals, *e.RunNode)
		}
		if e.RunLeased != nil {
			cols = append(cols, "run_leased")
			vals = append(vals, *e.RunLeased)
		}
		if e.RunPendingTS != nil {
			cols = append(cols, "run_pending_ts")
			vals = append(vals, *e.RunPendingTS)
		}
		if e.RunStartedTS != nil {
			cols = append(cols, "run_started_ts")
			vals = append(vals, *e.RunStartedTS)
		}
		if e.LastTransitionTime != nil {
			cols = append(cols, "last_transition_time")
			vals = append(vals, *e.LastTransitionTime)
		}
		cols = append(cols, "last_update_ts")
		vals = append(vals, e.LastUpdateTS)
		if e.Merged != nil {
			cols = append(cols, "merged")
			vals = append(vals, *e.Merged)
		}

		query := fmt.Sprintf("INSERT INTO jobs (%s) VALUES (%s)",
			strings.Join(cols, ", "),
			placeholders(len(cols)),
		)

		if err := conn.Exec(ctx, query, vals...); err != nil {
			return fmt.Errorf("failed to insert job_id=%s: %w", e.JobID, err)
		}
	}
	return nil
}

func placeholders(n int) string {
	ph := make([]string, n)
	for i := 0; i < n; i++ {
		ph[i] = "?"
	}
	return strings.Join(ph, ", ")
}
