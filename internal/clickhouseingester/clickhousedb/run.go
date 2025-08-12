package clickhousedb

import (
	"fmt"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"

	"github.com/armadaproject/armada/internal/clickhouseingester/model"
	"github.com/armadaproject/armada/internal/common/armadacontext"
)

// insertJobRuns inserts job run rows into the job_runs table.
func insertJobRuns(ctx *armadacontext.Context, conn clickhouse.Conn, runs []model.JobRunRow) error {
	for _, r := range runs {
		cols := []string{"job_id", "run_id"}
		vals := []interface{}{r.JobId, r.RunId}

		if r.Cluster != nil {
			cols = append(cols, "cluster")
			vals = append(vals, *r.Cluster)
		}
		if r.ExitCode != nil {
			cols = append(cols, "exit_code")
			vals = append(vals, *r.ExitCode)
		}
		if r.State != nil {
			cols = append(cols, "state")
			vals = append(vals, *r.State)
		}
		if r.Node != nil {
			cols = append(cols, "run_node")
			vals = append(vals, *r.Node)
		}
		if r.LeasedTs != nil {
			cols = append(cols, "leased_ts")
			vals = append(vals, *r.LeasedTs)
		}
		if r.PendingTs != nil {
			cols = append(cols, "pending_ts")
			vals = append(vals, *r.PendingTs)
		}
		if r.StartedTS != nil {
			cols = append(cols, "started_ts")
			vals = append(vals, *r.StartedTS)
		}
		if r.FinishedTS != nil {
			cols = append(cols, "finished_ts")
			vals = append(vals, *r.FinishedTS)
		}
		if r.Merged != nil {
			cols = append(cols, "merged")
			vals = append(vals, *r.Merged)
		}

		query := fmt.Sprintf(
			"INSERT INTO job_runs (%s) VALUES (%s)",
			strings.Join(cols, ", "),
			placeholders(len(cols)),
		)

		if err := conn.Exec(ctx, query, vals...); err != nil {
			return fmt.Errorf("failed to insert job run (job_id=%s, run_id=%s): %w", r.JobId, r.RunId, err)
		}
	}
	return nil
}
