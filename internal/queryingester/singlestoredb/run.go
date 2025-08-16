package singlestoredb

import (
	"fmt"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/queryingester/instructions"
)

// insertJobRuns inserts job run rows into the job_runs table efficiently.
func insertJobRuns(ctx *armadacontext.Context, conn clickhouse.Conn, runs []instructions.JobRunRow) error {
	cols := []string{
		"job_id",
		"run_id",
		"cluster",
		"exit_code",
		"state",
		"node",
		"leased_ts",
		"pending_ts",
		"started_ts",
		"finished_ts",
		"merged",
	}

	query := fmt.Sprintf("INSERT INTO job_runs (%s) VALUES", strings.Join(cols, ", "))

	batch, err := conn.PrepareBatch(ctx, query)
	if err != nil {
		return err
	}

	for _, r := range runs {
		if err := batch.Append(
			r.JobId,
			r.RunId,
			orNil(r.Cluster),
			orNil(r.ExitCode),
			orNil(r.State),
			orNil(r.Node),
			orNil(r.LeasedTs),
			orNil(r.PendingTs),
			orNil(r.StartedTS),
			orNil(r.FinishedTS),
			orNil(r.Merged),
		); err != nil {
			_ = batch.Abort()
			return fmt.Errorf("append job_id=%s run_id=%s: %w", r.JobId, r.RunId, err)
		}
	}

	if err := batch.Send(); err != nil {
		_ = batch.Abort()
		return fmt.Errorf("send batch: %w", err)
	}
	return nil
}
