package singlestoredb

import (
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/pkg/errors"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/queryingester/instructions"
)

// insertJobRunErrors inserts one or more JobDebugRow records into job_run_debugs.
func insertJobRunDebugs(ctx *armadacontext.Context, conn clickhouse.Conn, rows []instructions.JobDebugRow) error {
	if len(rows) == 0 {
		return nil
	}

	batch, err := conn.PrepareBatch(ctx, "INSERT INTO job_run_debugs (run_id, debug_message) VALUES")
	if err != nil {
		return errors.WithMessage(err, "prepare batch for job_run_debugs failed")
	}

	for _, r := range rows {
		if err := batch.Append(r.RunId, r.DebugMessage); err != nil {
			return errors.WithMessagef(err, "appending debug info for runId %s failed", r.RunId)
		}
	}

	if err := batch.Send(); err != nil {
		return errors.WithMessage(err, "sending batch failed")
	}
	return nil
}
