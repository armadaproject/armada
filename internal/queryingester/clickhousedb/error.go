package clickhousedb

import (
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/armadaproject/armada/internal/queryingester/instructions"
	"github.com/pkg/errors"

	"github.com/armadaproject/armada/internal/common/armadacontext"
)

// insertJobRunErrors inserts one or more JobErrorRow records into job_run_errors.
func insertJobRunErrors(ctx *armadacontext.Context, conn clickhouse.Conn, rows []instructions.JobErrorRow) error {
	if len(rows) == 0 {
		return nil
	}

	batch, err := conn.PrepareBatch(ctx, "INSERT INTO job_run_errors (run_id, error_message) VALUES")
	if err != nil {
		return errors.WithMessage(err, "prepare batch for job_run_errors failed")
	}

	for _, r := range rows {
		if err := batch.Append(r.RunId, r.ErrorMessage); err != nil {
			return errors.WithMessagef(err, "appending error for runId %s failed", r.RunId)
		}
	}

	if err := batch.Send(); err != nil {
		return errors.WithMessage(err, "sending batch failed")
	}
	return nil
}
