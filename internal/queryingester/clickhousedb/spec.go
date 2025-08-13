package clickhousedb

import (
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/armadaproject/armada/internal/queryingester/instructions"
	"github.com/pkg/errors"

	"github.com/armadaproject/armada/internal/common/armadacontext"
)

// insertJobRunErrors inserts one or more JobErrorRow records into job_run_errors.
func insertJobSpecs(ctx *armadacontext.Context, conn clickhouse.Conn, rows []instructions.JobSpecRow) error {
	if len(rows) == 0 {
		return nil
	}

	batch, err := conn.PrepareBatch(ctx, "INSERT INTO job_specs (job_id, job_spec) VALUES")
	if err != nil {
		return errors.WithMessage(err, "prepare batch for job_specs failed")
	}

	for _, r := range rows {
		if err := batch.Append(r.JobId, r.JobSpec); err != nil {
			return errors.WithMessagef(err, "appending job_spec for jobId %s failed", r.JobId)
		}
	}

	if err := batch.Send(); err != nil {
		return errors.WithMessage(err, "sending batch of job specs failed")
	}
	return nil
}
