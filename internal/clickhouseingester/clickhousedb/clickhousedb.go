package clickhousedb

import (
	"github.com/ClickHouse/clickhouse-go/v2"

	"github.com/armadaproject/armada/internal/clickhouseingester/model"
	"github.com/armadaproject/armada/internal/common/armadacontext"
)

type ClickhouseDb struct {
	db clickhouse.Conn
}

func New(db clickhouse.Conn) *ClickhouseDb {
	return &ClickhouseDb{
		db: db,
	}
}

// Store updates clickhouse with the supplied rows
func (l *ClickhouseDb) Store(ctx *armadacontext.Context, ins *model.Instructions) error {
	g, ctx := armadacontext.ErrGroup(ctx)

	g.SetLimit(4)

	g.Go(func() error {
		if len(ins.Jobs) == 0 {
			return nil
		}
		return insertJobs(ctx, l.db, ins.Jobs)
	})
	g.Go(func() error {
		if len(ins.JobRuns) == 0 {
			return nil
		}
		return insertJobRuns(ctx, l.db, ins.JobRuns)
	})
	g.Go(func() error {
		if len(ins.JobSpecs) == 0 {
			return nil
		}
		return insertJobSpecs(ctx, l.db, ins.JobSpecs)
	})
	g.Go(func() error {
		if len(ins.JobDebugs) == 0 {
			return nil
		}
		return insertJobRunDebugs(ctx, l.db, ins.JobDebugs)
	})
	return g.Wait()
}
