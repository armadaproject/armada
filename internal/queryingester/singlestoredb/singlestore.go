package singlestoredb

import (
	"database/sql"
	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/queryingester/instructions"
)

type SinglestoreDb struct {
	db *sql.DB
}

func New(db *sql.DB) *SinglestoreDb {
	return &SinglestoreDb{
		db: db,
	}
}

// Store updates clickhouse with the supplied rows
func (l *SinglestoreDb) Store(ctx *armadacontext.Context, ins *instructions.Instructions) error {
	g, ctx := armadacontext.ErrGroup(ctx)

	g.SetLimit(4)

	g.Go(func() error {
		if len(ins.Jobs) == 0 {
			return nil
		}
		return upsertJobs(ctx, l.db, ins.Jobs)
	})
	//g.Go(func() error {
	//	if len(ins.JobRuns) == 0 {
	//		return nil
	//	}
	//	return insertJobRuns(ctx, l.db, ins.JobRuns)
	//})
	//g.Go(func() error {
	//	if len(ins.JobSpecs) == 0 {
	//		return nil
	//	}
	//	return insertJobSpecs(ctx, l.db, ins.JobSpecs)
	//})
	//g.Go(func() error {
	//	if len(ins.JobDebugs) == 0 {
	//		return nil
	//	}
	//	return insertJobRunDebugs(ctx, l.db, ins.JobDebugs)
	//})
	//g.Go(func() error {
	//	if len(ins.JobErrors) == 0 {
	//		return nil
	//	}
	//	return insertJobRunErrors(ctx, l.db, ins.JobErrors)
	//})
	return g.Wait()
}
