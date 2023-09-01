package repository

import (
	"database/sql"
	"time"

	"github.com/doug-martin/goqu/v9"
	"github.com/doug-martin/goqu/v9/exp"
	"github.com/gogo/protobuf/types"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/database"
	"github.com/armadaproject/armada/pkg/api/lookout"
)

type jobSetCountsRow struct {
	JobSet    string        `db:"jobset"`
	Queued    sql.NullInt64 `db:"queued"`
	Pending   sql.NullInt64 `db:"pending"`
	Running   sql.NullInt64 `db:"running"`
	Succeeded sql.NullInt64 `db:"succeeded"`
	Failed    sql.NullInt64 `db:"failed"`
	Cancelled sql.NullInt64 `db:"cancelled"`
	Submitted sql.NullTime  `db:"submitted"`

	RunningStatsMin     sql.NullTime `db:"running_min"`
	RunningStatsMax     sql.NullTime `db:"running_max"`
	RunningStatsAverage sql.NullTime `db:"running_average"`
	RunningStatsMedian  sql.NullTime `db:"running_median"`
	RunningStatsQ1      sql.NullTime `db:"running_q1"`
	RunningStatsQ3      sql.NullTime `db:"running_q3"`

	QueuedStatsMin     sql.NullTime `db:"queued_min"`
	QueuedStatsMax     sql.NullTime `db:"queued_max"`
	QueuedStatsAverage sql.NullTime `db:"queued_average"`
	QueuedStatsMedian  sql.NullTime `db:"queued_median"`
	QueuedStatsQ1      sql.NullTime `db:"queued_q1"`
	QueuedStatsQ3      sql.NullTime `db:"queued_q3"`
}

func (r *SQLJobRepository) GetJobSetInfos(ctx *armadacontext.Context, opts *lookout.GetJobSetsRequest) ([]*lookout.JobSetInfo, error) {
	rows, err := r.queryJobSetInfos(ctx, opts)
	if err != nil {
		return nil, err
	}

	return r.rowsToJobSets(rows, opts.Queue), nil
}

func (r *SQLJobRepository) queryJobSetInfos(ctx *armadacontext.Context, opts *lookout.GetJobSetsRequest) ([]*jobSetCountsRow, error) {
	ds := r.createJobSetsDataset(opts)

	jobsInQueueRows := make([]*jobSetCountsRow, 0)
	err := ds.Prepared(true).ScanStructsContext(ctx, &jobsInQueueRows)
	if err != nil {
		return nil, err
	}

	return jobsInQueueRows, nil
}

func (r *SQLJobRepository) createJobSetsDataset(opts *lookout.GetJobSetsRequest) *goqu.SelectDataset {
	countsDs := r.goquDb.
		From(r.goquDb.
			From(jobTable).
			Select(
				job_jobset,
				goqu.L("COUNT(*) FILTER (WHERE job.state = 1)").As("queued"),
				goqu.L("COUNT(*) FILTER (WHERE job.state = 2)").As("pending"),
				goqu.L("COUNT(*) FILTER (WHERE job.state = 3)").As("running"),
				goqu.L("COUNT(*) FILTER (WHERE job.state = 4)").As("succeeded"),
				goqu.L("COUNT(*) FILTER (WHERE job.state = 5)").As("failed"),
				goqu.L("COUNT(*) FILTER (WHERE job.state = 6)").As("cancelled"),
				goqu.MAX(job_submitted).As("submitted")).
			Where(goqu.And(
				job_queue.Eq(opts.Queue),
				job_state.In(
					JobStateToIntMap[JobCancelled],
					JobStateToIntMap[JobQueued],
					JobStateToIntMap[JobPending],
					JobStateToIntMap[JobRunning],
					JobStateToIntMap[JobSucceeded],
					JobStateToIntMap[JobFailed]))).
			GroupBy(job_jobset).
			As("counts")).
		Select(
			goqu.I("counts.jobset").As("jobset"),
			goqu.I("counts.queued"),
			goqu.I("counts.pending"),
			goqu.I("counts.running"),
			goqu.I("counts.succeeded"),
			goqu.I("counts.failed"),
			goqu.I("counts.cancelled"),
			goqu.I("counts.submitted")).
		Where(activeOnlyFilter(opts.ActiveOnly)).
		As("counts")

	runningStatsDs := r.goquDb.
		From(jobTable).
		LeftJoin(jobRunTable, goqu.On(job_jobId.Eq(jobRun_jobId))).
		Select(
			job_jobset,
			goqu.MIN(jobRun_started).As("min"),
			goqu.MAX(jobRun_started).As("max"),
			goqu.L("to_timestamp(AVG(EXTRACT(EPOCH FROM job_run.started))) AS average"),
			goqu.L("to_timestamp(percentile_cont(0.5) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM job_run.started))) AS median"),
			goqu.L("to_timestamp(percentile_cont(0.25) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM job_run.started))) AS q1"),
			goqu.L("to_timestamp(percentile_cont(0.75) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM job_run.started))) AS q3")).
		Where(goqu.And(
			job_queue.Eq(opts.Queue),
			job_state.Eq(JobStateToIntMap[JobRunning]),
			jobRun_finished.IsNull(),
			jobRun_started.IsNotNull())).
		GroupBy(job_jobset).
		As("running_stats")

	queuedStatsDs := r.goquDb.
		From(jobTable).
		Select(
			job_jobset,
			goqu.MIN(job_submitted).As("min"),
			goqu.MAX(job_submitted).As("max"),
			goqu.L("to_timestamp(AVG(EXTRACT(EPOCH FROM job.submitted))) AS average"),
			goqu.L("to_timestamp(percentile_cont(0.5) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM job.submitted))) AS median"),
			goqu.L("to_timestamp(percentile_cont(0.25) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM job.submitted))) AS q1"),
			goqu.L("to_timestamp(percentile_cont(0.75) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM job.submitted))) AS q3")).
		Where(goqu.And(
			job_queue.Eq(opts.Queue),
			job_state.Eq(JobStateToIntMap[JobQueued]))).
		GroupBy(job_jobset).
		As("queued_stats")

	ds := r.goquDb.
		From(countsDs).
		FullJoin(runningStatsDs, goqu.On(goqu.I("counts.jobset").Eq(goqu.I("running_stats.jobset")))).
		FullJoin(queuedStatsDs, goqu.On(goqu.I("counts.jobset").Eq(goqu.I("queued_stats.jobset")))).
		Select(
			goqu.I("counts.jobset").As("jobset"),
			goqu.I("counts.queued"),
			goqu.I("counts.pending"),
			goqu.I("counts.running"),
			goqu.I("counts.succeeded"),
			goqu.I("counts.failed"),
			goqu.I("counts.cancelled"),
			goqu.I("counts.submitted").As("submitted"),
			goqu.I("running_stats.min").As("running_min"),
			goqu.I("running_stats.max").As("running_max"),
			goqu.I("running_stats.average").As("running_average"),
			goqu.I("running_stats.median").As("running_median"),
			goqu.I("running_stats.q1").As("running_q1"),
			goqu.I("running_stats.q3").As("running_q3"),
			goqu.I("queued_stats.min").As("queued_min"),
			goqu.I("queued_stats.max").As("queued_max"),
			goqu.I("queued_stats.average").As("queued_average"),
			goqu.I("queued_stats.median").As("queued_median"),
			goqu.I("queued_stats.q1").As("queued_q1"),
			goqu.I("queued_stats.q3").As("queued_q3")).
		Order(createJobSetOrdering(opts.NewestFirst))

	return ds
}

func activeOnlyFilter(onlyActive bool) goqu.Expression {
	if !onlyActive {
		return goqu.Ex{}
	}

	return goqu.Or(
		goqu.I("queued").Gt(0),
		goqu.I("pending").Gt(0),
		goqu.I("running").Gt(0))
}

func createJobSetOrdering(newestFirst bool) exp.OrderedExpression {
	if newestFirst {
		return goqu.I("submitted").Desc()
	}
	return goqu.I("submitted").Asc()
}

func (r *SQLJobRepository) rowsToJobSets(rows []*jobSetCountsRow, queue string) []*lookout.JobSetInfo {
	jobSetInfos := make([]*lookout.JobSetInfo, len(rows), len(rows))
	currentTime := r.clock.Now()

	for i, row := range rows {
		jobSetInfos[i] = &lookout.JobSetInfo{
			Queue:         queue,
			JobSet:        row.JobSet,
			JobsQueued:    uint32(ParseNullInt(row.Queued)),
			JobsPending:   uint32(ParseNullInt(row.Pending)),
			JobsRunning:   uint32(ParseNullInt(row.Running)),
			JobsSucceeded: uint32(ParseNullInt(row.Succeeded)),
			JobsFailed:    uint32(ParseNullInt(row.Failed)),
			JobsCancelled: uint32(ParseNullInt(row.Cancelled)),
			Submitted:     database.ParseNullTime(row.Submitted),
		}

		if row.RunningStatsMax.Valid {
			jobSetInfos[i].RunningStats = &lookout.DurationStats{
				Shortest: getProtoDuration(currentTime, row.RunningStatsMax),
				Longest:  getProtoDuration(currentTime, row.RunningStatsMin),
				Average:  getProtoDuration(currentTime, row.RunningStatsAverage),
				Median:   getProtoDuration(currentTime, row.RunningStatsMedian),
				Q1:       getProtoDuration(currentTime, row.RunningStatsQ3),
				Q3:       getProtoDuration(currentTime, row.RunningStatsQ1),
				// Q1 and Q3 are flipped because most recent => shortest duration and vice versa
			}
		}

		if row.QueuedStatsMax.Valid {
			jobSetInfos[i].QueuedStats = &lookout.DurationStats{
				Shortest: getProtoDuration(currentTime, row.QueuedStatsMax),
				Longest:  getProtoDuration(currentTime, row.QueuedStatsMin),
				Average:  getProtoDuration(currentTime, row.QueuedStatsAverage),
				Median:   getProtoDuration(currentTime, row.QueuedStatsMedian),
				Q1:       getProtoDuration(currentTime, row.QueuedStatsQ3),
				Q3:       getProtoDuration(currentTime, row.QueuedStatsQ1),
			}
		}
	}

	return jobSetInfos
}

func getProtoDuration(currentTime time.Time, maybeTime sql.NullTime) *types.Duration {
	var duration *types.Duration
	if maybeTime.Valid {
		duration = types.DurationProto(currentTime.Sub(maybeTime.Time))
	}
	return duration
}
