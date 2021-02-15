package repository

import (
	"context"
	"database/sql"
	"sort"
	"time"

	"github.com/doug-martin/goqu/v9"
	"github.com/gogo/protobuf/types"
	"github.com/lib/pq"

	"github.com/G-Research/armada/pkg/api/lookout"
)

type jobSetCountsRow struct {
	JobSet        string        `db:"jobset"`
	Jobs          sql.NullInt64 `db:"jobs"`
	JobsCreated   sql.NullInt64 `db:"jobs_created"`
	JobsStarted   sql.NullInt64 `db:"jobs_started"`
	JobsFinished  sql.NullInt64 `db:"jobs_finished"`
	JobsSucceeded sql.NullInt64 `db:"jobs_succeeded"`

	RunningStatsMin     pq.NullTime `db:"running_min"`
	RunningStatsMax     pq.NullTime `db:"running_max"`
	RunningStatsAverage pq.NullTime `db:"running_average"`
	RunningStatsMedian  pq.NullTime `db:"running_median"`
	RunningStatsQ1      pq.NullTime `db:"running_q1"`
	RunningStatsQ3      pq.NullTime `db:"running_q3"`

	QueuedStatsMin     pq.NullTime `db:"queued_min"`
	QueuedStatsMax     pq.NullTime `db:"queued_max"`
	QueuedStatsAverage pq.NullTime `db:"queued_average"`
	QueuedStatsMedian  pq.NullTime `db:"queued_median"`
	QueuedStatsQ1      pq.NullTime `db:"queued_q1"`
	QueuedStatsQ3      pq.NullTime `db:"queued_q3"`
}

func (r *SQLJobRepository) GetJobSetInfos(ctx context.Context, opts *lookout.GetJobSetsRequest) ([]*lookout.JobSetInfo, error) {
	rows, err := r.queryJobSetInfos(ctx, opts)
	if err != nil {
		return nil, err
	}

	return r.rowsToJobSets(rows, opts.Queue), nil
}

func (r *SQLJobRepository) queryJobSetInfos(ctx context.Context, opts *lookout.GetJobSetsRequest) ([]*jobSetCountsRow, error) {
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
		From(jobTable).
		LeftJoin(jobRunTable, goqu.On(job_jobId.Eq(jobRun_jobId))).
		Select(
			job_jobset,
			goqu.COUNT("*").As("jobs"),
			goqu.COUNT(goqu.COALESCE(
				jobRun_created,
				jobRun_started)).As("jobs_created"),
			goqu.COUNT(jobRun_started).As("jobs_started")).
		Where(goqu.And(
			job_queue.Eq(opts.Queue),
			job_cancelled.IsNull(),
			jobRun_finished.IsNull(),
			jobRun_unableToSchedule.IsNull())).
		GroupBy(job_jobset).
		As("counts")

	finishedCountsDs := r.goquDb.
		From(jobTable).
		LeftJoin(jobRunTable, goqu.On(job_jobId.Eq(jobRun_jobId))).
		Select(
			job_jobset,
			goqu.COUNT("*").As("jobs_finished"),
			goqu.SUM(goqu.L("job_run.succeeded::int")).As("jobs_succeeded")).
		Where(goqu.And(
			job_queue.Eq(opts.Queue),
			job_cancelled.IsNull(),
			jobRun_finished.IsNotNull(),
			jobRun_unableToSchedule.IsNull())).
		GroupBy(job_jobset).
		As("finished_counts")

	runningStatsDs := r.getStatsDs(goqu.And(
		job_queue.Eq(opts.Queue),
		job_cancelled.IsNull(),
		jobRun_finished.IsNull(),
		jobRun_started.IsNotNull(),
		jobRun_unableToSchedule.IsNull())).As("running_stats")

	queuedStatsDs := r.getStatsDs(goqu.And(
		job_queue.Eq(opts.Queue),
		job_submitted.IsNotNull(),
		job_cancelled.IsNull(),
		jobRun_created.IsNull(),
		jobRun_started.IsNull(),
		jobRun_finished.IsNull(),
		jobRun_unableToSchedule.IsNull())).As("queued_stats")

	ds := r.goquDb.
		From(countsDs).
		FullJoin(finishedCountsDs, goqu.On(goqu.I("counts.jobset").Eq(goqu.I("finished_counts.jobset")))).
		FullJoin(runningStatsDs, goqu.On(goqu.I("counts.jobset").Eq(goqu.I("running_stats.jobset")))).
		FullJoin(queuedStatsDs, goqu.On(goqu.I("counts.jobset").Eq(goqu.I("queued_stats.jobset")))).
		Select(
			goqu.COALESCE(goqu.I("counts.jobset"), goqu.I("finished_counts.jobset")).As("jobset"),
			goqu.I("counts.jobs"),
			goqu.I("counts.jobs_created"),
			goqu.I("counts.jobs_started"),
			goqu.I("finished_counts.jobs_finished"),
			goqu.I("finished_counts.jobs_succeeded"),
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
			goqu.I("queued_stats.q3").As("queued_q3"))

	return ds
}

func (r *SQLJobRepository) getStatsDs(filters ...goqu.Expression) *goqu.SelectDataset {
	return r.goquDb.
		From(jobTable).
		LeftJoin(jobRunTable, goqu.On(job_jobId.Eq(jobRun_jobId))).
		Select(
			job_jobset,
			goqu.MIN(job_submitted).As("min"),
			goqu.MAX(job_submitted).As("max"),
			goqu.L("to_timestamp(AVG(EXTRACT(EPOCH FROM job.submitted))) AS average"),
			goqu.L("to_timestamp(percentile_cont(0.5) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM job.submitted))) AS median"),
			goqu.L("to_timestamp(percentile_cont(0.25) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM job.submitted))) AS q1"),
			goqu.L("to_timestamp(percentile_cont(0.75) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM job.submitted))) AS q3")).
		Where(filters...).
		GroupBy(job_jobset)
}

func (r *SQLJobRepository) rowsToJobSets(rows []*jobSetCountsRow, queue string) []*lookout.JobSetInfo {
	jobSetInfoMap := make(map[string]*lookout.JobSetInfo)
	currentTime := r.clock.Now()

	for _, row := range rows {
		jobs := uint32(ParseNullInt(row.Jobs))
		jobsCreated := uint32(ParseNullInt(row.JobsCreated))
		jobsStarted := uint32(ParseNullInt(row.JobsStarted))
		jobsSucceeded := uint32(ParseNullInt(row.JobsSucceeded))
		jobsFinished := uint32(ParseNullInt(row.JobsFinished))
		jobSetInfoMap[row.JobSet] = &lookout.JobSetInfo{
			Queue:         queue,
			JobSet:        row.JobSet,
			JobsQueued:    jobs - jobsCreated,
			JobsPending:   jobsCreated - jobsStarted,
			JobsRunning:   jobsStarted,
			JobsSucceeded: jobsSucceeded,
			JobsFailed:    jobsFinished - jobsSucceeded,
		}

		if row.RunningStatsMax.Valid {
			jobSetInfoMap[row.JobSet].RunningStats = &lookout.DurationStats{
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
			jobSetInfoMap[row.JobSet].QueuedStats = &lookout.DurationStats{
				Shortest: getProtoDuration(currentTime, row.QueuedStatsMax),
				Longest:  getProtoDuration(currentTime, row.QueuedStatsMin),
				Average:  getProtoDuration(currentTime, row.QueuedStatsAverage),
				Median:   getProtoDuration(currentTime, row.QueuedStatsMedian),
				Q1:       getProtoDuration(currentTime, row.QueuedStatsQ3),
				Q3:       getProtoDuration(currentTime, row.QueuedStatsQ1),
			}
		}
	}

	return getSortedJobSets(jobSetInfoMap)
}

func getProtoDuration(currentTime time.Time, maybeTime pq.NullTime) *types.Duration {
	var duration *types.Duration
	if maybeTime.Valid {
		duration = types.DurationProto(currentTime.Sub(maybeTime.Time))
	}
	return duration
}

func getSortedJobSets(resultMap map[string]*lookout.JobSetInfo) []*lookout.JobSetInfo {
	var jobSets []string
	for jobSet := range resultMap {
		jobSets = append(jobSets, jobSet)
	}
	sort.Strings(jobSets)

	var result []*lookout.JobSetInfo
	for _, jobSet := range jobSets {
		result = append(result, resultMap[jobSet])
	}
	return result
}
