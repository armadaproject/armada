package repository

import (
	"context"
	"database/sql"
	"sort"

	"github.com/doug-martin/goqu/v9"

	"github.com/G-Research/armada/pkg/api/lookout"
)

type jobSetCountsRow struct {
	JobSet        string        `db:"jobset"`
	Jobs          sql.NullInt64 `db:"jobs"`
	JobsCreated   sql.NullInt64 `db:"jobs_created"`
	JobsStarted   sql.NullInt64 `db:"jobs_started"`
	JobsFinished  sql.NullInt64 `db:"jobs_finished"`
	JobsSucceeded sql.NullInt64 `db:"jobs_succeeded"`
}

func (r *SQLJobRepository) GetJobSetInfos(ctx context.Context, opts *lookout.GetJobSetsRequest) ([]*lookout.JobSetInfo, error) {
	rows, err := r.queryJobSetInfos(ctx, opts)
	if err != nil {
		return nil, err
	}

	return rowsToJobSets(rows, opts.Queue), nil
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

	ds := r.goquDb.
		From(finishedCountsDs).
		FullJoin(countsDs, goqu.On(goqu.I("finished_counts.jobset").Eq(goqu.I("counts.jobset")))).
		Select(
			goqu.COALESCE(goqu.I("counts.jobset"), goqu.I("finished_counts.jobset")).As("jobset"),
			goqu.I("counts.jobs"),
			goqu.I("counts.jobs_created"),
			goqu.I("counts.jobs_started"),
			goqu.I("finished_counts.jobs_finished"),
			goqu.I("finished_counts.jobs_succeeded"))

	return ds
}

func rowsToJobSets(rows []*jobSetCountsRow, queue string) []*lookout.JobSetInfo {
	jobSetInfoMap := make(map[string]*lookout.JobSetInfo)

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
	}

	return getSortedJobSets(jobSetInfoMap)
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
