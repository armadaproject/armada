package repository

import (
	"context"
	"fmt"

	"github.com/doug-martin/goqu/v9"
	"github.com/doug-martin/goqu/v9/exp"

	"github.com/G-Research/armada/pkg/api"
	"github.com/G-Research/armada/pkg/api/lookout"
)

func (r *SQLJobRepository) GetJobsInQueue(ctx context.Context, opts *lookout.GetJobsInQueueRequest) ([]*lookout.JobInfo, error) {
	if valid, jobState := validateJobStates(opts.JobStates); !valid {
		return nil, fmt.Errorf("unknown job state: %q", jobState)
	}

	rows, err := r.queryJobs(ctx, opts)
	if err != nil {
		return nil, err
	}

	return rowsToJobs(rows), nil
}

func (r *SQLJobRepository) GetJob(ctx context.Context, jobId string) (*lookout.JobInfo, error) {
	rows, err := r.queryJob(ctx, jobId)
	if err != nil {
		return nil, err
	}

	return rowsToJob(rows), nil
}

func validateJobStates(jobStates []string) (bool, JobState) {
	for _, jobState := range jobStates {
		if !isJobState(jobState) {
			return false, JobState(jobState)
		}
	}
	return true, ""
}

func isJobState(val string) bool {
	for _, jobState := range AllJobStates {
		if JobState(val) == jobState {
			return true
		}
	}
	return false
}

func (r *SQLJobRepository) queryJobs(ctx context.Context, opts *lookout.GetJobsInQueueRequest) ([]*JobRow, error) {
	ds := r.createJobsDataset(opts)

	jobsInQueueRows := make([]*JobRow, 0)
	err := ds.Prepared(true).ScanStructsContext(ctx, &jobsInQueueRows)
	if err != nil {
		return nil, err
	}

	return jobsInQueueRows, nil
}

func (r *SQLJobRepository) createJobsDataset(opts *lookout.GetJobsInQueueRequest) *goqu.SelectDataset {
	subDs := r.goquDb.
		From(jobTable).
		LeftJoin(jobRunTable, goqu.On(
			job_jobId.Eq(jobRun_jobId))).
		Select(job_jobId).
		Where(goqu.And(
			job_queue.Eq(opts.Queue),
			goqu.Or(createJobSetFilters(opts.JobSetIds)...))).
		GroupBy(job_jobId).
		Having(goqu.Or(createJobStateFilters(opts.JobStates)...)).
		Order(createJobOrdering(opts.NewestFirst)).
		Limit(uint(opts.Take)).
		Offset(uint(opts.Skip))

	ds := r.goquDb.
		From(jobTable).
		LeftJoin(jobRunTable, goqu.On(
			job_jobId.Eq(jobRun_jobId))).
		Select(job_jobId,
			job_queue,
			job_owner,
			job_jobset,
			job_priority,
			job_submitted,
			job_cancelled,
			job_job,
			jobRun_runId,
			jobRun_podNumber,
			jobRun_cluster,
			jobRun_node,
			jobRun_created,
			jobRun_started,
			jobRun_finished,
			jobRun_succeeded,
			jobRun_error).
		Where(job_jobId.In(subDs)).
		Order(createJobOrdering(opts.NewestFirst)) // Ordering from sub query not guaranteed to be preserved

	return ds
}

func createJobSetFilters(jobSetIds []string) []goqu.Expression {
	filters := make([]goqu.Expression, 0)
	for _, jobSetId := range jobSetIds {
		filter := job_jobset.Like(jobSetId + "%")
		filters = append(filters, filter)
	}
	return filters
}

func createJobStateFilters(jobStates []string) []goqu.Expression {
	filters := make([]goqu.Expression, 0)
	for _, state := range jobStates {
		filter := goqu.And(FiltersForState[JobState(state)]...)
		filters = append(filters, filter)
	}
	return filters
}

func createJobOrdering(newestFirst bool) exp.OrderedExpression {
	if newestFirst {
		return job_jobId.Desc()
	}
	return job_jobId.Asc()
}

func (r *SQLJobRepository) queryJob(ctx context.Context, jobId string) ([]*JobRow, error) {
	ds := r.createJobDataset(jobId)

	jobRows := make([]*JobRow, 0)
	err := ds.Prepared(true).ScanStructsContext(ctx, &jobRows)
	if err != nil {
		return nil, err
	}

	return jobRows, nil
}

func (r *SQLJobRepository) createJobDataset(jobId string) *goqu.SelectDataset {
	ds := r.goquDb.
		From(jobTable).
		LeftJoin(jobRunTable, goqu.On(
			job_jobId.Eq(jobRun_jobId))).
		Select(job_jobId,
			job_queue,
			job_owner,
			job_jobset,
			job_priority,
			job_submitted,
			job_cancelled,
			job_job,
			jobRun_runId,
			jobRun_podNumber,
			jobRun_cluster,
			jobRun_node,
			jobRun_created,
			jobRun_started,
			jobRun_finished,
			jobRun_succeeded,
			jobRun_error).
		Where(job_jobId.Eq(jobId))

	return ds
}

func rowsToJobs(rows []*JobRow) []*lookout.JobInfo {
	result := make([]*lookout.JobInfo, 0)

	for i, row := range rows {
		if row.JobId.Valid &&
			(i == 0 || result[len(result)-1].Job.Id != row.JobId.String) {
			result = append(result, &lookout.JobInfo{
				Job:       makeJobFromRow(row),
				Cancelled: ParseNullTime(row.Cancelled),
				JobState:  "",
				Runs:      []*lookout.RunInfo{},
			})
		}

		if row.RunId.Valid {
			result[len(result)-1].Runs = append(result[len(result)-1].Runs, makeRunFromRow(row))
		}
	}

	for _, jobInfo := range result {
		determineJobState(jobInfo)
	}

	return result
}

func rowsToJob(rows []*JobRow) *lookout.JobInfo {
	jobs := rowsToJobs(rows)
	if len(jobs) != 1 {
		return nil
	}

	return jobs[0]
}

func makeJobFromRow(row *JobRow) *api.Job {
	if row == nil {
		return nil
	}
	return &api.Job{
		Id:       ParseNullString(row.JobId),
		JobSetId: ParseNullString(row.JobSet),
		Queue:    ParseNullString(row.Queue),
		Owner:    ParseNullString(row.Owner),
		Priority: ParseNullFloat(row.Priority),
		Created:  ParseNullTimeDefault(row.Submitted),
	}
}

func makeRunFromRow(row *JobRow) *lookout.RunInfo {
	if row == nil {
		return nil
	}
	return &lookout.RunInfo{
		K8SId:     ParseNullString(row.RunId),
		PodNumber: row.PodNUmber,
		Cluster:   ParseNullString(row.Cluster),
		Node:      ParseNullString(row.Node),
		Succeeded: ParseNullBool(row.Succeeded),
		Error:     ParseNullString(row.Error),
		Created:   ParseNullTime(row.Created), // Pod created (Pending)
		Started:   ParseNullTime(row.Started), // Pod Running
		Finished:  ParseNullTime(row.Finished),
	}
}

func determineRunState(runInfo *lookout.RunInfo) JobState {
	if runInfo.Finished != nil && runInfo.Succeeded {
		return JobSucceeded
	}
	if runInfo.Finished != nil && !runInfo.Succeeded {
		return JobFailed
	}
	if runInfo.Started != nil {
		return JobRunning
	}
	if runInfo.Created != nil {
		return JobPending
	}
	return JobQueued
}

func determineJobState(jobInfo *lookout.JobInfo) {
	podStates := map[int32]JobState{}
	for _, run := range jobInfo.Runs {
		// this code assumes that runs are ordered by start
		// and the only latest run for specific pod number is relevant
		state := determineRunState(run)
		run.RunState = string(state)
		podStates[run.PodNumber] = state
	}

	if jobInfo.Cancelled != nil {
		jobInfo.JobState = string(JobCancelled)
		return
	}
	if len(jobInfo.Runs) > 0 {

		stateCounts := map[JobState]int{}
		for _, state := range podStates {
			stateCounts[state]++
		}

		if stateCounts[JobFailed] > 0 {
			jobInfo.JobState = string(JobFailed)
			return
		}
		if stateCounts[JobPending] > 0 {
			jobInfo.JobState = string(JobPending)
			return
		}
		if stateCounts[JobRunning] > 0 {
			jobInfo.JobState = string(JobRunning)
			return
		}
		jobInfo.JobState = string(JobSucceeded)
		return
	}
	jobInfo.JobState = string(JobQueued)
}
