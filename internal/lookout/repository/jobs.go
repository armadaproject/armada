package repository

import (
	"context"
	"fmt"
	"sort"

	"github.com/doug-martin/goqu/v9"
	"github.com/doug-martin/goqu/v9/exp"

	"github.com/G-Research/armada/pkg/api"
	"github.com/G-Research/armada/pkg/api/lookout"
)

func (r *SQLJobRepository) GetJobs(ctx context.Context, opts *lookout.GetJobsRequest) ([]*lookout.JobInfo, error) {
	if valid, jobState := validateJobStates(opts.JobStates); !valid {
		return nil, fmt.Errorf("unknown job state: %q", jobState)
	}

	rows, err := r.queryJobs(ctx, opts)
	if err != nil {
		return nil, err
	}

	result := rowsToJobs(rows)
	sortJobsByJobId(result, opts.NewestFirst)

	return result, nil
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

func (r *SQLJobRepository) queryJobs(ctx context.Context, opts *lookout.GetJobsRequest) ([]*JobRow, error) {
	ds := r.createJobsDataset(opts)

	jobsInQueueRows := make([]*JobRow, 0)
	err := ds.Prepared(true).ScanStructsContext(ctx, &jobsInQueueRows)
	if err != nil {
		return nil, err
	}

	return jobsInQueueRows, nil
}

func (r *SQLJobRepository) createJobsDataset(opts *lookout.GetJobsRequest) *goqu.SelectDataset {
	subDs := r.goquDb.
		From(jobTable).
		LeftJoin(jobRunTable, goqu.On(
			job_jobId.Eq(jobRun_jobId))).
		Select(job_jobId).
		Where(goqu.And(createWhereFilters(opts)...)).
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
		Where(job_jobId.In(subDs))

	return ds
}

func createWhereFilters(opts *lookout.GetJobsRequest) []goqu.Expression {
	filters := make([]goqu.Expression, 0)

	if opts.JobId != "" && opts.Queue == "" {
		filters = append(filters, job_jobId.Eq(opts.JobId))
	} else if opts.Queue != "" && opts.JobId == "" {
		filters = append(filters, job_queue.Eq(opts.Queue))
	} else {
		filters = append(filters, job_jobId.Eq(opts.JobId))
		filters = append(filters, job_queue.Eq(opts.Queue))
	}

	filters = append(filters, goqu.Or(createJobSetFilters(opts.JobSetIds)...))

	return filters
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

func rowsToJobs(rows []*JobRow) []*lookout.JobInfo {
	jobMap := make(map[string]*lookout.JobInfo)

	for _, row := range rows {
		if row.JobId.Valid {
			jobId := row.JobId.String
			if _, ok := jobMap[jobId]; !ok {
				jobMap[jobId] = &lookout.JobInfo{
					Job:       makeJobFromRow(row),
					Cancelled: ParseNullTime(row.Cancelled),
					JobState:  "",
					Runs:      []*lookout.RunInfo{},
				}
			}

			if row.RunId.Valid {
				if jobInfo, ok := jobMap[jobId]; ok {
					jobInfo.Runs = append(jobInfo.Runs, makeRunFromRow(row))
				}
			}
		}
	}

	for _, jobInfo := range jobMap {
		determineJobState(jobInfo)
	}

	return jobMapToSlice(jobMap)
}

func jobMapToSlice(jobMap map[string]*lookout.JobInfo) []*lookout.JobInfo {
	result := make([]*lookout.JobInfo, 0)

	for _, jobInfo := range jobMap {
		result = append(result, jobInfo)
	}

	return result
}

func sortJobsByJobId(jobInfos []*lookout.JobInfo, descending bool) {
	sort.SliceStable(jobInfos, func(i, j int) bool {
		if descending {
			return jobInfos[i].Job.Id > jobInfos[j].Job.Id
		} else {
			return jobInfos[i].Job.Id < jobInfos[j].Job.Id
		}
	})
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
