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
		LeftJoin(annotationTable, goqu.On(job_jobId.Eq(annotation_jobId))).
		Select(job_jobId).
		Where(goqu.And(createWhereFilters(opts)...)).
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
			job_state,
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
	var filters []goqu.Expression

	if opts.Queue != "" {
		filters = append(filters, StartsWith(job_queue, opts.Queue))
	}

	if opts.JobId != "" {
		filters = append(filters, StartsWith(job_jobId, opts.JobId))
	}

	if opts.Owner != "" {
		filters = append(filters, StartsWith(job_owner, opts.Owner))
	}

	filters = append(filters, goqu.Or(createJobSetFilters(opts.JobSetIds)...))

	filters = append(filters, goqu.Or(createAnnotationFilters(opts.Annotations)...))

	if len(opts.JobStates) > 0 {
		filters = append(filters, createJobStateFilter(opts.JobStates))
	}

	return filters
}

func createJobSetFilters(jobSetIds []string) []goqu.Expression {
	var filters []goqu.Expression
	for _, jobSetId := range jobSetIds {
		filter := StartsWith(job_jobset, jobSetId)
		filters = append(filters, filter)
	}
	return filters
}

func createAnnotationFilters(annotations map[string]string) []goqu.Expression {
	var filters []goqu.Expression
	for key, value := range annotations {
		filter := goqu.And(
			annotation_key.Eq(key),
			StartsWith(annotation_value, value))
		filters = append(filters, filter)
	}
	return filters
}

func createJobStateFilter(jobStates []string) goqu.Expression {
	stateInts := make([]interface{}, len(jobStates))
	for i, state := range jobStates {
		stateInts[i] = JobStateToIntMap[JobState(state)]
	}
	return job_state.In(stateInts...)
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
				state := ""
				if row.State.Valid {
					state = string(IntToJobStateMap[int(row.State.Int64)])
				}
				jobMap[jobId] = &lookout.JobInfo{
					Job:       makeJobFromRow(row),
					Cancelled: ParseNullTime(row.Cancelled),
					JobState:  state,
					Runs:      []*lookout.RunInfo{},
					JobJson:   ParseNullString(row.JobJson),
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
		updateRunStates(jobInfo)
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
		PodNumber: int32(ParseNullInt(row.PodNumber)),
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

func updateRunStates(jobInfo *lookout.JobInfo) {
	for _, run := range jobInfo.Runs {
		// this code assumes that runs are ordered by start
		// and the only latest run for specific pod number is relevant
		state := determineRunState(run)
		run.RunState = string(state)
	}
}
