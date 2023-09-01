package repository

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/doug-martin/goqu/v9"
	"github.com/doug-martin/goqu/v9/exp"
	log "github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/util/duration"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/compress"
	"github.com/armadaproject/armada/internal/common/database"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/api/lookout"
)

func (r *SQLJobRepository) GetJobs(ctx *armadacontext.ArmadaContext, opts *lookout.GetJobsRequest) ([]*lookout.JobInfo, error) {
	if valid, jobState := validateJobStates(opts.JobStates); !valid {
		return nil, fmt.Errorf("unknown job state: %q", jobState)
	}

	rows, err := r.queryJobs(ctx, opts)
	if err != nil {
		return nil, err
	}

	result, err := r.rowsToJobs(rows)
	if err != nil {
		return nil, err
	}
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

func (r *SQLJobRepository) queryJobs(ctx *armadacontext.ArmadaContext, opts *lookout.GetJobsRequest) ([]*JobRow, error) {
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
		Select(job_jobId).
		Where(goqu.And(r.createWhereFilters(opts)...)).
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
			job_orig_job_spec,
			job_state,
			jobRun_runId,
			jobRun_podNumber,
			jobRun_cluster,
			jobRun_node,
			jobRun_created,
			jobRun_started,
			jobRun_finished,
			jobRun_preempted,
			jobRun_succeeded,
			jobRun_error).
		Where(job_jobId.In(subDs))

	return ds
}

func (r *SQLJobRepository) createWhereFilters(opts *lookout.GetJobsRequest) []goqu.Expression {
	var filters []goqu.Expression

	if opts.Queue != "" {
		filters = append(filters, GlobSearchOrExact(job_queue, opts.Queue))
	}

	if opts.JobId != "" {
		filters = append(filters, job_jobId.Eq(opts.JobId))
	}

	if opts.Owner != "" {
		filters = append(filters, GlobSearchOrExact(job_owner, opts.Owner))
	}

	nonEmptyJobSetIds := util.Filter(opts.JobSetIds, func(jobSet string) bool { return jobSet != "" })
	if len(nonEmptyJobSetIds) > 0 {
		filters = append(filters, goqu.Or(createJobSetFilters(opts.JobSetIds)...))
	}

	if len(opts.UserAnnotations) > 0 {
		filters = append(filters, r.createUserAnnotationsFilter(opts.UserAnnotations))
	}

	if len(opts.JobStates) > 0 {
		filters = append(filters, createJobStateFilter(toJobStates(opts.JobStates)))
	} else {
		filters = append(filters, createJobStateFilter(defaultQueryStates))
	}

	return filters
}

func (r *SQLJobRepository) createUserAnnotationsFilter(annotations map[string]string) goqu.Expression {
	var caseFilters []goqu.Expression
	var whenFilters []goqu.Expression

	for key, value := range annotations {
		caseFilters = append(caseFilters, goqu.And(
			annotation_key.Eq(key),
			StartsWith(annotation_value, value)))
		whenFilters = append(whenFilters, annotation_key.Eq(key))
	}

	subDs := r.goquDb.From(userAnnotationLookupTable).
		Select(
			annotation_jobId,
			goqu.SUM(
				goqu.Case().
					When(goqu.Or(caseFilters...), goqu.L("1")).
					Else(goqu.L("0"))).As("total_matches")).
		Where(goqu.Or(whenFilters...)).
		GroupBy(annotation_jobId).
		As("annotation_matches")

	return job_jobId.In(
		r.goquDb.From(subDs).
			Select(goqu.I("annotation_matches.job_id")).
			Where(goqu.I("annotation_matches.total_matches").Eq(len(annotations))))
}

func createJobSetFilters(jobSetIds []string) []goqu.Expression {
	var filters []goqu.Expression
	for _, jobSetId := range jobSetIds {
		filter := GlobSearchOrExact(job_jobset, jobSetId)
		filters = append(filters, filter)
	}
	return filters
}

func toJobStates(jobStates []string) []JobState {
	result := []JobState{}
	for _, state := range jobStates {
		result = append(result, JobState(state))
	}
	return result
}

func createJobStateFilter(jobStates []JobState) goqu.Expression {
	stateInts := make([]interface{}, len(jobStates))
	for i, state := range jobStates {
		stateInts[i] = JobStateToIntMap[state]
	}
	return job_state.In(stateInts...)
}

func createJobOrdering(newestFirst bool) exp.OrderedExpression {
	if newestFirst {
		return job_jobId.Desc()
	}
	return job_jobId.Asc()
}

func (r *SQLJobRepository) rowsToJobs(rows []*JobRow) ([]*lookout.JobInfo, error) {
	jobMap := make(map[string]*lookout.JobInfo)

	for _, row := range rows {
		if row.JobId.Valid {
			jobId := row.JobId.String
			if _, ok := jobMap[jobId]; !ok {
				var state, stateDuration string
				if row.State.Valid {
					state = string(IntToJobStateMap[int(row.State.Int64)])
				}

				job, jobJson, err := makeJobFromRow(row)
				if err != nil {
					return nil, err
				}

				if job != nil {
					jobMap[jobId] = &lookout.JobInfo{
						Job:              job,
						Cancelled:        database.ParseNullTime(row.Cancelled),
						JobState:         state,
						JobStateDuration: stateDuration,
						Runs:             []*lookout.RunInfo{},
						JobJson:          jobJson,
					}
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
		r.updateTimeInState(jobInfo)
	}

	return jobMapToSlice(jobMap), nil
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

func (r *SQLJobRepository) updateTimeInState(jobInfo *lookout.JobInfo) {
	var timeStamp *time.Time

	state := JobState(jobInfo.JobState)
	switch state {
	case JobSucceeded, JobFailed:
		timeStamp = findLatest(jobInfo.Runs, func(run *lookout.RunInfo) *time.Time { return run.Finished })
		if timeStamp == nil {
			log.Warnf("No finished timestamp found for job with id %s", jobInfo.Job.Id)
		}
	case JobRunning:
		timeStamp = findLatest(jobInfo.Runs, func(run *lookout.RunInfo) *time.Time { return run.Started })
		if timeStamp == nil {
			log.Warnf("No running timestamp found for job with id %s", jobInfo.Job.Id)
		}
	case JobPending:
		timeStamp = findLatest(jobInfo.Runs, func(run *lookout.RunInfo) *time.Time { return run.Created })
		if timeStamp == nil {
			log.Warnf("No pending timestamp found for job with id %s", jobInfo.Job.Id)
		}
	case JobCancelled:
		timeStamp = jobInfo.Cancelled
		if timeStamp == nil {
			log.Warnf("No cancelled timestamp found for job with id %s", jobInfo.Job.Id)
		}
	case JobQueued, JobDuplicate:
		timeStamp = &jobInfo.Job.Created
	}

	if timeStamp != nil {
		jobInfo.JobStateDuration = duration.ShortHumanDuration(r.clock.Now().Sub(*timeStamp))
	}
}

// Find latest non-nil value in the runs, given some accessor
// Note that this assumes that the runs are ordered from earliest to latest
func findLatest[T any](runs []*lookout.RunInfo, accessor func(run *lookout.RunInfo) *T) *T {
	for i := len(runs) - 1; i >= 0; i-- {
		val := accessor(runs[i])
		if val != nil {
			return val
		}
	}
	return nil
}

// Returns api Job object, Job JSON, and any fatal error
func makeJobFromRow(row *JobRow) (*api.Job, string, error) {
	if row == nil {
		return nil, "", nil
	}

	var annotations map[string]string
	jobJson := ""
	unmarshalledJob, err := unmarshalJob(row.OrigJobSpec)
	if err != nil {
		log.Errorf("Failed to unmarshal job with job id %s: %v", database.ParseNullStringDefault(row.JobId), err)
	} else {
		annotations = unmarshalledJob.Annotations
		jobJson = jobToJson(unmarshalledJob)
	}

	job := &api.Job{
		Id:          database.ParseNullStringDefault(row.JobId),
		JobSetId:    database.ParseNullStringDefault(row.JobSet),
		Queue:       database.ParseNullStringDefault(row.Queue),
		Owner:       database.ParseNullStringDefault(row.Owner),
		Priority:    ParseNullFloat(row.Priority),
		Created:     ParseNullTimeDefault(row.Submitted),
		Annotations: annotations,
	}

	return job, jobJson, nil
}

// Convert *api.Job to JSON - returns empty string if it fails and logs the error
func jobToJson(unmarshalledJob *api.Job) string {
	jobJson, err := json.Marshal(unmarshalledJob)
	if err != nil {
		log.Errorf("Failed to convert *api.Job to JSON for job id %s: %v", unmarshalledJob.Id, err)
		return ""
	}
	return string(jobJson)
}

func unmarshalJob(origJobSpec []byte) (*api.Job, error) {
	var unmarshalledJob api.Job
	if len(origJobSpec) == 0 {
		return nil, errors.New("empty job spec provided")
	}

	decompressor := compress.NewZlibDecompressor()
	jobProto, err := decompressor.Decompress(origJobSpec)
	if err != nil {
		// possibly not compressed, so
		// try to unmarshal input directly also
		jobProto = origJobSpec
	}
	err = unmarshalledJob.Unmarshal(jobProto)
	if err != nil {
		return nil, err
	}
	return &unmarshalledJob, nil
}

func makeRunFromRow(row *JobRow) *lookout.RunInfo {
	if row == nil {
		return nil
	}
	return &lookout.RunInfo{
		K8SId:     database.ParseNullStringDefault(row.RunId),
		PodNumber: int32(ParseNullInt(row.PodNumber)),
		Cluster:   database.ParseNullStringDefault(row.Cluster),
		Node:      database.ParseNullStringDefault(row.Node),
		Succeeded: ParseNullBool(row.Succeeded),
		Error:     database.ParseNullStringDefault(row.Error),
		Created:   database.ParseNullTime(row.Created), // Pod created (Pending)
		Started:   database.ParseNullTime(row.Started), // Pod Running
		Finished:  database.ParseNullTime(row.Finished),
		Preempted: database.ParseNullTime(row.Preempted),
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
