package db

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"

	"github.com/armadaproject/armada/internal/lookout/model"
	"github.com/armadaproject/armada/pkg/api"
)

type jobRecord struct {
	JobID                     string
	Queue                     string
	Owner                     string
	JobSet                    string
	Namespace                 *string
	Cpu                       int64
	Memory                    int64
	EphemeralStorage          int64
	Gpu                       int64
	Priority                  int64
	PriorityClass             *string
	Submitted                 time.Time
	Cancelled                 *time.Time
	State                     int16
	LastTransitionTime        time.Time
	LastTransitionTimeSeconds int64
	Duplicate                 bool
	LatestRunID               *string
	CancelReason              *string
	CancelUser                *string
}

type jobRunRecord struct {
	RunID       string
	JobID       string
	Cluster     string
	Node        *string
	Pool        *string
	Leased      *time.Time
	Pending     *time.Time
	Started     *time.Time
	Finished    *time.Time
	JobRunState int16
	Error       []byte
	Debug       []byte
	ExitCode    *int32
}

type annotationRecord struct {
	JobID  string
	Key    string
	Value  string
	Queue  string
	JobSet string
}

type jobErrorRecord struct {
	JobID string
	Error []byte
}

type jobSpecRecord struct {
	JobID   string
	JobSpec *api.Job
}

type MemoryDatabase struct {
	mu          sync.RWMutex
	jobs        map[string]*jobRecord
	jobRuns     map[string]*jobRunRecord
	annotations map[string]map[string]*annotationRecord // job_id -> key -> record
	jobErrors   map[string]*jobErrorRecord
	jobSpecs    map[string]*jobSpecRecord
}

func NewMemoryDatabase() *MemoryDatabase {
	return &MemoryDatabase{
		jobs:        make(map[string]*jobRecord),
		jobRuns:     make(map[string]*jobRunRecord),
		annotations: make(map[string]map[string]*annotationRecord),
		jobErrors:   make(map[string]*jobErrorRecord),
		jobSpecs:    make(map[string]*jobSpecRecord),
	}
}

func (m *MemoryDatabase) InitialiseSchema(_ context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.jobs = make(map[string]*jobRecord)
	m.jobRuns = make(map[string]*jobRunRecord)
	m.annotations = make(map[string]map[string]*annotationRecord)
	m.jobErrors = make(map[string]*jobErrorRecord)
	m.jobSpecs = make(map[string]*jobSpecRecord)
	return nil
}

func (m *MemoryDatabase) ExecuteIngestionQueryBatch(_ context.Context, queries []IngestionQuery) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, query := range queries {
		if err := m.executeQuery(query); err != nil {
			return err
		}
	}
	return nil
}

func (m *MemoryDatabase) executeQuery(query IngestionQuery) error {
	switch q := query.(type) {
	case InsertJob:
		return m.insertJob(q)
	case InsertJobSpec:
		return m.insertJobSpec(q)
	case UpdateJobPriority:
		return m.updateJobPriority(q)
	case SetJobCancelled:
		return m.setJobCancelled(q)
	case SetJobSucceeded:
		return m.setJobSucceeded(q)
	case InsertJobError:
		return m.insertJobError(q)
	case SetJobPreempted:
		return m.setJobPreempted(q)
	case SetJobRejected:
		return m.setJobRejected(q)
	case SetJobErrored:
		return m.setJobErrored(q)
	case SetJobRunning:
		return m.setJobRunning(q)
	case SetJobRunStarted:
		return m.setJobRunStarted(q)
	case SetJobPending:
		return m.setJobPending(q)
	case SetJobRunPending:
		return m.setJobRunPending(q)
	case SetJobRunCancelled:
		return m.setJobRunCancelled(q)
	case SetJobRunFailed:
		return m.setJobRunFailed(q)
	case SetJobRunSucceeded:
		return m.setJobRunSucceeded(q)
	case SetJobRunPreempted:
		return m.setJobRunPreempted(q)
	case SetJobLeased:
		return m.setJobLeased(q)
	case InsertJobRun:
		return m.insertJobRun(q)
	default:
		return fmt.Errorf("unknown ingestion query type: %T", query)
	}
}

const (
	stateQueued    int16 = 1
	stateLeased    int16 = 2
	statePending   int16 = 3
	stateRunning   int16 = 4
	stateSucceeded int16 = 5
	stateFailed    int16 = 6
	stateCancelled int16 = 7
	statePreempted int16 = 8
	stateRejected  int16 = 9

	runStatePending   int16 = 1
	runStateRunning   int16 = 2
	runStateSucceeded int16 = 3
	runStateFailed    int16 = 4
	runStateCancelled int16 = 5
	runStatePreempted int16 = 6
	runStateLeased    int16 = 7
)

func (m *MemoryDatabase) insertJob(q InsertJob) error {
	job := q.Job
	var namespace *string
	if job.Namespace != "" {
		namespace = &job.Namespace
	}
	var priorityClass *string
	if job.PriorityClass != "" {
		priorityClass = &job.PriorityClass
	}

	m.jobs[job.JobID] = &jobRecord{
		JobID:                     job.JobID,
		Queue:                     job.Queue,
		Owner:                     job.Owner,
		JobSet:                    job.JobSet,
		Namespace:                 namespace,
		Cpu:                       job.Cpu,
		Memory:                    job.Memory,
		EphemeralStorage:          job.EphemeralStorage,
		Gpu:                       job.Gpu,
		Priority:                  job.Priority,
		PriorityClass:             priorityClass,
		Submitted:                 job.Submitted,
		State:                     stateQueued,
		LastTransitionTime:        job.Submitted,
		LastTransitionTimeSeconds: job.Submitted.Unix(),
	}

	if len(job.Annotations) > 0 {
		m.annotations[job.JobID] = make(map[string]*annotationRecord)
		for k, v := range job.Annotations {
			m.annotations[job.JobID][k] = &annotationRecord{
				JobID:  job.JobID,
				Key:    k,
				Value:  v,
				Queue:  job.Queue,
				JobSet: job.JobSet,
			}
		}
	}
	return nil
}

func (m *MemoryDatabase) insertJobSpec(q InsertJobSpec) error {
	var job api.Job
	if err := proto.Unmarshal([]byte(q.JobSpec), &job); err != nil {
		return fmt.Errorf("unmarshalling job spec for %s: %w", q.JobID, err)
	}
	m.jobSpecs[q.JobID] = &jobSpecRecord{
		JobID:   q.JobID,
		JobSpec: &job,
	}
	return nil
}

func (m *MemoryDatabase) updateJobPriority(q UpdateJobPriority) error {
	if job, ok := m.jobs[q.JobID]; ok {
		job.Priority = q.Priority
	}
	return nil
}

func (m *MemoryDatabase) setJobCancelled(q SetJobCancelled) error {
	if job, ok := m.jobs[q.JobID]; ok {
		job.State = stateCancelled
		job.Cancelled = &q.Time
		job.LastTransitionTime = q.Time
		job.LastTransitionTimeSeconds = q.Time.Unix()
		if q.CancelReason != "" {
			job.CancelReason = &q.CancelReason
		}
		if q.CancelUser != "" {
			job.CancelUser = &q.CancelUser
		}
	}
	return nil
}

func (m *MemoryDatabase) setJobSucceeded(q SetJobSucceeded) error {
	if job, ok := m.jobs[q.JobID]; ok {
		job.State = stateSucceeded
		job.LastTransitionTime = q.Time
		job.LastTransitionTimeSeconds = q.Time.Unix()
	}
	return nil
}

func (m *MemoryDatabase) insertJobError(q InsertJobError) error {
	m.jobErrors[q.JobID] = &jobErrorRecord{
		JobID: q.JobID,
		Error: q.Error,
	}
	return nil
}

func (m *MemoryDatabase) setJobPreempted(q SetJobPreempted) error {
	if job, ok := m.jobs[q.JobID]; ok {
		job.State = statePreempted
		job.LastTransitionTime = q.Time
		job.LastTransitionTimeSeconds = q.Time.Unix()
	}
	return nil
}

func (m *MemoryDatabase) setJobRejected(q SetJobRejected) error {
	if job, ok := m.jobs[q.JobID]; ok {
		job.State = stateRejected
		job.LastTransitionTime = q.Time
		job.LastTransitionTimeSeconds = q.Time.Unix()
	}
	return nil
}

func (m *MemoryDatabase) setJobErrored(q SetJobErrored) error {
	if job, ok := m.jobs[q.JobID]; ok {
		job.State = stateFailed
		job.LastTransitionTime = q.Time
		job.LastTransitionTimeSeconds = q.Time.Unix()
	}
	return nil
}

func (m *MemoryDatabase) setJobRunning(q SetJobRunning) error {
	if job, ok := m.jobs[q.JobID]; ok {
		job.State = stateRunning
		job.LastTransitionTime = q.Time
		job.LastTransitionTimeSeconds = q.Time.Unix()
		job.LatestRunID = &q.LatestRunID
	}
	return nil
}

func (m *MemoryDatabase) setJobRunStarted(q SetJobRunStarted) error {
	if run, ok := m.jobRuns[q.JobRunID]; ok {
		run.Started = &q.Time
		run.JobRunState = runStateRunning
		if q.Node != "" {
			run.Node = &q.Node
		}
	}
	return nil
}

func (m *MemoryDatabase) setJobPending(q SetJobPending) error {
	if job, ok := m.jobs[q.JobID]; ok {
		job.State = statePending
		job.LastTransitionTime = q.Time
		job.LastTransitionTimeSeconds = q.Time.Unix()
		job.LatestRunID = &q.RunID
	}
	return nil
}

func (m *MemoryDatabase) setJobRunPending(q SetJobRunPending) error {
	if run, ok := m.jobRuns[q.JobRunID]; ok {
		run.Pending = &q.Time
		run.JobRunState = runStatePending
	}
	return nil
}

func (m *MemoryDatabase) setJobRunCancelled(q SetJobRunCancelled) error {
	if run, ok := m.jobRuns[q.JobRunID]; ok {
		run.Finished = &q.Time
		run.JobRunState = runStateCancelled
	}
	return nil
}

func (m *MemoryDatabase) setJobRunFailed(q SetJobRunFailed) error {
	if run, ok := m.jobRuns[q.JobRunID]; ok {
		run.Finished = &q.Time
		run.JobRunState = runStateFailed
		run.Error = q.Error
		run.Debug = q.Debug
		run.ExitCode = &q.ExitCode
	}
	return nil
}

func (m *MemoryDatabase) setJobRunSucceeded(q SetJobRunSucceeded) error {
	if run, ok := m.jobRuns[q.JobRunID]; ok {
		run.Finished = &q.Time
		run.JobRunState = runStateSucceeded
		run.ExitCode = &q.ExitCode
	}
	return nil
}

func (m *MemoryDatabase) setJobRunPreempted(q SetJobRunPreempted) error {
	if run, ok := m.jobRuns[q.JobRunID]; ok {
		run.Finished = &q.Time
		run.JobRunState = runStatePreempted
		run.Error = q.Error
	}
	return nil
}

func (m *MemoryDatabase) setJobLeased(q SetJobLeased) error {
	if job, ok := m.jobs[q.JobID]; ok {
		job.State = stateLeased
		job.LastTransitionTime = q.Time
		job.LastTransitionTimeSeconds = q.Time.Unix()
		job.LatestRunID = &q.RunID
	}
	return nil
}

func (m *MemoryDatabase) insertJobRun(q InsertJobRun) error {
	var node *string
	if q.Node != "" {
		node = &q.Node
	}
	var pool *string
	if q.Pool != "" {
		pool = &q.Pool
	}
	m.jobRuns[q.JobRunID] = &jobRunRecord{
		RunID:       q.JobRunID,
		JobID:       q.JobID,
		Cluster:     q.Cluster,
		Node:        node,
		Pool:        pool,
		Leased:      &q.Time,
		JobRunState: runStateLeased,
	}
	return nil
}

func (m *MemoryDatabase) GetJobRunDebugMessage(_ context.Context, jobRunID string) (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if run, ok := m.jobRuns[jobRunID]; ok && run.Debug != nil {
		return string(run.Debug), nil
	}
	return "", fmt.Errorf("job run %s not found or has no debug message", jobRunID)
}

func (m *MemoryDatabase) GetJobRunError(_ context.Context, jobRunID string) (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if run, ok := m.jobRuns[jobRunID]; ok && run.Error != nil {
		return string(run.Error), nil
	}
	return "", fmt.Errorf("job run %s not found or has no error", jobRunID)
}

func (m *MemoryDatabase) GetJobSpec(_ context.Context, jobID string) (*api.Job, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	spec, ok := m.jobSpecs[jobID]
	if !ok {
		return nil, fmt.Errorf("job spec for %s not found", jobID)
	}
	return spec.JobSpec, nil
}

type queueJobSetPair struct {
	queue  string
	jobSet string
}

func isActiveState(state int16) bool {
	return state == stateQueued || state == stateLeased || state == statePending || state == stateRunning
}

func (m *MemoryDatabase) computeActiveJobSets() map[queueJobSetPair]struct{} {
	activeJobSetPairs := make(map[queueJobSetPair]struct{})
	for _, job := range m.jobs {
		if isActiveState(job.State) {
			pair := queueJobSetPair{queue: job.Queue, jobSet: job.JobSet}
			activeJobSetPairs[pair] = struct{}{}
		}
	}
	return activeJobSetPairs
}

func (m *MemoryDatabase) GetJobs(_ *context.Context, filters []*model.Filter, activeJobSets bool, order *model.Order, skip int, take int) ([]*model.Job, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var activeJobSetPairs map[queueJobSetPair]struct{}
	if activeJobSets {
		activeJobSetPairs = m.computeActiveJobSets()
	}

	var jobs []*model.Job
	for _, job := range m.jobs {
		if activeJobSets {
			pair := queueJobSetPair{queue: job.Queue, jobSet: job.JobSet}
			if _, ok := activeJobSetPairs[pair]; !ok {
				continue
			}
		}
		if m.matchesFilters(job, filters) {
			jobs = append(jobs, m.jobRecordToModel(job))
		}
	}

	sortJobs(jobs, order)
	return paginate(jobs, skip, take), nil
}

func (m *MemoryDatabase) matchesFilters(job *jobRecord, filters []*model.Filter) bool {
	for _, filter := range filters {
		if !m.matchesFilter(job, filter) {
			return false
		}
	}
	return true
}

func (m *MemoryDatabase) matchesFilter(job *jobRecord, filter *model.Filter) bool {
	if filter.IsAnnotation {
		return m.matchesAnnotationFilter(job.JobID, filter)
	}

	switch filter.Field {
	case "jobId":
		return matchString(job.JobID, filter)
	case "queue":
		return matchString(job.Queue, filter)
	case "jobSet":
		return matchString(job.JobSet, filter)
	case "owner":
		return matchString(job.Owner, filter)
	case "namespace":
		if job.Namespace == nil {
			return false
		}
		return matchString(*job.Namespace, filter)
	case "state":
		return matchState(job.State, filter)
	case "cpu":
		return matchInt64(job.Cpu, filter)
	case "memory":
		return matchInt64(job.Memory, filter)
	case "ephemeralStorage":
		return matchInt64(job.EphemeralStorage, filter)
	case "gpu":
		return matchInt64(job.Gpu, filter)
	case "priority":
		return matchInt64(job.Priority, filter)
	case "submitted":
		return matchTime(job.Submitted, filter)
	case "lastTransitionTime":
		return matchTime(job.LastTransitionTime, filter)
	case "priorityClass":
		if job.PriorityClass == nil {
			return false
		}
		return matchString(*job.PriorityClass, filter)
	case "cluster":
		return m.matchesRunFilter(job.LatestRunID, filter)
	case "node":
		return m.matchesRunFilter(job.LatestRunID, filter)
	default:
		return false
	}
}

func (m *MemoryDatabase) matchesAnnotationFilter(jobID string, filter *model.Filter) bool {
	annotations, ok := m.annotations[jobID]
	if !ok {
		return filter.Match == model.MatchExists && false
	}

	if filter.Match == model.MatchExists {
		_, exists := annotations[filter.Field]
		return exists
	}

	annotation, ok := annotations[filter.Field]
	if !ok {
		return false
	}
	return matchString(annotation.Value, filter)
}

func (m *MemoryDatabase) matchesRunFilter(latestRunID *string, filter *model.Filter) bool {
	if latestRunID == nil {
		return false
	}
	run, ok := m.jobRuns[*latestRunID]
	if !ok {
		return false
	}

	switch filter.Field {
	case "cluster":
		return matchString(run.Cluster, filter)
	case "node":
		if run.Node == nil {
			return false
		}
		return matchString(*run.Node, filter)
	default:
		return false
	}
}

func matchString(value string, filter *model.Filter) bool {
	filterValue := fmt.Sprintf("%v", filter.Value)
	switch filter.Match {
	case model.MatchExact:
		return value == filterValue
	case model.MatchStartsWith:
		return strings.HasPrefix(value, filterValue)
	case model.MatchContains:
		return strings.Contains(value, filterValue)
	case model.MatchAnyOf:
		return matchAnyOf(value, filter.Value)
	default:
		return false
	}
}

func matchAnyOf(value string, filterValue any) bool {
	switch v := filterValue.(type) {
	case []string:
		return slices.Contains(v, value)
	case []any:
		for _, item := range v {
			if value == fmt.Sprintf("%v", item) {
				return true
			}
		}
	}
	return false
}

func matchState(state int16, filter *model.Filter) bool {
	ordinal := int(state)
	switch filter.Match {
	case model.MatchExact:
		filterOrdinal, err := stateStringToOrdinal(filter.Value)
		if err != nil {
			return false
		}
		return ordinal == filterOrdinal
	case model.MatchAnyOf:
		return matchStateAnyOf(ordinal, filter.Value)
	default:
		return false
	}
}

func matchStateAnyOf(ordinal int, filterValue any) bool {
	switch v := filterValue.(type) {
	case []string:
		for _, s := range v {
			filterOrdinal, err := stateStringToOrdinal(s)
			if err != nil {
				continue
			}
			if ordinal == filterOrdinal {
				return true
			}
		}
	case []any:
		for _, item := range v {
			filterOrdinal, err := stateStringToOrdinal(item)
			if err != nil {
				continue
			}
			if ordinal == filterOrdinal {
				return true
			}
		}
	}
	return false
}

func stateStringToOrdinal(value any) (int, error) {
	stateStr := fmt.Sprintf("%v", value)
	switch stateStr {
	case "QUEUED":
		return int(stateQueued), nil
	case "LEASED":
		return int(stateLeased), nil
	case "PENDING":
		return int(statePending), nil
	case "RUNNING":
		return int(stateRunning), nil
	case "SUCCEEDED":
		return int(stateSucceeded), nil
	case "FAILED":
		return int(stateFailed), nil
	case "CANCELLED":
		return int(stateCancelled), nil
	case "PREEMPTED":
		return int(statePreempted), nil
	case "REJECTED":
		return int(stateRejected), nil
	default:
		return -1, fmt.Errorf("unknown state: %s", stateStr)
	}
}

func matchInt64(value int64, filter *model.Filter) bool {
	filterValue, ok := toInt64(filter.Value)
	if !ok {
		return false
	}
	switch filter.Match {
	case model.MatchExact:
		return value == filterValue
	case model.MatchGreaterThan:
		return value > filterValue
	case model.MatchLessThan:
		return value < filterValue
	case model.MatchGreaterThanOrEqualTo:
		return value >= filterValue
	case model.MatchLessThanOrEqualTo:
		return value <= filterValue
	default:
		return false
	}
}

func toInt64(value any) (int64, bool) {
	switch v := value.(type) {
	case int:
		return int64(v), true
	case int32:
		return int64(v), true
	case int64:
		return v, true
	case float64:
		return int64(v), true
	default:
		return 0, false
	}
}

func matchTime(value time.Time, filter *model.Filter) bool {
	filterTime, ok := toTime(filter.Value)
	if !ok {
		return false
	}
	switch filter.Match {
	case model.MatchExact:
		return value.Equal(filterTime)
	case model.MatchGreaterThan:
		return value.After(filterTime)
	case model.MatchLessThan:
		return value.Before(filterTime)
	case model.MatchGreaterThanOrEqualTo:
		return value.After(filterTime) || value.Equal(filterTime)
	case model.MatchLessThanOrEqualTo:
		return value.Before(filterTime) || value.Equal(filterTime)
	default:
		return false
	}
}

func toTime(value any) (time.Time, bool) {
	switch v := value.(type) {
	case time.Time:
		return v, true
	case string:
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			return time.Time{}, false
		}
		return t, true
	default:
		return time.Time{}, false
	}
}

func sortJobs(jobs []*model.Job, order *model.Order) {
	if order == nil || order.Field == "" {
		return
	}

	sort.Slice(jobs, func(i, j int) bool {
		cmp := compareJobsByField(jobs[i], jobs[j], order.Field)
		if order.Direction == model.DirectionDesc {
			return cmp > 0
		}
		return cmp < 0
	})
}

func compareJobsByField(a, b *model.Job, field string) int {
	switch field {
	case "jobId":
		return strings.Compare(a.JobId, b.JobId)
	case "queue":
		return strings.Compare(a.Queue, b.Queue)
	case "jobSet":
		return strings.Compare(a.JobSet, b.JobSet)
	case "submitted":
		return compareTime(a.Submitted, b.Submitted)
	case "lastTransitionTime":
		return compareTime(a.LastTransitionTime, b.LastTransitionTime)
	case "state":
		return strings.Compare(a.State, b.State)
	default:
		return 0
	}
}

func compareTime(a, b time.Time) int {
	if a.Before(b) {
		return -1
	}
	if a.After(b) {
		return 1
	}
	return 0
}

func paginate(jobs []*model.Job, skip, take int) []*model.Job {
	if skip >= len(jobs) {
		return nil
	}
	jobs = jobs[skip:]
	if take > 0 && take < len(jobs) {
		jobs = jobs[:take]
	}
	return jobs
}

func (m *MemoryDatabase) jobRecordToModel(job *jobRecord) *model.Job {
	result := &model.Job{
		Annotations:        make(map[string]string),
		Cancelled:          job.Cancelled,
		Cpu:                job.Cpu,
		Duplicate:          job.Duplicate,
		EphemeralStorage:   job.EphemeralStorage,
		Gpu:                job.Gpu,
		JobId:              job.JobID,
		JobSet:             job.JobSet,
		LastActiveRunId:    job.LatestRunID,
		LastTransitionTime: job.LastTransitionTime,
		Memory:             job.Memory,
		Owner:              job.Owner,
		Namespace:          job.Namespace,
		Priority:           job.Priority,
		PriorityClass:      job.PriorityClass,
		Queue:              job.Queue,
		Runs:               make([]*model.Run, 0),
		State:              stateOrdinalToString(job.State),
		Submitted:          job.Submitted,
		CancelReason:       job.CancelReason,
		CancelUser:         job.CancelUser,
	}

	if annotations, ok := m.annotations[job.JobID]; ok {
		for k, v := range annotations {
			result.Annotations[k] = v.Value
		}
	}

	result.Runs = m.getRunsForJob(job.JobID)

	if len(result.Runs) > 0 {
		lastRun := result.Runs[len(result.Runs)-1]
		result.Node = lastRun.Node
		result.Cluster = lastRun.Cluster
		result.ExitCode = lastRun.ExitCode
		result.RuntimeSeconds = calculateRuntime(lastRun.Started, lastRun.Finished)
	}

	return result
}

func stateOrdinalToString(state int16) string {
	switch state {
	case stateQueued:
		return "QUEUED"
	case stateLeased:
		return "LEASED"
	case statePending:
		return "PENDING"
	case stateRunning:
		return "RUNNING"
	case stateSucceeded:
		return "SUCCEEDED"
	case stateFailed:
		return "FAILED"
	case stateCancelled:
		return "CANCELLED"
	case statePreempted:
		return "PREEMPTED"
	case stateRejected:
		return "REJECTED"
	default:
		return "UNKNOWN"
	}
}

func (m *MemoryDatabase) getRunsForJob(jobID string) []*model.Run {
	var runs []*model.Run
	for _, run := range m.jobRuns {
		if run.JobID == jobID {
			runs = append(runs, &model.Run{
				Cluster:     run.Cluster,
				ExitCode:    run.ExitCode,
				Finished:    model.NewPostgreSQLTime(run.Finished),
				JobRunState: int(run.JobRunState),
				Node:        run.Node,
				Leased:      model.NewPostgreSQLTime(run.Leased),
				Pending:     model.NewPostgreSQLTime(run.Pending),
				RunId:       run.RunID,
				Started:     model.NewPostgreSQLTime(run.Started),
			})
		}
	}
	sort.Slice(runs, func(i, j int) bool {
		iTime := getRunSortTime(runs[i])
		jTime := getRunSortTime(runs[j])
		return iTime.Before(jTime)
	})
	return runs
}

func getRunSortTime(run *model.Run) time.Time {
	if run.Leased != nil {
		return run.Leased.Time
	}
	if run.Pending != nil {
		return run.Pending.Time
	}
	return time.Time{}
}

func calculateRuntime(started, finished *model.PostgreSQLTime) int32 {
	if started == nil {
		return 0
	}
	endTime := time.Now()
	if finished != nil {
		endTime = finished.Time
	}
	return int32(endTime.Sub(started.Time).Seconds())
}

func (m *MemoryDatabase) GetJobGroups(_ *context.Context, filters []*model.Filter, order *model.Order, groupedField *model.GroupedField, aggregates []string, skip int, take int) ([]*model.JobGroup, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	groups := make(map[string]*jobGroupAccumulator)

	for _, job := range m.jobs {
		if !m.matchesFilters(job, filters) {
			continue
		}

		groupKey := m.getGroupKey(job, groupedField.Field)
		if groupKey == "" && groupedField.Field != "state" {
			continue
		}

		acc, ok := groups[groupKey]
		if !ok {
			acc = newJobGroupAccumulator(groupKey)
			groups[groupKey] = acc
		}
		acc.addJob(job)
	}

	var result []*model.JobGroup
	for _, acc := range groups {
		result = append(result, acc.toJobGroup(aggregates))
	}

	sortGroups(result, order)
	return paginateGroups(result, skip, take), nil
}

type jobGroupAccumulator struct {
	name                  string
	count                 int64
	stateCounts           map[string]int
	minSubmitted          time.Time
	maxLastTransitionTime time.Time
	minLastTransitionTime time.Time
	sumLastTransitionTime int64
	hasSubmitted          bool
	hasLastTransitionTime bool
}

func newJobGroupAccumulator(name string) *jobGroupAccumulator {
	return &jobGroupAccumulator{
		name:        name,
		stateCounts: make(map[string]int),
	}
}

func (a *jobGroupAccumulator) addJob(job *jobRecord) {
	a.count++

	stateStr := stateOrdinalToString(job.State)
	a.stateCounts[stateStr]++

	if !a.hasSubmitted || job.Submitted.Before(a.minSubmitted) {
		a.minSubmitted = job.Submitted
		a.hasSubmitted = true
	}

	if a.hasLastTransitionTime {
		if job.LastTransitionTime.After(a.maxLastTransitionTime) {
			a.maxLastTransitionTime = job.LastTransitionTime
		}
		if job.LastTransitionTime.Before(a.minLastTransitionTime) {
			a.minLastTransitionTime = job.LastTransitionTime
		}
	} else {
		a.maxLastTransitionTime = job.LastTransitionTime
		a.minLastTransitionTime = job.LastTransitionTime
		a.hasLastTransitionTime = true
	}
	a.sumLastTransitionTime += job.LastTransitionTimeSeconds
}

func (a *jobGroupAccumulator) toJobGroup(aggregates []string) *model.JobGroup {
	group := &model.JobGroup{
		Name:       a.name,
		Count:      a.count,
		Aggregates: make(map[string]any),
	}

	for _, agg := range aggregates {
		switch agg {
		case "state":
			group.Aggregates["state"] = a.stateCounts
		case "submitted":
			if a.hasSubmitted {
				group.Aggregates["submitted"] = a.minSubmitted
			}
		case "lastTransitionTime":
			if a.hasLastTransitionTime && a.count > 0 {
				avgSeconds := a.sumLastTransitionTime / a.count
				group.Aggregates["lastTransitionTime"] = time.Unix(avgSeconds, 0).Format(time.RFC3339)
			}
		}
	}

	return group
}

func (m *MemoryDatabase) getGroupKey(job *jobRecord, field string) string {
	switch field {
	case "queue":
		return job.Queue
	case "jobSet":
		return job.JobSet
	case "owner":
		return job.Owner
	case "namespace":
		if job.Namespace != nil {
			return *job.Namespace
		}
		return ""
	case "state":
		return stateOrdinalToString(job.State)
	case "cluster":
		if job.LatestRunID == nil {
			return ""
		}
		if run, ok := m.jobRuns[*job.LatestRunID]; ok {
			return run.Cluster
		}
		return ""
	case "node":
		if job.LatestRunID == nil {
			return ""
		}
		if run, ok := m.jobRuns[*job.LatestRunID]; ok && run.Node != nil {
			return *run.Node
		}
		return ""
	default:
		if annotations, ok := m.annotations[job.JobID]; ok {
			if ann, ok := annotations[field]; ok {
				return ann.Value
			}
		}
		return ""
	}
}

func sortGroups(groups []*model.JobGroup, order *model.Order) {
	if order == nil || order.Field == "" {
		return
	}

	sort.Slice(groups, func(i, j int) bool {
		cmp := compareGroupsByField(groups[i], groups[j], order.Field)
		if order.Direction == model.DirectionDesc {
			return cmp > 0
		}
		return cmp < 0
	})
}

func compareGroupsByField(a, b *model.JobGroup, field string) int {
	switch field {
	case "count":
		if a.Count < b.Count {
			return -1
		}
		if a.Count > b.Count {
			return 1
		}
		return 0
	case "submitted":
		aTime, aOk := a.Aggregates["submitted"].(time.Time)
		bTime, bOk := b.Aggregates["submitted"].(time.Time)
		if !aOk || !bOk {
			return 0
		}
		return compareTime(aTime, bTime)
	case "lastTransitionTime":
		aStr, aOk := a.Aggregates["lastTransitionTime"].(string)
		bStr, bOk := b.Aggregates["lastTransitionTime"].(string)
		if !aOk || !bOk {
			return 0
		}
		aTime, _ := time.Parse(time.RFC3339, aStr)
		bTime, _ := time.Parse(time.RFC3339, bStr)
		return compareTime(aTime, bTime)
	default:
		return strings.Compare(a.Name, b.Name)
	}
}

func paginateGroups(groups []*model.JobGroup, skip, take int) []*model.JobGroup {
	if skip >= len(groups) {
		return nil
	}
	groups = groups[skip:]
	if take > 0 && take < len(groups) {
		groups = groups[:take]
	}
	return groups
}

func (m *MemoryDatabase) PopulateHistoricalJobs(ctx context.Context, params HistoricalJobsParams) error {
	queries := make([]IngestionQuery, 0, params.NJobs*10)
	for jobNum := 0; jobNum < params.NJobs; jobNum++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		queries = append(queries, buildHistoricalJobQueries(jobNum, params)...)
	}
	return m.ExecuteIngestionQueryBatch(ctx, queries)
}

func (m *MemoryDatabase) TearDown(_ context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.jobs = make(map[string]*jobRecord)
	m.jobRuns = make(map[string]*jobRunRecord)
	m.annotations = make(map[string]map[string]*annotationRecord)
	m.jobErrors = make(map[string]*jobErrorRecord)
	m.jobSpecs = make(map[string]*jobSpecRecord)
	return nil
}

func (m *MemoryDatabase) Close() {
}
