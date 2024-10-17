package scheduleringester

import (
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"golang.org/x/exp/maps"

	schedulerdb "github.com/armadaproject/armada/internal/scheduler/database"
)

type Operation int

const (
	JobSetOperation       Operation = iota
	ControlPlaneOperation Operation = iota
)

// DbOperationsWithMessageIds bundles a sequence of schedulerdb ops with the ids of all Pulsar
// messages that were consumed to produce it.
type DbOperationsWithMessageIds struct {
	Ops        []DbOperation
	MessageIds []pulsar.MessageID
}

func (d *DbOperationsWithMessageIds) GetMessageIDs() []pulsar.MessageID {
	return d.MessageIds
}

type JobRunFailed struct {
	LeaseReturned bool
	RunAttempted  bool
	FailureTime   time.Time
}

type JobSchedulingInfoUpdate struct {
	JobSchedulingInfo        []byte
	JobSchedulingInfoVersion int32
}

type JobSetCancelAction struct {
	cancelQueued bool
	cancelLeased bool
}

type JobSetKey struct {
	queue  string
	jobSet string
}

type JobReprioritiseKey struct {
	JobSetKey
	Priority int64
}

type JobRunDetails struct {
	Queue string
	DbRun *schedulerdb.Run
}

type JobQueuedStateUpdate struct {
	Queued             bool
	QueuedStateVersion int32
}

type ExecutorSettingsUpsert struct {
	ExecutorID   string
	Cordoned     bool
	CordonReason string
}

type ExecutorSettingsDelete struct {
	ExecutorID string
}

// DbOperation captures a generic batch database operation.
//
// There are 5 types of operations:
// - Insert jobs (i.e., add new jobs to the schedulerdb).
// - Insert runs (i.e., add new runs to the schedulerdb).
// - Job set operations (i.e., modify all jobs and runs in the schedulerdb part of a given job set).
// - Job operations (i.e., modify particular jobs).
// - Job run operations (i.e., modify particular runs).
// - Control Plane Operations (i.e., upsert settings for a given executor)
//
// To improve performance, several ops can be merged into a single op if of the same type.
// To increase the number of ops that can be merged, ops can sometimes be reordered.
//
// Specifically, an op can be applied before another if:
// - Insert jobs: if prior op doesn't affect the job set.
// - Insert runs: if prior op doesn't affect the job set or defines the corresponding job.
// - Job set operations: if not affecting a job defined in prior op.
// - Job operations: if not affecting a job defined in a prior op.
// - Job run operations: if not affecting a run defined in a prior op.
// - Control Plane Operations: if not conflicting with prior op.
//
// In addition, UpdateJobPriorities can never be applied beforee UpdateJobSetPriorities
// and vice versa, since one may overwrite values set by the other.
type DbOperation interface {
	// a.Merge(b) attempts to merge b into a, creating a single combined op.
	// Returns true if merging was successful.
	// If successful, modifies a in-place.
	// If not successful, neither op is mutated.
	Merge(DbOperation) bool
	// a.CanBeAppliedBefore(b) returns true if a can be placed before b
	// without changing the end result of the overall set of operations.
	CanBeAppliedBefore(DbOperation) bool
	// GetOperation returns the Operation/grouping that this DbOperation belongs to.
	GetOperation() Operation
}

// AppendDbOperation appends a sql operation,
// possibly merging it with a previous operation if that can be done in such a way
// that the end result of applying the entire sequence of operations is unchanged.
func AppendDbOperation(ops []DbOperation, op DbOperation) []DbOperation {
	ops = append(ops, op)
	for i := len(ops) - 1; i > 0; i-- {
		if ops[i-1] == nil || ops[i] == nil {
			continue
		}
		if ops[i-1].Merge(ops[i]) {
			ops[i] = nil
			break
		} else if ops[i].CanBeAppliedBefore(ops[i-1]) {
			ops[i-1], ops[i] = ops[i], ops[i-1]
		} else {
			break
		}
	}
	return discardNilOps(ops)
}

func discardNilOps(ops []DbOperation) []DbOperation {
	rv := make([]DbOperation, 0, len(ops))
	for _, op := range ops {
		if op != nil {
			rv = append(rv, op)
		}
	}
	return rv
}

type (
	InsertJobs                     map[string]*schedulerdb.Job
	InsertRuns                     map[string]*JobRunDetails
	UpdateJobSetPriorities         map[JobSetKey]int64
	MarkJobSetsCancelRequested     map[JobSetKey]*JobSetCancelAction
	MarkJobsCancelRequested        map[JobSetKey][]string
	MarkJobsCancelled              map[string]time.Time
	MarkJobsSucceeded              map[string]bool
	MarkJobsFailed                 map[string]bool
	UpdateJobSchedulingInfo        map[string]*JobSchedulingInfoUpdate
	UpdateJobQueuedState           map[string]*JobQueuedStateUpdate
	MarkRunsSucceeded              map[string]time.Time
	MarkRunsFailed                 map[string]*JobRunFailed
	MarkRunsForJobPreemptRequested map[JobSetKey][]string
	MarkRunsRunning                map[string]time.Time
	MarkRunsPending                map[string]time.Time
	MarkRunsPreempted              map[string]time.Time
	InsertJobRunErrors             map[string]*schedulerdb.JobRunError
	UpdateJobPriorities            struct {
		key    JobReprioritiseKey
		jobIds []string
	}
	MarkJobsValidated     map[string][]string
	InsertPartitionMarker struct {
		markers []*schedulerdb.Marker
	}

	UpsertExecutorSettings map[string]*ExecutorSettingsUpsert
	DeleteExecutorSettings map[string]*ExecutorSettingsDelete
)

type jobSetOperation interface {
	AffectsJobSet(queue string, jobSet string) bool
}

func (a UpdateJobSetPriorities) AffectsJobSet(queue string, jobSet string) bool {
	_, ok := a[JobSetKey{queue: queue, jobSet: jobSet}]
	return ok
}

func (a MarkJobSetsCancelRequested) AffectsJobSet(queue string, jobSet string) bool {
	_, ok := a[JobSetKey{queue: queue, jobSet: jobSet}]
	return ok
}

func (a InsertJobs) Merge(b DbOperation) bool {
	return mergeInMap(a, b)
}

func (a InsertRuns) Merge(b DbOperation) bool {
	return mergeInMap(a, b)
}

func (a UpdateJobSetPriorities) Merge(b DbOperation) bool {
	return mergeInMap(a, b)
}

func (a MarkJobSetsCancelRequested) Merge(b DbOperation) bool {
	return mergeInMap(a, b)
}

func (a MarkJobsCancelRequested) Merge(b DbOperation) bool {
	return mergeListMaps(a, b)
}

func (a MarkRunsForJobPreemptRequested) Merge(b DbOperation) bool {
	return mergeListMaps(a, b)
}

func (a UpdateJobSchedulingInfo) Merge(b DbOperation) bool {
	switch op := b.(type) {
	case UpdateJobSchedulingInfo:
		for key, value := range op {
			aValue, present := a[key]
			if !present {
				a[key] = value
			} else {
				if value.JobSchedulingInfoVersion > aValue.JobSchedulingInfoVersion {
					a[key] = value
				}
			}
		}
		return true
	}
	return false
}

func (a UpdateJobQueuedState) Merge(b DbOperation) bool {
	switch op := b.(type) {
	case UpdateJobQueuedState:
		for key, value := range op {
			currentValue, present := a[key]
			if !present {
				a[key] = value
			} else {
				if value.QueuedStateVersion > currentValue.QueuedStateVersion {
					a[key] = value
				}
			}
		}
		return true
	}
	return false
}

func (a MarkJobsCancelled) Merge(b DbOperation) bool {
	return mergeInMap(a, b)
}

func (a MarkJobsSucceeded) Merge(b DbOperation) bool {
	return mergeInMap(a, b)
}

func (a MarkJobsFailed) Merge(b DbOperation) bool {
	return mergeInMap(a, b)
}

func (a *UpdateJobPriorities) Merge(b DbOperation) bool {
	switch op := b.(type) {
	case *UpdateJobPriorities:
		if a.key == op.key {
			a.jobIds = append(a.jobIds, op.jobIds...)
			return true
		}
	}
	return false
}

func (a MarkRunsSucceeded) Merge(b DbOperation) bool {
	return mergeInMap(a, b)
}

func (a MarkRunsFailed) Merge(b DbOperation) bool {
	return mergeInMap(a, b)
}

func (a MarkRunsRunning) Merge(b DbOperation) bool {
	return mergeInMap(a, b)
}

func (a MarkRunsPending) Merge(b DbOperation) bool {
	return mergeInMap(a, b)
}

func (a MarkRunsPreempted) Merge(b DbOperation) bool {
	return mergeInMap(a, b)
}

func (a InsertJobRunErrors) Merge(b DbOperation) bool {
	return mergeInMap(a, b)
}

func (a MarkJobsValidated) Merge(b DbOperation) bool {
	return mergeInMap(a, b)
}

func (a *InsertPartitionMarker) Merge(b DbOperation) bool {
	switch op := b.(type) {
	case *InsertPartitionMarker:
		a.markers = append(a.markers, op.markers...)
		return true
	}
	return false
}

func (a UpsertExecutorSettings) Merge(_ DbOperation) bool {
	return false
}

func (a DeleteExecutorSettings) Merge(_ DbOperation) bool {
	return false
}

// MergeInMap merges an op b into a, provided that b is of the same type as a.
// For example, if a is of type MarkJobSetsCancelRequested, b is only merged if also of type MarkJobSetsCancelRequested.
// Returns true if the ops were merged and false otherwise.
func mergeInMap[M ~map[K]V, K comparable, V any](a M, b DbOperation) bool {
	// Using a type switch here, since using a type assertion
	// (which should also work in theory) crashes the go1.19 compiler.
	switch op := b.(type) {
	case M:
		maps.Copy(a, op)
		return true
	}
	return false
}

// mergeListMaps merges an op b into a, provided that b is of the same type as a.
// If merged, the resulting map will contain all keys from a and b
// In case both a and b have the same key, the values for that key will be combined
// Returns true if the ops were merged and false otherwise.
func mergeListMaps[M ~map[K][]V, K comparable, V any](a M, b DbOperation) bool {
	// Using a type switch here, since using a type assertion
	// (which should also work in theory) crashes the go1.19 compiler.
	switch op := b.(type) {
	case M:
		for k, v := range op {
			if _, present := a[k]; present {
				a[k] = append(a[k], v...)
			} else {
				a[k] = v
			}
		}
		return true
	}
	return false
}

func (a InsertJobs) CanBeAppliedBefore(b DbOperation) bool {
	// We don't check for job and run ops here,
	// since job and run ops can never appear before the corresponding InsertJobs.
	switch op := b.(type) {
	case jobSetOperation:
		for _, job := range a {
			if op.AffectsJobSet(job.Queue, job.JobSet) {
				return false
			}
		}
	}
	return true
}

func (a InsertRuns) CanBeAppliedBefore(b DbOperation) bool {
	// We don't check for run ops here,
	// since run ops can never appear before the corresponding InsertRuns.
	switch op := b.(type) {
	case jobSetOperation:
		for _, run := range a {
			if op.AffectsJobSet(run.Queue, run.DbRun.JobSet) {
				return false
			}
		}
	case InsertJobs:
		for _, run := range a {
			if _, ok := op[run.DbRun.JobID]; ok {
				return false
			}
		}
	}
	return true
}

func (a UpdateJobSetPriorities) CanBeAppliedBefore(b DbOperation) bool {
	_, isUpdateJobPriorities := b.(*UpdateJobPriorities)
	return !isUpdateJobPriorities && !definesJobInSet(a, b)
}

func (a MarkJobSetsCancelRequested) CanBeAppliedBefore(b DbOperation) bool {
	return !definesJobInSet(a, b) && !definesRunInSet(a, b)
}

func (a MarkJobsCancelRequested) CanBeAppliedBefore(b DbOperation) bool {
	return !definesJobInSet(a, b) && !definesRunInSet(a, b)
}

func (a MarkRunsForJobPreemptRequested) CanBeAppliedBefore(b DbOperation) bool {
	return !definesJobInSet(a, b) && !definesRunInSet(a, b)
}

func (a MarkJobsSucceeded) CanBeAppliedBefore(b DbOperation) bool {
	return !definesJob(a, b)
}

func (a MarkJobsFailed) CanBeAppliedBefore(b DbOperation) bool {
	return !definesJob(a, b)
}

func (a MarkJobsCancelled) CanBeAppliedBefore(b DbOperation) bool {
	return !definesJob(a, b)
}

func (a UpdateJobSchedulingInfo) CanBeAppliedBefore(b DbOperation) bool {
	return !definesJob(a, b)
}

func (a UpdateJobQueuedState) CanBeAppliedBefore(b DbOperation) bool {
	return !definesJob(a, b)
}

func (a *UpdateJobPriorities) CanBeAppliedBefore(b DbOperation) bool {
	_, isUpdateJobSetPriorities := b.(UpdateJobSetPriorities)
	_, isUpdateJobPriorities := b.(*UpdateJobPriorities)
	return !isUpdateJobPriorities && !isUpdateJobSetPriorities &&
		!definesJobInSet(map[JobSetKey]bool{a.key.JobSetKey: true}, b) &&
		!definesRunInSet(map[JobSetKey]bool{a.key.JobSetKey: true}, b)
}

func (a MarkRunsSucceeded) CanBeAppliedBefore(b DbOperation) bool {
	return !definesRun(a, b)
}

func (a MarkRunsFailed) CanBeAppliedBefore(b DbOperation) bool {
	return !definesRun(a, b)
}

func (a MarkRunsRunning) CanBeAppliedBefore(b DbOperation) bool {
	return !definesRun(a, b)
}

func (a MarkRunsPending) CanBeAppliedBefore(b DbOperation) bool {
	return !definesRun(a, b)
}

func (a MarkRunsPreempted) CanBeAppliedBefore(b DbOperation) bool {
	return !definesRun(a, b)
}

func (a *InsertPartitionMarker) CanBeAppliedBefore(b DbOperation) bool {
	// Partition markers can never be brought forward
	return false
}

func (a InsertJobRunErrors) CanBeAppliedBefore(_ DbOperation) bool {
	// Inserting errors before a run has been marked as failed is ok.
	// We only require that errors are written to the schedulerdb before the run is marked as failed.
	return true
}

func (a MarkJobsValidated) CanBeAppliedBefore(b DbOperation) bool {
	return !definesJob(a, b)
}

// Can be applied before another operation only if it relates to a different executor
func (a UpsertExecutorSettings) CanBeAppliedBefore(b DbOperation) bool {
	switch op := b.(type) {
	case UpsertExecutorSettings:
		for k := range a {
			if _, ok := op[k]; ok {
				return false
			}
		}
	case DeleteExecutorSettings:
		for k := range a {
			if _, ok := op[k]; ok {
				return false
			}
		}
	}
	return true
}

// Can be applied before another operation only if it relates to a different executor
func (a DeleteExecutorSettings) CanBeAppliedBefore(b DbOperation) bool {
	switch op := b.(type) {
	case UpsertExecutorSettings:
		for k := range a {
			if _, ok := op[k]; ok {
				return false
			}
		}
	case DeleteExecutorSettings:
		for k := range a {
			if _, ok := op[k]; ok {
				return false
			}
		}
	}
	return true
}

// definesJobInSet returns true if b is an InsertJobs operation
// that inserts at least one job in any of the job sets that make
// up the keys of a.
func definesJobInSet[M ~map[JobSetKey]V, V any](a M, b DbOperation) bool {
	if op, ok := b.(InsertJobs); ok {
		for _, job := range op {
			if _, ok := a[JobSetKey{queue: job.Queue, jobSet: job.JobSet}]; ok {
				return true
			}
		}
	}
	return false
}

// Like definesJobInSet, but checks if b defines a run.
func definesRunInSet[M ~map[JobSetKey]V, V any](a M, b DbOperation) bool {
	if op, ok := b.(InsertRuns); ok {
		for _, run := range op {
			if _, ok := a[JobSetKey{queue: run.Queue, jobSet: run.DbRun.JobSet}]; ok {
				return true
			}
		}
	}
	return false
}

// definesJob returns true if b is an InsertJobs operation
// that inserts at least one job with id equal to any of the keys of a.
func definesJob[M ~map[string]V, V any](a M, b DbOperation) bool {
	if op, ok := b.(InsertJobs); ok {
		for _, job := range op {
			if _, ok := a[job.JobID]; ok {
				return true
			}
		}
	}
	return false
}

// definesRun returns true if b is an InsertRuns operation
// that inserts at least one run with id equal to any of the keys of a.
func definesRun[M ~map[string]V, V any](a M, b DbOperation) bool {
	if op, ok := b.(InsertRuns); ok {
		for _, run := range op {
			if _, ok := a[run.DbRun.RunID]; ok {
				return true
			}
		}
	}
	return false
}

func (a InsertJobs) GetOperation() Operation {
	return JobSetOperation
}

func (a InsertRuns) GetOperation() Operation {
	return JobSetOperation
}

func (a UpdateJobSetPriorities) GetOperation() Operation {
	return JobSetOperation
}

func (a MarkJobSetsCancelRequested) GetOperation() Operation {
	return JobSetOperation
}

func (a MarkJobsCancelRequested) GetOperation() Operation {
	return JobSetOperation
}

func (a MarkRunsForJobPreemptRequested) GetOperation() Operation {
	return JobSetOperation
}

func (a UpdateJobSchedulingInfo) GetOperation() Operation {
	return JobSetOperation
}

func (a UpdateJobQueuedState) GetOperation() Operation {
	return JobSetOperation
}

func (a MarkJobsCancelled) GetOperation() Operation {
	return JobSetOperation
}

func (a MarkJobsSucceeded) GetOperation() Operation {
	return JobSetOperation
}

func (a MarkJobsFailed) GetOperation() Operation {
	return JobSetOperation
}

func (a *UpdateJobPriorities) GetOperation() Operation {
	return JobSetOperation
}

func (a MarkRunsSucceeded) GetOperation() Operation {
	return JobSetOperation
}

func (a MarkRunsFailed) GetOperation() Operation {
	return JobSetOperation
}

func (a MarkRunsRunning) GetOperation() Operation {
	return JobSetOperation
}

func (a MarkRunsPending) GetOperation() Operation {
	return JobSetOperation
}

func (a MarkRunsPreempted) GetOperation() Operation {
	return JobSetOperation
}

func (a InsertJobRunErrors) GetOperation() Operation {
	return JobSetOperation
}

func (a MarkJobsValidated) GetOperation() Operation {
	return JobSetOperation
}

func (a *InsertPartitionMarker) GetOperation() Operation {
	return JobSetOperation
}

func (a UpsertExecutorSettings) GetOperation() Operation {
	return ControlPlaneOperation
}

func (a DeleteExecutorSettings) GetOperation() Operation {
	return ControlPlaneOperation
}
