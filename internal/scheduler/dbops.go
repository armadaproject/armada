package scheduler

import (
	"github.com/google/uuid"
	"golang.org/x/exp/maps"
)

// DbOperation captures a generic batch database operation.
//
// There are 5 types of operations:
// - Insert jobs (i.e., add new jobs to the db).
// - Insert runs (i.e., add new runs to the db).
// - Job set operations (i.e., modify all jobs and runs in the db part of a given job set).
// - Job operations (i.e., modify particular jobs).
// - Job run operations (i.e., modify particular runs).
//
// To improve performance, several ops can be merged into a single op if of the same type.
// To increase the number of ops that can be merged, ops can sometimes be reordered:
// - Insert jobs: if prior op doesn't affect the job set.
// - Insert runs: if prior op doesn't affect the job set or defines the job.
// - Job set operations: if not affecting a job defined in prior op.
// - Job operations: if not affecting a job defined in a prior op.
// - Job run operations: if not affecting a run defined in a prior op.
type DbOperation interface {
	// a.Merge(b) merges b into a.
	// Returns true if merging was successful.
	// If not successful, neither op is mutated.
	Merge(DbOperation) bool
	// a.CanBeAppliedBefore(b) returns true if a can be placed before b
	// without changing the end result of the overall set of operations.
	CanBeAppliedBefore(DbOperation) bool
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
	return discardNilOps(ops) // TODO: Can be made more efficient.
}

// Db operations (implements DbOperation).
type InsertJobs map[uuid.UUID]Job
type InsertRuns map[uuid.UUID]Run
type InsertRunAssignments map[uuid.UUID]JobRunAssignment
type UpdateJobSetPriorities map[string]int32
type MarkJobSetsCancelled map[string]bool
type MarkJobsCancelled map[uuid.UUID]bool
type MarkJobsSucceeded map[uuid.UUID]bool
type MarkJobsFailed map[uuid.UUID]bool
type UpdateJobPriorities map[uuid.UUID]int32
type MarkRunsSucceeded map[uuid.UUID]bool
type MarkRunsFailed map[uuid.UUID]bool
type MarkRunsRunning map[uuid.UUID]bool
type InsertJobErrors map[uuid.UUID]JobError
type InsertJobRunErrors map[uuid.UUID]JobRunError

type JobSetOperation interface {
	AffectsJobSet(string) bool
}

func (a UpdateJobSetPriorities) AffectsJobSet(jobSet string) bool {
	_, ok := a[jobSet]
	return ok
}
func (a MarkJobSetsCancelled) AffectsJobSet(jobSet string) bool {
	_, ok := a[jobSet]
	return ok
}

type JobOperation interface {
	AffectsJob(uuid.UUID) bool
}

func (a InsertJobs) AffectsJob(jobId uuid.UUID) bool {
	_, ok := a[jobId]
	return ok
}

type JobRunOperation interface {
	AffectsJobRun(uuid.UUID) bool
}

func (a InsertJobs) Merge(b DbOperation) bool {
	return mergeInMap(a, b)
}
func (a InsertRuns) Merge(b DbOperation) bool {
	return mergeInMap(a, b)
}
func (a InsertRunAssignments) Merge(b DbOperation) bool {
	return mergeInMap(a, b)
}
func (a UpdateJobSetPriorities) Merge(b DbOperation) bool {
	return mergeInMap(a, b)
}
func (a MarkJobSetsCancelled) Merge(b DbOperation) bool {
	return mergeInMap(a, b)
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
func (a UpdateJobPriorities) Merge(b DbOperation) bool {
	return mergeInMap(a, b)
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
func (a InsertJobErrors) Merge(b DbOperation) bool {
	return mergeInMap(a, b)
}
func (a InsertJobRunErrors) Merge(b DbOperation) bool {
	return mergeInMap(a, b)
}

// mergeInMap merges a sql op b into a, provided that b is of the same type as a.
// For example, if a is of type MarkJobsCancelled, b is only merged if also of type MarkJobsCancelled.
// Returns true if the operations were merged and false otherwise.
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

// Returns true if a can be placed before b.
func (a InsertJobs) CanBeAppliedBefore(b DbOperation) bool {
	// We don't check for job and run ops here,
	// since job and run ops can never appear before the corresponding InsertJobs.
	switch op := b.(type) {
	case JobSetOperation:
		for _, job := range a {
			if op.AffectsJobSet(job.JobSet) {
				return false
			}
		}
	}
	return true
}

// Returns true if a can be placed before b.
func (a InsertRuns) CanBeAppliedBefore(b DbOperation) bool {
	// We don't check for run ops here,
	// since run ops can never appear before the corresponding InsertRuns.
	switch op := b.(type) {
	case JobSetOperation:
		for _, run := range a {
			if op.AffectsJobSet(run.JobSet) {
				return false
			}
		}
	case InsertJobs:
		for _, run := range a {
			if _, ok := op[run.JobID]; ok {
				return false
			}
		}
	}
	return true
}

func (a InsertRunAssignments) CanBeAppliedBefore(b DbOperation) bool {
	return !definesRun(a, b)
}
func (a UpdateJobSetPriorities) CanBeAppliedBefore(b DbOperation) bool {
	return !definesJobInSet(a, b)
}
func (a MarkJobSetsCancelled) CanBeAppliedBefore(b DbOperation) bool {
	return !definesJobInSet(a, b) && !definesRunInSet(a, b)
}
func (a MarkJobsCancelled) CanBeAppliedBefore(b DbOperation) bool {
	return !definesJob(a, b) && !definesRunForJob(a, b)
}
func (a MarkJobsSucceeded) CanBeAppliedBefore(b DbOperation) bool {
	return !definesJob(a, b)
}
func (a MarkJobsFailed) CanBeAppliedBefore(b DbOperation) bool {
	return !definesJob(a, b)
}
func (a UpdateJobPriorities) CanBeAppliedBefore(b DbOperation) bool {
	return !definesJob(a, b)
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
func (a InsertJobErrors) CanBeAppliedBefore(b DbOperation) bool {
	return !definesJob(a, b)
}
func (a InsertJobRunErrors) CanBeAppliedBefore(b DbOperation) bool {
	return !definesRun(a, b)
}

// definesJobInSet returns true if b is an InsertJobs operation
// that inserts at least one job in any of the job sets that make
// up the keys of a.
func definesJobInSet[M ~map[string]V, V any](a M, b DbOperation) bool {
	if op, ok := b.(InsertJobs); ok {
		for _, job := range op {
			if _, ok := a[job.JobSet]; ok {
				return true
			}
		}
	}
	return false
}

// Like definesJobInSet, but checks if b defines a run.
func definesRunInSet[M ~map[string]V, V any](a M, b DbOperation) bool {
	if op, ok := b.(InsertRuns); ok {
		for _, run := range op {
			if _, ok := a[run.JobSet]; ok {
				return true
			}
		}
	}
	return false
}

// definesJob returns true if b is an InsertJobs operation
// that inserts at least one job with id equal to any of the keys of a.
func definesJob[M ~map[uuid.UUID]V, V any](a M, b DbOperation) bool {
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
func definesRun[M ~map[uuid.UUID]V, V any](a M, b DbOperation) bool {
	if op, ok := b.(InsertRuns); ok {
		for _, run := range op {
			if _, ok := a[run.RunID]; ok {
				return true
			}
		}
	}
	return false
}

// definesRunForJob returns true if b is an InsertRuns operation
// that inserts at least one run with job id equal to any of the keys of a.
func definesRunForJob[M ~map[uuid.UUID]V, V any](a M, b DbOperation) bool {
	if op, ok := b.(InsertRuns); ok {
		for _, run := range op {
			if _, ok := a[run.JobID]; ok {
				return true
			}
		}
	}
	return false
}

// CompactOps merges sql operations to produce a (hopefully) smaller number of operations.
// In doing so, it may change the order of operations.
// However, the resulting operations is guaranteed to produce the same end state
// as the original operations.
func CompactOps(ops []DbOperation) []DbOperation {
	ops = discardNilOps(ops)
	for len(ops) > 1 {
		n := len(ops)
		for i := len(ops) - 1; i > 0; i-- {
			for j := i - 1; j >= 0; j-- {
				if ops[j-1].Merge(ops[j]) { // Returns true if merge was successful.
					ops[j] = nil
					break
				} else if ops[j-1].CanBeAppliedBefore(ops[j]) {
					ops[j-1], ops[j] = ops[j], ops[j-1]
				}
			}
		}
		ops = discardNilOps(ops)
		if len(ops) == n { // Return if we made no progress.
			return ops
		}
	}
	return ops
}

// CompactOps merges sql operations to produce a (hopefully) smaller number of operations.
// In doing so, it may change the order of operations.
// However, the resulting operations is guaranteed to produce the same end state
// as the original operations.
func CompactOpsOld(ops []DbOperation) []DbOperation {
	ops = discardNilOps(ops)
	for len(ops) > 0 {
		n := len(ops)
		for i := range ops {
			for j := i - 1; j > 0; j-- {
				if ops[j-1].Merge(ops[j]) { // Returns true if merge was successful.
					ops[j] = nil
					break
				} else if ops[j-1].CanBeAppliedBefore(ops[j]) {
					ops[j-1], ops[j] = ops[j], ops[j-1]
				}
			}
		}
		ops = discardNilOps(ops)
		if len(ops) == n { // Return if we made no progress.
			return ops
		}
	}
	return ops
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
