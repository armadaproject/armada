package scheduler

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/severinson/pulsar-client-go/pulsar"
	"golang.org/x/exp/maps"

	"github.com/G-Research/armada/internal/common/eventutil"
)

// SqlOperation captures a generic batch sql operations.
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
type SqlOperation interface {
	// a.Merge(b) merges b into a.
	// Returns true if merging was successful.
	// If not successful, neither op is mutated.
	Merge(SqlOperation) bool
	// a.CanBeAppliedBefore(b) returns true if a can be placed before b
	// without changing the end result of the overall set of operations.
	CanBeAppliedBefore(SqlOperation) bool
}

// AppendSqlOperation appends a sql operation,
// possibly merging it with a previous operation if that can be done in such a way
// that the end result of applying the entire sequence of operations is unchanged.
func AppendSqlOperation(ops []SqlOperation, op SqlOperation) []SqlOperation {
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

func (a InsertJobs) Merge(b SqlOperation) bool {
	return mergeInMap(a, b)
}
func (a InsertRuns) Merge(b SqlOperation) bool {
	return mergeInMap(a, b)
}
func (a InsertRunAssignments) Merge(b SqlOperation) bool {
	return mergeInMap(a, b)
}
func (a UpdateJobSetPriorities) Merge(b SqlOperation) bool {
	return mergeInMap(a, b)
}
func (a MarkJobSetsCancelled) Merge(b SqlOperation) bool {
	return mergeInMap(a, b)
}
func (a MarkJobsCancelled) Merge(b SqlOperation) bool {
	return mergeInMap(a, b)
}
func (a MarkJobsSucceeded) Merge(b SqlOperation) bool {
	return mergeInMap(a, b)
}
func (a MarkJobsFailed) Merge(b SqlOperation) bool {
	return mergeInMap(a, b)
}
func (a UpdateJobPriorities) Merge(b SqlOperation) bool {
	return mergeInMap(a, b)
}
func (a MarkRunsSucceeded) Merge(b SqlOperation) bool {
	return mergeInMap(a, b)
}
func (a MarkRunsFailed) Merge(b SqlOperation) bool {
	return mergeInMap(a, b)
}
func (a MarkRunsRunning) Merge(b SqlOperation) bool {
	return mergeInMap(a, b)
}
func (a InsertJobErrors) Merge(b SqlOperation) bool {
	return mergeInMap(a, b)
}
func (a InsertJobRunErrors) Merge(b SqlOperation) bool {
	return mergeInMap(a, b)
}

// mergeInMap merges a sql op b into a, provided that b is of the same type as a.
// For example, if a is of type MarkJobsCancelled, b is only merged if also of type MarkJobsCancelled.
// Returns true if the operations were merged and false otherwise.
func mergeInMap[M ~map[K]V, K comparable, V any](a M, b SqlOperation) bool {
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
func (a InsertJobs) CanBeAppliedBefore(b SqlOperation) bool {
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
func (a InsertRuns) CanBeAppliedBefore(b SqlOperation) bool {
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

func (a InsertRunAssignments) CanBeAppliedBefore(b SqlOperation) bool {
	return !definesRun(a, b)
}
func (a UpdateJobSetPriorities) CanBeAppliedBefore(b SqlOperation) bool {
	return !definesJobInSet(a, b)
}
func (a MarkJobSetsCancelled) CanBeAppliedBefore(b SqlOperation) bool {
	return !definesJobInSet(a, b) && !definesRunInSet(a, b)
}
func (a MarkJobsCancelled) CanBeAppliedBefore(b SqlOperation) bool {
	return !definesJob(a, b) && !definesRunForJob(a, b)
}
func (a MarkJobsSucceeded) CanBeAppliedBefore(b SqlOperation) bool {
	return !definesJob(a, b)
}
func (a MarkJobsFailed) CanBeAppliedBefore(b SqlOperation) bool {
	return !definesJob(a, b)
}
func (a UpdateJobPriorities) CanBeAppliedBefore(b SqlOperation) bool {
	return !definesJob(a, b)
}
func (a MarkRunsSucceeded) CanBeAppliedBefore(b SqlOperation) bool {
	return !definesRun(a, b)
}
func (a MarkRunsFailed) CanBeAppliedBefore(b SqlOperation) bool {
	return !definesRun(a, b)
}
func (a MarkRunsRunning) CanBeAppliedBefore(b SqlOperation) bool {
	return !definesRun(a, b)
}
func (a InsertJobErrors) CanBeAppliedBefore(b SqlOperation) bool {
	return !definesJob(a, b)
}
func (a InsertJobRunErrors) CanBeAppliedBefore(b SqlOperation) bool {
	return !definesRun(a, b)
}

// definesJobInSet returns true if b is an InsertJobs operation
// that inserts at least one job in any of the job sets that make
// up the keys of a.
func definesJobInSet[M ~map[string]V, V any](a M, b SqlOperation) bool {
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
func definesRunInSet[M ~map[string]V, V any](a M, b SqlOperation) bool {
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
func definesJob[M ~map[uuid.UUID]V, V any](a M, b SqlOperation) bool {
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
func definesRun[M ~map[uuid.UUID]V, V any](a M, b SqlOperation) bool {
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
func definesRunForJob[M ~map[uuid.UUID]V, V any](a M, b SqlOperation) bool {
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
func CompactOps(ops []SqlOperation) []SqlOperation {
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
func CompactOpsOld(ops []SqlOperation) []SqlOperation {
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

func discardNilOps(ops []SqlOperation) []SqlOperation {
	rv := make([]SqlOperation, 0, len(ops))
	for _, op := range ops {
		if op != nil {
			rv = append(rv, op)
		}
	}
	return rv
}

// Batch of changes to be written to postgres.
type Batch struct {
	// Time at which this batch was created.
	CreatedAt time.Time
	// Ids of messages processed to create this batch.
	// Note that a batch may contain several message ids but not changes to be written to postgres.
	// Since only a subset of messages result in changes to postgres.
	MessageIds []pulsar.MessageID
	// New jobs to be inserted.
	// Should always be inserted first.
	Jobs []Job
	// New job runs to be inserted.
	// Should be inserted after jobs.
	Runs []Run
	// Reprioritisations.
	// When writing to postgres, the priority that was last written to Pulsar for each job wins out.
	// For ReprioritiseJobSet, the priority
	Reprioritisations []*ReprioritisationBatch
	// Set of job sets to be canceled.
	// The map is used as a set, i.e., the value of the bool doesn't matter.
	JobSetsCancelled map[string]bool
	// Set of jobs to be canceled.
	// The map is used as a set, i.e., the value of the bool doesn't matter.
	JobsCancelled map[uuid.UUID]bool
	// Set of jobs that have succeeded.
	JobsSucceeded map[uuid.UUID]bool
	// Any job error messages received.
	JobErrors []JobError
	// Map from run id to a struct describing the set of physical resources assigned to that run.
	JobRunAssignments map[uuid.UUID]*JobRunAssignment
	// Ids of job runs that have started running.
	JobRunsRunning map[uuid.UUID]bool
	// Ids of job runs that have succeeded.
	JobRunsSucceeded map[uuid.UUID]bool
	// Any job run error messages received.
	JobRunErrors []JobRunError
}

func NewBatch() *Batch {
	return &Batch{
		CreatedAt:         time.Now(),
		JobSetsCancelled:  make(map[string]bool),
		JobsCancelled:     make(map[uuid.UUID]bool),
		JobsSucceeded:     make(map[uuid.UUID]bool),
		JobRunAssignments: make(map[uuid.UUID]*JobRunAssignment),
		JobRunsRunning:    make(map[uuid.UUID]bool),
		JobRunsSucceeded:  make(map[uuid.UUID]bool),
	}
}

// A particular sequence may result in more than one batch
// Compaction is possible as long as

func AddEventsToBatch(ctx context.Context, batches []*Batch, sequenceWithIds *eventutil.EventSequenceWithMessageIds) ([]*Batch, error) {
	if sequenceWithIds == nil || sequenceWithIds.Sequence == nil {
		return batches, nil
	}
	// log := ctxlogrus.Extract(ctx)

	// Create a fresh batch if we don't already have one.
	if len(batches) == 0 {
		batches = append(batches, NewBatch())
	}
	// batch := batches[len(batches)-1]

	return batches, nil

	// // Store the message id in the batch.
	// // So we can ack messages once they've been written to postgres.
	// //
	// // It's fine to write to postgres and then not ack, since writes are idempotent.
	// // But it's not fine to ack messages not written to postgres,
	// // since skipping messages isn't detected.
	// //
	// // We defer adding the message id to ensure it's added to the last batch created
	// // from this sequence.
	// defer func() {
	// 	batch := batches[len(batches)-1]
	// 	batch.MessageIds = append(batch.MessageIds, sequenceWithIds.MessageIds...)
	// }()

	// // Unmarshal and validate the message.
	// sequence, err := eventutil.UnmarshalEventSequence(ctx, msg.Payload())
	// if err != nil {
	// 	return batches, err
	// }
	// if sequence == nil || len(sequence.Events) == 0 {
	// 	return batches, nil
	// }

	// // Update the current batch.
	// for i, event := range sequence.GetEvents() {
	// 	switch e := event.Event.(type) {
	// 	case *armadaevents.EventSequence_Event_SubmitJob:

	// 		// If there are job set operations to be applied,
	// 		// we can't add new jobs for any affected job sets to this batch.
	// 		// Since jobs submitted after those operations should not be affected.
	// 		// To that end, we create new batches as necessary here.
	// 		if _, ok := batch.JobSetsCancelled[sequence.GetJobSetName()]; ok {
	// 			batches = append(batches, NewBatch())
	// 			batch = batches[len(batches)-1]
	// 		}
	// 		for _, reprioritisation := range batch.Reprioritisations {
	// 			if _, ok := reprioritisation.PrioritiesByJobSet[sequence.GetJobSetName()]; ok {
	// 				batches = append(batches, NewBatch())
	// 				batch = batches[len(batches)-1]
	// 				break
	// 			}
	// 		}

	// 		// Store the job submit message so that it can be sent to an executor.
	// 		submitJobBytes, err := proto.Marshal(e.SubmitJob)
	// 		if err != nil {
	// 			return batches, errors.WithStack(err)
	// 		}

	// 		// Produce a minimal representation of the job for the scheduler.
	// 		// To avoid the scheduler needing to load the entire job spec.
	// 		schedulingInfo, err := schedulingInfoFromSubmitJob(e.SubmitJob)
	// 		if err != nil {
	// 			return batches, err
	// 		}
	// 		schedulingInfoBytes, err := proto.Marshal(schedulingInfo)
	// 		if err != nil {
	// 			return batches, errors.WithStack(err)
	// 		}

	// 		batch.Jobs = append(batch.Jobs, Job{
	// 			JobID:          armadaevents.UuidFromProtoUuid(e.SubmitJob.JobId),
	// 			JobSet:         sequence.GetJobSetName(),
	// 			UserID:         sequence.GetUserId(),
	// 			Groups:         sequence.GetGroups(),
	// 			Queue:          sequence.GetQueue(),
	// 			Priority:       int64(e.SubmitJob.Priority),
	// 			SubmitMessage:  submitJobBytes,
	// 			SchedulingInfo: schedulingInfoBytes,
	// 		})
	// 	case *armadaevents.EventSequence_Event_JobRunLeased:
	// 		batch.Runs = append(batch.Runs, Run{
	// 			RunID:    armadaevents.UuidFromProtoUuid(e.JobRunLeased.GetRunId()),
	// 			JobID:    armadaevents.UuidFromProtoUuid(e.JobRunLeased.GetJobId()),
	// 			JobSet:   sequence.GetJobSetName(),
	// 			Executor: e.JobRunLeased.GetExecutorId(),
	// 		})
	// 	case *armadaevents.EventSequence_Event_ReprioritiseJob:
	// 		if len(batch.Reprioritisations) == 0 {
	// 			batch.Reprioritisations = append(batch.Reprioritisations, NewReprioritisationBatch())
	// 		}
	// 		reprioritisation := batch.Reprioritisations[len(batch.Reprioritisations)-1]
	// 		newPriority := int64(e.ReprioritiseJob.GetPriority())
	// 		if priority, ok := reprioritisation.PrioritiesByJobSet[sequence.JobSetName]; ok && priority == newPriority {
	// 			break // This operation is redundant.
	// 		}
	// 		jobId := armadaevents.UuidFromProtoUuid(e.ReprioritiseJob.GetJobId())
	// 		reprioritisation.PrioritiesByJob[jobId] = newPriority
	// 	case *armadaevents.EventSequence_Event_ReprioritiseJobSet:
	// 		if len(batch.Reprioritisations) == 0 {
	// 			batch.Reprioritisations = append(batch.Reprioritisations, NewReprioritisationBatch())
	// 		}
	// 		reprioritisation := batch.Reprioritisations[len(batch.Reprioritisations)-1]

	// 		// To ensure the most priority last written to Pulsar is applied last,
	// 		// and since we apply ReprioritiseJobSet before ReprioritiseJob messages for each ReprioritisationBatch,
	// 		// we need to create a new ReprioritisationBatch if len(reprioritisation.PrioritiesByJob) != 0.
	// 		if len(reprioritisation.PrioritiesByJob) != 0 {
	// 			batch.Reprioritisations = append(batch.Reprioritisations, NewReprioritisationBatch())
	// 			reprioritisation = batch.Reprioritisations[len(batch.Reprioritisations)-1]
	// 		}

	// 		newPriority := int64(e.ReprioritiseJobSet.GetPriority())
	// 		reprioritisation.PrioritiesByJobSet[sequence.GetJobSetName()] = newPriority
	// 	case *armadaevents.EventSequence_Event_CancelJobSet:
	// 		batch.JobSetsCancelled[sequence.GetJobSetName()] = true
	// 	case *armadaevents.EventSequence_Event_CancelJob:
	// 		jobId := armadaevents.UuidFromProtoUuid(e.CancelJob.GetJobId())
	// 		batch.JobsCancelled[jobId] = true
	// 	case *armadaevents.EventSequence_Event_JobSucceeded:
	// 		jobId := armadaevents.UuidFromProtoUuid(e.JobSucceeded.GetJobId())
	// 		batch.JobsSucceeded[jobId] = true
	// 	case *armadaevents.EventSequence_Event_JobErrors:
	// 		eventId := eventid.New(msg.ID(), i).String()
	// 		for j, jobError := range e.JobErrors.GetErrors() {
	// 			bytes, err := proto.Marshal(jobError)
	// 			if err != nil {
	// 				err = errors.WithStack(err)
	// 				logging.WithStacktrace(log, err).Error("failed to marshal JobError")
	// 			}
	// 			batch.JobErrors = append(batch.JobErrors, JobError{
	// 				// To ensure inserts are idempotent,
	// 				// we need to mark each row with a deterministic id.
	// 				ID:       fmt.Sprintf("%s-%d", eventId, j),
	// 				JobID:    armadaevents.UuidFromProtoUuid(e.JobErrors.GetJobId()),
	// 				Error:    bytes,
	// 				Terminal: jobError.GetTerminal(),
	// 			})
	// 		}
	// 	case *armadaevents.EventSequence_Event_JobRunAssigned:
	// 		runId := armadaevents.UuidFromProtoUuid(e.JobRunAssigned.GetRunId())
	// 		bytes, err := proto.Marshal(e.JobRunAssigned)
	// 		if err != nil {
	// 			err = errors.WithStack(err)
	// 			logging.WithStacktrace(log, err).Error("failed to marshal JobRunAssigned")
	// 		}
	// 		batch.JobRunAssignments[runId] = &JobRunAssignment{
	// 			RunID:      runId,
	// 			Assignment: bytes,
	// 		}
	// 	case *armadaevents.EventSequence_Event_JobRunRunning:
	// 		jobId := armadaevents.UuidFromProtoUuid(e.JobRunRunning.GetJobId())
	// 		batch.JobRunsRunning[jobId] = true
	// 	case *armadaevents.EventSequence_Event_JobRunSucceeded:
	// 		jobId := armadaevents.UuidFromProtoUuid(e.JobRunSucceeded.GetJobId())
	// 		batch.JobRunsSucceeded[jobId] = true
	// 	case *armadaevents.EventSequence_Event_JobRunErrors:
	// 		eventId := eventid.New(msg.ID(), i).String()
	// 		for j, jobRunError := range e.JobRunErrors.GetErrors() {
	// 			bytes, err := proto.Marshal(jobRunError)
	// 			if err != nil {
	// 				err = errors.WithStack(err)
	// 				logging.WithStacktrace(log, err).Error("failed to marshal JobRunError")
	// 			}
	// 			batch.JobRunErrors = append(batch.JobRunErrors, JobRunError{
	// 				// To ensure inserts are idempotent,
	// 				// we need to mark each row with a deterministic id.
	// 				ID:       fmt.Sprintf("%s-%d", eventId, j),
	// 				RunID:    armadaevents.UuidFromProtoUuid(e.JobRunErrors.GetRunId()),
	// 				Error:    bytes,
	// 				Terminal: jobRunError.GetTerminal(),
	// 			})
	// 		}
	// 	}
	// }
	// return batches, nil
}
