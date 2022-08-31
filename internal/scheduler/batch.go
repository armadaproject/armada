package scheduler

import (
	"context"
	"fmt"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/google/uuid"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/pkg/errors"
	"github.com/severinson/pulsar-client-go/pulsar"
	"golang.org/x/exp/maps"

	"github.com/G-Research/armada/internal/common/eventutil"
	"github.com/G-Research/armada/internal/common/eventutil/eventid"
	"github.com/G-Research/armada/internal/common/logging"
	"github.com/G-Research/armada/pkg/armadaevents"
)

// Jobs: if prior op doesn't affect this job set.
// Runs: if prior op doesn't define the job.
// Reprioritisations: if prior op doesn't define the job.
// Job set cancelled: if not affecting job defined in prior op.
// Job cancelled: if not affecting the job defined in the prior op.
// Job succeeded: if not affecting the job defined in the prior op.
// Job errors: always (but I need a separate job failed op)
// Run assignments: always.
// Run running: if not affecting run defined in prior op.
// Run succeeded: if not affecting run defined in prior op.
// Run errors: always (but I need a separate run failed op).

type SqlOperation interface {
	// Attempts to combine two operations into a single operation.
	// Returns true if merging was successful.
	// If not successful, neither operation is mutated.
	MergeIn(SqlOperation) bool
	// Returns true if the operation provided as an argument can be placed before.
	CanSwap(SqlOperation) bool
}

type InsertJobs struct {
	Jobs map[uuid.UUID]Job
}

func (a *InsertJobs) MergeIn(b SqlOperation) bool {
	if op, ok := b.(*InsertJobs); ok {
		maps.Copy(a.Jobs, op.Jobs)
		return true
	}
	return false
}

func (a *InsertJobs) CanSwap(b SqlOperation) bool {
	switch op := b.(type) {
	case *MarkJobSetsCancelled:
		for _, job := range a.Jobs {
			if _, ok := op.JobSets[job.JobSet]; ok {
				return false
			}
		}
	case *MarkJobsCancelled:
		for jobId := range op.JobIds {
			if _, ok := a.Jobs[jobId]; ok {
				return false
			}
		}
	case *UpdateJobSetPriorities:
		for _, job := range a.Jobs {
			if _, ok := op.Priorities[job.JobSet]; ok {
				return false
			}
		}
	case *UpdateJobPriorities:
		for jobId := range op.Priorities {
			if _, ok := a.Jobs[jobId]; ok {
				return false
			}
		}
	case *InsertRuns:
		for _, run := range op.Runs {
			if _, ok := a.Jobs[run.JobID]; ok {
				return false
			}
		}
	}
	return true
}

type InsertRuns struct {
	Runs map[uuid.UUID]Run
}

func (a *InsertRuns) MergeIn(b SqlOperation) bool {
	if op, ok := b.(*InsertRuns); ok {
		maps.Copy(a.Runs, op.Runs)
		return true
	}
	return false
}

func (a *InsertRuns) CanSwap(b SqlOperation) bool {
	switch op := b.(type) {
	case *InsertJobs:
		for _, run := range op.Runs {
			if _, ok := op.JobSets[job.JobSet]; ok {
				return false
			}
		}
	case *UpdateJobSetPriorities:
		for _, job := range a.Jobs {
			if _, ok := op.Priorities[job.JobSet]; ok {
				return false
			}
		}
	}
	return true
}

type UpdateJobPriorities struct {
	Priorities map[uuid.UUID]int32
}

func (a *UpdateJobPriorities) MergeIn(b SqlOperation) bool {
	if op, ok := b.(*UpdateJobPriorities); ok {
		maps.Copy(a.Priorities, op.Priorities)
		return true
	}
	return false
}

func (a *UpdateJobPriorities) CanSwap(b SqlOperation) bool {
	return false
}

type UpdateJobSetPriorities struct {
	Priorities map[string]int32
}

func (a *UpdateJobSetPriorities) MergeIn(b SqlOperation) bool {
	if op, ok := b.(*UpdateJobSetPriorities); ok {
		maps.Copy(a.Priorities, op.Priorities)
		return true
	}
	return false
}

func (a *UpdateJobSetPriorities) CanSwap(b SqlOperation) bool {
	return false
}

type MarkJobSetsCancelled struct {
	JobSets map[string]bool
}

func (a *MarkJobSetsCancelled) MergeIn(b SqlOperation) bool {
	if op, ok := b.(*MarkJobSetsCancelled); ok {
		maps.Copy(a.JobSets, op.JobSets)
		return true
	}
	return false
}

func (a *MarkJobSetsCancelled) CanSwap(b SqlOperation) bool {
	return false
}

type MarkJobsCancelled struct {
	JobIds map[uuid.UUID]bool
}

func (a *MarkJobsCancelled) MergeIn(b SqlOperation) bool {
	if op, ok := b.(*MarkJobsCancelled); ok {
		maps.Copy(a.JobIds, op.JobIds)
		return true
	}
	return false
}

func (a *MarkJobsCancelled) CanSwap(b SqlOperation) bool {
	return false
}

type MarkJobsSucceeded struct {
	JobIds map[uuid.UUID]bool
}

func (a *MarkJobsSucceeded) MergeIn(b SqlOperation) bool {
	if op, ok := b.(*MarkJobsSucceeded); ok {
		maps.Copy(a.JobIds, op.JobIds)
		return true
	}
	return false
}

func (a *MarkJobsSucceeded) CanSwap(b SqlOperation) bool {
	return false
}

type MarkRunsSucceeded struct {
	RunIds map[uuid.UUID]bool
}

func (a *MarkRunsSucceeded) MergeIn(b SqlOperation) bool {
	if op, ok := b.(*MarkRunsSucceeded); ok {
		maps.Copy(a.RunIds, op.RunIds)
		return true
	}
	return false
}

func (a *MarkRunsSucceeded) CanSwap(b SqlOperation) bool {
	return false
}

type InsertJobErrors struct {
	Errors []JobError
}

func (a *InsertJobErrors) MergeIn(b SqlOperation) bool {
	if op, ok := b.(*InsertJobErrors); ok {
		a.Errors = append(a.Errors, op.Errors...)
		return true
	}
	return false
}

func (a *InsertJobErrors) CanSwap(b SqlOperation) bool {
	return false
}

type InsertJobRunErrors struct {
	Errors []JobRunError
}

func (a *InsertJobRunErrors) MergeIn(b SqlOperation) bool {
	if op, ok := b.(*InsertJobRunErrors); ok {
		a.Errors = append(a.Errors, op.Errors...)
		for jobId, priority := range op.Priorities {
			a.Priorities[jobId] = priority
		}
		return true
	}
	return false
}

func (a *UpdateJobPriorities) CanSwap(b SqlOperation) bool {
	return false
}

type InsertRunAssignments struct {
	Assignments map[uuid.UUID]JobRunAssignment
}

func (a *InsertRunAssignments) MergeIn(b SqlOperation) bool {
	if op, ok := b.(*InsertRunAssignments); ok {
		maps.Copy(a.Priorities, op.Priorities)
		return true
	}
	return false
}

func (a *InsertRunAssignments) CanSwap(b SqlOperation) bool {
	return false
}

type MarkJobsFailed struct {
	JobIds map[uuid.UUID]bool
}

func (a *MarkJobsFailed) MergeIn(b SqlOperation) bool {
	if op, ok := b.(*MarkJobsFailed); ok {
		maps.Copy(a.JobIds, op.JobIds)
		return true
	}
	return false
}

func (a *MarkJobsFailed) CanSwap(b SqlOperation) bool {
	return false
}

type MarkRunsFailed struct {
	RunIds map[uuid.UUID]bool
}

func (a *MarkRunsFailed) MergeIn(b SqlOperation) bool {
	if op, ok := b.(*MarkRunsFailed); ok {
		maps.Copy(a.RunIds, op.RunIds)
		return true
	}
	return false
}

func (a *MarkRunsFailed) CanSwap(b SqlOperation) bool {
	return false
}

type MarkRunsRunning struct {
	RunIds map[uuid.UUID]bool
}

func (a *MarkRunsRunning) MergeIn(b SqlOperation) bool {
	if op, ok := b.(*MarkRunsRunning); ok {
		maps.Copy(a.RunIds, op.RunIds)
		return true
	}
	return false
}

func (a *MarkRunsRunning) CanSwap(b SqlOperation) bool {
	return false
}

// AppendSqlOperation appends a sql operation,
// possibly merging it with a previous operation if that can be done in such a way
// that the end result of applying the entire sequence of operations is unchanged.
func AppendSqlOperation(ops []SqlOperation, op SqlOperation) []SqlOperation {
	ops = append(ops, op)
	for i := len(ops) - 1; i > 0; i-- {
		if ops[i-1].MergeIn(ops[j]) { // Returns true if merge was successful.
			ops[i] = nil
			break
		} else if ops[i-1].CanSwap(ops[i]) {
			ops[i-1], ops[i] = ops[i], ops[i-1]
		} else {
			break
		}
	}
	return discardNilOps(ops)
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
				if ops[j-1].MergeIn(ops[j]) { // Returns true if merge was successful.
					ops[j] = nil
					break
				} else if ops[j-1].CanSwap(ops[j]) {
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
				if ops[j-1].MergeIn(ops[j]) { // Returns true if merge was successful.
					ops[j] = nil
					break
				} else if ops[j-1].CanSwap(ops[j]) {
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
	log := ctxlogrus.Extract(ctx)

	// Create a fresh batch if we don't already have one.
	if len(batches) == 0 {
		batches = append(batches, NewBatch())
	}
	batch := batches[len(batches)-1]

	// Store the message id in the batch.
	// So we can ack messages once they've been written to postgres.
	//
	// It's fine to write to postgres and then not ack, since writes are idempotent.
	// But it's not fine to ack messages not written to postgres,
	// since skipping messages isn't detected.
	//
	// We defer adding the message id to ensure it's added to the last batch created
	// from this sequence.
	defer func() {
		batch := batches[len(batches)-1]
		batch.MessageIds = append(batch.MessageIds, sequenceWithIds.MessageIds...)
	}()

	// Unmarshal and validate the message.
	sequence, err := eventutil.UnmarshalEventSequence(ctx, msg.Payload())
	if err != nil {
		return batches, err
	}
	if sequence == nil || len(sequence.Events) == 0 {
		return batches, nil
	}

	// Update the current batch.
	for i, event := range sequence.GetEvents() {
		switch e := event.Event.(type) {
		case *armadaevents.EventSequence_Event_SubmitJob:

			// If there are job set operations to be applied,
			// we can't add new jobs for any affected job sets to this batch.
			// Since jobs submitted after those operations should not be affected.
			// To that end, we create new batches as necessary here.
			if _, ok := batch.JobSetsCancelled[sequence.GetJobSetName()]; ok {
				batches = append(batches, NewBatch())
				batch = batches[len(batches)-1]
			}
			for _, reprioritisation := range batch.Reprioritisations {
				if _, ok := reprioritisation.PrioritiesByJobSet[sequence.GetJobSetName()]; ok {
					batches = append(batches, NewBatch())
					batch = batches[len(batches)-1]
					break
				}
			}

			// Store the job submit message so that it can be sent to an executor.
			submitJobBytes, err := proto.Marshal(e.SubmitJob)
			if err != nil {
				return batches, errors.WithStack(err)
			}

			// Produce a minimal representation of the job for the scheduler.
			// To avoid the scheduler needing to load the entire job spec.
			schedulingInfo, err := schedulingInfoFromSubmitJob(e.SubmitJob)
			if err != nil {
				return batches, err
			}
			schedulingInfoBytes, err := proto.Marshal(schedulingInfo)
			if err != nil {
				return batches, errors.WithStack(err)
			}

			batch.Jobs = append(batch.Jobs, Job{
				JobID:          armadaevents.UuidFromProtoUuid(e.SubmitJob.JobId),
				JobSet:         sequence.GetJobSetName(),
				UserID:         sequence.GetUserId(),
				Groups:         sequence.GetGroups(),
				Queue:          sequence.GetQueue(),
				Priority:       int64(e.SubmitJob.Priority),
				SubmitMessage:  submitJobBytes,
				SchedulingInfo: schedulingInfoBytes,
			})
		case *armadaevents.EventSequence_Event_JobRunLeased:
			batch.Runs = append(batch.Runs, Run{
				RunID:    armadaevents.UuidFromProtoUuid(e.JobRunLeased.GetRunId()),
				JobID:    armadaevents.UuidFromProtoUuid(e.JobRunLeased.GetJobId()),
				JobSet:   sequence.GetJobSetName(),
				Executor: e.JobRunLeased.GetExecutorId(),
			})
		case *armadaevents.EventSequence_Event_ReprioritiseJob:
			if len(batch.Reprioritisations) == 0 {
				batch.Reprioritisations = append(batch.Reprioritisations, NewReprioritisationBatch())
			}
			reprioritisation := batch.Reprioritisations[len(batch.Reprioritisations)-1]
			newPriority := int64(e.ReprioritiseJob.GetPriority())
			if priority, ok := reprioritisation.PrioritiesByJobSet[sequence.JobSetName]; ok && priority == newPriority {
				break // This operation is redundant.
			}
			jobId := armadaevents.UuidFromProtoUuid(e.ReprioritiseJob.GetJobId())
			reprioritisation.PrioritiesByJob[jobId] = newPriority
		case *armadaevents.EventSequence_Event_ReprioritiseJobSet:
			if len(batch.Reprioritisations) == 0 {
				batch.Reprioritisations = append(batch.Reprioritisations, NewReprioritisationBatch())
			}
			reprioritisation := batch.Reprioritisations[len(batch.Reprioritisations)-1]

			// To ensure the most priority last written to Pulsar is applied last,
			// and since we apply ReprioritiseJobSet before ReprioritiseJob messages for each ReprioritisationBatch,
			// we need to create a new ReprioritisationBatch if len(reprioritisation.PrioritiesByJob) != 0.
			if len(reprioritisation.PrioritiesByJob) != 0 {
				batch.Reprioritisations = append(batch.Reprioritisations, NewReprioritisationBatch())
				reprioritisation = batch.Reprioritisations[len(batch.Reprioritisations)-1]
			}

			newPriority := int64(e.ReprioritiseJobSet.GetPriority())
			reprioritisation.PrioritiesByJobSet[sequence.GetJobSetName()] = newPriority
		case *armadaevents.EventSequence_Event_CancelJobSet:
			batch.JobSetsCancelled[sequence.GetJobSetName()] = true
		case *armadaevents.EventSequence_Event_CancelJob:
			jobId := armadaevents.UuidFromProtoUuid(e.CancelJob.GetJobId())
			batch.JobsCancelled[jobId] = true
		case *armadaevents.EventSequence_Event_JobSucceeded:
			jobId := armadaevents.UuidFromProtoUuid(e.JobSucceeded.GetJobId())
			batch.JobsSucceeded[jobId] = true
		case *armadaevents.EventSequence_Event_JobErrors:
			eventId := eventid.New(msg.ID(), i).String()
			for j, jobError := range e.JobErrors.GetErrors() {
				bytes, err := proto.Marshal(jobError)
				if err != nil {
					err = errors.WithStack(err)
					logging.WithStacktrace(log, err).Error("failed to marshal JobError")
				}
				batch.JobErrors = append(batch.JobErrors, JobError{
					// To ensure inserts are idempotent,
					// we need to mark each row with a deterministic id.
					ID:       fmt.Sprintf("%s-%d", eventId, j),
					JobID:    armadaevents.UuidFromProtoUuid(e.JobErrors.GetJobId()),
					Error:    bytes,
					Terminal: jobError.GetTerminal(),
				})
			}
		case *armadaevents.EventSequence_Event_JobRunAssigned:
			runId := armadaevents.UuidFromProtoUuid(e.JobRunAssigned.GetRunId())
			bytes, err := proto.Marshal(e.JobRunAssigned)
			if err != nil {
				err = errors.WithStack(err)
				logging.WithStacktrace(log, err).Error("failed to marshal JobRunAssigned")
			}
			batch.JobRunAssignments[runId] = &JobRunAssignment{
				RunID:      runId,
				Assignment: bytes,
			}
		case *armadaevents.EventSequence_Event_JobRunRunning:
			jobId := armadaevents.UuidFromProtoUuid(e.JobRunRunning.GetJobId())
			batch.JobRunsRunning[jobId] = true
		case *armadaevents.EventSequence_Event_JobRunSucceeded:
			jobId := armadaevents.UuidFromProtoUuid(e.JobRunSucceeded.GetJobId())
			batch.JobRunsSucceeded[jobId] = true
		case *armadaevents.EventSequence_Event_JobRunErrors:
			eventId := eventid.New(msg.ID(), i).String()
			for j, jobRunError := range e.JobRunErrors.GetErrors() {
				bytes, err := proto.Marshal(jobRunError)
				if err != nil {
					err = errors.WithStack(err)
					logging.WithStacktrace(log, err).Error("failed to marshal JobRunError")
				}
				batch.JobRunErrors = append(batch.JobRunErrors, JobRunError{
					// To ensure inserts are idempotent,
					// we need to mark each row with a deterministic id.
					ID:       fmt.Sprintf("%s-%d", eventId, j),
					RunID:    armadaevents.UuidFromProtoUuid(e.JobRunErrors.GetRunId()),
					Error:    bytes,
					Terminal: jobRunError.GetTerminal(),
				})
			}
		}
	}
	return batches, nil
}
