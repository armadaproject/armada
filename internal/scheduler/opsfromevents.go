package scheduler

import (
	"crypto/sha256"
	"encoding/binary"

	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"

	"github.com/G-Research/armada/pkg/armadaevents"
)

// DbOpFromEventInSequence returns a DbOperation produced from the i-th event in sequence,
// or nil if the i-th event doesn't correspond to any DbOperation.
func DbOpFromEventInSequence(sequence *armadaevents.EventSequence, i int) (DbOperation, error) {
	if sequence == nil {
		return nil, errors.New("received nil sequence")
	}
	if i < 0 || i >= len(sequence.Events) {
		return nil, errors.Errorf("expected i to be in [0, len(sequence.Events)), but got %d", i)
	}
	switch e := sequence.Events[i].Event.(type) {
	case *armadaevents.EventSequence_Event_SubmitJob:

		// Store the job submit message so that it can be sent to an executor.
		submitJobBytes, err := proto.Marshal(e.SubmitJob)
		if err != nil {
			return nil, errors.WithStack(err)
		}

		// Produce a minimal representation of the job for the scheduler.
		// To avoid the scheduler needing to load the entire job spec.
		schedulingInfo, err := schedulingInfoFromSubmitJob(e.SubmitJob)
		if err != nil {
			return nil, err
		}
		schedulingInfoBytes, err := proto.Marshal(schedulingInfo)
		if err != nil {
			return nil, errors.WithStack(err)
		}

		jobId := armadaevents.UuidFromProtoUuid(e.SubmitJob.JobId)
		return InsertJobs{jobId: &Job{
			JobID:          jobId,
			JobSet:         sequence.GetJobSetName(),
			UserID:         sequence.GetUserId(),
			Groups:         sequence.GetGroups(),
			Queue:          sequence.GetQueue(),
			Priority:       int64(e.SubmitJob.Priority),
			SubmitMessage:  submitJobBytes,
			SchedulingInfo: schedulingInfoBytes,
		}}, nil
	case *armadaevents.EventSequence_Event_JobRunLeased:
		runId := armadaevents.UuidFromProtoUuid(e.JobRunLeased.GetRunId())
		return InsertRuns{runId: &Run{
			RunID:    runId,
			JobID:    armadaevents.UuidFromProtoUuid(e.JobRunLeased.GetJobId()),
			JobSet:   sequence.GetJobSetName(),
			Executor: e.JobRunLeased.GetExecutorId(),
		}}, nil
	case *armadaevents.EventSequence_Event_ReprioritiseJob:
		jobId := armadaevents.UuidFromProtoUuid(e.ReprioritiseJob.GetJobId())
		return UpdateJobPriorities{
			jobId: int64(e.ReprioritiseJob.Priority),
		}, nil
	case *armadaevents.EventSequence_Event_ReprioritiseJobSet:
		return UpdateJobSetPriorities{
			sequence.GetJobSetName(): int64(e.ReprioritiseJobSet.Priority),
		}, nil
	case *armadaevents.EventSequence_Event_CancelJobSet:
		return MarkJobSetsCancelled{
			sequence.GetJobSetName(): true,
		}, nil
	case *armadaevents.EventSequence_Event_CancelJob:
		jobId := armadaevents.UuidFromProtoUuid(e.CancelJob.GetJobId())
		return MarkJobsCancelled{
			jobId: true,
		}, nil
	case *armadaevents.EventSequence_Event_JobSucceeded:
		jobId := armadaevents.UuidFromProtoUuid(e.JobSucceeded.GetJobId())
		return MarkJobsSucceeded{
			jobId: true,
		}, nil
	case *armadaevents.EventSequence_Event_JobErrors:
		jobId := e.JobErrors.GetJobId()
		jobIdBytes, err := proto.Marshal(jobId)
		if err != nil {
			return nil, errors.Wrap(err, "failed to marshal jobId")
		}
		op := make(InsertJobErrors)
		for _, jobError := range e.JobErrors.GetErrors() {
			bytes, err := proto.Marshal(jobError)
			if err != nil {
				return nil, errors.Wrap(err, "failed to marshal JobError")
			}

			// To ensure inserts are idempotent, each row must have a unique deterministic.
			// We use the hash of (job id, error message),
			// which isn't entirely correct since it deduplicates identical error message.
			hash := sha256.Sum256(append(bytes, jobIdBytes...))
			key := binary.BigEndian.Uint32(hash[:])
			op[key] = &JobError{
				ID:       int32(key),
				JobID:    armadaevents.UuidFromProtoUuid(e.JobErrors.GetJobId()),
				Error:    bytes,
				Terminal: jobError.GetTerminal(),
			}
		}
		return op, nil
	case *armadaevents.EventSequence_Event_JobRunAssigned:
		runId := armadaevents.UuidFromProtoUuid(e.JobRunAssigned.GetRunId())
		bytes, err := proto.Marshal(e.JobRunAssigned)
		if err != nil {
			return nil, errors.Wrap(err, "failed to marshal JobRunAssigned")
		}
		return InsertRunAssignments{runId: &JobRunAssignment{
			RunID:      runId,
			Assignment: bytes,
		}}, nil
	case *armadaevents.EventSequence_Event_JobRunRunning:
		runId := armadaevents.UuidFromProtoUuid(e.JobRunRunning.GetRunId())
		return MarkRunsRunning{runId: true}, nil
	case *armadaevents.EventSequence_Event_JobRunSucceeded:
		runId := armadaevents.UuidFromProtoUuid(e.JobRunSucceeded.GetRunId())
		return MarkRunsSucceeded{runId: true}, nil
	case *armadaevents.EventSequence_Event_JobRunErrors:
		jobId := e.JobRunErrors.GetJobId()
		runId := e.JobRunErrors.GetRunId()
		jobIdBytes, err := proto.Marshal(jobId)
		if err != nil {
			return nil, errors.Wrap(err, "failed to marshal jobId")
		}
		runIdBytes, err := proto.Marshal(runId)
		if err != nil {
			return nil, errors.Wrap(err, "failed to marshal runId")
		}
		op := make(InsertJobRunErrors)
		for _, runError := range e.JobRunErrors.GetErrors() {
			bytes, err := proto.Marshal(runError)
			if err != nil {
				return nil, errors.Wrap(err, "failed to marshal RunError")
			}

			// To ensure inserts are idempotent, each row must have a unique deterministic.
			// We use the hash of (job id, error message),
			// which isn't entirely correct since it deduplicates identical error message.
			hash := sha256.Sum256(append(bytes, append(jobIdBytes, runIdBytes...)...))
			key := binary.BigEndian.Uint32(hash[:])
			op[key] = &JobRunError{
				ID:       int32(key),
				RunID:    armadaevents.UuidFromProtoUuid(e.JobRunErrors.GetRunId()),
				Error:    bytes,
				Terminal: runError.GetTerminal(),
			}
		}
		return op, nil
	default:
		return nil, nil
	}
}
