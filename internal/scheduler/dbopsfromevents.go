package scheduler

import (
	"crypto/sha256"
	"encoding/binary"

	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"

	"github.com/G-Research/armada/internal/scheduler/schedulerobjects"
	"github.com/G-Research/armada/pkg/armadaevents"
)

// DbOpsFromEventInSequence returns a DbOperation produced from the i-th event in sequence,
// or nil if the i-th event doesn't correspond to any DbOperation.
func DbOpsFromEventInSequence(sequence *armadaevents.EventSequence, i int) ([]DbOperation, error) {
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
		return []DbOperation{InsertJobs{jobId: &Job{
			JobID:          jobId,
			JobSet:         sequence.GetJobSetName(),
			UserID:         sequence.GetUserId(),
			Groups:         sequence.GetGroups(),
			Queue:          sequence.GetQueue(),
			Priority:       int64(e.SubmitJob.Priority),
			SubmitMessage:  submitJobBytes,
			SchedulingInfo: schedulingInfoBytes,
		}}}, nil
	case *armadaevents.EventSequence_Event_JobRunLeased:
		runId := armadaevents.UuidFromProtoUuid(e.JobRunLeased.GetRunId())
		return []DbOperation{InsertRuns{runId: &Run{
			RunID:    runId,
			JobID:    armadaevents.UuidFromProtoUuid(e.JobRunLeased.GetJobId()),
			JobSet:   sequence.GetJobSetName(),
			Executor: e.JobRunLeased.GetExecutorId(),
		}}}, nil
	case *armadaevents.EventSequence_Event_ReprioritiseJob:
		jobId := armadaevents.UuidFromProtoUuid(e.ReprioritiseJob.GetJobId())
		return []DbOperation{UpdateJobPriorities{
			jobId: int64(e.ReprioritiseJob.Priority),
		}}, nil
	case *armadaevents.EventSequence_Event_ReprioritiseJobSet:
		return []DbOperation{UpdateJobSetPriorities{
			sequence.GetJobSetName(): int64(e.ReprioritiseJobSet.Priority),
		}}, nil
	case *armadaevents.EventSequence_Event_CancelJobSet:
		return []DbOperation{MarkJobSetsCancelled{
			sequence.GetJobSetName(): true,
		}}, nil
	case *armadaevents.EventSequence_Event_CancelJob:
		jobId := armadaevents.UuidFromProtoUuid(e.CancelJob.GetJobId())
		return []DbOperation{MarkJobsCancelled{
			jobId: true,
		}}, nil
	case *armadaevents.EventSequence_Event_JobSucceeded:
		jobId := armadaevents.UuidFromProtoUuid(e.JobSucceeded.GetJobId())
		return []DbOperation{MarkJobsSucceeded{
			jobId: true,
		}}, nil
	case *armadaevents.EventSequence_Event_JobErrors:
		jobId := e.JobErrors.GetJobId()
		jobIdBytes, err := proto.Marshal(jobId)
		if err != nil {
			return nil, errors.Wrap(err, "failed to marshal jobId")
		}
		insertJobErrors := make(InsertJobErrors)
		markJobsFailed := make(MarkJobsFailed)
		for _, jobError := range e.JobErrors.GetErrors() {
			bytes, err := proto.Marshal(jobError)
			if err != nil {
				return nil, errors.Wrap(err, "failed to marshal JobError")
			}

			// To ensure inserts are idempotent, each row must have a unique deterministic.
			// We use the hash of (job id, error message),
			// which isn't entirely correct since it deduplicates identical error message.
			hash := sha256.Sum256(append(bytes, jobIdBytes...))
			key := int32(binary.BigEndian.Uint32(hash[:]))
			insertJobErrors[key] = &JobError{
				ID:       key,
				JobID:    armadaevents.UuidFromProtoUuid(jobId),
				Error:    bytes,
				Terminal: jobError.GetTerminal(),
			}

			// For terminal errors, we also need to mark the job as failed.
			if jobError.GetTerminal() {
				markJobsFailed[armadaevents.UuidFromProtoUuid(jobId)] = true
			}
		}
		if len(markJobsFailed) > 0 {
			return []DbOperation{insertJobErrors, markJobsFailed}, nil
		} else {
			return []DbOperation{insertJobErrors}, nil
		}
	case *armadaevents.EventSequence_Event_JobRunAssigned:
		runId := armadaevents.UuidFromProtoUuid(e.JobRunAssigned.GetRunId())
		bytes, err := proto.Marshal(e.JobRunAssigned)
		if err != nil {
			return nil, errors.Wrap(err, "failed to marshal JobRunAssigned")
		}
		return []DbOperation{InsertRunAssignments{runId: &JobRunAssignment{
			RunID:      runId,
			Assignment: bytes,
		}}}, nil
	case *armadaevents.EventSequence_Event_JobRunRunning:
		runId := armadaevents.UuidFromProtoUuid(e.JobRunRunning.GetRunId())
		return []DbOperation{MarkRunsRunning{runId: true}}, nil
	case *armadaevents.EventSequence_Event_JobRunSucceeded:
		runId := armadaevents.UuidFromProtoUuid(e.JobRunSucceeded.GetRunId())
		return []DbOperation{MarkRunsSucceeded{runId: true}}, nil
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
		insertJobRunErrors := make(InsertJobRunErrors)
		markRunsFailed := make(MarkRunsFailed)
		for _, runError := range e.JobRunErrors.GetErrors() {
			bytes, err := proto.Marshal(runError)
			if err != nil {
				return nil, errors.Wrap(err, "failed to marshal RunError")
			}

			// To ensure inserts are idempotent, each row must have a unique deterministic.
			// We use the hash of (job id, error message),
			// which isn't entirely correct since it deduplicates identical error message.
			hash := sha256.Sum256(append(bytes, append(jobIdBytes, runIdBytes...)...))
			key := int32(binary.BigEndian.Uint32(hash[:]))
			insertJobRunErrors[key] = &JobRunError{
				ID:       key,
				RunID:    armadaevents.UuidFromProtoUuid(runId),
				Error:    bytes,
				Terminal: runError.GetTerminal(),
			}

			// For terminal errors, we also need to mark the run as failed.
			if runError.GetTerminal() {
				markRunsFailed[armadaevents.UuidFromProtoUuid(runId)] = true
			}
		}
		if len(markRunsFailed) > 0 {
			return []DbOperation{insertJobRunErrors, markRunsFailed}, nil
		} else {
			return []DbOperation{insertJobRunErrors}, nil
		}
	default:
		return nil, nil
	}
}

// schedulingInfoFromSubmitJob returns a minimal representation of a job
// containing only the info needed by the scheduler.
func schedulingInfoFromSubmitJob(submitJob *armadaevents.SubmitJob) (*schedulerobjects.JobSchedulingInfo, error) {
	// Component common to all jobs.
	schedulingInfo := &schedulerobjects.JobSchedulingInfo{
		Lifetime:        submitJob.Lifetime,
		AtMostOnce:      submitJob.AtMostOnce,
		Preemptible:     submitJob.Preemptible,
		ConcurrencySafe: submitJob.ConcurrencySafe,
	}

	// Scheduling requirements specific to the objects that make up this job.
	switch object := submitJob.MainObject.Object.(type) {
	case *armadaevents.KubernetesMainObject_PodSpec:
		podSpec := object.PodSpec.PodSpec
		requirements := &schedulerobjects.ObjectRequirements_PodRequirements{
			PodRequirements: schedulerobjects.PodRequirementsFromPodSpec(podSpec),
		}
		schedulingInfo.ObjectRequirements = append(
			schedulingInfo.ObjectRequirements,
			&schedulerobjects.ObjectRequirements{Requirements: requirements},
		)
	default:
		return nil, errors.Errorf("unsupported object type %T", object)
	}
	return schedulingInfo, nil
}
