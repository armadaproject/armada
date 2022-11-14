package scheduleringester

import (
	"context"
	"crypto/sha256"
	"encoding/binary"

	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/G-Research/armada/internal/common/ingest"
	"github.com/G-Research/armada/internal/common/ingest/metrics"
	"github.com/G-Research/armada/internal/scheduler/schedulerobjects"
	"github.com/G-Research/armada/internal/scheduler/sqlc"
	"github.com/G-Research/armada/pkg/armadaevents"
)

type eventSequenceCommon struct {
	queue  string
	jobset string
	user   string
	groups []string
}

type InstructionConverter struct {
	metrics     *metrics.Metrics
	eventFilter func(event *armadaevents.EventSequence_Event) bool
}

func NewInstructionConverter(metrics *metrics.Metrics,
	filter func(event *armadaevents.EventSequence_Event) bool,
) ingest.InstructionConverter[*DbOperationsWithMessageIds] {
	return &InstructionConverter{
		metrics:     metrics,
		eventFilter: filter,
	}
}

func (c *InstructionConverter) Convert(_ context.Context, sequencesWithIds *ingest.EventSequencesWithIds) *DbOperationsWithMessageIds {
	operations := make([]DbOperation, 0)
	for _, es := range sequencesWithIds.EventSequences {
		for _, op := range c.convertSequence(es) {
			operations = AppendDbOperation(operations, op)
		}
	}
	return &DbOperationsWithMessageIds{
		Ops:        operations,
		MessageIds: sequencesWithIds.MessageIds,
	}
}

func (c *InstructionConverter) convertSequence(es *armadaevents.EventSequence) []DbOperation {
	meta := eventSequenceCommon{
		queue:  es.Queue,
		jobset: es.JobSetName,
		user:   es.UserId,
		groups: es.Groups,
	}

	operations := make([]DbOperation, 0, len(es.Events))
	for idx, event := range es.Events {
		if c.eventFilter(event) {
			var err error = nil
			var operationsFromEvent []DbOperation
			switch event.GetEvent().(type) {
			case *armadaevents.EventSequence_Event_SubmitJob:
				operationsFromEvent, err = c.handleSubmitJob(event.GetSubmitJob(), meta)
			case *armadaevents.EventSequence_Event_JobRunAssigned:
				operationsFromEvent, err = c.handleJobRunAssigned(event.GetJobRunAssigned())
			case *armadaevents.EventSequence_Event_JobRunLeased:
				operationsFromEvent, err = c.handleJobRunLeased(event.GetJobRunLeased(), meta)
			case *armadaevents.EventSequence_Event_JobRunRunning:
				operationsFromEvent, err = c.handleJobRunRunning(event.GetJobRunRunning())
			case *armadaevents.EventSequence_Event_JobRunSucceeded:
				operationsFromEvent, err = c.handleJobRunSucceeded(event.GetJobRunSucceeded())
			case *armadaevents.EventSequence_Event_JobRunErrors:
				operationsFromEvent, err = c.handleJobRunErrors(event.GetJobRunErrors())
			case *armadaevents.EventSequence_Event_JobSucceeded:
				operationsFromEvent, err = c.handleJobSucceeded(event.GetJobSucceeded())
			case *armadaevents.EventSequence_Event_JobErrors:
				operationsFromEvent, err = c.handleJobErrors(event.GetJobErrors())
			case *armadaevents.EventSequence_Event_ReprioritiseJob:
				operationsFromEvent, err = c.handleReprioritiseJob(event.GetReprioritiseJob())
			case *armadaevents.EventSequence_Event_ReprioritiseJobSet:
				operationsFromEvent, err = c.handleReprioritiseJobSet(event.GetReprioritiseJobSet(), meta)
			case *armadaevents.EventSequence_Event_CancelJob:
				operationsFromEvent, err = c.handleCancelJob(event.GetCancelJob())
			case *armadaevents.EventSequence_Event_CancelJobSet:
				operationsFromEvent, err = c.handleCancelJobSet(meta.jobset)
			case *armadaevents.EventSequence_Event_CancelledJob,
				*armadaevents.EventSequence_Event_ReprioritisedJob,
				*armadaevents.EventSequence_Event_JobDuplicateDetected,
				*armadaevents.EventSequence_Event_ResourceUtilisation,
				*armadaevents.EventSequence_Event_StandaloneIngressInfo,
				*armadaevents.EventSequence_Event_JobRunPreempted:
				// These events can all be safely ignored
				log.Debugf("Ignoring event type %T", event)
			default:
				// This is an event type we haven't considered. Log a warning
				log.Warnf("Ignoring unknown event type %T", event)
			}
			if err != nil {
				c.metrics.RecordPulsarMessageError(metrics.PulsarMessageErrorProcessing)
				log.WithError(err).Warnf("Could not convert event at index %d.", idx)
			} else {
				operations = append(operations, operationsFromEvent...)
			}
		}
	}
	return operations
}

func (c *InstructionConverter) handleSubmitJob(job *armadaevents.SubmitJob, meta eventSequenceCommon) ([]DbOperation, error) {
	// Store the job submit message so that it can be sent to an executor.
	submitJobBytes, err := proto.Marshal(job)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// Produce a minimal representation of the job for the scheduler.
	// To avoid the scheduler needing to load the entire job spec.
	schedulingInfo, err := schedulingInfoFromSubmitJob(job)
	if err != nil {
		return nil, err
	}
	schedulingInfoBytes, err := proto.Marshal(schedulingInfo)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	jobId := armadaevents.UuidFromProtoUuid(job.JobId)
	return []DbOperation{InsertJobs{jobId: &sqlc.Job{
		JobID:          jobId,
		JobSet:         meta.jobset,
		UserID:         meta.user,
		Groups:         meta.groups,
		Queue:          meta.queue,
		Priority:       int64(job.Priority),
		SubmitMessage:  submitJobBytes,
		SchedulingInfo: schedulingInfoBytes,
	}}}, nil
}

func (c *InstructionConverter) handleJobRunLeased(jobRunLeased *armadaevents.JobRunLeased, meta eventSequenceCommon) ([]DbOperation, error) {
	runId := armadaevents.UuidFromProtoUuid(jobRunLeased.GetRunId())
	return []DbOperation{InsertRuns{runId: &sqlc.Run{
		RunID:    runId,
		JobID:    armadaevents.UuidFromProtoUuid(jobRunLeased.GetJobId()),
		JobSet:   meta.jobset,
		Executor: jobRunLeased.GetExecutorId(),
	}}}, nil
}

func (c *InstructionConverter) handleJobRunAssigned(jobRunAssigned *armadaevents.JobRunAssigned) ([]DbOperation, error) {
	runId := armadaevents.UuidFromProtoUuid(jobRunAssigned.GetRunId())
	bytes, err := proto.Marshal(jobRunAssigned)
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal JobRunAssigned")
	}
	return []DbOperation{InsertRunAssignments{runId: &sqlc.JobRunAssignment{
		RunID:      runId,
		Assignment: bytes,
	}}}, nil
}

func (c *InstructionConverter) handleJobRunRunning(jobRunRunning *armadaevents.JobRunRunning) ([]DbOperation, error) {
	runId := armadaevents.UuidFromProtoUuid(jobRunRunning.GetRunId())
	return []DbOperation{MarkRunsRunning{runId: true}}, nil
}

func (c *InstructionConverter) handleJobRunSucceeded(jobRunSucceeded *armadaevents.JobRunSucceeded) ([]DbOperation, error) {
	runId := armadaevents.UuidFromProtoUuid(jobRunSucceeded.GetRunId())
	return []DbOperation{MarkRunsSucceeded{runId: true}}, nil
}

func (c *InstructionConverter) handleJobRunErrors(jobRunErrors *armadaevents.JobRunErrors) ([]DbOperation, error) {
	jobId := jobRunErrors.GetJobId()
	runId := jobRunErrors.GetRunId()
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
	for _, runError := range jobRunErrors.GetErrors() {
		bytes, err := proto.Marshal(runError)
		if err != nil {
			return nil, errors.Wrap(err, "failed to marshal RunError")
		}

		// To ensure inserts are idempotent, each row must have a unique deterministic.
		// We use the hash of (job id, error message),
		// which isn't entirely correct since it deduplicates identical error message.
		hash := sha256.Sum256(append(bytes, append(jobIdBytes, runIdBytes...)...))
		key := int32(binary.BigEndian.Uint32(hash[:]))
		insertJobRunErrors[key] = &sqlc.JobRunError{
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
	}
	return []DbOperation{insertJobRunErrors}, nil
}

func (c *InstructionConverter) handleJobSucceeded(jobSucceeded *armadaevents.JobSucceeded) ([]DbOperation, error) {
	jobId := armadaevents.UuidFromProtoUuid(jobSucceeded.GetJobId())
	return []DbOperation{MarkJobsSucceeded{
		jobId: true,
	}}, nil
}

func (c *InstructionConverter) handleJobErrors(jobErrors *armadaevents.JobErrors) ([]DbOperation, error) {
	jobId := jobErrors.GetJobId()
	jobIdBytes, err := proto.Marshal(jobId)
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal jobId")
	}
	insertJobErrors := make(InsertJobErrors)
	markJobsFailed := make(MarkJobsFailed)
	for _, jobError := range jobErrors.GetErrors() {
		bytes, err := proto.Marshal(jobError)
		if err != nil {
			return nil, errors.Wrap(err, "failed to marshal JobError")
		}

		// To ensure inserts are idempotent, each row must have a unique deterministic.
		// We use the hash of (job id, error message),
		// which isn't entirely correct since it deduplicates identical error message.
		hash := sha256.Sum256(append(bytes, jobIdBytes...))
		key := int32(binary.BigEndian.Uint32(hash[:]))
		insertJobErrors[key] = &sqlc.JobError{
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
	}
	return []DbOperation{insertJobErrors}, nil
}

func (c *InstructionConverter) handleReprioritiseJob(reprioritiseJob *armadaevents.ReprioritiseJob) ([]DbOperation, error) {
	jobId := armadaevents.UuidFromProtoUuid(reprioritiseJob.GetJobId())
	return []DbOperation{UpdateJobPriorities{
		jobId: int64(reprioritiseJob.Priority),
	}}, nil
}

func (c *InstructionConverter) handleReprioritiseJobSet(reprioritiseJobSet *armadaevents.ReprioritiseJobSet, meta eventSequenceCommon) ([]DbOperation, error) {
	return []DbOperation{UpdateJobSetPriorities{
		meta.jobset: int64(reprioritiseJobSet.Priority),
	}}, nil
}

func (c *InstructionConverter) handleCancelJob(cancelJob *armadaevents.CancelJob) ([]DbOperation, error) {
	jobId := armadaevents.UuidFromProtoUuid(cancelJob.GetJobId())
	return []DbOperation{MarkJobsCancelled{
		jobId: true,
	}}, nil
}

func (c *InstructionConverter) handleCancelJobSet(jobset string) ([]DbOperation, error) {
	return []DbOperation{MarkJobSetsCancelled{
		jobset: true,
	}}, nil
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
			// TODO: We should not pass in nil here. Priority will not be set correctly.
			PodRequirements: schedulerobjects.PodRequirementsFromPodSpec(podSpec, nil),
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
