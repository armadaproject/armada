package scheduleringester

import (
	"context"

	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/armadaproject/armada/internal/common/compress"

	"github.com/armadaproject/armada/internal/common/ingest"
	"github.com/armadaproject/armada/internal/common/ingest/metrics"
	schedulerdb "github.com/armadaproject/armada/internal/scheduler/database"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/pkg/armadaevents"
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
	compressor  compress.Compressor
}

func NewInstructionConverter(metrics *metrics.Metrics,
	filter func(event *armadaevents.EventSequence_Event) bool, compressor compress.Compressor,
) ingest.InstructionConverter[*DbOperationsWithMessageIds] {
	return &InstructionConverter{
		metrics:     metrics,
		eventFilter: filter,
		compressor:  compressor,
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

	compressedSubmitJobBytes, err := c.compressor.Compress(submitJobBytes)
	if err != nil {
		return nil, err
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
	compressedGroups, err := compress.CompressStringArray(meta.groups, c.compressor)
	if err != nil {
		return nil, err
	}

	jobId, err := armadaevents.UlidStringFromProtoUuid(job.JobId)
	if err != nil {
		return nil, err
	}
	return []DbOperation{InsertJobs{jobId: &schedulerdb.Job{
		JobID:          jobId,
		JobSet:         meta.jobset,
		UserID:         meta.user,
		Groups:         compressedGroups,
		Queue:          meta.queue,
		Priority:       int64(job.Priority),
		SubmitMessage:  compressedSubmitJobBytes,
		SchedulingInfo: schedulingInfoBytes,
	}}}, nil
}

func (c *InstructionConverter) handleJobRunLeased(jobRunLeased *armadaevents.JobRunLeased, meta eventSequenceCommon) ([]DbOperation, error) {
	runId := armadaevents.UuidFromProtoUuid(jobRunLeased.GetRunId())
	jobId, err := armadaevents.UlidStringFromProtoUuid(jobRunLeased.GetJobId())
	if err != nil {
		return nil, err
	}
	return []DbOperation{InsertRuns{runId: &schedulerdb.Run{
		RunID:    runId,
		JobID:    jobId,
		JobSet:   meta.jobset,
		Executor: jobRunLeased.GetExecutorId(),
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
	runId := jobRunErrors.GetRunId()
	for _, runError := range jobRunErrors.GetErrors() {
		// For terminal errors, we also need to mark the run as failed.
		if runError.GetTerminal() {
			markRunsFailed := make(MarkRunsFailed)
			markRunsFailed[armadaevents.UuidFromProtoUuid(runId)] = true
			return []DbOperation{markRunsFailed}, nil
		}
	}
	return nil, nil
}

func (c *InstructionConverter) handleJobSucceeded(jobSucceeded *armadaevents.JobSucceeded) ([]DbOperation, error) {
	jobId, err := armadaevents.UlidStringFromProtoUuid(jobSucceeded.GetJobId())
	if err != nil {
		return nil, err
	}
	return []DbOperation{MarkJobsSucceeded{
		jobId: true,
	}}, nil
}

func (c *InstructionConverter) handleJobErrors(jobErrors *armadaevents.JobErrors) ([]DbOperation, error) {
	jobId, err := armadaevents.UlidStringFromProtoUuid(jobErrors.GetJobId())
	if err != nil {
		return nil, err
	}
	for _, jobError := range jobErrors.GetErrors() {
		// For terminal errors, we also need to mark the job as failed.
		if jobError.GetTerminal() {
			markJobsFailed := make(MarkJobsFailed)
			markJobsFailed[jobId] = true
			return []DbOperation{markJobsFailed}, nil
		}
	}
	return nil, nil
}

func (c *InstructionConverter) handleReprioritiseJob(reprioritiseJob *armadaevents.ReprioritiseJob) ([]DbOperation, error) {
	jobId, err := armadaevents.UlidStringFromProtoUuid(reprioritiseJob.GetJobId())
	if err != nil {
		return nil, err
	}
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
	jobId, err := armadaevents.UlidStringFromProtoUuid(cancelJob.GetJobId())
	if err != nil {
		return nil, err
	}
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
