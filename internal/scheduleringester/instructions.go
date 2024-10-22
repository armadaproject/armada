package scheduleringester

import (
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/compress"
	"github.com/armadaproject/armada/internal/common/ingest/metrics"
	"github.com/armadaproject/armada/internal/common/ingest/utils"
	protoutil "github.com/armadaproject/armada/internal/common/proto"
	"github.com/armadaproject/armada/internal/scheduler/adapters"
	schedulerdb "github.com/armadaproject/armada/internal/scheduler/database"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/internal/server/configuration"
	"github.com/armadaproject/armada/pkg/armadaevents"
	"github.com/armadaproject/armada/pkg/controlplaneevents"
)

type eventSequenceCommon struct {
	queue  string
	jobset string
	user   string
	groups []string
}

type JobSetEventsInstructionConverter struct {
	metrics    *metrics.Metrics
	compressor compress.Compressor
}

type ControlPlaneEventsInstructionConverter struct {
	metrics *metrics.Metrics
}

func NewJobSetEventsInstructionConverter(
	metrics *metrics.Metrics,
) (*JobSetEventsInstructionConverter, error) {
	compressor, err := compress.NewZlibCompressor(1024)
	if err != nil {
		return nil, errors.WithMessage(err, "failed to create compressor")
	}
	return &JobSetEventsInstructionConverter{
		metrics:    metrics,
		compressor: compressor,
	}, nil
}

func (c *JobSetEventsInstructionConverter) Convert(ctx *armadacontext.Context, eventsWithIds *utils.EventsWithIds[*armadaevents.EventSequence]) *DbOperationsWithMessageIds {
	operations := make([]DbOperation, 0)
	for _, es := range eventsWithIds.Events {
		for _, op := range c.dbOperationsFromEventSequence(es) {
			operations = AppendDbOperation(operations, op)
		}
	}
	log.Infof("Converted sequences into %d db operations", len(operations))
	return &DbOperationsWithMessageIds{
		Ops:        operations,
		MessageIds: eventsWithIds.MessageIds,
	}
}

func (c *JobSetEventsInstructionConverter) dbOperationsFromEventSequence(es *armadaevents.EventSequence) []DbOperation {
	meta := eventSequenceCommon{
		queue:  es.Queue,
		jobset: es.JobSetName,
		user:   es.UserId,
		groups: es.Groups,
	}
	operations := make([]DbOperation, 0, len(es.Events))
	for idx, event := range es.Events {
		eventTime := protoutil.ToStdTime(event.Created)
		var err error = nil
		var operationsFromEvent []DbOperation
		switch eventType := event.GetEvent().(type) {
		case *armadaevents.EventSequence_Event_SubmitJob:
			operationsFromEvent, err = c.handleSubmitJob(event.GetSubmitJob(), eventTime, meta)
		case *armadaevents.EventSequence_Event_JobRunLeased:
			operationsFromEvent, err = c.handleJobRunLeased(event.GetJobRunLeased(), eventTime, meta)
		case *armadaevents.EventSequence_Event_JobRunRunning:
			operationsFromEvent, err = c.handleJobRunRunning(event.GetJobRunRunning(), eventTime)
		case *armadaevents.EventSequence_Event_JobRunSucceeded:
			operationsFromEvent, err = c.handleJobRunSucceeded(event.GetJobRunSucceeded(), eventTime)
		case *armadaevents.EventSequence_Event_JobRunErrors:
			operationsFromEvent, err = c.handleJobRunErrors(event.GetJobRunErrors(), eventTime)
		case *armadaevents.EventSequence_Event_JobSucceeded:
			operationsFromEvent, err = c.handleJobSucceeded(event.GetJobSucceeded())
		case *armadaevents.EventSequence_Event_JobErrors:
			operationsFromEvent, err = c.handleJobErrors(event.GetJobErrors())
		case *armadaevents.EventSequence_Event_JobPreemptionRequested:
			operationsFromEvent, err = c.handleJobPreemptionRequested(event.GetJobPreemptionRequested(), meta)
		case *armadaevents.EventSequence_Event_ReprioritiseJob:
			operationsFromEvent, err = c.handleReprioritiseJob(event.GetReprioritiseJob(), meta)
		case *armadaevents.EventSequence_Event_ReprioritiseJobSet:
			operationsFromEvent, err = c.handleReprioritiseJobSet(event.GetReprioritiseJobSet(), meta)
		case *armadaevents.EventSequence_Event_CancelJob:
			operationsFromEvent, err = c.handleCancelJob(event.GetCancelJob(), meta)
		case *armadaevents.EventSequence_Event_CancelJobSet:
			operationsFromEvent, err = c.handleCancelJobSet(event.GetCancelJobSet(), meta)
		case *armadaevents.EventSequence_Event_CancelledJob:
			operationsFromEvent, err = c.handleCancelledJob(event.GetCancelledJob(), eventTime)
		case *armadaevents.EventSequence_Event_JobRequeued:
			operationsFromEvent, err = c.handleJobRequeued(event.GetJobRequeued())
		case *armadaevents.EventSequence_Event_PartitionMarker:
			operationsFromEvent, err = c.handlePartitionMarker(event.GetPartitionMarker(), eventTime)
		case *armadaevents.EventSequence_Event_JobRunPreempted:
			operationsFromEvent, err = c.handleJobRunPreempted(event.GetJobRunPreempted(), eventTime)
		case *armadaevents.EventSequence_Event_JobRunAssigned:
			operationsFromEvent, err = c.handleJobRunAssigned(event.GetJobRunAssigned(), eventTime)
		case *armadaevents.EventSequence_Event_JobValidated:
			operationsFromEvent, err = c.handleJobValidated(event.GetJobValidated())
		case *armadaevents.EventSequence_Event_ReprioritisedJob,
			*armadaevents.EventSequence_Event_ResourceUtilisation,
			*armadaevents.EventSequence_Event_JobRunCancelled,
			*armadaevents.EventSequence_Event_StandaloneIngressInfo:
			// These events can all be safely ignored
			log.Debugf("Ignoring event type %T", event)
		default:
			// This is an event type we haven't considered. Log a warning
			log.Warnf("Ignoring unknown event type %T", eventType)
		}
		if err != nil {
			c.metrics.RecordPulsarMessageError(metrics.PulsarMessageErrorProcessing)
			log.WithError(err).Errorf("Could not convert event at index %d.", idx)
		} else {
			operations = append(operations, operationsFromEvent...)
		}
	}
	return operations
}

func (c *JobSetEventsInstructionConverter) handleSubmitJob(job *armadaevents.SubmitJob, submitTime time.Time, meta eventSequenceCommon) ([]DbOperation, error) {
	jobId := job.JobId

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
	schedulingInfo, err := c.schedulingInfoFromSubmitJob(job, submitTime)
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

	return []DbOperation{InsertJobs{jobId: &schedulerdb.Job{
		JobID:                 jobId,
		JobSet:                meta.jobset,
		UserID:                meta.user,
		Groups:                compressedGroups,
		Queue:                 meta.queue,
		Queued:                true,
		QueuedVersion:         0,
		Submitted:             submitTime.UnixNano(),
		Priority:              int64(job.Priority),
		SubmitMessage:         compressedSubmitJobBytes,
		SchedulingInfo:        schedulingInfoBytes,
		SchedulingInfoVersion: int32(schedulingInfo.Version),
	}}}, nil
}

func (c *JobSetEventsInstructionConverter) handleJobRunLeased(jobRunLeased *armadaevents.JobRunLeased, eventTime time.Time, meta eventSequenceCommon) ([]DbOperation, error) {
	runId := jobRunLeased.RunId
	var scheduledAtPriority *int32
	if jobRunLeased.HasScheduledAtPriority {
		scheduledAtPriority = &jobRunLeased.ScheduledAtPriority
	}
	PodRequirementsOverlay, err := proto.Marshal(jobRunLeased.GetPodRequirementsOverlay())
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return []DbOperation{
		InsertRuns{runId: &JobRunDetails{
			Queue: meta.queue,
			DbRun: &schedulerdb.Run{
				RunID:                  runId,
				JobID:                  jobRunLeased.JobId,
				Created:                eventTime.UnixNano(),
				JobSet:                 meta.jobset,
				Queue:                  meta.queue,
				Executor:               jobRunLeased.GetExecutorId(),
				Node:                   jobRunLeased.GetNodeId(),
				Pool:                   jobRunLeased.GetPool(),
				ScheduledAtPriority:    scheduledAtPriority,
				LeasedTimestamp:        &eventTime,
				PodRequirementsOverlay: PodRequirementsOverlay,
			},
		}},
		UpdateJobQueuedState{jobRunLeased.JobId: &JobQueuedStateUpdate{
			Queued:             false,
			QueuedStateVersion: jobRunLeased.UpdateSequenceNumber,
		}},
	}, nil
}

func (c *JobSetEventsInstructionConverter) handleJobRequeued(jobRequeued *armadaevents.JobRequeued) ([]DbOperation, error) {
	schedulingInfoBytes, err := proto.Marshal(jobRequeued.SchedulingInfo)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return []DbOperation{
		UpdateJobQueuedState{jobRequeued.JobId: &JobQueuedStateUpdate{
			Queued:             true,
			QueuedStateVersion: jobRequeued.UpdateSequenceNumber,
		}},
		UpdateJobSchedulingInfo{jobRequeued.JobId: &JobSchedulingInfoUpdate{
			JobSchedulingInfo:        schedulingInfoBytes,
			JobSchedulingInfoVersion: int32(jobRequeued.SchedulingInfo.Version),
		}},
	}, nil
}

func (c *JobSetEventsInstructionConverter) handleJobRunRunning(jobRunRunning *armadaevents.JobRunRunning, runningTime time.Time) ([]DbOperation, error) {
	runId := jobRunRunning.RunId
	return []DbOperation{MarkRunsRunning{runId: runningTime}}, nil
}

func (c *JobSetEventsInstructionConverter) handleJobRunSucceeded(jobRunSucceeded *armadaevents.JobRunSucceeded, successTime time.Time) ([]DbOperation, error) {
	runId := jobRunSucceeded.RunId
	return []DbOperation{MarkRunsSucceeded{runId: successTime}}, nil
}

func (c *JobSetEventsInstructionConverter) handleJobRunPreempted(jobRunPreempted *armadaevents.JobRunPreempted, preemptedTime time.Time) ([]DbOperation, error) {
	runId := jobRunPreempted.PreemptedRunId
	return []DbOperation{MarkRunsPreempted{runId: preemptedTime}}, nil
}

func (c *JobSetEventsInstructionConverter) handleJobRunAssigned(jobRunAssigned *armadaevents.JobRunAssigned, assignedTime time.Time) ([]DbOperation, error) {
	runId := jobRunAssigned.RunId
	return []DbOperation{MarkRunsPending{runId: assignedTime}}, nil
}

func (c *JobSetEventsInstructionConverter) handleJobRunErrors(jobRunErrors *armadaevents.JobRunErrors, failureTime time.Time) ([]DbOperation, error) {
	runId := jobRunErrors.RunId
	jobId := jobRunErrors.JobId
	insertJobRunErrors := make(InsertJobRunErrors)
	markRunsFailed := make(MarkRunsFailed)
	for _, runError := range jobRunErrors.GetErrors() {
		// There should only be one terminal error.
		if runError.GetTerminal() {
			bytes, err := protoutil.MarshallAndCompress(runError, c.compressor)
			if err != nil {
				return nil, errors.Wrap(err, "failed to marshal RunError")
			}
			insertJobRunErrors[runId] = &schedulerdb.JobRunError{
				RunID: runId,
				JobID: jobId,
				Error: bytes,
			}
			runAttempted := true
			if runError.GetPodLeaseReturned() != nil {
				runAttempted = runError.GetPodLeaseReturned().RunAttempted
			}
			markRunsFailed[runId] = &JobRunFailed{
				LeaseReturned: runError.GetPodLeaseReturned() != nil,
				RunAttempted:  runAttempted,
				FailureTime:   failureTime,
			}
			return []DbOperation{insertJobRunErrors, markRunsFailed}, nil
		}
	}
	return nil, nil
}

func (c *JobSetEventsInstructionConverter) handleJobSucceeded(jobSucceeded *armadaevents.JobSucceeded) ([]DbOperation, error) {
	return []DbOperation{MarkJobsSucceeded{
		jobSucceeded.JobId: true,
	}}, nil
}

func (c *JobSetEventsInstructionConverter) handleJobErrors(jobErrors *armadaevents.JobErrors) ([]DbOperation, error) {
	for _, jobError := range jobErrors.GetErrors() {
		// For terminal errors, we also need to mark the job as failed.
		if jobError.GetTerminal() {
			markJobsFailed := make(MarkJobsFailed)
			markJobsFailed[jobErrors.JobId] = true
			return []DbOperation{markJobsFailed}, nil
		}
	}
	return nil, nil
}

func (c *JobSetEventsInstructionConverter) handleJobPreemptionRequested(preemptionRequested *armadaevents.JobPreemptionRequested, meta eventSequenceCommon) ([]DbOperation, error) {
	return []DbOperation{MarkRunsForJobPreemptRequested{
		JobSetKey{
			queue:  meta.queue,
			jobSet: meta.jobset,
		}: []string{preemptionRequested.JobId},
	}}, nil
}

func (c *JobSetEventsInstructionConverter) handleReprioritiseJob(reprioritiseJob *armadaevents.ReprioritiseJob, meta eventSequenceCommon) ([]DbOperation, error) {
	return []DbOperation{&UpdateJobPriorities{
		key: JobReprioritiseKey{
			JobSetKey: JobSetKey{
				queue:  meta.queue,
				jobSet: meta.jobset,
			},
			Priority: int64(reprioritiseJob.Priority),
		},
		jobIds: []string{reprioritiseJob.JobId},
	}}, nil
}

func (c *JobSetEventsInstructionConverter) handleReprioritiseJobSet(reprioritiseJobSet *armadaevents.ReprioritiseJobSet, meta eventSequenceCommon) ([]DbOperation, error) {
	return []DbOperation{UpdateJobSetPriorities{
		JobSetKey{queue: meta.queue, jobSet: meta.jobset}: int64(reprioritiseJobSet.Priority),
	}}, nil
}

func (c *JobSetEventsInstructionConverter) handleCancelJob(cancelJob *armadaevents.CancelJob, meta eventSequenceCommon) ([]DbOperation, error) {
	return []DbOperation{MarkJobsCancelRequested{
		JobSetKey{
			queue:  meta.queue,
			jobSet: meta.jobset,
		}: []string{cancelJob.JobId},
	}}, nil
}

func (c *JobSetEventsInstructionConverter) handleCancelJobSet(cancelJobSet *armadaevents.CancelJobSet, meta eventSequenceCommon) ([]DbOperation, error) {
	cancelQueued := len(cancelJobSet.States) == 0 || slices.Contains(cancelJobSet.States, armadaevents.JobState_QUEUED)
	cancelLeased := len(cancelJobSet.States) == 0 || slices.Contains(cancelJobSet.States, armadaevents.JobState_PENDING) || slices.Contains(cancelJobSet.States, armadaevents.JobState_RUNNING)

	return []DbOperation{MarkJobSetsCancelRequested{
		JobSetKey{
			queue:  meta.queue,
			jobSet: meta.jobset,
		}: &JobSetCancelAction{
			cancelQueued: cancelQueued,
			cancelLeased: cancelLeased,
		},
	}}, nil
}

func (c *JobSetEventsInstructionConverter) handleCancelledJob(cancelledJob *armadaevents.CancelledJob, cancelTime time.Time) ([]DbOperation, error) {
	return []DbOperation{MarkJobsCancelled{
		cancelledJob.JobId: cancelTime,
	}}, nil
}

func (c *JobSetEventsInstructionConverter) handlePartitionMarker(pm *armadaevents.PartitionMarker, created time.Time) ([]DbOperation, error) {
	return []DbOperation{&InsertPartitionMarker{
		markers: []*schedulerdb.Marker{
			{
				GroupID:     uuid.MustParse(pm.GroupId),
				PartitionID: int32(pm.Partition),
				Created:     created,
			},
		},
	}}, nil
}

func (c *JobSetEventsInstructionConverter) handleJobValidated(checked *armadaevents.JobValidated) ([]DbOperation, error) {
	return []DbOperation{
		MarkJobsValidated{checked.JobId: checked.Pools},
	}, nil
}

func NewControlPlaneEventsInstructionConverter(
	metrics *metrics.Metrics,
) (*ControlPlaneEventsInstructionConverter, error) {
	return &ControlPlaneEventsInstructionConverter{
		metrics: metrics,
	}, nil
}

func (c *ControlPlaneEventsInstructionConverter) Convert(ctx *armadacontext.Context, controlPlaneEvents *utils.EventsWithIds[*controlplaneevents.Event]) *DbOperationsWithMessageIds {
	operations := make([]DbOperation, 0)
	for _, controlPlaneEvent := range controlPlaneEvents.Events {
		for _, op := range c.dbOperationFromControlPlaneEvent(controlPlaneEvent) {
			operations = AppendDbOperation(operations, op)
		}
	}

	log.Infof("Converted events into %d db operations", len(operations))
	return &DbOperationsWithMessageIds{
		Ops:        operations,
		MessageIds: controlPlaneEvents.MessageIds,
	}
}

func (c *ControlPlaneEventsInstructionConverter) dbOperationFromControlPlaneEvent(event *controlplaneevents.Event) []DbOperation {
	var operations []DbOperation
	var err error

	switch ev := event.Event.(type) {
	case *controlplaneevents.Event_ExecutorSettingsUpsert:
		eventTime := protoutil.ToStdTime(event.Created)
		operations, err = c.handleExecutorSettingsUpsert(event.GetExecutorSettingsUpsert(), eventTime)
	case *controlplaneevents.Event_ExecutorSettingsDelete:
		operations, err = c.handleExecutorSettingsDelete(event.GetExecutorSettingsDelete())
	default:
		log.Errorf("Unknown event of type %T", ev)
	}

	if err != nil {
		log.Errorf("Failed to convert event to db operations: %v", err)
	}

	return operations
}

func (c *ControlPlaneEventsInstructionConverter) handleExecutorSettingsUpsert(upsert *controlplaneevents.ExecutorSettingsUpsert, setAtTime time.Time) ([]DbOperation, error) {
	return []DbOperation{
		UpsertExecutorSettings{
			upsert.Name: &ExecutorSettingsUpsert{
				ExecutorID:   upsert.Name,
				Cordoned:     upsert.Cordoned,
				CordonReason: upsert.CordonReason,
				SetByUser:    upsert.SetByUser,
				SetAtTime:    setAtTime,
			},
		},
	}, nil
}

func (c *ControlPlaneEventsInstructionConverter) handleExecutorSettingsDelete(delete *controlplaneevents.ExecutorSettingsDelete) ([]DbOperation, error) {
	return []DbOperation{
		DeleteExecutorSettings{
			delete.Name: &ExecutorSettingsDelete{
				ExecutorID: delete.Name,
			},
		},
	}, nil
}

// schedulingInfoFromSubmitJob returns a minimal representation of a job containing only the info needed by the scheduler.
func (c *JobSetEventsInstructionConverter) schedulingInfoFromSubmitJob(submitJob *armadaevents.SubmitJob, submitTime time.Time) (*schedulerobjects.JobSchedulingInfo, error) {
	return SchedulingInfoFromSubmitJob(submitJob, submitTime)
}

// SchedulingInfoFromSubmitJob returns a minimal representation of a job containing only the info needed by the scheduler.
func SchedulingInfoFromSubmitJob(submitJob *armadaevents.SubmitJob, submitTime time.Time) (*schedulerobjects.JobSchedulingInfo, error) {
	// Component common to all jobs.
	schedulingInfo := &schedulerobjects.JobSchedulingInfo{
		Lifetime:        submitJob.Lifetime,
		AtMostOnce:      submitJob.AtMostOnce,
		Preemptible:     submitJob.Preemptible,
		ConcurrencySafe: submitJob.ConcurrencySafe,
		SubmitTime:      submitTime,
		Priority:        submitJob.Priority,
		Version:         0,
	}

	// Scheduling requirements specific to the objects that make up this job.
	switch object := submitJob.MainObject.Object.(type) {
	case *armadaevents.KubernetesMainObject_PodSpec:
		podSpec := object.PodSpec.PodSpec
		schedulingInfo.PriorityClassName = podSpec.PriorityClassName
		podRequirements := adapters.PodRequirementsFromPodSpec(podSpec)
		if submitJob.ObjectMeta != nil {
			podRequirements.Annotations = maps.Clone(submitJob.ObjectMeta.Annotations)
		}
		if submitJob.MainObject.ObjectMeta != nil {
			if podRequirements.Annotations == nil {
				podRequirements.Annotations = make(map[string]string, len(submitJob.MainObject.ObjectMeta.Annotations))
			}
			for k, v := range submitJob.MainObject.ObjectMeta.Annotations {
				if configuration.IsSchedulingAnnotation(k) {
					podRequirements.Annotations[k] = v
				}
			}
		}
		schedulingInfo.ObjectRequirements = append(
			schedulingInfo.ObjectRequirements,
			&schedulerobjects.ObjectRequirements{
				Requirements: &schedulerobjects.ObjectRequirements_PodRequirements{
					PodRequirements: podRequirements,
				},
			},
		)
	default:
		return nil, errors.Errorf("unsupported object type %T", object)
	}
	return schedulingInfo, nil
}
