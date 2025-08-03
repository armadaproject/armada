package clickhouseingester

import (
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/utils/pointer"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/database/lookout"
	"github.com/armadaproject/armada/internal/common/eventutil"
	"github.com/armadaproject/armada/internal/common/ingest/utils"
	armadamaps "github.com/armadaproject/armada/internal/common/maps"
	protoutil "github.com/armadaproject/armada/internal/common/proto"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

type InstructionConverter struct {
	userAnnotationPrefix string
}

func NewInstructionConverter(userAnnotationPrefix string) *InstructionConverter {
	return &InstructionConverter{
		userAnnotationPrefix: userAnnotationPrefix,
	}
}

func (c *InstructionConverter) Convert(ctx *armadacontext.Context, sequences *utils.EventsWithIds[*armadaevents.EventSequence]) *Instructions {
	instructions := &Instructions{
		MessageIds: sequences.MessageIds,
		Rows:       []JobRow{},
	}
	for _, es := range sequences.Events {
		for _, event := range es.Events {
			row, err := c.convertEvent(
				ctx,
				es.Queue,
				es.JobSetName,
				es.UserId,
				event,
			)
			if err != nil {
				ctx.Logger().WithError(err).Warnf("Could not convert event")
			} else if row != nil {
				instructions.Rows = append(instructions.Rows, *row)
			}
		}
	}
	return instructions
}

func (c *InstructionConverter) convertEvent(
	ctx *armadacontext.Context,
	queue,
	jobset,
	owner string,
	event *armadaevents.EventSequence_Event,
) (*JobRow, error) {
	ts := protoutil.ToStdTime(event.Created)
	switch event.GetEvent().(type) {
	case *armadaevents.EventSequence_Event_SubmitJob:
		return c.handleSubmitJob(queue, owner, jobset, ts, event.GetSubmitJob())
	case *armadaevents.EventSequence_Event_ReprioritisedJob:
		return c.handleReprioritiseJob(ts, event.GetReprioritisedJob())
	case *armadaevents.EventSequence_Event_CancelledJob:
		return c.handleCancelledJob(ts, event.GetCancelledJob())
	case *armadaevents.EventSequence_Event_JobSucceeded:
		return c.handleJobSucceeded(ts, event.GetJobSucceeded())
	case *armadaevents.EventSequence_Event_JobErrors:
		return c.handleJobErrors(ts, event.GetJobErrors())
	case *armadaevents.EventSequence_Event_JobRunAssigned:
		return c.handleJobRunAssigned(ts, event.GetJobRunAssigned())
	case *armadaevents.EventSequence_Event_JobRunRunning:
		return c.handleJobRunRunning(ts, event.GetJobRunRunning())
	case *armadaevents.EventSequence_Event_JobRunCancelled:
		return c.handleJobRunCancelled(ts, event.GetJobRunCancelled())
	case *armadaevents.EventSequence_Event_JobRunSucceeded:
		return c.handleJobRunSucceeded(ts, event.GetJobRunSucceeded())
	case *armadaevents.EventSequence_Event_JobRunErrors:
		return c.handleJobRunErrors(ts, event.GetJobRunErrors())
	case *armadaevents.EventSequence_Event_JobRunPreempted:
		return c.handleJobRunPreempted(ts, event.GetJobRunPreempted())
	case *armadaevents.EventSequence_Event_JobRequeued:
		return c.handleJobRequeued(ts, event.GetJobRequeued())
	case *armadaevents.EventSequence_Event_JobRunLeased:
		return c.handleJobRunLeased(ts, event.GetJobRunLeased())
	default:
		ctx.Debugf("Ignoring event %T", event.GetEvent())
		return nil, nil
	}
}

func (c *InstructionConverter) handleSubmitJob(
	queue,
	owner,
	jobSet string,
	ts time.Time,
	event *armadaevents.SubmitJob,
) (*JobRow, error) {
	apiJob, err := eventutil.ApiJobFromLogSubmitJob(owner, []string{}, queue, jobSet, ts, event)
	if err != nil {
		return nil, err
	}
	resources := getJobResources(apiJob)
	priorityClass := apiJob.GetMainPodSpec().PriorityClassName

	annotations := event.GetObjectMeta().GetAnnotations()
	userAnnotations := armadamaps.MapKeys(annotations, func(k string) string {
		return strings.TrimPrefix(k, c.userAnnotationPrefix)
	})

	return &JobRow{
		JobID:              event.JobId,
		Queue:              &queue,
		Namespace:          &apiJob.Namespace,
		JobSet:             &jobSet,
		CPU:                &resources.Cpu,
		Memory:             &resources.Memory,
		EphemeralStorage:   &resources.EphemeralStorage,
		GPU:                &resources.Gpu,
		Priority:           pointer.Int64(int64(event.Priority)),
		SubmitTS:           &ts,
		PriorityClass:      &priorityClass,
		Annotations:        userAnnotations,
		JobState:           pointer.String(string(lookout.JobQueued)),
		LastTransitionTime: &ts,
		LastUpdateTS:       ts,
		Merged:             pointer.Bool(true),
	}, nil
}

func (c *InstructionConverter) handleReprioritiseJob(ts time.Time, event *armadaevents.ReprioritisedJob) (*JobRow, error) {
	return &JobRow{
		JobID:        event.JobId,
		Priority:     pointer.Int64(int64(event.Priority)),
		LastUpdateTS: ts,
	}, nil
}

func (c *InstructionConverter) handleCancelledJob(ts time.Time, event *armadaevents.CancelledJob) (*JobRow, error) {
	return &JobRow{
		JobID:              event.JobId,
		JobState:           pointer.String(string(lookout.JobCancelled)),
		CancelTS:           &ts,
		CancelReason:       &event.Reason,
		CancelUser:         &event.CancelUser,
		LastTransitionTime: &ts,
		LastUpdateTS:       ts,
	}, nil
}

func (c *InstructionConverter) handleJobSucceeded(ts time.Time, event *armadaevents.JobSucceeded) (*JobRow, error) {
	return &JobRow{
		JobID:              event.JobId,
		JobState:           pointer.String(string(lookout.JobSucceeded)),
		RunFinishedTS:      &ts,
		LastTransitionTime: &ts,
		LastUpdateTS:       ts,
	}, nil
}

func (c *InstructionConverter) handleJobErrors(ts time.Time, event *armadaevents.JobErrors) (*JobRow, error) {
	for _, e := range event.GetErrors() {
		// We don't care about non-terminal errors
		if !e.Terminal {
			continue
		}

		state := lookout.JobFailed
		switch e.Reason.(type) {
		// Preempted and Rejected jobs are modelled as Reasons on a JobErrors msg
		case *armadaevents.Error_JobRunPreemptedError:
			state = lookout.JobPreempted
		case *armadaevents.Error_JobRejected:
			state = lookout.JobRejected
		}

		return &JobRow{
			JobID:              event.JobId,
			JobState:           pointer.String(string(state)),
			LastTransitionTime: &ts,
			LastUpdateTS:       ts,
		}, nil
	}
	return nil, nil
}

func (c *InstructionConverter) handleJobRunRunning(ts time.Time, event *armadaevents.JobRunRunning) (*JobRow, error) {
	return &JobRow{
		JobID:              event.JobId,
		JobState:           pointer.String(string(lookout.JobRunning)),
		RunState:           pointer.String(string(lookout.JobRunRunning)),
		RunStartedTS:       &ts,
		LastTransitionTime: &ts,
		LastUpdateTS:       ts,
	}, nil
}

func (c *InstructionConverter) handleJobRequeued(ts time.Time, event *armadaevents.JobRequeued) (*JobRow, error) {
	return &JobRow{
		JobID:              event.JobId,
		JobState:           pointer.String(string(lookout.JobQueued)),
		LastTransitionTime: &ts,
		LastUpdateTS:       ts,
	}, nil
}

func (c *InstructionConverter) handleJobRunLeased(ts time.Time, event *armadaevents.JobRunLeased) (*JobRow, error) {
	return &JobRow{
		JobID:              event.JobId,
		JobState:           pointer.String(string(lookout.JobLeased)),
		LatestRunID:        &event.RunId,
		RunCluster:         &event.ExecutorId,
		RunState:           pointer.String(string(lookout.JobRunLeased)),
		RunNode:            &event.NodeId,
		RunLeased:          &ts,
		LastTransitionTime: &ts,
		LastUpdateTS:       ts,
	}, nil
}

func (c *InstructionConverter) handleJobRunAssigned(ts time.Time, event *armadaevents.JobRunAssigned) (*JobRow, error) {
	return &JobRow{
		JobID:              event.JobId,
		JobState:           pointer.String(string(lookout.JobPending)),
		RunState:           pointer.String(string(lookout.JobRunPending)),
		RunPendingTS:       &ts,
		LastTransitionTime: &ts,
		LastUpdateTS:       ts,
	}, nil
}

func (c *InstructionConverter) handleJobRunCancelled(ts time.Time, event *armadaevents.JobRunCancelled) (*JobRow, error) {
	return &JobRow{
		JobID:         event.JobId,
		RunState:      pointer.String(string(lookout.JobRunCancelled)),
		RunFinishedTS: &ts,
		LastUpdateTS:  ts,
	}, nil
}

func (c *InstructionConverter) handleJobRunSucceeded(ts time.Time, event *armadaevents.JobRunSucceeded) (*JobRow, error) {
	return &JobRow{
		JobID:         event.JobId,
		RunState:      pointer.String(string(lookout.JobRunSucceeded)),
		RunFinishedTS: &ts,
		LastUpdateTS:  ts,
	}, nil
}

func (c *InstructionConverter) handleJobRunErrors(ts time.Time, event *armadaevents.JobRunErrors) (*JobRow, error) {
	for _, e := range event.GetErrors() {

		if !e.Terminal {
			continue
		}

		var exitCode int32 = 0
		var runState string

		switch reason := e.Reason.(type) {
		case *armadaevents.Error_PodError:
			for _, containerError := range reason.PodError.ContainerErrors {
				if containerError.ExitCode != 0 {
					exitCode = containerError.ExitCode
					break
				}
			}
			runState = string(lookout.JobRunFailed)
		case *armadaevents.Error_PodLeaseReturned:
			runState = string(lookout.JobRunLeaseReturned)
		case *armadaevents.Error_LeaseExpired:
			runState = string(lookout.JobRunLeaseExpired)
		case *armadaevents.Error_JobRunPreemptedError:
			// This case is already handled by the JobRunPreempted event
			// When we formalise that as a terminal event, we'll remove this JobRunError getting produced
			continue
		default:
			runState = string(lookout.JobRunFailed)
		}
		return &JobRow{
			JobID:         event.JobId,
			RunState:      &runState,
			RunExitCode:   &exitCode,
			RunFinishedTS: &ts,
			LastUpdateTS:  ts,
		}, nil
	}
	return nil, nil
}

func (c *InstructionConverter) handleJobRunPreempted(ts time.Time, event *armadaevents.JobRunPreempted) (*JobRow, error) {
	return &JobRow{
		JobID:         event.PreemptedRunId,
		RunState:      pointer.String(string(lookout.JobRunPreempted)),
		RunFinishedTS: &ts,
		LastUpdateTS:  ts,
	}, nil
}

func getJobResources(job *api.Job) jobResources {
	resources := jobResources{}

	podSpec := job.GetMainPodSpec()

	for _, container := range podSpec.Containers {
		resources.Cpu += getResource(container, v1.ResourceCPU, true)
		resources.Memory += getResource(container, v1.ResourceMemory, false)
		resources.EphemeralStorage += getResource(container, v1.ResourceEphemeralStorage, false)
		resources.Gpu += getResource(container, "nvidia.com/gpu", false)
	}

	return resources
}

func getResource(container v1.Container, resourceName v1.ResourceName, useMillis bool) int64 {
	resource, ok := container.Resources.Requests[resourceName]
	if !ok {
		return 0
	}
	if useMillis {
		return resource.MilliValue()
	}
	return resource.Value()
}
