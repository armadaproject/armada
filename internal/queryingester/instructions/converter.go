package instructions

import (
	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/ingest/utils"
	protoutil "github.com/armadaproject/armada/internal/common/proto"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

type Converter struct {
	userAnnotationPrefix string
}

func NewConverter(userAnnotationPrefix string) *Converter {
	return &Converter{
		userAnnotationPrefix: userAnnotationPrefix,
	}
}

func (c *Converter) Convert(ctx *armadacontext.Context, sequences *utils.EventsWithIds[*armadaevents.EventSequence]) *Instructions {
	instructions := &Instructions{
		MessageIds: sequences.MessageIds,
	}
	for _, es := range sequences.Events {
		for _, event := range es.Events {
			update, err := c.toInstruction(
				ctx,
				es.Queue,
				es.JobSetName,
				es.UserId,
				event,
			)
			if err != nil {
				ctx.Logger().WithError(err).Warnf("Could not convert event")
			}
			instructions.Add(update)
		}
	}
	return instructions
}

func (c *Converter) toInstruction(
	ctx *armadacontext.Context,
	queue,
	jobset,
	owner string,
	event *armadaevents.EventSequence_Event,
) (Update, error) {
	ts := protoutil.ToStdTime(event.Created)
	switch event.GetEvent().(type) {
	case *armadaevents.EventSequence_Event_SubmitJob:
		return handleSubmitJob(queue, owner, jobset, c.userAnnotationPrefix, ts, event.GetSubmitJob())
	case *armadaevents.EventSequence_Event_ReprioritisedJob:
		return handleReprioritiseJob(ts, queue, event.GetReprioritisedJob())
	case *armadaevents.EventSequence_Event_CancelledJob:
		return handleCancelledJob(ts, queue, event.GetCancelledJob())
	case *armadaevents.EventSequence_Event_JobSucceeded:
		return handleJobSucceeded(ts, queue, event.GetJobSucceeded())
	case *armadaevents.EventSequence_Event_JobErrors:
		return handleJobErrors(ts, queue, event.GetJobErrors())
	case *armadaevents.EventSequence_Event_JobRunAssigned:
		return handleJobRunAssigned(ts, queue, event.GetJobRunAssigned())
	case *armadaevents.EventSequence_Event_JobRunRunning:
		return handleJobRunRunning(ts, queue, event.GetJobRunRunning())
	case *armadaevents.EventSequence_Event_JobRunCancelled:
		return handleJobRunCancelled(ts, queue, event.GetJobRunCancelled())
	case *armadaevents.EventSequence_Event_JobRunSucceeded:
		return handleJobRunSucceeded(ts, queue, event.GetJobRunSucceeded())
	case *armadaevents.EventSequence_Event_JobRunErrors:
		return handleJobRunErrors(ts, queue, event.GetJobRunErrors())
	case *armadaevents.EventSequence_Event_JobRunPreempted:
		return handleJobRunPreempted(ts, queue, event.GetJobRunPreempted())
	case *armadaevents.EventSequence_Event_JobRequeued:
		return handleJobRequeued(ts, queue, event.GetJobRequeued())
	case *armadaevents.EventSequence_Event_JobRunLeased:
		return handleJobRunLeased(ts, queue, event.GetJobRunLeased())
	default:
		ctx.Debugf("Ignoring event %T", event.GetEvent())
		return Update{}, nil
	}
}
