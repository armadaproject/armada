package events

import (
	"fmt"

	"github.com/G-Research/armada/internal/common/eventstream"
	"github.com/G-Research/armada/internal/lookout/repository"
	"github.com/G-Research/armada/pkg/api"
)

type EventProcessor struct {
	queue    string
	stream   eventstream.EventStream
	recorder repository.JobRecorder
}

func NewEventProcessor(queue string, stream eventstream.EventStream, repository repository.JobRecorder) *EventProcessor {
	return &EventProcessor{queue: queue, stream: stream, recorder: repository}
}

func (p *EventProcessor) Start() {
	err := p.stream.Subscribe(p.queue, p.handleMessage)
	if err != nil {
		panic(err)
	}
}

func (p *EventProcessor) handleMessage(eventMessage *eventstream.Message) error {
	// TODO: batching???
	event, err := api.UnwrapEvent(eventMessage.EventMessage)
	if err != nil {
		return fmt.Errorf("error while unwrapping event message: %v", err)
	}
	err = p.processEvent(event)
	if err != nil {
		return fmt.Errorf("Error while reporting event from nats: %v (event: %v)", err, eventMessage)
	}
	err = eventMessage.Ack()
	if err != nil {
		return fmt.Errorf("error while attempting to acknowledge event: %v", err)
	}
	return nil
}

func (p *EventProcessor) processEvent(event api.Event) error {
	switch typed := event.(type) {
	case *api.JobSubmittedEvent:
		return p.recorder.RecordJob(&typed.Job, typed.Created)

	case *api.JobQueuedEvent:
		// this event just attest saving job to redis

	case *api.JobDuplicateFoundEvent:
		return p.recorder.RecordJobDuplicate(typed)

	case *api.JobPendingEvent:
		return p.recorder.RecordJobPending(typed)

	case *api.JobRunningEvent:
		return p.recorder.RecordJobRunning(typed)

	case *api.JobSucceededEvent:
		return p.recorder.RecordJobSucceeded(typed)

	case *api.JobFailedEvent:
		return p.recorder.RecordJobFailed(typed)

	case *api.JobLeasedEvent:
	case *api.JobLeaseReturnedEvent:
		return p.recorder.RecordJobUnableToSchedule(&api.JobUnableToScheduleEvent{
			JobId:     typed.JobId,
			JobSetId:  typed.JobSetId,
			Queue:     typed.Queue,
			Created:   typed.Created,
			ClusterId: typed.ClusterId,
			Reason:    typed.Reason,
		})
	case *api.JobLeaseExpiredEvent:
		// TODO record leasing as messages?

	case *api.JobUnableToScheduleEvent:
		return p.recorder.RecordJobUnableToSchedule(typed)

	case *api.JobReprioritizedEvent:
		return p.recorder.RecordJobReprioritized(typed)

	case *api.JobUpdatedEvent:
		return p.recorder.RecordJob(&typed.Job, typed.Created)

	case *api.JobCancellingEvent: // noop
	case *api.JobCancelledEvent:
		// job marked for cancellation
		return p.recorder.MarkCancelled(typed)

	case *api.JobTerminatedEvent:
		return p.recorder.RecordJobTerminated(typed)

	case *api.JobUtilisationEvent:
		// TODO

	case *api.JobIngressInfoEvent: // noop
	}

	return nil
}
