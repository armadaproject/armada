package api

import (
	"fmt"
	"reflect"
	"time"
)

type Event interface {
	GetJobId() string
	GetJobSetId() string
	GetQueue() string
	GetCreated() time.Time
}

func UnwrapEvent(message *EventMessage) (Event, error) {
	switch event := message.Events.(type) {
	case *EventMessage_Submitted:
		return event.Submitted, nil
	case *EventMessage_Queued:
		return event.Queued, nil
	case *EventMessage_Leased:
		return event.Leased, nil
	case *EventMessage_LeaseExpired:
		return event.LeaseExpired, nil
	case *EventMessage_LeaseReturned:
		return event.LeaseReturned, nil
	case *EventMessage_Pending:
		return event.Pending, nil
	case *EventMessage_Running:
		return event.Running, nil
	case *EventMessage_Failed:
		return event.Failed, nil
	case *EventMessage_Succeeded:
		return event.Succeeded, nil
	case *EventMessage_Reprioritized:
		return event.Reprioritized, nil
	case *EventMessage_Cancelling:
		return event.Cancelling, nil
	case *EventMessage_Cancelled:
		return event.Cancelled, nil
	}
	return nil, fmt.Errorf("unknow event type: %s", reflect.TypeOf(message.Events))
}

func Wrap(event Event) (*EventMessage, error) {
	switch typed := event.(type) {
	case *JobSubmittedEvent:
		return &EventMessage{
			Events: &EventMessage_Submitted{
				Submitted: typed,
			},
		}, nil
	case *JobQueuedEvent:
		return &EventMessage{
			Events: &EventMessage_Queued{
				Queued: typed,
			},
		}, nil
	case *JobLeasedEvent:
		return &EventMessage{
			Events: &EventMessage_Leased{
				Leased: typed,
			},
		}, nil
	case *JobLeaseExpiredEvent:
		return &EventMessage{
			Events: &EventMessage_LeaseExpired{
				LeaseExpired: typed,
			},
		}, nil
	case *JobLeaseReturnedEvent:
		return &EventMessage{
			Events: &EventMessage_LeaseReturned{
				LeaseReturned: typed,
			},
		}, nil
	case *JobPendingEvent:
		return &EventMessage{
			Events: &EventMessage_Pending{
				Pending: typed,
			},
		}, nil
	case *JobRunningEvent:
		return &EventMessage{
			Events: &EventMessage_Running{
				Running: typed,
			},
		}, nil
	case *JobFailedEvent:
		return &EventMessage{
			Events: &EventMessage_Failed{
				Failed: typed,
			},
		}, nil
	case *JobSucceededEvent:
		return &EventMessage{
			Events: &EventMessage_Succeeded{
				Succeeded: typed,
			},
		}, nil
	case *JobReprioritizedEvent:
		return &EventMessage{
			Events: &EventMessage_Reprioritized{
				Reprioritized: typed,
			},
		}, nil
	case *JobCancellingEvent:
		return &EventMessage{
			Events: &EventMessage_Cancelling{
				Cancelling: typed,
			},
		}, nil
	case *JobCancelledEvent:
		return &EventMessage{
			Events: &EventMessage_Cancelled{
				Cancelled: typed,
			},
		}, nil
	}
	return nil, fmt.Errorf("unknown event type: %s", reflect.TypeOf(event))
}
