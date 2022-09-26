package api

import (
	"encoding/json"
	"reflect"
	"time"

	"github.com/pkg/errors"
)

type Event interface {
	GetJobId() string
	GetJobSetId() string
	GetQueue() string
	GetCreated() time.Time
}

type KubernetesEvent interface {
	Event
	GetKubernetesId() string
	GetClusterId() string
	GetPodNumber() int32
	GetPodName() string
	GetPodNamespace() string
}

// customise oneof serialisation
func (message *EventMessage) MarshalJSON() ([]byte, error) {
	return json.Marshal(message.Events)
}

func (message *EventMessage) UnmarshalJSON(data []byte) error {
	return json.Unmarshal(data, message.Events)
}

func UnwrapEvent(message *EventMessage) (Event, error) {
	switch event := message.Events.(type) {
	case *EventMessage_Submitted:
		return event.Submitted, nil
	case *EventMessage_Queued:
		return event.Queued, nil
	case *EventMessage_DuplicateFound:
		return event.DuplicateFound, nil
	case *EventMessage_Leased:
		return event.Leased, nil
	case *EventMessage_LeaseReturned:
		return event.LeaseReturned, nil
	case *EventMessage_LeaseExpired:
		return event.LeaseExpired, nil
	case *EventMessage_Pending:
		return event.Pending, nil
	case *EventMessage_Running:
		return event.Running, nil
	case *EventMessage_UnableToSchedule:
		return event.UnableToSchedule, nil
	case *EventMessage_Failed:
		return event.Failed, nil
	case *EventMessage_Succeeded:
		return event.Succeeded, nil
	case *EventMessage_Reprioritizing:
		return event.Reprioritizing, nil
	case *EventMessage_Reprioritized:
		return event.Reprioritized, nil
	case *EventMessage_Cancelling:
		return event.Cancelling, nil
	case *EventMessage_Cancelled:
		return event.Cancelled, nil
	case *EventMessage_Terminated:
		return event.Terminated, nil
	case *EventMessage_Utilisation:
		return event.Utilisation, nil
	case *EventMessage_IngressInfo:
		return event.IngressInfo, nil
	case *EventMessage_Updated:
		return event.Updated, nil
	case *EventMessage_Preempted:
		return event.Preempted, nil
	}
	return nil, errors.Errorf("unknown event type: %s", reflect.TypeOf(message.Events))
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
	case *JobDuplicateFoundEvent:
		return &EventMessage{
			Events: &EventMessage_DuplicateFound{
				DuplicateFound: typed,
			},
		}, nil
	case *JobLeasedEvent:
		return &EventMessage{
			Events: &EventMessage_Leased{
				Leased: typed,
			},
		}, nil
	case *JobLeaseReturnedEvent:
		return &EventMessage{
			Events: &EventMessage_LeaseReturned{
				LeaseReturned: typed,
			},
		}, nil
	case *JobLeaseExpiredEvent:
		return &EventMessage{
			Events: &EventMessage_LeaseExpired{
				LeaseExpired: typed,
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
	case *JobUnableToScheduleEvent:
		return &EventMessage{
			Events: &EventMessage_UnableToSchedule{
				UnableToSchedule: typed,
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
	case *JobReprioritizingEvent:
		return &EventMessage{
			Events: &EventMessage_Reprioritizing{
				Reprioritizing: typed,
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
	case *JobTerminatedEvent:
		return &EventMessage{
			Events: &EventMessage_Terminated{
				Terminated: typed,
			},
		}, nil
	case *JobUtilisationEvent:
		return &EventMessage{
			Events: &EventMessage_Utilisation{
				Utilisation: typed,
			},
		}, nil
	case *JobIngressInfoEvent:
		return &EventMessage{
			Events: &EventMessage_IngressInfo{
				IngressInfo: typed,
			},
		}, nil
	case *JobUpdatedEvent:
		return &EventMessage{
			Events: &EventMessage_Updated{
				Updated: typed,
			},
		}, nil
	case *JobPreemptedEvent:
		return &EventMessage{
			Events: &EventMessage_Preempted{
				Preempted: typed,
			},
		}, nil
	}
	return nil, errors.Errorf("unknown event type: %s", reflect.TypeOf(event))
}
