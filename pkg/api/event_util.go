package api

import (
	"encoding/json"
	"reflect"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Event interface {
	GetJobId() string
	GetJobSetId() string
	GetQueue() string
	GetCreated() *timestamppb.Timestamp
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
	case *EventMessage_Utilisation:
		return event.Utilisation, nil
	case *EventMessage_IngressInfo:
		return event.IngressInfo, nil
	case *EventMessage_Preempting:
		return event.Preempting, nil
	case *EventMessage_Preempted:
		return event.Preempted, nil
	}
	return nil, errors.Errorf("unknown event type: %s", reflect.TypeOf(message.Events))
}
