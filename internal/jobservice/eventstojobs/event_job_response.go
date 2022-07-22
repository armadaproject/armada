package eventstojobs

import (
	"fmt"

	"github.com/G-Research/armada/pkg/api"
	js "github.com/G-Research/armada/pkg/api/jobservice"
)

func EventsToJobResponse(message api.EventMessage) (*js.JobServiceResponse, error) {
	switch message.Events.(type) {
	case *api.EventMessage_Submitted:
		return &js.JobServiceResponse{State: js.JobServiceResponse_SUBMITTED}, nil
	case *api.EventMessage_DuplicateFound:
		return &js.JobServiceResponse{State: js.JobServiceResponse_DUPLICATE_FOUND}, nil
	case *api.EventMessage_Running:
		return &js.JobServiceResponse{State: js.JobServiceResponse_RUNNING}, nil
	case *api.EventMessage_Failed:
		return &js.JobServiceResponse{State: js.JobServiceResponse_FAILED, Error: message.GetFailed().Reason}, nil
	case *api.EventMessage_Succeeded:
		return &js.JobServiceResponse{State: js.JobServiceResponse_SUCCEEDED}, nil
	case *api.EventMessage_Cancelled:
		return &js.JobServiceResponse{State: js.JobServiceResponse_CANCELLED}, nil
	}

	return nil, fmt.Errorf("unknown event type")
}

func IsEventAJobResponse(message api.EventMessage) bool {
	switch message.Events.(type) {
	case *api.EventMessage_Submitted, *api.EventMessage_DuplicateFound, *api.EventMessage_Running, *api.EventMessage_Failed, *api.EventMessage_Succeeded, *api.EventMessage_Cancelled:
		return true
	default:
		return false
	}
}
func IsEventTerminal(message api.EventMessage) bool {
	switch message.Events.(type) {
	case *api.EventMessage_DuplicateFound, *api.EventMessage_Cancelled, *api.EventMessage_Succeeded, *api.EventMessage_Failed:
		return true
	default:
		return false
	}

}

// A Utility function for testing if State is terminal
func IsStateTerminal(State js.JobServiceResponse_State) bool {
	switch State {
	case js.JobServiceResponse_DUPLICATE_FOUND, js.JobServiceResponse_CANCELLED, js.JobServiceResponse_SUCCEEDED, js.JobServiceResponse_FAILED:
		return true
	default:
		return false
	}

}
