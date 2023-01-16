package eventstojobs

import (
	"github.com/armadaproject/armada/pkg/api"
	js "github.com/armadaproject/armada/pkg/api/jobservice"
)

// Translates api.EventMessage to a JobServiceReponse.
// Nil if api.EventMessage is not a relevant event for JobServiceResponse
func EventsToJobResponse(message api.EventMessage) *js.JobServiceResponse {
	switch message.Events.(type) {
	case *api.EventMessage_Submitted:
		return &js.JobServiceResponse{State: js.JobServiceResponse_SUBMITTED}
	case *api.EventMessage_DuplicateFound:
		return &js.JobServiceResponse{State: js.JobServiceResponse_DUPLICATE_FOUND}
	case *api.EventMessage_Running:
		return &js.JobServiceResponse{State: js.JobServiceResponse_RUNNING}
	case *api.EventMessage_Failed:
		return &js.JobServiceResponse{State: js.JobServiceResponse_FAILED, Error: message.GetFailed().Reason}
	case *api.EventMessage_Succeeded:
		return &js.JobServiceResponse{State: js.JobServiceResponse_SUCCEEDED}
	case *api.EventMessage_Cancelled:
		return &js.JobServiceResponse{State: js.JobServiceResponse_CANCELLED}
	}

	return nil
}

// Check if api.EventMessage is terminal event
func IsEventTerminal(message api.EventMessage) bool {
	switch message.Events.(type) {
	case *api.EventMessage_DuplicateFound, *api.EventMessage_Cancelled, *api.EventMessage_Succeeded, *api.EventMessage_Failed:
		return true
	default:
		return false
	}
}

// Check if JobServiceResponse is terminal
func IsStateTerminal(State js.JobServiceResponse_State) bool {
	switch State {
	case js.JobServiceResponse_DUPLICATE_FOUND, js.JobServiceResponse_CANCELLED, js.JobServiceResponse_SUCCEEDED, js.JobServiceResponse_FAILED:
		return true
	default:
		return false
	}
}
