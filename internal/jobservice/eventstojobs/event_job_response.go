package eventstojobs

import (
	"fmt"

	"github.com/G-Research/armada/pkg/api"
	"github.com/G-Research/armada/pkg/api/jobservice"
)

func EventsToJobResponse(message api.EventMessage) (*jobservice.JobServiceResponse, error) {
	switch message.Events.(type) {
	case *api.EventMessage_Submitted:
		return &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_SUBMITTED}, nil
	case *api.EventMessage_DuplicateFound:
		return &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_DUPLICATE_FOUND}, nil
	case *api.EventMessage_Running:
		return &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_RUNNING}, nil
	case *api.EventMessage_Failed:
		return &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_FAILED, Error: message.GetFailed().Reason}, nil
	case *api.EventMessage_Succeeded:
		return &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_SUCCEEDED}, nil
	case *api.EventMessage_Cancelled:
		return &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_CANCELLED}, nil
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
	case *api.EventMessage_Cancelled, *api.EventMessage_Succeeded, *api.EventMessage_Failed:
		return true
	default:
		return false
	}

}
