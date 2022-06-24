package eventstojobs

import (
	"fmt"

	"github.com/G-Research/armada/pkg/api"
	"github.com/G-Research/armada/pkg/api/jobservice"
)

func EventsToJobResponse(message api.EventMessage) (*jobservice.JobServiceResponse, error) {
	switch message.Events.(type) {
	case *api.EventMessage_Submitted:
		return &jobservice.JobServiceResponse{State: "Submitted"}, nil
	case *api.EventMessage_Queued:
		return &jobservice.JobServiceResponse{State: "Queued"}, nil
	case *api.EventMessage_DuplicateFound:
		return &jobservice.JobServiceResponse{State: "DuplicateFound"}, nil
	case *api.EventMessage_Leased:
		return &jobservice.JobServiceResponse{State: "Leased"}, nil
	case *api.EventMessage_LeaseReturned:
		return &jobservice.JobServiceResponse{State: "LeaseReturned", Error: message.GetLeaseReturned().Reason}, nil
	case *api.EventMessage_LeaseExpired:
		return &jobservice.JobServiceResponse{State: "LeaseExpired"}, nil
	case *api.EventMessage_Pending:
		return &jobservice.JobServiceResponse{State: "Pending"}, nil
	case *api.EventMessage_Running:
		return &jobservice.JobServiceResponse{State: "Running"}, nil
	case *api.EventMessage_UnableToSchedule:
		return &jobservice.JobServiceResponse{State: "UnableToSchedule", Error: message.GetUnableToSchedule().Reason}, nil
	case *api.EventMessage_Failed:
		return &jobservice.JobServiceResponse{State: "Failed", Error: message.GetFailed().Reason}, nil
	case *api.EventMessage_Succeeded:
		return &jobservice.JobServiceResponse{State: "Succeeded"}, nil
	case *api.EventMessage_Reprioritizing:
		return &jobservice.JobServiceResponse{State: "Reprioritizing"}, nil
	case *api.EventMessage_Reprioritized:
		return &jobservice.JobServiceResponse{State: "Reprioritized"}, nil
	case *api.EventMessage_Cancelling:
		return &jobservice.JobServiceResponse{State: "Cancelling"}, nil
	case *api.EventMessage_Cancelled:
		return &jobservice.JobServiceResponse{State: "Cancelled"}, nil
	case *api.EventMessage_Terminated:
		return &jobservice.JobServiceResponse{State: "Terminated", Error: message.GetTerminated().Reason}, nil
	case *api.EventMessage_Utilisation:
		return &jobservice.JobServiceResponse{State: "Utilisation"}, nil
	case *api.EventMessage_IngressInfo:
		return &jobservice.JobServiceResponse{State: "IngressInfo"}, nil
	case *api.EventMessage_Updated:
		return &jobservice.JobServiceResponse{State: "Updated"}, nil
	}
	return nil, fmt.Errorf("unknown event type")
}
