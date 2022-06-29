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
	case *api.EventMessage_Queued:
		return &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_QUEUED}, nil
	case *api.EventMessage_DuplicateFound:
		return &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_DUPLICATE_FOUND}, nil
	case *api.EventMessage_Leased:
		return &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_LEASED}, nil
	case *api.EventMessage_LeaseReturned:
		return &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_LEASE_RETURNED, Error: message.GetLeaseReturned().Reason}, nil
	case *api.EventMessage_LeaseExpired:
		return &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_LEASE_EXPIRED}, nil
	case *api.EventMessage_Pending:
		return &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_PENDING}, nil
	case *api.EventMessage_Running:
		return &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_RUNNING}, nil
	case *api.EventMessage_UnableToSchedule:
		return &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_UNABLE_TO_SCHEDULE, Error: message.GetUnableToSchedule().Reason}, nil
	case *api.EventMessage_Failed:
		return &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_FAILED, Error: message.GetFailed().Reason}, nil
	case *api.EventMessage_Succeeded:
		return &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_SUCCEEDED}, nil
	case *api.EventMessage_Reprioritizing:
		return &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_REPRIORITIZING}, nil
	case *api.EventMessage_Reprioritized:
		return &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_REPRIORITIZED}, nil
	case *api.EventMessage_Cancelling:
		return &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_CANCELLING}, nil
	case *api.EventMessage_Cancelled:
		return &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_CANCELLED}, nil
	case *api.EventMessage_Terminated:
		return &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_TERMINATED, Error: message.GetTerminated().Reason}, nil
	case *api.EventMessage_Utilisation:
		return &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_UTILISATION}, nil
	case *api.EventMessage_IngressInfo:
		return &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_INGRESS_INFO}, nil
	case *api.EventMessage_Updated:
		return &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_UPDATED}, nil
	}
	return nil, fmt.Errorf("unknown event type")
}
