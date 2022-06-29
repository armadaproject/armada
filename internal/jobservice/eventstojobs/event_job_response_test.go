package eventstojobs

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/G-Research/armada/pkg/api"
	"github.com/G-Research/armada/pkg/api/jobservice"
)

type response struct {
	eventMessage api.EventMessage
	jobResponse  jobservice.JobServiceResponse
}

func TestEventsToJobResponse(t *testing.T) {
	eventMessages := []response{
		{
			eventMessage: api.EventMessage{&api.EventMessage_Submitted{}},
			jobResponse:  jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_SUBMITTED},
		},
		{
			eventMessage: api.EventMessage{&api.EventMessage_Queued{}},
			jobResponse:  jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_QUEUED},
		},
		{
			eventMessage: api.EventMessage{&api.EventMessage_DuplicateFound{}},
			jobResponse:  jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_DUPLICATE_FOUND},
		},
		{
			eventMessage: api.EventMessage{&api.EventMessage_Leased{}},
			jobResponse:  jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_LEASED},
		},
		{
			eventMessage: api.EventMessage{&api.EventMessage_LeaseReturned{&api.JobLeaseReturnedEvent{Reason: "LeasureReturned test"}}},
			jobResponse:  jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_LEASE_RETURNED, Error: "LeasureReturned test"},
		},
		{
			eventMessage: api.EventMessage{&api.EventMessage_LeaseExpired{}},
			jobResponse:  jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_LEASE_EXPIRED},
		},
		{
			eventMessage: api.EventMessage{&api.EventMessage_Pending{}},
			jobResponse:  jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_PENDING},
		},
		{
			eventMessage: api.EventMessage{&api.EventMessage_Running{}},
			jobResponse:  jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_RUNNING},
		},
		{
			eventMessage: api.EventMessage{&api.EventMessage_UnableToSchedule{&api.JobUnableToScheduleEvent{Reason: "Unable To Schedule test"}}},
			jobResponse:  jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_UNABLE_TO_SCHEDULE, Error: "Unable To Schedule test"},
		},
		{
			eventMessage: api.EventMessage{&api.EventMessage_Failed{&api.JobFailedEvent{Reason: "Failed Test"}}},
			jobResponse:  jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_FAILED, Error: "Failed Test"},
		},
		{
			eventMessage: api.EventMessage{&api.EventMessage_Terminated{&api.JobTerminatedEvent{Reason: "Terminated Test"}}},
			jobResponse:  jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_TERMINATED, Error: "Terminated Test"},
		},
		{
			eventMessage: api.EventMessage{&api.EventMessage_Succeeded{}},
			jobResponse:  jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_SUCCEEDED},
		},
		{
			eventMessage: api.EventMessage{&api.EventMessage_Reprioritizing{}},
			jobResponse:  jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_REPRIORITIZING},
		},
		{
			eventMessage: api.EventMessage{&api.EventMessage_Reprioritized{}},
			jobResponse:  jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_REPRIORITIZED},
		},
		{
			eventMessage: api.EventMessage{&api.EventMessage_Cancelling{}},
			jobResponse:  jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_CANCELLING},
		},
		{
			eventMessage: api.EventMessage{&api.EventMessage_Utilisation{}},
			jobResponse:  jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_UTILISATION},
		},
		{
			eventMessage: api.EventMessage{&api.EventMessage_IngressInfo{}},
			jobResponse:  jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_INGRESS_INFO},
		},
		{
			eventMessage: api.EventMessage{&api.EventMessage_Cancelled{}},
			jobResponse:  jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_CANCELLED},
		},
		{
			eventMessage: api.EventMessage{&api.EventMessage_Updated{}},
			jobResponse:  jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_UPDATED},
		},
	}
	length := len(eventMessages)
	assert.Equal(t, length, 19)
	for i := range eventMessages {
		jobResponse, err := EventsToJobResponse(eventMessages[i].eventMessage)
		assert.NoError(t, err)
		assert.Equal(t, jobResponse, &eventMessages[i].jobResponse)
	}
}
