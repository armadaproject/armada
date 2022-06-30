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
			eventMessage: api.EventMessage{&api.EventMessage_DuplicateFound{}},
			jobResponse:  jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_DUPLICATE_FOUND},
		},
		{
			eventMessage: api.EventMessage{&api.EventMessage_Running{}},
			jobResponse:  jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_RUNNING},
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
			eventMessage: api.EventMessage{&api.EventMessage_Cancelled{}},
			jobResponse:  jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_CANCELLED},
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
