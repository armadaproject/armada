package eventstojobs

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/G-Research/armada/pkg/api"
	"github.com/G-Research/armada/pkg/api/jobservice"
)

type response struct {
	eventMessage api.EventMessage
	jobResponse  jobservice.JobServiceResponse
}

type isEventReponse struct {
	eventMessage    api.EventMessage
	jobServiceEvent bool
}

func TestIsEventResponse(t *testing.T) {
	eventMessages := []isEventReponse{
		{
			eventMessage:    api.EventMessage{&api.EventMessage_Submitted{}},
			jobServiceEvent: true,
		},
		{
			eventMessage:    api.EventMessage{&api.EventMessage_DuplicateFound{}},
			jobServiceEvent: true,
		},
		{
			eventMessage:    api.EventMessage{&api.EventMessage_Running{}},
			jobServiceEvent: true,
		},
		{
			eventMessage:    api.EventMessage{&api.EventMessage_Failed{&api.JobFailedEvent{Reason: "Failed Test"}}},
			jobServiceEvent: true,
		},
		{
			eventMessage:    api.EventMessage{&api.EventMessage_Succeeded{}},
			jobServiceEvent: true,
		},
		{
			eventMessage:    api.EventMessage{&api.EventMessage_Cancelled{}},
			jobServiceEvent: true,
		},
		{
			eventMessage:    api.EventMessage{&api.EventMessage_Queued{}},
			jobServiceEvent: false,
		},
		{
			eventMessage:    api.EventMessage{&api.EventMessage_Pending{}},
			jobServiceEvent: false,
		},
		{
			eventMessage:    api.EventMessage{&api.EventMessage_Cancelling{}},
			jobServiceEvent: false,
		},
		{
			eventMessage:    api.EventMessage{&api.EventMessage_IngressInfo{}},
			jobServiceEvent: false,
		},
		{
			eventMessage:    api.EventMessage{&api.EventMessage_Updated{}},
			jobServiceEvent: false,
		},
		{
			eventMessage:    api.EventMessage{&api.EventMessage_LeaseExpired{}},
			jobServiceEvent: false,
		},
		{
			eventMessage:    api.EventMessage{&api.EventMessage_LeaseReturned{}},
			jobServiceEvent: false,
		},
		{
			eventMessage:    api.EventMessage{&api.EventMessage_Leased{}},
			jobServiceEvent: false,
		},
		{
			eventMessage:    api.EventMessage{&api.EventMessage_Terminated{}},
			jobServiceEvent: false,
		},
		{
			eventMessage:    api.EventMessage{&api.EventMessage_UnableToSchedule{}},
			jobServiceEvent: false,
		},
		{
			eventMessage:    api.EventMessage{&api.EventMessage_Reprioritized{}},
			jobServiceEvent: false,
		},
		{
			eventMessage:    api.EventMessage{&api.EventMessage_Reprioritized{}},
			jobServiceEvent: false,
		},
		{
			eventMessage:    api.EventMessage{&api.EventMessage_Utilisation{}},
			jobServiceEvent: false,
		},
	}
	length := len(eventMessages)
	assert.Equal(t, length, 19)
	for i := range eventMessages {
		jobResponse := IsEventAJobResponse(eventMessages[i].eventMessage)
		assert.Equal(t, jobResponse, eventMessages[i].jobServiceEvent)
	}
}

func TestEventAJobResponse(t *testing.T) {
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
			eventMessage: api.EventMessage{&api.EventMessage_Succeeded{}},
			jobResponse:  jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_SUCCEEDED},
		},
		{
			eventMessage: api.EventMessage{&api.EventMessage_Cancelled{}},
			jobResponse:  jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_CANCELLED},
		},
	}
	length := len(eventMessages)
	assert.Equal(t, length, 6)
	for i := range eventMessages {
		jobResponse, err := EventsToJobResponse(eventMessages[i].eventMessage)
		fmt.Print(eventMessages[i].eventMessage)
		fmt.Print(jobResponse)
		assert.NoError(t, err)
		assert.Equal(t, jobResponse, &eventMessages[i].jobResponse)
	}
}
func TestIsTerminalEvent(t *testing.T) {
	eventMessages := []isEventReponse{
		{
			eventMessage:    api.EventMessage{&api.EventMessage_Submitted{}},
			jobServiceEvent: false,
		},
		{
			eventMessage:    api.EventMessage{&api.EventMessage_DuplicateFound{}},
			jobServiceEvent: false,
		},
		{
			eventMessage:    api.EventMessage{&api.EventMessage_Running{}},
			jobServiceEvent: false,
		},
		{
			eventMessage:    api.EventMessage{&api.EventMessage_Failed{&api.JobFailedEvent{Reason: "Failed Test"}}},
			jobServiceEvent: true,
		},
		{
			eventMessage:    api.EventMessage{&api.EventMessage_Succeeded{}},
			jobServiceEvent: true,
		},
		{
			eventMessage:    api.EventMessage{&api.EventMessage_Cancelled{}},
			jobServiceEvent: true,
		},
		{
			eventMessage:    api.EventMessage{&api.EventMessage_Queued{}},
			jobServiceEvent: false,
		},
		{
			eventMessage:    api.EventMessage{&api.EventMessage_Pending{}},
			jobServiceEvent: false,
		},
		{
			eventMessage:    api.EventMessage{&api.EventMessage_Cancelling{}},
			jobServiceEvent: false,
		},
		{
			eventMessage:    api.EventMessage{&api.EventMessage_IngressInfo{}},
			jobServiceEvent: false,
		},
		{
			eventMessage:    api.EventMessage{&api.EventMessage_Updated{}},
			jobServiceEvent: false,
		},
		{
			eventMessage:    api.EventMessage{&api.EventMessage_LeaseExpired{}},
			jobServiceEvent: false,
		},
		{
			eventMessage:    api.EventMessage{&api.EventMessage_LeaseReturned{}},
			jobServiceEvent: false,
		},
		{
			eventMessage:    api.EventMessage{&api.EventMessage_Leased{}},
			jobServiceEvent: false,
		},
		{
			eventMessage:    api.EventMessage{&api.EventMessage_Terminated{}},
			jobServiceEvent: false,
		},
		{
			eventMessage:    api.EventMessage{&api.EventMessage_UnableToSchedule{}},
			jobServiceEvent: false,
		},
		{
			eventMessage:    api.EventMessage{&api.EventMessage_Reprioritized{}},
			jobServiceEvent: false,
		},
		{
			eventMessage:    api.EventMessage{&api.EventMessage_Reprioritized{}},
			jobServiceEvent: false,
		},
		{
			eventMessage:    api.EventMessage{&api.EventMessage_Utilisation{}},
			jobServiceEvent: false,
		},
	}
	length := len(eventMessages)
	assert.Equal(t, length, 19)
	for i := range eventMessages {
		jobResponse := IsEventTerminal(eventMessages[i].eventMessage)
		assert.Equal(t, jobResponse, eventMessages[i].jobServiceEvent)
	}
}
