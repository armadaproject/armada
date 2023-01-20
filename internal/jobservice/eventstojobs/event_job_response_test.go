package eventstojobs

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/api/jobservice"
)

type response struct {
	eventMessage api.EventMessage
	jobResponse  *jobservice.JobServiceResponse
}

type eventResponse struct {
	eventMessage    api.EventMessage
	jobServiceEvent bool
}

func TestIsEventResponse(t *testing.T) {
	eventMessages := []response{
		{
			eventMessage: api.EventMessage{Events: &api.EventMessage_Submitted{}},
			jobResponse:  &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_SUBMITTED},
		},
		{
			eventMessage: api.EventMessage{Events: &api.EventMessage_DuplicateFound{}},
			jobResponse:  &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_DUPLICATE_FOUND},
		},
		{
			eventMessage: api.EventMessage{Events: &api.EventMessage_Running{}},
			jobResponse:  &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_RUNNING},
		},
		{
			eventMessage: api.EventMessage{Events: &api.EventMessage_Failed{Failed: &api.JobFailedEvent{Reason: "Failed Test"}}},
			jobResponse:  &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_FAILED, Error: "Failed Test"},
		},
		{
			eventMessage: api.EventMessage{Events: &api.EventMessage_Succeeded{}},
			jobResponse:  &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_SUCCEEDED},
		},
		{
			eventMessage: api.EventMessage{Events: &api.EventMessage_Cancelled{}},
			jobResponse:  &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_CANCELLED},
		},
		{
			eventMessage: api.EventMessage{Events: &api.EventMessage_Queued{}},
			jobResponse:  nil,
		},
		{
			eventMessage: api.EventMessage{Events: &api.EventMessage_Pending{}},
			jobResponse:  nil,
		},
		{
			eventMessage: api.EventMessage{Events: &api.EventMessage_Cancelling{}},
			jobResponse:  nil,
		},
		{
			eventMessage: api.EventMessage{Events: &api.EventMessage_IngressInfo{}},
			jobResponse:  nil,
		},
		{
			eventMessage: api.EventMessage{Events: &api.EventMessage_Updated{}},
			jobResponse:  nil,
		},
		{
			eventMessage: api.EventMessage{Events: &api.EventMessage_LeaseExpired{}},
			jobResponse:  nil,
		},
		{
			eventMessage: api.EventMessage{Events: &api.EventMessage_LeaseReturned{}},
			jobResponse:  nil,
		},
		{
			eventMessage: api.EventMessage{Events: &api.EventMessage_Leased{}},
			jobResponse:  nil,
		},
		{
			eventMessage: api.EventMessage{Events: &api.EventMessage_Terminated{}},
			jobResponse:  nil,
		},
		{
			eventMessage: api.EventMessage{Events: &api.EventMessage_UnableToSchedule{}},
			jobResponse:  nil,
		},
		{
			eventMessage: api.EventMessage{Events: &api.EventMessage_Reprioritized{}},
			jobResponse:  nil,
		},
		{
			eventMessage: api.EventMessage{Events: &api.EventMessage_Reprioritized{}},
			jobResponse:  nil,
		},
		{
			eventMessage: api.EventMessage{Events: &api.EventMessage_Utilisation{}},
			jobResponse:  nil,
		},
	}
	length := len(eventMessages)
	assert.Equal(t, length, 19)
	for i := range eventMessages {
		jobResponse := EventsToJobResponse(eventMessages[i].eventMessage)
		assert.Equal(t, jobResponse, eventMessages[i].jobResponse)
	}
}

func TestIsTerminalEvent(t *testing.T) {
	eventMessages := []eventResponse{
		{
			eventMessage:    api.EventMessage{Events: &api.EventMessage_Submitted{}},
			jobServiceEvent: false,
		},
		{
			eventMessage:    api.EventMessage{Events: &api.EventMessage_DuplicateFound{}},
			jobServiceEvent: true,
		},
		{
			eventMessage:    api.EventMessage{Events: &api.EventMessage_Running{}},
			jobServiceEvent: false,
		},
		{
			eventMessage:    api.EventMessage{Events: &api.EventMessage_Failed{Failed: &api.JobFailedEvent{Reason: "Failed Test"}}},
			jobServiceEvent: true,
		},
		{
			eventMessage:    api.EventMessage{Events: &api.EventMessage_Succeeded{}},
			jobServiceEvent: true,
		},
		{
			eventMessage:    api.EventMessage{Events: &api.EventMessage_Cancelled{}},
			jobServiceEvent: true,
		},
		{
			eventMessage:    api.EventMessage{Events: &api.EventMessage_Queued{}},
			jobServiceEvent: false,
		},
		{
			eventMessage:    api.EventMessage{Events: &api.EventMessage_Pending{}},
			jobServiceEvent: false,
		},
		{
			eventMessage:    api.EventMessage{Events: &api.EventMessage_Cancelling{}},
			jobServiceEvent: false,
		},
		{
			eventMessage:    api.EventMessage{Events: &api.EventMessage_IngressInfo{}},
			jobServiceEvent: false,
		},
		{
			eventMessage:    api.EventMessage{Events: &api.EventMessage_Updated{}},
			jobServiceEvent: false,
		},
		{
			eventMessage:    api.EventMessage{Events: &api.EventMessage_LeaseExpired{}},
			jobServiceEvent: false,
		},
		{
			eventMessage:    api.EventMessage{Events: &api.EventMessage_LeaseReturned{}},
			jobServiceEvent: false,
		},
		{
			eventMessage:    api.EventMessage{Events: &api.EventMessage_Leased{}},
			jobServiceEvent: false,
		},
		{
			eventMessage:    api.EventMessage{Events: &api.EventMessage_Terminated{}},
			jobServiceEvent: false,
		},
		{
			eventMessage:    api.EventMessage{Events: &api.EventMessage_UnableToSchedule{}},
			jobServiceEvent: false,
		},
		{
			eventMessage:    api.EventMessage{Events: &api.EventMessage_Reprioritized{}},
			jobServiceEvent: false,
		},
		{
			eventMessage:    api.EventMessage{Events: &api.EventMessage_Reprioritized{}},
			jobServiceEvent: false,
		},
		{
			eventMessage:    api.EventMessage{Events: &api.EventMessage_Utilisation{}},
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

type isStateResponse struct {
	state         *jobservice.JobServiceResponse
	terminalState bool
}

func TestIsTerminalState(t *testing.T) {
	stateMessages := []isStateResponse{
		{
			state:         &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_CANCELLED},
			terminalState: true,
		},
		{
			state:         &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_SUCCEEDED},
			terminalState: true,
		},
		{
			state:         &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_FAILED},
			terminalState: true,
		},
		{
			state:         &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_SUBMITTED},
			terminalState: false,
		},
		{
			state:         &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_RUNNING},
			terminalState: false,
		},
		{
			state:         &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_DUPLICATE_FOUND},
			terminalState: true,
		},
	}
	length := len(stateMessages)
	assert.Equal(t, length, 6)
	for i := range stateMessages {
		stateTerminal := IsStateTerminal(stateMessages[i].state.State)
		assert.Equal(t, stateTerminal, stateMessages[i].terminalState)
	}
}
