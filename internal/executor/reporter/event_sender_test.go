package reporter

import (
	"context"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"

	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/armadaevents"
	"github.com/armadaproject/armada/pkg/executorapi"
)

func TestSendEvents_OnlySendsJobRunEvents(t *testing.T) {
	tests := map[string]struct {
		event                  api.Event
		expectedNumberOfEvents int
	}{
		"JobPendingEvent": {
			event:                  &api.JobPendingEvent{JobId: util.ULID().String(), KubernetesId: uuid.New().String()},
			expectedNumberOfEvents: 1,
		},
		"JobRunningEvent": {
			event:                  &api.JobRunningEvent{JobId: util.ULID().String(), KubernetesId: uuid.New().String()},
			expectedNumberOfEvents: 1,
		},
		"JobSucceededEvent": {
			event:                  &api.JobSucceededEvent{JobId: util.ULID().String(), KubernetesId: uuid.New().String()},
			expectedNumberOfEvents: 1,
		},
		"JobFailedEvent": {
			event:                  &api.JobFailedEvent{JobId: util.ULID().String(), KubernetesId: uuid.New().String()},
			expectedNumberOfEvents: 1,
		},
		"JobLeaseReturnedEvent": {
			event:                  &api.JobLeaseReturnedEvent{JobId: util.ULID().String(), KubernetesId: uuid.New().String()},
			expectedNumberOfEvents: 1,
		},
		"JobUnableToScheduleEvent": {
			event:                  &api.JobUnableToScheduleEvent{JobId: util.ULID().String(), KubernetesId: uuid.New().String()},
			expectedNumberOfEvents: 1,
		},
		"JobPreemptedEvent": {
			event:                  &api.JobPreemptedEvent{JobId: util.ULID().String(), RunId: uuid.New().String(), PreemptiveRunId: uuid.New().String()},
			expectedNumberOfEvents: 1,
		},
		"JobIngressInfoEvent": {
			event:                  &api.JobIngressInfoEvent{JobId: util.ULID().String(), KubernetesId: uuid.New().String()},
			expectedNumberOfEvents: 1,
		},
		"JobUtilisationEvent": {
			event:                  &api.JobUtilisationEvent{JobId: util.ULID().String(), KubernetesId: uuid.New().String()},
			expectedNumberOfEvents: 1,
		},
		"JobQueuedEvent": {
			event:                  &api.JobQueuedEvent{JobId: util.ULID().String()},
			expectedNumberOfEvents: 0,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			eventSender, fakeExecutorApiClient := setup()
			err := eventSender.SendEvents([]EventMessage{{Event: test.event, JobRunId: uuid.New().String()}})

			assert.NoError(t, err)
			if test.expectedNumberOfEvents <= 0 {
				assert.Equal(t, 0, fakeExecutorApiClient.GetNumberOfReportEventCalls())
			} else {
				assert.Equal(t, test.expectedNumberOfEvents, fakeExecutorApiClient.GetNumberOfReportEventCalls())
				for i := 0; i < test.expectedNumberOfEvents; i++ {
					assert.Len(t, getAllSentEvents(fakeExecutorApiClient.GetReportedEvents(i)), test.expectedNumberOfEvents)
				}
			}
		})
	}
}

func TestSendEvents_ShouldPopulateRunIdWithProvidedId(t *testing.T) {
	eventSender, fakeExecutorApiClient := setup()

	jobRunId := uuid.New().String()
	event := &api.JobRunningEvent{JobId: util.ULID().String(), KubernetesId: uuid.New().String()}

	err := eventSender.SendEvents([]EventMessage{{Event: event, JobRunId: jobRunId}})

	assert.NoError(t, err)
	assert.Equal(t, 1, fakeExecutorApiClient.GetNumberOfReportEventCalls())

	for _, e := range getAllSentEvents(fakeExecutorApiClient.GetReportedEvents(0)) {
		runIdString, err := armadaevents.UuidStringFromProtoUuid(e.GetJobRunRunning().RunId)
		assert.NoError(t, err)
		assert.Equal(t, jobRunId, runIdString)
	}
}

func TestSendEvents_MakesNoCallToClient_IfNoEventsToSend(t *testing.T) {
	eventSender, fakeExecutorApiClient := setup()
	// This is not an event type supported by the event sender, so should get filtered out
	event := &api.JobQueuedEvent{JobId: util.ULID().String()}

	err := eventSender.SendEvents([]EventMessage{{Event: event, JobRunId: uuid.New().String()}})
	assert.NoError(t, err)
	assert.Equal(t, 0, fakeExecutorApiClient.GetNumberOfReportEventCalls())
}

func TestSendEvents_ReturnsError_OnInvalidIdFormats(t *testing.T) {
	tests := map[string]struct {
		jobId       string
		runId       string
		shouldError bool
	}{
		"ValidIds": {
			jobId:       util.ULID().String(),
			runId:       uuid.New().String(),
			shouldError: false,
		},
		"InvalidJobId": {
			jobId:       "invalid-job-id-format",
			runId:       uuid.New().String(),
			shouldError: true,
		},
		"InvalidRunId": {
			jobId:       util.ULID().String(),
			runId:       "invalid-job-run-id",
			shouldError: true,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			eventSender, _ := setup()
			event := &api.JobRunningEvent{
				JobId:        test.jobId,
				KubernetesId: uuid.New().String(),
			}
			err := eventSender.SendEvents([]EventMessage{{Event: event, JobRunId: test.runId}})
			if test.shouldError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestSendEvents_LimitsMessageSize(t *testing.T) {
	maxMessageSize := 1024
	eventSender, fakeExecutorApiClient := setupWithMaxMessageSize(maxMessageSize)
	err := eventSender.SendEvents(generateEventMessages(100))
	assert.NoError(t, err)

	numberOfApiCalls := fakeExecutorApiClient.GetNumberOfReportEventCalls()
	assert.True(t, numberOfApiCalls > 1)
	for i := 0; i < numberOfApiCalls; i++ {
		sentMessage := fakeExecutorApiClient.GetReportedEvents(i)
		assert.True(t, proto.Size(sentMessage) < maxMessageSize)
	}
}

func generateEventMessages(count int) []EventMessage {
	result := make([]EventMessage, 0, count)

	for i := 0; i < count; i++ {
		event := &api.JobRunningEvent{JobId: util.ULID().String(), KubernetesId: uuid.New().String()}
		result = append(result, EventMessage{Event: event, JobRunId: uuid.New().String()})
	}
	return result
}

func getAllSentEvents(eventList *executorapi.EventList) []*armadaevents.EventSequence_Event {
	result := []*armadaevents.EventSequence_Event{}

	for _, sequence := range eventList.Events {
		for _, e := range sequence.Events {
			result = append(result, e)
		}
	}

	return result
}

func setupWithMaxMessageSize(maxMessageSize int) (EventSender, *fakeExecutorApiClient) {
	fakeExecutorApiClient := newFakeExecutorApiClient()
	eventSender := NewExecutorApiEventSender(fakeExecutorApiClient, maxMessageSize)
	return eventSender, fakeExecutorApiClient
}

func setup() (EventSender, *fakeExecutorApiClient) {
	return setupWithMaxMessageSize(4 * 1024 * 1024)
}

type fakeExecutorApiClient struct {
	reportedEvents []*executorapi.EventList
}

func newFakeExecutorApiClient() *fakeExecutorApiClient {
	return &fakeExecutorApiClient{
		reportedEvents: []*executorapi.EventList{},
	}
}

func (fakeClient *fakeExecutorApiClient) LeaseJobRuns(ctx context.Context, opts ...grpc.CallOption) (executorapi.ExecutorApi_LeaseJobRunsClient, error) {
	// Not implemented
	return nil, nil
}

// Reports job run events to the scheduler
func (fakeClient *fakeExecutorApiClient) ReportEvents(ctx context.Context, in *executorapi.EventList, opts ...grpc.CallOption) (*types.Empty, error) {
	fakeClient.reportedEvents = append(fakeClient.reportedEvents, in)
	return nil, nil
}

func (fakeClient *fakeExecutorApiClient) GetNumberOfReportEventCalls() int {
	return len(fakeClient.reportedEvents)
}

func (fakeClient *fakeExecutorApiClient) GetReportedEvents(callIndex int) *executorapi.EventList {
	return fakeClient.reportedEvents[callIndex]
}
