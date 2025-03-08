package reporter

import (
	"context"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sTypes "k8s.io/apimachinery/pkg/types"

	"github.com/armadaproject/armada/internal/executor/domain"

	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/pkg/executorapi"
)

func TestSendEvents_MakesNoCallToClient_IfNoEventsToSend(t *testing.T) {
	eventSender, fakeExecutorApiClient := setup()

	err := eventSender.SendEvents([]EventMessage{})
	assert.NoError(t, err)
	assert.Equal(t, 0, fakeExecutorApiClient.GetNumberOfReportEventCalls())
}

func TestSendEvents_LimitsMessageSize(t *testing.T) {
	maxMessageSize := 1024
	eventSender, fakeExecutorApiClient := setupWithMaxMessageSize(maxMessageSize)
	err := eventSender.SendEvents(generateEventMessages(t, 100))
	assert.NoError(t, err)

	numberOfApiCalls := fakeExecutorApiClient.GetNumberOfReportEventCalls()
	assert.True(t, numberOfApiCalls > 1)
	for i := 0; i < numberOfApiCalls; i++ {
		sentMessage := fakeExecutorApiClient.GetReportedEvents(i)
		assert.True(t, proto.Size(sentMessage) < maxMessageSize)
	}
}

func generateEventMessages(t *testing.T, count int) []EventMessage {
	result := make([]EventMessage, 0, count)

	for i := 0; i < count; i++ {
		pod := makeTestPod(v1.PodRunning)
		event, err := CreateEventForCurrentState(pod, "cluster-1")
		require.NoError(t, err)
		result = append(result, EventMessage{Event: event, JobRunId: uuid.New().String()})
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

func (fakeClient *fakeExecutorApiClient) LeaseJobRuns(_ context.Context, opts ...grpc.CallOption) (executorapi.ExecutorApi_LeaseJobRunsClient, error) {
	// Not implemented
	return nil, nil
}

// Reports job run events to the scheduler
func (fakeClient *fakeExecutorApiClient) ReportEvents(_ context.Context, in *executorapi.EventList, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	fakeClient.reportedEvents = append(fakeClient.reportedEvents, in)
	return nil, nil
}

func (fakeClient *fakeExecutorApiClient) GetNumberOfReportEventCalls() int {
	return len(fakeClient.reportedEvents)
}

func (fakeClient *fakeExecutorApiClient) GetReportedEvents(callIndex int) *executorapi.EventList {
	return fakeClient.reportedEvents[callIndex]
}

func makeTestPod(phase v1.PodPhase) *v1.Pod {
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{
				domain.JobId:    util.ULID().String(),
				domain.Queue:    "queue-id-1",
				domain.JobRunId: uuid.New().String(),
			},
			Annotations: map[string]string{
				domain.JobSetId: "job-set-id-1",
			},
			CreationTimestamp: metav1.Time{time.Now().Add(-10 * time.Minute)},
			UID:               k8sTypes.UID(util.NewULID()),
		},
		Spec: v1.PodSpec{
			NodeName: "node1",
		},
		Status: v1.PodStatus{
			Phase: phase,
		},
	}
	return pod
}
