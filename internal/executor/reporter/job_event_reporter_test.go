package reporter

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	clock "k8s.io/utils/clock/testing"

	util2 "github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/executor/domain"
	"github.com/armadaproject/armada/internal/executor/util"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

func TestJobEventReporter_SendsEventImmediately_OnceNumberOfWaitingEventsMatchesBatchSize(t *testing.T) {
	jobEventReporter, eventSender, _ := setupBatchEventsTest(2)
	pod1 := createPod(1)
	pod2 := createPod(2)

	jobEventReporter.QueueEvent(EventMessage{createFailedEvent(t, pod1), util.ExtractJobRunId(pod1)}, func(err error) {})
	waitForQueueDrained(t, jobEventReporter)
	// Batch size is 2, so a single buffered event must not be sent
	assert.Equal(t, 0, eventSender.GetNumberOfSendEventCalls())

	jobEventReporter.QueueEvent(EventMessage{createFailedEvent(t, pod2), util.ExtractJobRunId(pod2)}, func(err error) {})
	require.Eventually(t, func() bool {
		return eventSender.GetNumberOfSendEventCalls() == 1
	}, time.Second, 10*time.Millisecond, "filling the batch must trigger a send")

	sentMessages := eventSender.GetSentEvents(0)
	assert.Len(t, sentMessages, 2)
}

func TestJobEventReporter_SendsAllEventsInBuffer_EachBatchTickInterval(t *testing.T) {
	jobEventReporter, eventSender, testClock := setupBatchEventsTest(2)
	pod1 := createPod(1)

	jobEventReporter.QueueEvent(EventMessage{createFailedEvent(t, pod1), util.ExtractJobRunId(pod1)}, func(err error) {})
	waitForQueueDrained(t, jobEventReporter)
	assert.Equal(t, 0, eventSender.GetNumberOfSendEventCalls())

	// The queue is drained, so the event sits in the partial batch and the tick
	// below must flush it. The tick is buffered by the fake clock, so it is not
	// lost if the reporter is between selects when it fires.
	require.Eventually(t, testClock.HasWaiters, time.Second, 10*time.Millisecond, "reporter never registered its ticker")
	testClock.Step(time.Second * 5)
	require.Eventually(t, func() bool {
		return eventSender.GetNumberOfSendEventCalls() == 1
	}, time.Second, 10*time.Millisecond, "the tick must flush the buffered event")

	sentMessages := eventSender.GetSentEvents(0)
	assert.Len(t, sentMessages, 1)
}

// Waits until the reporter goroutine has moved every queued event from its
// intake channel into the in-memory batch. After that, asserting "nothing sent
// yet" is deterministic: with the batch below maxBatchSize and no tick fired,
// there is nothing that could have triggered a send.
func waitForQueueDrained(t *testing.T, reporter *JobEventReporter) {
	t.Helper()
	require.Eventually(t, func() bool {
		return len(reporter.eventBuffer) == 0
	}, time.Second, 10*time.Millisecond, "reporter never drained its event buffer")
}

func createFailedEvent(t *testing.T, pod *v1.Pod) *armadaevents.EventSequence {
	event, err := CreateSimpleJobFailedEvent(pod, "failed", "cluster1", armadaevents.KubernetesReason_AppError)
	require.NoError(t, err)
	return event
}

func setupBatchEventsTest(batchSize int) (*JobEventReporter, *FakeEventSender, *clock.FakeClock) {
	eventSender := NewFakeEventSender()
	testClock := clock.NewFakeClock(time.Now())
	jobEventReporter, _ := NewJobEventReporter(eventSender, testClock, batchSize)
	return jobEventReporter, eventSender, testClock
}

func createPod(index int) *v1.Pod {
	jobId := util2.NewULID()
	runId := uuid.New().String()
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID:       types.UID(fmt.Sprintf("job-uid-%d", index)),
			Name:      fmt.Sprintf("armada-%s-0", jobId),
			Namespace: util2.NewULID(),
			Labels: map[string]string{
				domain.JobId:    jobId,
				domain.JobRunId: runId,
				domain.Queue:    fmt.Sprintf("queue-%d", index),
			},
			Annotations: map[string]string{
				domain.JobSetId: fmt.Sprintf("job-set-%d", index),
			},
		},
		Spec: v1.PodSpec{
			NodeName: "node-1",
		},
	}
}

type FakeEventSender struct {
	receivedEvents [][]EventMessage
	mutex          sync.Mutex
}

func NewFakeEventSender() *FakeEventSender {
	return &FakeEventSender{}
}

func (eventSender *FakeEventSender) SendEvents(events []EventMessage) error {
	eventSender.mutex.Lock()
	defer eventSender.mutex.Unlock()
	eventSender.receivedEvents = append(eventSender.receivedEvents, events)
	return nil
}

func (eventSender *FakeEventSender) GetNumberOfSendEventCalls() int {
	eventSender.mutex.Lock()
	defer eventSender.mutex.Unlock()
	return len(eventSender.receivedEvents)
}

func (eventSender *FakeEventSender) GetSentEvents(callIndex int) []EventMessage {
	eventSender.mutex.Lock()
	defer eventSender.mutex.Unlock()
	return eventSender.receivedEvents[callIndex]
}
