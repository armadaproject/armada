package reporter

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/clock"

	util2 "github.com/armadaproject/armada/internal/common/util"
	fakecontext "github.com/armadaproject/armada/internal/executor/context/fake"
	"github.com/armadaproject/armada/internal/executor/domain"
	"github.com/armadaproject/armada/internal/executor/job"
	"github.com/armadaproject/armada/internal/executor/util"
	"github.com/armadaproject/armada/pkg/api"
)

func TestRequiresIngressToBeReported_FalseWhenIngressHasBeenReported(t *testing.T) {
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				domain.HasIngress:      "true",
				domain.IngressReported: time.Now().String(),
			},
		},
	}
	assert.False(t, requiresIngressToBeReported(pod))
}

func TestRequiresIngressToBeReported_FalseWhenNonIngressPod(t *testing.T) {
	pod := &v1.Pod{}
	assert.False(t, requiresIngressToBeReported(pod))
}

func TestRequiresIngressToBeReported_TrueWhenHasIngressButNotIngressReportedAnnotation(t *testing.T) {
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{domain.HasIngress: "true"},
		},
	}
	assert.True(t, requiresIngressToBeReported(pod))
}

func TestJobEventReporter_SendsEventImmediately_OnceNumberOfWaitingEventsMatchesBatchSize(t *testing.T) {
	jobEventReporter, eventSender, _ := setupBatchEventsTest(2)
	pod1 := createPod(1)
	pod2 := createPod(2)

	jobEventReporter.QueueEvent(EventMessage{CreateSimpleJobFailedEvent(pod1, "failed", "cluster1", api.Cause_Error), util.ExtractJobRunId(pod1)}, func(err error) {})
	time.Sleep(time.Millisecond * 100) // Allow time for async event processing
	// No calls excepted, as batch size is 2 and only 1 event has been sent
	assert.Equal(t, 0, eventSender.GetNumberOfSendEventCalls())

	jobEventReporter.QueueEvent(EventMessage{CreateSimpleJobFailedEvent(pod2, "failed", "cluster1", api.Cause_Error), util.ExtractJobRunId(pod2)}, func(err error) {})
	time.Sleep(time.Millisecond * 100) // Allow time for async event processing

	assert.Equal(t, 1, eventSender.GetNumberOfSendEventCalls())
	sentMessages := eventSender.GetSentEvents(0)
	assert.Len(t, sentMessages, 2)
}

func TestJobEventReporter_SendsAllEventsInBuffer_EachBatchTickInterval(t *testing.T) {
	jobEventReporter, eventSender, testClock := setupBatchEventsTest(2)
	pod1 := createPod(1)

	jobEventReporter.QueueEvent(EventMessage{CreateSimpleJobFailedEvent(pod1, "failed", "cluster1", api.Cause_Error), util.ExtractJobRunId(pod1)}, func(err error) {})
	time.Sleep(time.Millisecond * 100)
	assert.Equal(t, 0, eventSender.GetNumberOfSendEventCalls())

	// Increment time
	testClock.Step(time.Second * 5)
	time.Sleep(time.Millisecond * 100)

	assert.Equal(t, 1, eventSender.GetNumberOfSendEventCalls())
	sentMessages := eventSender.GetSentEvents(0)
	assert.Len(t, sentMessages, 1)
}

func setupBatchEventsTest(batchSize int) (*JobEventReporter, *FakeEventSender, *clock.FakeClock) {
	executorContext := fakecontext.NewSyncFakeClusterContext()
	eventSender := NewFakeEventSender()
	jobRunState := job.NewJobRunStateStore(executorContext)
	testClock := clock.NewFakeClock(time.Now())
	jobEventReporter, _ := NewJobEventReporter(executorContext, jobRunState, eventSender, testClock, batchSize)
	return jobEventReporter, eventSender, testClock
}

func createPod(index int) *v1.Pod {
	jobId := util2.NewULID()
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID:       types.UID(fmt.Sprintf("job-uid-%d", index)),
			Name:      fmt.Sprintf("armada-%s-0", jobId),
			Namespace: util2.NewULID(),
			Labels: map[string]string{
				domain.JobId:    jobId,
				domain.JobRunId: fmt.Sprintf("job-run-%d", index),
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
}

func NewFakeEventSender() *FakeEventSender {
	return &FakeEventSender{}
}

func (eventSender *FakeEventSender) SendEvents(events []EventMessage) error {
	eventSender.receivedEvents = append(eventSender.receivedEvents, events)
	return nil
}

func (eventSender *FakeEventSender) GetNumberOfSendEventCalls() int {
	return len(eventSender.receivedEvents)
}

func (eventSender *FakeEventSender) GetSentEvents(callIndex int) []EventMessage {
	return eventSender.receivedEvents[callIndex]
}
