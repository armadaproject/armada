package reporter

import (
	"fmt"
	"testing"
	"time"

	"github.com/armadaproject/armada/internal/executor/job/state"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	util2 "github.com/armadaproject/armada/internal/common/util"
	fakecontext "github.com/armadaproject/armada/internal/executor/context/fake"
	"github.com/armadaproject/armada/internal/executor/domain"
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

func TestJobEventReporter_SendsPreemptionEvent(t *testing.T) {
	preemptedPod := createPod(1)
	preemptivePod := createPod(2)

	tests := map[string]struct {
		podsInJobRunState            []*v1.Pod
		leasedPods                   []*v1.Pod
		expectPreemptedEventToBeSent bool
		expectedPreemptiveRunId      string
	}{
		"AllPodsPresentInJobRunState": {
			podsInJobRunState:            []*v1.Pod{preemptedPod, preemptivePod},
			expectPreemptedEventToBeSent: true,
			expectedPreemptiveRunId:      util.ExtractJobRunId(preemptivePod),
		},
		"PreemptivePodMissingInJobRunState": {
			// As the preemptive run id is optional, we can handle the preemptive being missing from the state
			podsInJobRunState:            []*v1.Pod{preemptedPod},
			expectPreemptedEventToBeSent: true,
			expectedPreemptiveRunId:      "",
		},
		"PreemptedPodMissingInJobRunState": {
			// If preempted pod is missing from the state - we can't gather enough data to create a valid event
			// Therefore no event is sent
			podsInJobRunState:            []*v1.Pod{preemptivePod},
			expectPreemptedEventToBeSent: false,
			expectedPreemptiveRunId:      "",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			_, executorContext, _, eventSender := setupTest(t, tc.podsInJobRunState)
			preemptedClusterEvent := createPreemptedClusterEvent(preemptedPod, preemptivePod)
			executorContext.SimulateClusterAddEvent(preemptedClusterEvent)
			// Event processing is async, sleep shortly to give it time to process
			time.Sleep(time.Millisecond * 100)

			if tc.expectPreemptedEventToBeSent {
				assert.Equal(t, 1, eventSender.GetNumberOfSendEventCalls())
				sentMessages := eventSender.GetSentEvents(0)
				assert.Len(t, sentMessages, 1)
				message := sentMessages[0]
				event, err := api.Wrap(message.Event)
				assert.NoError(t, err)

				assert.Equal(t, message.JobRunId, util.ExtractJobRunId(preemptedPod))
				assert.NotNil(t, event.GetPreempted())
				assert.Equal(t, event.GetPreempted().JobId, util.ExtractJobId(preemptedPod))
				assert.Equal(t, event.GetPreempted().JobSetId, util.ExtractJobSet(preemptedPod))
				assert.Equal(t, event.GetPreempted().Queue, util.ExtractQueue(preemptedPod))
				assert.Equal(t, event.GetPreempted().PreemptiveRunId, tc.expectedPreemptiveRunId)
				assert.Equal(t, event.GetPreempted().PreemptiveJobId, util.ExtractJobId(preemptivePod))
			} else {
				assert.Equal(t, 0, eventSender.GetNumberOfSendEventCalls())
			}
		})
	}
}

func setupTest(t *testing.T, existingPods []*v1.Pod) (EventReporter, *fakecontext.SyncFakeClusterContext, *state.JobRunStateStore, *FakeEventSender) {
	executorContext := fakecontext.NewSyncFakeClusterContext()
	for _, pod := range existingPods {
		_, err := executorContext.SubmitPod(pod, "test", []string{})
		assert.NoError(t, err)
	}

	eventSender := NewFakeEventSender()
	jobRunState := state.NewJobRunStateStore(executorContext)
	jobEventReporter, _ := NewJobEventReporter(executorContext, jobRunState, eventSender)

	return jobEventReporter, executorContext, jobRunState, eventSender
}

func createPreemptedClusterEvent(preemptedPod *v1.Pod, preemptivePod *v1.Pod) *v1.Event {
	return &v1.Event{
		InvolvedObject: v1.ObjectReference{
			Name: preemptedPod.Name,
			UID:  preemptedPod.UID,
		},
		Related: &v1.ObjectReference{
			Name: preemptivePod.Name,
			UID:  preemptivePod.UID,
		},
		Reason:  util.EventReasonPreempted,
		Message: fmt.Sprintf("Preempted by %s/%s on node %s", preemptivePod.Namespace, preemptivePod.Name, preemptivePod.Spec.NodeName),
	}
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
