package service

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	fakecontext "github.com/armadaproject/armada/internal/executor/context/fake"
	"github.com/armadaproject/armada/internal/executor/domain"
	"github.com/armadaproject/armada/internal/executor/reporter"
	"github.com/armadaproject/armada/internal/executor/reporter/mocks"
	"github.com/armadaproject/armada/internal/executor/util"
	"github.com/armadaproject/armada/pkg/armadaevents"
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

func TestJobStateReporter_HandlesPodAddEvents(t *testing.T) {
	type podAddEventTest struct {
		pod              *v1.Pod
		expectEvent      bool
		expectAnnotation bool
		expectedType     reflect.Type
	}

	tests := []podAddEventTest{
		{pod: &v1.Pod{Status: v1.PodStatus{Phase: v1.PodPending}}, expectAnnotation: false, expectEvent: false},
		{pod: makeTestPod(v1.PodStatus{Phase: v1.PodPending}), expectEvent: true, expectAnnotation: false, expectedType: reflect.TypeOf(&armadaevents.EventSequence_Event_JobRunAssigned{})},
		{pod: makeTestPod(v1.PodStatus{Phase: v1.PodRunning}), expectEvent: true, expectAnnotation: true, expectedType: reflect.TypeOf(&armadaevents.EventSequence_Event_JobRunRunning{})},
		{pod: makeTestPod(v1.PodStatus{Phase: v1.PodSucceeded}), expectEvent: true, expectAnnotation: true, expectedType: reflect.TypeOf(&armadaevents.EventSequence_Event_JobRunSucceeded{})},
		{pod: makeTestPod(v1.PodStatus{Phase: v1.PodFailed}), expectEvent: true, expectAnnotation: true, expectedType: reflect.TypeOf(&armadaevents.EventSequence_Event_JobRunErrors{})},
	}

	for _, test := range tests {
		_, _, eventReporter, fakeClusterContext := setUpJobStateReporterTest(t)

		addPod(t, fakeClusterContext, test.pod)
		fakeClusterContext.SimulatePodAddEvent(test.pod)
		time.Sleep(time.Millisecond * 100) // Give time for async routine to process message

		if test.expectEvent {
			assertExpectedEvents(t, test.pod, eventReporter.ReceivedEvents, test.expectedType)
		} else {
			assert.Len(t, eventReporter.ReceivedEvents, 0)
		}

		if test.expectAnnotation {
			assertExpectedAnnotations(t, test.pod, fakeClusterContext)
		} else {
			assert.Len(t, fakeClusterContext.AnnotationsAdded, 0)
		}
	}
}

func TestJobStateReporter_HandlesPodUpdateEvents(t *testing.T) {
	type podAddEventTest struct {
		before       v1.PodPhase
		after        v1.PodPhase
		expectEvent  bool
		expectedType reflect.Type
	}

	tests := []podAddEventTest{
		{before: v1.PodPending, after: v1.PodPending, expectEvent: false}, // No update sent if pod doesn't change phase
		{before: v1.PodPending, after: v1.PodRunning, expectEvent: true, expectedType: reflect.TypeOf(&armadaevents.EventSequence_Event_JobRunRunning{})},
		{before: v1.PodPending, after: v1.PodFailed, expectEvent: true, expectedType: reflect.TypeOf(&armadaevents.EventSequence_Event_JobRunErrors{})},
		{before: v1.PodPending, after: v1.PodSucceeded, expectEvent: true, expectedType: reflect.TypeOf(&armadaevents.EventSequence_Event_JobRunSucceeded{})},

		{before: v1.PodRunning, after: v1.PodRunning, expectEvent: false}, // No update sent if pod doesn't change phase
		{before: v1.PodRunning, after: v1.PodFailed, expectEvent: true, expectedType: reflect.TypeOf(&armadaevents.EventSequence_Event_JobRunErrors{})},
		{before: v1.PodRunning, after: v1.PodSucceeded, expectEvent: true, expectedType: reflect.TypeOf(&armadaevents.EventSequence_Event_JobRunSucceeded{})},

		{before: v1.PodFailed, after: v1.PodFailed, expectEvent: false},       // No update sent if pod doesn't change phase
		{before: v1.PodSucceeded, after: v1.PodSucceeded, expectEvent: false}, // No update sent if pod doesn't change phase
	}

	for _, test := range tests {
		_, _, eventReporter, fakeClusterContext := setUpJobStateReporterTest(t)

		before := makeTestPod(v1.PodStatus{Phase: test.before})
		after := copyWithUpdatedPhase(before, test.after)

		addPod(t, fakeClusterContext, before)
		fakeClusterContext.SimulateUpdateAddEvent(before, after)
		time.Sleep(time.Millisecond * 100) // Give time for async routine to process message

		if test.expectEvent {
			assertExpectedEvents(t, before, eventReporter.ReceivedEvents, test.expectedType)
			assertExpectedAnnotations(t, after, fakeClusterContext)
		} else {
			assert.Len(t, eventReporter.ReceivedEvents, 0)
			assert.Len(t, fakeClusterContext.AnnotationsAdded, 0)
		}
	}
}

func TestJobStateReporter_HandlesPodUpdateEvents_IgnoreUnmanagedPods(t *testing.T) {
	_, _, eventReporter, fakeClusterContext := setUpJobStateReporterTest(t)

	before := &v1.Pod{Status: v1.PodStatus{Phase: v1.PodPending}}
	after := &v1.Pod{Status: v1.PodStatus{Phase: v1.PodRunning}}

	fakeClusterContext.SimulateUpdateAddEvent(before, after)
	time.Sleep(time.Millisecond * 100) // Give time for async routine to process message

	assert.Len(t, eventReporter.ReceivedEvents, 0)
}

func TestJobStateReporter_HandlesFailedPod_WithRetryableError(t *testing.T) {
	jobStateReporter, _, eventReporter, fakeClusterContext := setUpJobStateReporterTest(t)

	before := makeTestPod(v1.PodStatus{Phase: v1.PodRunning})
	after := copyWithUpdatedPhase(before, v1.PodFailed)

	// Does not send event if issue handler detects an issue
	jobStateReporter.podIssueHandler = &stubIssueHandler{detectAndRegisterFailedPodIssueResult: true, detectAndRegisterFailedPodIssueError: nil}
	fakeClusterContext.SimulateUpdateAddEvent(before, after)
	time.Sleep(time.Millisecond * 100) // Give time for async routine to process message
	assert.Len(t, eventReporter.ReceivedEvents, 0)

	// Does not send event if issue handler already knows of issue for run
	jobStateReporter.podIssueHandler = &stubIssueHandler{
		runIdsWithIssues:                      map[string]bool{util.ExtractJobRunId(after): true},
		detectAndRegisterFailedPodIssueResult: false,
		detectAndRegisterFailedPodIssueError:  nil,
	}
	fakeClusterContext.SimulateUpdateAddEvent(before, after)
	time.Sleep(time.Millisecond * 100) // Give time for async routine to process message
	assert.Len(t, eventReporter.ReceivedEvents, 0)

	// Does send event if issue handler errors
	jobStateReporter.podIssueHandler = &stubIssueHandler{detectAndRegisterFailedPodIssueResult: false, detectAndRegisterFailedPodIssueError: fmt.Errorf("error")}
	fakeClusterContext.SimulateUpdateAddEvent(before, after)
	time.Sleep(time.Millisecond * 100) // Give time for async routine to process message
	assert.Len(t, eventReporter.ReceivedEvents, 1)
	assertExpectedEvents(t, before, eventReporter.ReceivedEvents, reflect.TypeOf(&armadaevents.EventSequence_Event_JobRunErrors{}))
}

func setUpJobStateReporterTest(t *testing.T) (*JobStateReporter, *stubIssueHandler, *mocks.FakeEventReporter, *fakecontext.SyncFakeClusterContext) {
	fakeClusterContext := fakecontext.NewSyncFakeClusterContext()
	eventReporter := mocks.NewFakeEventReporter()
	issueHandler := &stubIssueHandler{detectAndRegisterFailedPodIssueResult: false, detectAndRegisterFailedPodIssueError: nil}
	jobStateReporter, err := NewJobStateReporter(fakeClusterContext, eventReporter, issueHandler)
	require.NoError(t, err)
	return jobStateReporter, issueHandler, eventReporter, fakeClusterContext
}

func assertExpectedEvents(t *testing.T, pod *v1.Pod, messages []reporter.EventMessage, expectedType reflect.Type) {
	assert.Len(t, messages, 1)
	assert.Len(t, messages[0].Event.Events, 1)

	assert.Equal(t, util.ExtractJobRunId(pod), messages[0].JobRunId)
	assert.Equal(t, util.ExtractQueue(pod), messages[0].Event.Queue)
	assert.Equal(t, util.ExtractJobSet(pod), messages[0].Event.JobSetName)

	event := messages[0].Event.Events[0]
	resultType := reflect.TypeOf(event.Event)
	assert.Equal(t, expectedType, resultType)
}

func assertExpectedAnnotations(t *testing.T, pod *v1.Pod, clusterContext *fakecontext.SyncFakeClusterContext) {
	jobAnnotations := clusterContext.AnnotationsAdded[util.ExtractJobId(pod)]
	assert.Len(t, jobAnnotations, 1)
	_, exists := jobAnnotations[string(pod.Status.Phase)]
	assert.True(t, exists)
}

type stubIssueHandler struct {
	runIdsWithIssues                      map[string]bool
	detectAndRegisterFailedPodIssueResult bool
	detectAndRegisterFailedPodIssueError  error
}

func (s *stubIssueHandler) HasIssue(runId string) bool {
	_, exists := s.runIdsWithIssues[runId]
	return exists
}
func (s *stubIssueHandler) DetectAndRegisterFailedPodIssue(pod *v1.Pod) (bool, error) {
	return s.detectAndRegisterFailedPodIssueResult, s.detectAndRegisterFailedPodIssueError
}

func copyWithUpdatedPhase(pod *v1.Pod, newPhase v1.PodPhase) *v1.Pod {
	result := pod.DeepCopy()
	result.Status.Phase = newPhase
	return result
}
