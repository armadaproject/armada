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

	"github.com/armadaproject/armada/internal/common/errormatch"
	"github.com/armadaproject/armada/internal/executor/categorizer"
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
			assert.Len(t, filterOutJobRunTerminated(eventReporter.ReceivedEvents), 0)
		}

		if test.expectAnnotation {
			assertExpectedAnnotations(t, test.pod, fakeClusterContext)
		} else {
			assert.Len(t, annotationsExcludingTerminated(fakeClusterContext.AnnotationsAdded), 0)
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
			assert.Len(t, filterOutJobRunTerminated(eventReporter.ReceivedEvents), 0)
			assert.Len(t, annotationsExcludingTerminated(fakeClusterContext.AnnotationsAdded), 0)
		}
	}
}

func TestJobStateReporter_HandlesPodUpdateEvents_IgnoreUnmanagedPods(t *testing.T) {
	_, _, eventReporter, fakeClusterContext := setUpJobStateReporterTest(t)

	before := &v1.Pod{Status: v1.PodStatus{Phase: v1.PodPending}}
	after := &v1.Pod{Status: v1.PodStatus{Phase: v1.PodRunning}}

	fakeClusterContext.SimulateUpdateAddEvent(before, after)
	time.Sleep(time.Millisecond * 100) // Give time for async routine to process message

	assert.Len(t, filterOutJobRunTerminated(eventReporter.ReceivedEvents), 0)
}

func TestJobStateReporter_HandlesFailedPod_WithRetryableError(t *testing.T) {
	jobStateReporter, _, eventReporter, fakeClusterContext := setUpJobStateReporterTest(t)

	before := makeTestPod(v1.PodStatus{Phase: v1.PodRunning})
	after := copyWithUpdatedPhase(before, v1.PodFailed)

	// Does not send event if issue handler detects an issue
	jobStateReporter.podIssueHandler = &stubIssueHandler{detectAndRegisterFailedPodIssueResult: true, detectAndRegisterFailedPodIssueError: nil}
	fakeClusterContext.SimulateUpdateAddEvent(before, after)
	time.Sleep(time.Millisecond * 100) // Give time for async routine to process message
	assert.Len(t, filterOutJobRunTerminated(eventReporter.ReceivedEvents), 0)

	// Does not send event if issue handler already knows of issue for run
	jobStateReporter.podIssueHandler = &stubIssueHandler{
		runIdsWithIssues:                      map[string]bool{util.ExtractJobRunId(after): true},
		detectAndRegisterFailedPodIssueResult: false,
		detectAndRegisterFailedPodIssueError:  nil,
	}
	fakeClusterContext.SimulateUpdateAddEvent(before, after)
	time.Sleep(time.Millisecond * 100) // Give time for async routine to process message
	assert.Len(t, filterOutJobRunTerminated(eventReporter.ReceivedEvents), 0)

	// Does send event if issue handler errors
	jobStateReporter.podIssueHandler = &stubIssueHandler{detectAndRegisterFailedPodIssueResult: false, detectAndRegisterFailedPodIssueError: fmt.Errorf("error")}
	fakeClusterContext.SimulateUpdateAddEvent(before, after)
	time.Sleep(time.Millisecond * 100) // Give time for async routine to process message
	assert.Len(t, filterOutJobRunTerminated(eventReporter.ReceivedEvents), 1)
	assertExpectedEvents(t, before, eventReporter.ReceivedEvents, reflect.TypeOf(&armadaevents.EventSequence_Event_JobRunErrors{}))
}

func setUpJobStateReporterTest(t *testing.T) (*JobStateReporter, *stubIssueHandler, *mocks.FakeEventReporter, *fakecontext.SyncFakeClusterContext) {
	return setUpJobStateReporterTestWithClassifier(t, nil, &stubIssueHandler{})
}

func setUpJobStateReporterTestWithClassifier(
	t *testing.T,
	classifier *categorizer.Classifier,
	issueHandler *stubIssueHandler,
) (*JobStateReporter, *stubIssueHandler, *mocks.FakeEventReporter, *fakecontext.SyncFakeClusterContext) {
	fakeClusterContext := fakecontext.NewSyncFakeClusterContext()
	eventReporter := mocks.NewFakeEventReporter()
	jobStateReporter, err := NewJobStateReporter(fakeClusterContext, eventReporter, issueHandler, classifier)
	require.NoError(t, err)
	return jobStateReporter, issueHandler, eventReporter, fakeClusterContext
}

func makeFailedPodWithExitCode(t *testing.T, exitCode int32) *v1.Pod {
	t.Helper()
	pod := makeTestPod(v1.PodStatus{
		Phase: v1.PodFailed,
		ContainerStatuses: []v1.ContainerStatus{
			{
				Name: "main",
				State: v1.ContainerState{
					Terminated: &v1.ContainerStateTerminated{ExitCode: exitCode, Reason: "Error"},
				},
			},
		},
	})
	return pod
}

func classifierForExitCode(t *testing.T, category, subcategory string, exitCode int32) *categorizer.Classifier {
	t.Helper()
	c, err := categorizer.NewClassifier(categorizer.ErrorCategoriesConfig{
		Categories: []categorizer.CategoryConfig{
			{
				Name: category,
				Rules: []categorizer.CategoryRule{
					{
						OnExitCodes: &errormatch.ExitCodeMatcher{
							Operator: errormatch.ExitCodeOperatorIn,
							Values:   []int32{exitCode},
						},
						Subcategory: subcategory,
					},
				},
			},
		},
	})
	require.NoError(t, err)
	return c
}

// The metric counter itself is tested in the metrics package.
// These tests cover the emission gating that governs when the counter is
// advanced: a JobFailedEvent must be emitted (not a ReturnLease or a no-op).

func TestJobStateReporter_PodFailed_EmitsJobFailedEventWhenClassifierMatches(t *testing.T) {
	classifier := classifierForExitCode(t, "jsr-emit-cat", "jsr-emit-sub", 42)
	_, _, eventReporter, fakeClusterContext := setUpJobStateReporterTestWithClassifier(t, classifier, &stubIssueHandler{})

	pod := makeFailedPodWithExitCode(t, 42)
	addPod(t, fakeClusterContext, pod)
	fakeClusterContext.SimulatePodAddEvent(pod)
	time.Sleep(time.Millisecond * 100)

	assert.Len(t, filterOutJobRunTerminated(eventReporter.ReceivedEvents), 1)
}

func TestJobStateReporter_PodFailed_SuppressesEmissionWhenRetryableIssueRegistered(t *testing.T) {
	classifier := classifierForExitCode(t, "jsr-retryable-cat", "jsr-retryable-sub", 42)
	_, _, eventReporter, fakeClusterContext := setUpJobStateReporterTestWithClassifier(
		t, classifier,
		&stubIssueHandler{detectAndRegisterFailedPodIssueResult: true},
	)

	pod := makeFailedPodWithExitCode(t, 42)
	addPod(t, fakeClusterContext, pod)
	fakeClusterContext.SimulatePodAddEvent(pod)
	time.Sleep(time.Millisecond * 100)

	assert.Len(t, filterOutJobRunTerminated(eventReporter.ReceivedEvents), 0, "retryable issue path emits ReturnLease, not JobFailed")
}

func TestJobStateReporter_PodFailed_SuppressesEmissionWhenIssueAlreadyExists(t *testing.T) {
	classifier := classifierForExitCode(t, "jsr-existing-cat", "jsr-existing-sub", 42)
	pod := makeFailedPodWithExitCode(t, 42)
	_, _, eventReporter, fakeClusterContext := setUpJobStateReporterTestWithClassifier(
		t, classifier,
		&stubIssueHandler{runIdsWithIssues: map[string]bool{util.ExtractJobRunId(pod): true}},
	)

	addPod(t, fakeClusterContext, pod)
	fakeClusterContext.SimulatePodAddEvent(pod)
	time.Sleep(time.Millisecond * 100)

	assert.Len(t, filterOutJobRunTerminated(eventReporter.ReceivedEvents), 0, "run already owned by issue handler - no direct emission from this path")
}

func TestJobStateReporter_PodFailed_DropsEventWhenReporterErrors(t *testing.T) {
	classifier := classifierForExitCode(t, "jsr-report-error-cat", "jsr-report-error-sub", 42)
	_, _, eventReporter, fakeClusterContext := setUpJobStateReporterTestWithClassifier(t, classifier, &stubIssueHandler{})
	eventReporter.ErrorOnReport = true

	pod := makeFailedPodWithExitCode(t, 42)
	addPod(t, fakeClusterContext, pod)
	fakeClusterContext.SimulatePodAddEvent(pod)
	time.Sleep(time.Millisecond * 100)

	assert.Len(t, filterOutJobRunTerminated(eventReporter.ReceivedEvents), 0, "event is dropped when reporter errors - counter increment is gated behind successful emission")
}

func TestJobStateReporter_PodFailed_EmitsEventWithEmptyCategoryWhenClassifierIsNil(t *testing.T) {
	_, _, eventReporter, fakeClusterContext := setUpJobStateReporterTestWithClassifier(t, nil, &stubIssueHandler{})

	pod := makeFailedPodWithExitCode(t, 42)
	addPod(t, fakeClusterContext, pod)
	fakeClusterContext.SimulatePodAddEvent(pod)
	time.Sleep(time.Millisecond * 100)

	assert.Len(t, filterOutJobRunTerminated(eventReporter.ReceivedEvents), 1, "event still emitted with empty category/subcategory when classification is disabled")
}

func assertExpectedEvents(t *testing.T, pod *v1.Pod, messages []reporter.EventMessage, expectedType reflect.Type) {
	// Terminal-phase pods produce both the phase event and JobRunTerminated. Phase-event
	// assertions filter out JobRunTerminated; tests that target JobRunTerminated keep it.
	target := messages
	if expectedType != reflect.TypeOf(&armadaevents.EventSequence_Event_JobRunTerminated{}) {
		target = filterOutJobRunTerminated(messages)
	}
	assert.Len(t, target, 1)
	assert.Len(t, target[0].Event.Events, 1)

	assert.Equal(t, util.ExtractJobRunId(pod), target[0].JobRunId)
	assert.Equal(t, util.ExtractQueue(pod), target[0].Event.Queue)
	assert.Equal(t, util.ExtractJobSet(pod), target[0].Event.JobSetName)

	event := target[0].Event.Events[0]
	resultType := reflect.TypeOf(event.Event)
	assert.Equal(t, expectedType, resultType)
}

func assertExpectedAnnotations(t *testing.T, pod *v1.Pod, clusterContext *fakecontext.SyncFakeClusterContext) {
	jobAnnotations := annotationsExcludingTerminated(clusterContext.AnnotationsAdded)[util.ExtractJobId(pod)]
	assert.Len(t, jobAnnotations, 1)
	_, exists := jobAnnotations[string(pod.Status.Phase)]
	assert.True(t, exists)
}

func filterOutJobRunTerminated(messages []reporter.EventMessage) []reporter.EventMessage {
	out := make([]reporter.EventMessage, 0, len(messages))
	for _, m := range messages {
		if len(m.Event.Events) == 1 {
			if _, isTerminated := m.Event.Events[0].Event.(*armadaevents.EventSequence_Event_JobRunTerminated); isTerminated {
				continue
			}
		}
		out = append(out, m)
	}
	return out
}

func annotationsExcludingTerminated(added map[string]map[string]string) map[string]map[string]string {
	out := make(map[string]map[string]string, len(added))
	for jobId, anns := range added {
		filtered := make(map[string]string, len(anns))
		for k, v := range anns {
			if k == domain.JobRunTerminatedReported {
				continue
			}
			filtered[k] = v
		}
		if len(filtered) > 0 {
			out[jobId] = filtered
		}
	}
	return out
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

func TestJobStateReporter_ReportJobRunTerminatedIfNeeded(t *testing.T) {
	terminalManaged := makeTestPod(v1.PodStatus{Phase: v1.PodSucceeded})
	unmanaged := &v1.Pod{Status: v1.PodStatus{Phase: v1.PodFailed}}
	nonTerminal := makeTestPod(v1.PodStatus{Phase: v1.PodRunning})
	alreadyReported := makeTestPod(v1.PodStatus{Phase: v1.PodSucceeded})
	alreadyReported.Annotations[domain.JobRunTerminatedReported] = time.Now().String()
	markedForDeletion := makeTestPod(v1.PodStatus{Phase: v1.PodFailed})
	markedForDeletion.Annotations[domain.MarkedForDeletion] = time.Now().String()

	tests := map[string]struct {
		pod              *v1.Pod
		addToCluster     bool
		expectEmit       bool
		expectAnnotation bool
	}{
		"emits for terminal managed pod": {
			pod: terminalManaged, addToCluster: true, expectEmit: true, expectAnnotation: true,
		},
		"ignores unmanaged pod": {
			pod: unmanaged,
		},
		"ignores non-terminal pod": {
			pod: nonTerminal, addToCluster: true,
		},
		"no-op when already annotated": {
			pod: alreadyReported, addToCluster: true,
		},
		// The motivating case: armada-initiated cancel/preempt sets MarkedForDeletion. reportCurrentStatus
		// skips those, but JobRunTerminated must still fire so terminated_timestamp can be filled.
		"fires for marked-for-deletion pod": {
			pod: markedForDeletion, addToCluster: true, expectEmit: true, expectAnnotation: true,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			jobStateReporter, _, eventReporter, fakeClusterContext := setUpJobStateReporterTest(t)
			if tc.addToCluster {
				addPod(t, fakeClusterContext, tc.pod)
			}

			jobStateReporter.attemptToReportJobRunTerminatedEvent(tc.pod)

			emitted := hasJobRunTerminated(eventReporter.ReceivedEvents)
			assert.Equal(t, tc.expectEmit, emitted)
			_, annotated := fakeClusterContext.AnnotationsAdded[util.ExtractJobId(tc.pod)][domain.JobRunTerminatedReported]
			assert.Equal(t, tc.expectAnnotation, annotated)
		})
	}
}

func TestJobStateReporter_ReportMissingJobEvents_JobRunTerminatedReconciliation(t *testing.T) {
	fresh := makeTestPod(v1.PodStatus{Phase: v1.PodFailed})
	alreadyReported := makeTestPod(v1.PodStatus{Phase: v1.PodSucceeded})
	alreadyReported.Annotations[domain.JobRunTerminatedReported] = time.Now().String()
	alreadyReported.Annotations[string(v1.PodSucceeded)] = time.Now().String() // suppress phase reconcile too

	tests := map[string]struct {
		pod        *v1.Pod
		expectEmit bool
	}{
		// Pod reached terminal phase but JobRunTerminated was never emitted (e.g. informer watch hiccup).
		// Reconciliation must catch it.
		"reconciles missed event":         {pod: fresh, expectEmit: true},
		"does not re-emit once annotated": {pod: alreadyReported, expectEmit: false},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			jobStateReporter, _, eventReporter, fakeClusterContext := setUpJobStateReporterTest(t)
			addPod(t, fakeClusterContext, tc.pod)

			jobStateReporter.ReportMissingJobEvents()

			assert.Equal(t, tc.expectEmit, hasJobRunTerminated(eventReporter.ReceivedEvents))
		})
	}
}

func hasJobRunTerminated(messages []reporter.EventMessage) bool {
	for _, m := range messages {
		for _, e := range m.Event.Events {
			if _, ok := e.Event.(*armadaevents.EventSequence_Event_JobRunTerminated); ok {
				return true
			}
		}
	}
	return false
}
