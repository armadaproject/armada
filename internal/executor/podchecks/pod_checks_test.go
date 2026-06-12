package podchecks

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clock "k8s.io/utils/clock/testing"
)

var currentTime = time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC)

type podCheckTest struct {
	eventAction          Action
	containerStateAction Action
	expectedAction       Action
	expectedCause        Cause
}

func Test_GetAction(t *testing.T) {
	// ActionFail trumps ActionRetry trumps ActionWait
	tests := []podCheckTest{
		{eventAction: ActionWait, containerStateAction: ActionWait, expectedAction: ActionWait, expectedCause: None},
		{eventAction: ActionWait, containerStateAction: ActionRetry, expectedAction: ActionRetry, expectedCause: PodStartupIssue},
		{eventAction: ActionWait, containerStateAction: ActionFail, expectedAction: ActionFail, expectedCause: PodStartupIssue},
		{eventAction: ActionRetry, containerStateAction: ActionWait, expectedAction: ActionRetry, expectedCause: PodStartupIssue},
		{eventAction: ActionRetry, containerStateAction: ActionRetry, expectedAction: ActionRetry, expectedCause: PodStartupIssue},
		{eventAction: ActionRetry, containerStateAction: ActionFail, expectedAction: ActionFail, expectedCause: PodStartupIssue},
		{eventAction: ActionFail, containerStateAction: ActionWait, expectedAction: ActionFail, expectedCause: PodStartupIssue},
		{eventAction: ActionFail, containerStateAction: ActionRetry, expectedAction: ActionFail, expectedCause: PodStartupIssue},
		{eventAction: ActionFail, containerStateAction: ActionFail, expectedAction: ActionFail, expectedCause: PodStartupIssue},
	}

	for _, test := range tests {
		podChecks := podChecksWithMocks(test.eventAction, test.containerStateAction)
		result, cause, _ := podChecks.GetAction(createBasicPod(true), []*v1.Event{{Message: "MockEvent", Type: "None"}})
		assert.Equal(t, test.expectedAction, result)
		assert.Equal(t, test.expectedCause, cause)
	}
}

func podChecksWithMocks(eventResult Action, containerStateResult Action) *PodChecks {
	return &PodChecks{
		clock:                     clock.NewFakeClock(currentTime),
		eventChecks:               &mockEventChecks{result: eventResult, message: mockMessage(eventResult)},
		containerStateChecks:      &mockContainerStateChecks{result: containerStateResult, message: mockMessage(containerStateResult)},
		deadlineForUpdates:        time.Minute,
		deadlineForNodeAssignment: time.Minute,
	}
}

func Test_GetAction_PodNotScheduled(t *testing.T) {
	podChecks := podChecksWithMocks(ActionWait, ActionWait)

	// No issue if pod isn't scheduled in less than deadlineForNodeAssignment
	result, cause, _ := podChecks.GetAction(createBasicPodInStateFor(false, 10*time.Second), []*v1.Event{{Message: "MockEvent", Type: "None"}})
	assert.Equal(t, result, ActionWait)
	assert.Equal(t, cause, None)

	// Issue if pod isn't scheduled for longer than deadlineForNodeAssignment
	result, cause, _ = podChecks.GetAction(createBasicPodInStateFor(false, 2*time.Minute), []*v1.Event{{Message: "MockEvent", Type: "None"}})
	assert.Equal(t, result, ActionRetry)
	assert.Equal(t, cause, NoNodeAssigned)
}

func Test_GetAction_BadNode(t *testing.T) {
	podChecks := podChecksWithMocks(ActionWait, ActionWait)
	result, cause, message := podChecks.GetAction(createBasicPodInStateFor(true, 10*time.Minute), []*v1.Event{})
	assert.Equal(t, result, ActionRetry)
	assert.Equal(t, cause, NoStatusUpdates)
	assert.Equal(t, message, "Pod has received no updates within 1m0s deadline - likely the node is bad. Retrying")
}

func Test_GetAction_BadNode_ShouldIgnoreScheduledEvents(t *testing.T) {
	podChecks := podChecksWithMocks(ActionWait, ActionWait)
	result, cause, message := podChecks.GetAction(createBasicPodInStateFor(true, 10*time.Minute), []*v1.Event{{Message: "Scheduled pod onto node", Reason: EventReasonScheduled}})
	assert.Equal(t, result, ActionRetry)
	assert.Equal(t, cause, NoStatusUpdates)
	assert.Equal(t, message, "Pod has received no updates within 1m0s deadline - likely the node is bad. Retrying")
}

func Test_GetAction_BadNode_ShouldIgnoreFailedSchedulingEvents(t *testing.T) {
	podChecks := podChecksWithMocks(ActionWait, ActionWait)
	result, cause, message := podChecks.GetAction(createBasicPodInStateFor(true, 10*time.Minute), []*v1.Event{{Message: "Failed to schedule onto node", Reason: EvenReasonFailedScheduling}})
	assert.Equal(t, result, ActionRetry)
	assert.Equal(t, cause, NoStatusUpdates)
	assert.Equal(t, message, "Pod has received no updates within 1m0s deadline - likely the node is bad. Retrying")
}

func Test_GetAction_BadNodeButUnderTimeLimit(t *testing.T) {
	podChecks := podChecksWithMocks(ActionWait, ActionWait)
	result, cause, message := podChecks.GetAction(createBasicPodInStateFor(true, 10*time.Second), []*v1.Event{})
	assert.Equal(t, result, ActionWait)
	assert.Equal(t, cause, NoStatusUpdates)
	assert.Equal(t, message, "Pod status and pod events are both empty but we are under time limit. Waiting")
}

func Test_GetAction_ReturnsWait_WhenLastStateChangeIsNotReady(t *testing.T) {
	podChecks := podChecksWithMocks(ActionFail, ActionFail)

	result, cause, message := podChecks.GetAction(&v1.Pod{}, []*v1.Event{{Message: "MockEvent", Type: "None"}})

	assert.Equal(t, ActionWait, result)
	assert.Equal(t, None, cause)
	assert.Empty(t, message)
}

func Test_GetAction_InitContainerDeadlineUsesFirstInitStart(t *testing.T) {
	podChecks := podChecksWithMocks(ActionWait, ActionWait)
	podChecks.deadlineForInitContainers = time.Minute
	pod := createPodWithRunningInitContainer("init", currentTime.Add(-2*time.Minute))

	result, cause, message := podChecks.GetAction(pod, []*v1.Event{{Message: "MockEvent", Type: "None"}})

	assert.Equal(t, ActionFail, result)
	assert.Equal(t, PodStartupIssue, cause)
	assert.Contains(t, message, "Init containers did not complete within deadline of 1m0s")
	assert.NotContains(t, message, "Errors from init containers")
	assert.Contains(t, message, "Init container init has been running for 2m0s")
}

func Test_GetAction_InitContainerDeadlineWaitsUntilFirstInitStarts(t *testing.T) {
	podChecks := podChecksWithMocks(ActionWait, ActionWait)
	podChecks.deadlineForInitContainers = time.Minute
	pod := createPodWithWaitingInitContainer("init")

	result, cause, _ := podChecks.GetAction(pod, []*v1.Event{{Message: "MockEvent", Type: "None"}})

	assert.Equal(t, ActionWait, result)
	assert.Equal(t, None, cause)
}

func Test_GetAction_InitContainerDeadlineDoesNotFailAfterSuccessfulCompletion(t *testing.T) {
	podChecks := podChecksWithMocks(ActionWait, ActionWait)
	podChecks.deadlineForInitContainers = time.Minute
	pod := createPodWithTerminatedInitContainer("init", currentTime.Add(-5*time.Minute), currentTime.Add(-3*time.Minute), 0)

	result, cause, _ := podChecks.GetAction(pod, []*v1.Event{{Message: "MockEvent", Type: "None"}})

	assert.Equal(t, ActionWait, result)
	assert.Equal(t, None, cause)
}

func Test_GetAction_InitContainerDeadlineRequiresAllRegularInitContainersToComplete(t *testing.T) {
	podChecks := podChecksWithMocks(ActionWait, ActionWait)
	podChecks.deadlineForInitContainers = time.Minute
	pod := createPodWithTerminatedInitContainer("first-init", currentTime.Add(-5*time.Minute), currentTime.Add(-4*time.Minute), 0)
	pod.Spec.InitContainers = append(pod.Spec.InitContainers, v1.Container{Name: "second-init"})
	pod.Status.InitContainerStatuses = append(pod.Status.InitContainerStatuses, v1.ContainerStatus{
		Name: "second-init",
		State: v1.ContainerState{Running: &v1.ContainerStateRunning{
			StartedAt: metav1.NewTime(currentTime.Add(-30 * time.Second)),
		}},
	})

	result, cause, message := podChecks.GetAction(pod, []*v1.Event{{Message: "MockEvent", Type: "None"}})

	assert.Equal(t, ActionFail, result)
	assert.Equal(t, PodStartupIssue, cause)
	assert.Contains(t, message, "Init containers did not complete within deadline of 1m0s")
	assert.NotContains(t, message, "Errors from init containers")
	assert.Contains(t, message, "Init container second-init has been running for 30s")
}

func createBasicPod(scheduled bool) *v1.Pod {
	return createBasicPodInStateFor(scheduled, time.Minute)
}

func createBasicPodInStateFor(scheduled bool, duration time.Duration) *v1.Pod {
	pod := &v1.Pod{}
	pod.CreationTimestamp = metav1.NewTime(currentTime.Add(-duration))
	if scheduled {
		pod.Spec = v1.PodSpec{NodeName: "node1"}
	}
	return pod
}

func createPodWithWaitingInitContainer(name string) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{CreationTimestamp: metav1.NewTime(currentTime.Add(-2 * time.Minute))},
		Spec:       v1.PodSpec{NodeName: "node1", InitContainers: []v1.Container{{Name: name}}},
		Status: v1.PodStatus{InitContainerStatuses: []v1.ContainerStatus{{
			Name:  name,
			State: v1.ContainerState{Waiting: &v1.ContainerStateWaiting{}},
		}}},
	}
}

func createPodWithRunningInitContainer(name string, startedAt time.Time) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{CreationTimestamp: metav1.NewTime(startedAt.Add(-time.Minute))},
		Spec:       v1.PodSpec{NodeName: "node1", InitContainers: []v1.Container{{Name: name}}},
		Status: v1.PodStatus{InitContainerStatuses: []v1.ContainerStatus{{
			Name: name,
			State: v1.ContainerState{Running: &v1.ContainerStateRunning{
				StartedAt: metav1.NewTime(startedAt),
			}},
		}}},
	}
}

func createPodWithTerminatedInitContainer(name string, startedAt time.Time, finishedAt time.Time, exitCode int32) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{CreationTimestamp: metav1.NewTime(startedAt.Add(-time.Minute))},
		Spec:       v1.PodSpec{NodeName: "node1", InitContainers: []v1.Container{{Name: name}}},
		Status: v1.PodStatus{InitContainerStatuses: []v1.ContainerStatus{{
			Name: name,
			State: v1.ContainerState{Terminated: &v1.ContainerStateTerminated{
				StartedAt:  metav1.NewTime(startedAt),
				FinishedAt: metav1.NewTime(finishedAt),
				ExitCode:   exitCode,
			}},
		}}},
	}
}

func mockMessage(result Action) string {
	switch result {
	case ActionFail:
		return "please fail"
	case ActionRetry:
		return "please retry"
	case ActionWait:
		return ""
	default:
		panic("Unexpected action")
	}
}

type mockEventChecks struct {
	result  Action
	message string
}

type mockContainerStateChecks struct {
	result  Action
	message string
}

func (ec *mockEventChecks) getAction(podName string, podEvents []*v1.Event, timeInState time.Duration) (Action, string) {
	return ec.result, ec.message
}

func (csc *mockContainerStateChecks) getAction(pod *v1.Pod, timeInState time.Duration) (Action, string) {
	return csc.result, csc.message
}
