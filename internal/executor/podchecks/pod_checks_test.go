package podchecks

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
)

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
		result, cause, _ := podChecks.GetAction(createBasicPod(true), []*v1.Event{{Message: "MockEvent", Type: "None"}}, time.Minute)
		assert.Equal(t, test.expectedAction, result)
		assert.Equal(t, test.expectedCause, cause)
	}
}

func podChecksWithMocks(eventResult Action, containerStateResult Action) *PodChecks {
	return &PodChecks{
		eventChecks:               &mockEventChecks{result: eventResult, message: mockMessage(eventResult)},
		containerStateChecks:      &mockContainerStateChecks{result: containerStateResult, message: mockMessage(containerStateResult)},
		deadlineForUpdates:        time.Minute,
		deadlineForNodeAssignment: time.Minute,
	}
}

func Test_GetAction_PodNotScheduled(t *testing.T) {
	podChecks := podChecksWithMocks(ActionWait, ActionWait)

	// No issue if pod isn't scheduled in less than deadlineForNodeAssignment
	result, cause, _ := podChecks.GetAction(createBasicPod(false), []*v1.Event{{Message: "MockEvent", Type: "None"}}, 10*time.Second)
	assert.Equal(t, result, ActionWait)
	assert.Equal(t, cause, None)

	// Issue if pod isn't scheduled for longer than deadlineForNodeAssignment
	result, cause, _ = podChecks.GetAction(createBasicPod(false), []*v1.Event{{Message: "MockEvent", Type: "None"}}, 2*time.Minute)
	assert.Equal(t, result, ActionRetry)
	assert.Equal(t, cause, NoNodeAssigned)
}

func Test_GetAction_BadNode(t *testing.T) {
	podChecks := podChecksWithMocks(ActionWait, ActionWait)
	result, cause, message := podChecks.GetAction(createBasicPod(true), []*v1.Event{}, 10*time.Minute)
	assert.Equal(t, result, ActionRetry)
	assert.Equal(t, cause, NoStatusUpdates)
	assert.Equal(t, message, "Pod status and pod events are both empty. Retrying")
}

func Test_GetAction_BadNodeButUnderTimeLimit(t *testing.T) {
	podChecks := podChecksWithMocks(ActionWait, ActionWait)
	result, cause, message := podChecks.GetAction(createBasicPod(true), []*v1.Event{}, 10*time.Second)
	assert.Equal(t, result, ActionWait)
	assert.Equal(t, cause, NoStatusUpdates)
	assert.Equal(t, message, "Pod status and pod events are both empty but we are under timelimit. Waiting")
}

func createBasicPod(scheduled bool) *v1.Pod {
	pod := &v1.Pod{}
	if scheduled {
		pod.Spec = v1.PodSpec{NodeName: "node1"}
	}
	return pod
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
