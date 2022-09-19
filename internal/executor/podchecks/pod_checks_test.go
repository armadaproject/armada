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
	expectedResult       Action
}

func Test_GetAction(t *testing.T) {
	// ActionFail trumps ActionRetry trumps ActionWait
	tests := []podCheckTest{
		{eventAction: ActionWait, containerStateAction: ActionWait, expectedResult: ActionWait},
		{eventAction: ActionWait, containerStateAction: ActionRetry, expectedResult: ActionRetry},
		{eventAction: ActionWait, containerStateAction: ActionFail, expectedResult: ActionFail},
		{eventAction: ActionRetry, containerStateAction: ActionWait, expectedResult: ActionRetry},
		{eventAction: ActionRetry, containerStateAction: ActionRetry, expectedResult: ActionRetry},
		{eventAction: ActionRetry, containerStateAction: ActionFail, expectedResult: ActionFail},
		{eventAction: ActionFail, containerStateAction: ActionWait, expectedResult: ActionFail},
		{eventAction: ActionFail, containerStateAction: ActionRetry, expectedResult: ActionFail},
		{eventAction: ActionFail, containerStateAction: ActionFail, expectedResult: ActionFail},
	}

	for _, test := range tests {
		podChecks := podChecksWithMocks(test.eventAction, test.containerStateAction)
		result, _ := podChecks.GetAction(basicPod(), []*v1.Event{{Message: "MockEvent", Type: "None"}}, time.Minute)
		assert.Equal(t, test.expectedResult, result)
	}
}

func podChecksWithMocks(eventResult Action, containerStateResult Action) *PodChecks {
	return &PodChecks{
		eventChecks:          &mockEventChecks{result: eventResult, message: mockMessage(eventResult)},
		containerStateChecks: &mockContainerStateChecks{result: containerStateResult, message: mockMessage(containerStateResult)},
	}
}

func Test_GetActionBadNode(t *testing.T) {
	badPodCheck := PodChecks{eventChecks: nil, containerStateChecks: nil, deadlineForUpdates: time.Minute}
	result, message := badPodCheck.GetAction(&v1.Pod{}, []*v1.Event{}, 10*time.Minute)
	assert.Equal(t, result, ActionRetry)
	assert.Equal(t, message, "Pod status and pod events are both empty. Retrying")
}

func Test_GetActionBadNodeButUnderTimeLimit(t *testing.T) {
	badPodCheck := PodChecks{eventChecks: nil, containerStateChecks: nil, deadlineForUpdates: time.Minute}
	result, message := badPodCheck.GetAction(&v1.Pod{}, []*v1.Event{}, 10*time.Second)
	assert.Equal(t, result, ActionWait)
	assert.Equal(t, message, "Pod status and pod events are both empty but we are under timelimit. Waiting")
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
