package podchecks

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	config "github.com/G-Research/armada/internal/executor/configuration/podchecks"
)

func Test_getAction_WhenNoContainers_AndNoChecks_ReturnsWait(t *testing.T) {

	csc, err := newContainerStateChecks([]config.ContainerStatusCheck{})
	assert.Nil(t, err)

	action, message := csc.getAction(&v1.Pod{}, time.Second)
	assert.Equal(t, ActionWait, action)
	assert.Empty(t, message)
}

func Test_getAction_WhenOneFailedContainer_AndOneMatchingFailCheck_ReturnsFail(t *testing.T) {
	csc, err := newContainerStateChecks([]config.ContainerStatusCheck{{Action: config.ActionFail, State: config.ContainerStateWaiting, Timeout: time.Minute, ReasonRegexp: "InvalidImageName"}})
	assert.Nil(t, err)

	pod := basicPod()
	pod.Status.ContainerStatuses[0].State.Waiting = &v1.ContainerStateWaiting{Reason: "InvalidImageName", Message: "Image name is wrong"}

	action, message := csc.getAction(pod, time.Minute*2)
	assert.Equal(t, ActionFail, action)
	assert.NotEmpty(t, message)
}

func Test_getAction_WhenOneFailedContainer_ButNotHitTimeout_ReturnsWait(t *testing.T) {
	csc, err := newContainerStateChecks([]config.ContainerStatusCheck{{Action: config.ActionFail, State: config.ContainerStateWaiting, Timeout: time.Minute, ReasonRegexp: "InvalidImageName"}})
	assert.Nil(t, err)

	pod := basicPod()
	pod.Status.ContainerStatuses[0].State.Waiting = &v1.ContainerStateWaiting{Reason: "InvalidImageName", Message: "Image name is wrong"}

	action, message := csc.getAction(pod, time.Second)
	assert.Equal(t, ActionWait, action)
	assert.Empty(t, message)
}

func Test_getAction_WhenOneFailedContainer_ButReasonDoesNotMatch_ReturnsWait(t *testing.T) {
	csc, err := newContainerStateChecks([]config.ContainerStatusCheck{{Action: config.ActionFail, State: config.ContainerStateWaiting, Timeout: time.Minute, ReasonRegexp: "InvalidImageName"}})
	assert.Nil(t, err)

	pod := basicPod()
	pod.Status.ContainerStatuses[0].State.Waiting = &v1.ContainerStateWaiting{Reason: "AnotherReason", Message: "Some other error"}

	action, message := csc.getAction(pod, time.Minute*2)
	assert.Equal(t, ActionWait, action)
	assert.Empty(t, message)
}

func Test_getAction_WhenOneFailedContainer_ButStateNotWaiting_ReturnsWait(t *testing.T) {
	csc, err := newContainerStateChecks([]config.ContainerStatusCheck{{Action: config.ActionFail, State: config.ContainerStateWaiting, Timeout: time.Minute, ReasonRegexp: "InvalidImageName"}})
	assert.Nil(t, err)

	pod := basicPod()
	pod.Status.ContainerStatuses[0].State.Terminated = &v1.ContainerStateTerminated{Reason: "InvalidImageName", Message: "Image name is wrong"}

	action, message := csc.getAction(pod, time.Minute*2)
	assert.Equal(t, ActionWait, action)
	assert.Empty(t, message)
}

func Test_getAction_WhenTwoFailedContainers_AndTwoChecks_FirstCheckNotFirstContainerWins(t *testing.T) {

	csc, err := newContainerStateChecks([]config.ContainerStatusCheck{
		{Action: config.ActionFail, State: config.ContainerStateWaiting, Timeout: time.Minute, ReasonRegexp: "InvalidImageName"},
		{Action: config.ActionRetry, State: config.ContainerStateWaiting, Timeout: time.Minute, ReasonRegexp: "ImagePullBackOff"},
	})
	assert.Nil(t, err)

	pod := basicPod()
	pod.Status.ContainerStatuses = []v1.ContainerStatus{{Name: "my-container"}, {Name: "my-container2"}}

	pod.Status.ContainerStatuses[0].State.Waiting = &v1.ContainerStateWaiting{Reason: "ImagePullBackOff", Message: "Backing off"}
	pod.Status.ContainerStatuses[1].State.Waiting = &v1.ContainerStateWaiting{Reason: "InvalidImageName", Message: "Image name is wrong"}

	action, message := csc.getAction(pod, time.Minute*2)
	assert.Equal(t, ActionFail, action)
	assert.NotEmpty(t, message)
}

func Test_newContainerStateChecks_InvalidRegexp_ReturnsError(t *testing.T) {

	csc, err := newContainerStateChecks([]config.ContainerStatusCheck{{Action: config.ActionFail, State: config.ContainerStateWaiting, Timeout: time.Minute, ReasonRegexp: "["}})
	assert.Nil(t, csc)
	assert.NotNil(t, err)
}

func basicPod() *v1.Pod {

	meta := metav1.ObjectMeta{Name: "my-pod", Namespace: "my-namespace"}

	containerStatus := v1.ContainerStatus{Name: "my-container"}
	status := v1.PodStatus{ContainerStatuses: []v1.ContainerStatus{containerStatus}}

	return &v1.Pod{ObjectMeta: meta, Status: status}
}
