package podchecks

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	config "github.com/armadaproject/armada/internal/executor/configuration/podchecks"
)

func TestContainerCheck_TemplateRender(t *testing.T) {
	csc, err := newContainerStateChecks([]config.ContainerStatusCheck{
		{
			Action:       config.ActionFail,
			State:        config.ContainerStateWaiting,
			GracePeriod:  time.Minute,
			ReasonRegexp: "ImagePullBackOff",
			Name:         "image-pull-timeout",
			Message:      "Container {{.MatchedContainerStatus.Name}} failed with reason {{.MatchedContainerStatus.State.Waiting.Reason}}",
		},
	})
	assert.Nil(t, err)

	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "test-pod", Namespace: "test-ns"},
		Status: v1.PodStatus{
			ContainerStatuses: []v1.ContainerStatus{
				{
					Name: "app-container",
					State: v1.ContainerState{
						Waiting: &v1.ContainerStateWaiting{
							Reason:  "ImagePullBackOff",
							Message: "Back-off pulling image",
						},
					},
				},
			},
		},
	}

	action, message := csc.getAction(pod, time.Minute*2)

	assert.Equal(t, ActionFail, action)
	assert.Contains(t, message, "image-pull-timeout")
	assert.Contains(t, message, "app-container")
	assert.Contains(t, message, "ImagePullBackOff")
}

func TestContainerCheck_TemplateFallback_InvalidSyntax(t *testing.T) {
	csc, err := newContainerStateChecks([]config.ContainerStatusCheck{
		{
			Action:       config.ActionFail,
			State:        config.ContainerStateWaiting,
			GracePeriod:  time.Minute,
			ReasonRegexp: "ImagePullBackOff",
			Name:         "bad-template-name",
			Message:      "{{.Invalid syntax",
		},
	})
	assert.NotNil(t, err)
	assert.Nil(t, csc)
}

func TestContainerCheck_TemplateFallback_ExecutionError(t *testing.T) {
	csc, err := newContainerStateChecks([]config.ContainerStatusCheck{
		{
			Action:       config.ActionFail,
			State:        config.ContainerStateWaiting,
			GracePeriod:  time.Minute,
			ReasonRegexp: "ImagePullBackOff",
			Name:         "exec-error-name",
			Message:      "{{.NonExistent.Field}}",
		},
	})
	assert.Nil(t, err)

	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "test-pod", Namespace: "test-ns"},
		Status: v1.PodStatus{
			ContainerStatuses: []v1.ContainerStatus{
				{
					Name: "app-container",
					State: v1.ContainerState{
						Waiting: &v1.ContainerStateWaiting{
							Reason:  "ImagePullBackOff",
							Message: "Back-off pulling image",
						},
					},
				},
			},
		},
	}

	action, message := csc.getAction(pod, time.Minute*2)

	assert.Equal(t, ActionFail, action)
	assert.NotEmpty(t, message)
	assert.Contains(t, message, "app-container")
	assert.Contains(t, message, "Waiting")
}

func TestContainerCheck_NoTemplate_LegacyBehavior(t *testing.T) {
	csc, err := newContainerStateChecks([]config.ContainerStatusCheck{
		{
			Action:       config.ActionFail,
			State:        config.ContainerStateWaiting,
			GracePeriod:  time.Minute,
			ReasonRegexp: "ImagePullBackOff",
		},
	})
	assert.Nil(t, err)

	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "test-pod", Namespace: "test-ns"},
		Status: v1.PodStatus{
			ContainerStatuses: []v1.ContainerStatus{
				{
					Name: "app-container",
					State: v1.ContainerState{
						Waiting: &v1.ContainerStateWaiting{
							Reason:  "ImagePullBackOff",
							Message: "Back-off pulling image",
						},
					},
				},
			},
		},
	}

	action, message := csc.getAction(pod, time.Minute*2)

	assert.Equal(t, ActionFail, action)
	assert.NotEmpty(t, message)
	assert.Contains(t, message, "unnamed")
	assert.Contains(t, message, "app-container")
	assert.Contains(t, message, "Waiting")
}

func TestEventCheck_TemplateRender(t *testing.T) {
	ec, err := newEventChecks([]config.EventCheck{
		{
			Action:      config.ActionFail,
			Regexp:      "FailedMount",
			Type:        "Warning",
			GracePeriod: time.Minute,
			Name:        "pvc-mount-error",
			Message:     "Event on pod {{.Pod.Name}}: {{.MatchedEvent.Message}}",
		},
	})
	assert.Nil(t, err)

	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "test-pod", Namespace: "test-ns"},
	}

	events := []*v1.Event{
		{
			Message: "FailedMount: unable to mount volumes",
			Type:    "Warning",
		},
	}

	action, message := ec.getAction(pod.Name, events, time.Minute*2)

	assert.Equal(t, ActionFail, action)
	assert.Contains(t, message, "unable to mount volumes")
}

func TestEventCheck_TemplateFallback_InvalidSyntax(t *testing.T) {
	ec, err := newEventChecks([]config.EventCheck{
		{
			Action:      config.ActionFail,
			Regexp:      "FailedMount",
			Type:        "Warning",
			GracePeriod: time.Minute,
			Name:        "bad-name",
			Message:     "{{.Invalid",
		},
	})
	assert.NotNil(t, err)
	assert.Nil(t, ec)
}

func TestEventCheck_TemplateFallback_ExecutionError(t *testing.T) {
	ec, err := newEventChecks([]config.EventCheck{
		{
			Action:      config.ActionFail,
			Regexp:      "FailedMount",
			Type:        "Warning",
			GracePeriod: time.Minute,
			Name:        "exec-error",
			Message:     "{{.NonExistent.Field}}",
		},
	})
	assert.Nil(t, err)

	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "test-pod", Namespace: "test-ns"},
	}

	events := []*v1.Event{
		{
			Message: "FailedMount: unable to mount volumes",
			Type:    "Warning",
		},
	}

	action, message := ec.getAction(pod.Name, events, time.Minute*2)

	assert.Equal(t, ActionFail, action)
	assert.NotEmpty(t, message)
	assert.Contains(t, message, "FailedMount")
}

func TestEventCheck_NoTemplate_LegacyBehavior(t *testing.T) {
	ec, err := newEventChecks([]config.EventCheck{
		{
			Action:      config.ActionFail,
			Regexp:      "FailedMount",
			Type:        "Warning",
			GracePeriod: time.Minute,
		},
	})
	assert.Nil(t, err)

	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "test-pod", Namespace: "test-ns"},
	}

	events := []*v1.Event{
		{
			Message: "FailedMount: unable to mount volumes",
			Type:    "Warning",
		},
	}

	action, message := ec.getAction(pod.Name, events, time.Minute*2)

	assert.Equal(t, ActionFail, action)
	assert.NotEmpty(t, message)
	assert.Contains(t, message, "FailedMount")
}

func TestContainerCheck_TemplateAccessesPodFields(t *testing.T) {
	csc, err := newContainerStateChecks([]config.ContainerStatusCheck{
		{
			Action:       config.ActionFail,
			State:        config.ContainerStateWaiting,
			GracePeriod:  time.Minute,
			ReasonRegexp: "ImagePullBackOff",
			Name:         "pod-info",
			Message:      "Pod {{.Pod.Name}} in namespace {{.Pod.Namespace}} has container issue",
		},
	})
	assert.Nil(t, err)

	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "my-pod", Namespace: "production"},
		Status: v1.PodStatus{
			ContainerStatuses: []v1.ContainerStatus{
				{
					Name: "app-container",
					State: v1.ContainerState{
						Waiting: &v1.ContainerStateWaiting{
							Reason:  "ImagePullBackOff",
							Message: "Back-off pulling image",
						},
					},
				},
			},
		},
	}

	action, message := csc.getAction(pod, time.Minute*2)

	assert.Equal(t, ActionFail, action)
	assert.Contains(t, message, "my-pod")
	assert.Contains(t, message, "production")
}

func TestEventCheck_TemplateAccessesEvents(t *testing.T) {
	ec, err := newEventChecks([]config.EventCheck{
		{
			Action:      config.ActionFail,
			Regexp:      "FailedMount",
			Type:        "Warning",
			GracePeriod: time.Minute,
			Name:        "event-list",
			Message:     "Matched event out of {{len .Events}} total events",
		},
	})
	assert.Nil(t, err)

	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "test-pod", Namespace: "test-ns"},
	}

	events := []*v1.Event{
		{Message: "Normal event 1", Type: "Normal"},
		{Message: "FailedMount: unable to mount volumes", Type: "Warning"},
		{Message: "Normal event 2", Type: "Normal"},
	}

	action, message := ec.getAction(pod.Name, events, time.Minute*2)

	assert.Equal(t, ActionFail, action)
	assert.Contains(t, message, "FailedMount")
	assert.NotContains(t, message, "3 total events")
}

func TestContainerCheck_EmptyNameTemplate_UsesUnnamedCheck(t *testing.T) {
	csc, err := newContainerStateChecks([]config.ContainerStatusCheck{
		{
			Action:       config.ActionFail,
			State:        config.ContainerStateWaiting,
			GracePeriod:  time.Minute,
			ReasonRegexp: "ImagePullBackOff",
			Name:         "",
			Message:      "Custom message",
		},
	})
	assert.Nil(t, err)

	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "test-pod", Namespace: "test-ns"},
		Status: v1.PodStatus{
			ContainerStatuses: []v1.ContainerStatus{
				{
					Name: "app-container",
					State: v1.ContainerState{
						Waiting: &v1.ContainerStateWaiting{
							Reason:  "ImagePullBackOff",
							Message: "Back-off pulling image",
						},
					},
				},
			},
		},
	}

	action, message := csc.getAction(pod, time.Minute*2)

	assert.Equal(t, ActionFail, action)
	assert.Contains(t, message, "unnamed")
	assert.Contains(t, message, "Custom message")
}

func TestEventCheck_EmptyMessageTemplate_UsesFallback(t *testing.T) {
	ec, err := newEventChecks([]config.EventCheck{
		{
			Action:      config.ActionFail,
			Regexp:      "FailedMount",
			Type:        "Warning",
			GracePeriod: time.Minute,
			Name:        "mount-error",
			Message:     "",
		},
	})
	assert.Nil(t, err)

	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "test-pod", Namespace: "test-ns"},
	}

	eventMessage := "FailedMount: unable to mount volumes"
	events := []*v1.Event{
		{
			Message: eventMessage,
			Type:    "Warning",
		},
	}

	action, message := ec.getAction(pod.Name, events, time.Minute*2)

	assert.Equal(t, ActionFail, action)
	assert.Contains(t, message, eventMessage)
}
