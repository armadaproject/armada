package reporter

import (
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	networking "k8s.io/api/networking/v1beta1"

	"github.com/G-Research/armada/pkg/api"
)

func TestCreateEventForCurrentState_WhenPodPending(t *testing.T) {
	pod := v1.Pod{
		Status: v1.PodStatus{
			Phase: v1.PodPending,
		},
	}

	result, err := CreateEventForCurrentState(&pod, "cluster1")
	assert.Nil(t, err)

	_, ok := result.(*api.JobPendingEvent)
	assert.True(t, ok)

}

func TestCreateEventForCurrentState_WhenPodRunning(t *testing.T) {
	pod := v1.Pod{
		Status: v1.PodStatus{
			Phase: v1.PodRunning,
		},
	}

	result, err := CreateEventForCurrentState(&pod, "cluster1")
	assert.Nil(t, err)

	_, ok := result.(*api.JobRunningEvent)
	assert.True(t, ok)
}

func TestCreateEventForCurrentState_WhenPodFailed(t *testing.T) {
	pod := v1.Pod{
		Status: v1.PodStatus{
			Phase: v1.PodFailed,
		},
	}

	result, err := CreateEventForCurrentState(&pod, "cluster1")
	assert.Nil(t, err)

	_, ok := result.(*api.JobFailedEvent)
	assert.True(t, ok)
}

func TestCreateEventForCurrentState_WhenPodSucceeded(t *testing.T) {
	pod := v1.Pod{
		Status: v1.PodStatus{
			Phase: v1.PodSucceeded,
		},
	}

	result, err := CreateEventForCurrentState(&pod, "cluster1")
	assert.Nil(t, err)

	_, ok := result.(*api.JobSucceededEvent)
	assert.True(t, ok)
}

func TestCreateEventForCurrentState_ShouldError_WhenPodPhaseUnknown(t *testing.T) {
	pod := v1.Pod{
		Status: v1.PodStatus{
			Phase: v1.PodUnknown,
		},
	}

	_, err := CreateEventForCurrentState(&pod, "cluster1")
	assert.NotNil(t, err)
}

func TestCreateJobIngressInfoEvent(t *testing.T) {
	expectedIngressMapping := map[int32]string{
		8080: "192.0.0.1:32001",
	}
	pod := &v1.Pod{
		Spec: v1.PodSpec{
			NodeName: "somenode",
		},
		Status: v1.PodStatus{
			HostIP: "192.0.0.1",
		},
	}
	service := &v1.Service{
		Spec: v1.ServiceSpec{
			Type: v1.ServiceTypeNodePort,
			Ports: []v1.ServicePort{
				{
					Port:     8080,
					NodePort: 32001,
				},
			},
		},
	}

	event, err := CreateJobIngressInfoEvent(pod, "cluster1", []*v1.Service{service}, []*networking.Ingress{})
	assert.NoError(t, err)

	ingressEvent, ok := event.(*api.JobIngressInfoEvent)
	assert.True(t, ok)

	assert.Equal(t, expectedIngressMapping, ingressEvent.IngressAddresses)
}

func TestCreateJobIngressInfoEvent_PodNotAllocatedToNode(t *testing.T) {
	service := &v1.Service{}

	noHostIpPod := &v1.Pod{
		Spec: v1.PodSpec{
			NodeName: "somenode",
		},
	}
	event, err := CreateJobIngressInfoEvent(noHostIpPod, "cluster1", []*v1.Service{service}, []*networking.Ingress{})
	assert.Error(t, err)
	assert.Nil(t, event)

	noNodeNamePod := &v1.Pod{
		Status: v1.PodStatus{
			HostIP: "192.0.0.1",
		},
	}
	event, err = CreateJobIngressInfoEvent(noNodeNamePod, "cluster1", []*v1.Service{service}, []*networking.Ingress{})
	assert.Error(t, err)
	assert.Nil(t, event)
}

func TestCreateJobIngressInfoEvent_NilService(t *testing.T) {
	pod := &v1.Pod{
		Spec: v1.PodSpec{
			NodeName: "somenode",
		},
		Status: v1.PodStatus{
			HostIP: "192.0.0.1",
		},
	}
	event, err := CreateJobIngressInfoEvent(pod, "cluster1", nil, nil)
	assert.Error(t, err)
	assert.Nil(t, event)
}
