package reporter

import (
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	networking "k8s.io/api/networking/v1"

	"github.com/armadaproject/armada/pkg/armadaevents"
)

func TestCreateEventForCurrentState_WhenPodPending(t *testing.T) {
	pod := makeTestPod(v1.PodPending)

	result, err := CreateEventForCurrentState(pod, "cluster1")
	assert.Nil(t, err)

	assert.Len(t, result.Events, 1)
	_, ok := result.Events[0].Event.(*armadaevents.EventSequence_Event_JobRunAssigned)
	assert.True(t, ok)
}

func TestCreateEventForCurrentState_WhenPodRunning(t *testing.T) {
	pod := makeTestPod(v1.PodRunning)

	result, err := CreateEventForCurrentState(pod, "cluster1")
	assert.Nil(t, err)

	assert.Len(t, result.Events, 1)
	_, ok := result.Events[0].Event.(*armadaevents.EventSequence_Event_JobRunRunning)
	assert.True(t, ok)
}

func TestCreateEventForCurrentState_WhenPodFailed(t *testing.T) {
	pod := makeTestPod(v1.PodFailed)

	result, err := CreateEventForCurrentState(pod, "cluster1")
	assert.Nil(t, err)

	assert.Len(t, result.Events, 1)
	event, ok := result.Events[0].Event.(*armadaevents.EventSequence_Event_JobRunErrors)
	assert.True(t, ok)
	assert.Len(t, event.JobRunErrors.Errors, 1)
	assert.True(t, event.JobRunErrors.Errors[0].GetPodError() != nil)
}

func TestCreateEventForCurrentState_WhenPodSucceeded(t *testing.T) {
	pod := makeTestPod(v1.PodSucceeded)

	result, err := CreateEventForCurrentState(pod, "cluster1")
	assert.Nil(t, err)

	assert.Len(t, result.Events, 1)
	_, ok := result.Events[0].Event.(*armadaevents.EventSequence_Event_JobRunSucceeded)
	assert.True(t, ok)
}

func TestCreateEventForCurrentState_ShouldError_WhenPodPhaseUnknown(t *testing.T) {
	pod := makeTestPod(v1.PodUnknown)

	_, err := CreateEventForCurrentState(pod, "cluster1")
	assert.Error(t, err)
}

func TestCreateJobIngressInfoEvent(t *testing.T) {
	expectedIngressMapping := map[int32]string{
		8080: "192.0.0.1:32001",
		9005: "pod.namespace.svc",
	}
	pod := createNodeAllocatedPod()
	service := createService(v1.ServiceTypeNodePort, 8080, 32001)
	ingress := createIngress("pod.namespace.svc", int32(9005))

	event, err := CreateJobIngressInfoEvent(pod, "cluster1", []*v1.Service{service}, []*networking.Ingress{ingress})
	assert.NoError(t, err)

	assert.Len(t, event.Events, 1)
	ingressEvent, ok := event.Events[0].Event.(*armadaevents.EventSequence_Event_StandaloneIngressInfo)
	assert.True(t, ok)

	assert.Equal(t, expectedIngressMapping, ingressEvent.StandaloneIngressInfo.IngressAddresses)
}

func TestCreateJobIngressInfoEvent_OnlyIncludesNodePortServices(t *testing.T) {
	expectedIngressMapping := map[int32]string{
		8080: "192.0.0.1:32001",
	}
	pod := createNodeAllocatedPod()

	nodePortService := createService(v1.ServiceTypeNodePort, 8080, 32001)
	clusterIpService := createService(v1.ServiceTypeClusterIP, 8081, 0)

	event, err := CreateJobIngressInfoEvent(pod, "cluster1", []*v1.Service{nodePortService, clusterIpService}, []*networking.Ingress{})
	assert.NoError(t, err)

	assert.Len(t, event.Events, 1)
	ingressEvent, ok := event.Events[0].Event.(*armadaevents.EventSequence_Event_StandaloneIngressInfo)
	assert.True(t, ok)

	assert.Equal(t, expectedIngressMapping, ingressEvent.StandaloneIngressInfo.IngressAddresses)
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

func TestCreateJobIngressInfoEvent_NilIngresses(t *testing.T) {
	pod := createNodeAllocatedPod()
	event, err := CreateJobIngressInfoEvent(pod, "cluster1", []*v1.Service{}, nil)
	assert.Error(t, err)
	assert.Nil(t, event)
	event, err = CreateJobIngressInfoEvent(pod, "cluster1", nil, []*networking.Ingress{})
	assert.Error(t, err)
	assert.Nil(t, event)
	event, err = CreateJobIngressInfoEvent(pod, "cluster1", nil, nil)
	assert.Error(t, err)
	assert.Nil(t, event)
}

func TestCreateJobIngressInfoEvent_EmptyIngresses(t *testing.T) {
	pod := createNodeAllocatedPod()
	event, err := CreateJobIngressInfoEvent(pod, "cluster1", []*v1.Service{}, []*networking.Ingress{})
	assert.Error(t, err)
	assert.Nil(t, event)
}

func createNodeAllocatedPod() *v1.Pod {
	pod := makeTestPod(v1.PodRunning)
	pod.Status.HostIP = "192.0.0.1"
	return pod
}

func createIngress(hostname string, port int32) *networking.Ingress {
	pathType := networking.PathTypePrefix
	return &networking.Ingress{
		Spec: networking.IngressSpec{
			Rules: []networking.IngressRule{
				{
					Host: hostname,
					IngressRuleValue: networking.IngressRuleValue{
						HTTP: &networking.HTTPIngressRuleValue{
							Paths: []networking.HTTPIngressPath{
								{
									Path:     "/",
									PathType: &pathType,
									Backend: networking.IngressBackend{
										Service: &networking.IngressServiceBackend{
											Port: networking.ServiceBackendPort{
												Number: port,
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}
}

func createService(serviceType v1.ServiceType, port int32, nodePort int32) *v1.Service {
	return &v1.Service{
		Spec: v1.ServiceSpec{
			Type: serviceType,
			Ports: []v1.ServicePort{
				{
					Port:     port,
					NodePort: nodePort,
				},
			},
		},
	}
}
