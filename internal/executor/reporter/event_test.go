package reporter

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	networking "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/armadaproject/armada/internal/common/errormatch"
	protoutil "github.com/armadaproject/armada/internal/common/proto"
	"github.com/armadaproject/armada/internal/executor/categorizer"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

func TestCreateEventForCurrentState_WhenPodPending(t *testing.T) {
	pod := makeTestPod(v1.PodPending)

	result, err := CreateEventForCurrentState(pod, "cluster1", categorizer.ClassifyResult{}, "")
	assert.Nil(t, err)

	assert.Len(t, result.Events, 1)
	assigned, ok := result.Events[0].Event.(*armadaevents.EventSequence_Event_JobRunAssigned)
	assert.True(t, ok)
	assert.Equal(t, "test-pool", assigned.JobRunAssigned.Pool)
}

func TestCreateEventForCurrentState_WhenPodRunning(t *testing.T) {
	pod := makeTestPod(v1.PodRunning)
	startedAt := time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC)
	pod.Status.ContainerStatuses = []v1.ContainerStatus{{
		State: v1.ContainerState{Running: &v1.ContainerStateRunning{StartedAt: metav1.NewTime(startedAt)}},
	}}

	result, err := CreateEventForCurrentState(pod, "cluster1", categorizer.ClassifyResult{}, "")
	assert.Nil(t, err)

	assert.Len(t, result.Events, 1)
	running, ok := result.Events[0].Event.(*armadaevents.EventSequence_Event_JobRunRunning)
	assert.True(t, ok)
	assert.Equal(t, "test-pool", running.JobRunRunning.Pool)
	assert.Equal(t, protoutil.ToTimestamp(startedAt), running.JobRunRunning.StartedAt)
}

func TestCreateEventForCurrentState_WhenPodFailed(t *testing.T) {
	pod := makeTestPod(v1.PodFailed)
	finishedAt := time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC)
	pod.Status.ContainerStatuses = []v1.ContainerStatus{{
		State: v1.ContainerState{Terminated: &v1.ContainerStateTerminated{FinishedAt: metav1.NewTime(finishedAt)}},
	}}

	result, err := CreateEventForCurrentState(pod, "cluster1", categorizer.ClassifyResult{}, "")
	assert.Nil(t, err)

	assert.Len(t, result.Events, 1)
	event, ok := result.Events[0].Event.(*armadaevents.EventSequence_Event_JobRunErrors)
	assert.True(t, ok)
	assert.Len(t, event.JobRunErrors.Errors, 1)
	assert.NotNil(t, event.JobRunErrors.Errors[0].GetPodError())
	assert.Empty(t, event.JobRunErrors.Errors[0].GetFailureCategory())
	assert.Equal(t, protoutil.ToTimestamp(finishedAt), event.JobRunErrors.FinishedAt)
}

func TestCreateEventForCurrentState_WhenPodFailed_WithClassifier(t *testing.T) {
	pod := makeTestPod(v1.PodFailed)
	pod.Status.ContainerStatuses = []v1.ContainerStatus{
		{
			Name: "main",
			State: v1.ContainerState{
				Terminated: &v1.ContainerStateTerminated{
					ExitCode: 74,
					Reason:   "Error",
					Message:  "custom error",
				},
			},
		},
	}

	classifier, err := categorizer.NewClassifier(categorizer.ErrorCategoriesConfig{
		Categories: []categorizer.CategoryConfig{
			{
				Name: "custom-error",
				Rules: []categorizer.CategoryRule{
					{
						OnExitCodes: &errormatch.ExitCodeMatcher{Operator: errormatch.ExitCodeOperatorIn, Values: []int32{74}},
						Subcategory: "exit-74",
					},
				},
			},
		},
	})
	require.NoError(t, err)

	result, err := CreateEventForCurrentState(pod, "cluster1", classifier.ClassifyContainerError(pod), "")
	assert.NoError(t, err)

	assert.Len(t, result.Events, 1)
	event, ok := result.Events[0].Event.(*armadaevents.EventSequence_Event_JobRunErrors)
	assert.True(t, ok)
	assert.Len(t, event.JobRunErrors.Errors, 1)

	assert.Equal(t, "custom-error", event.JobRunErrors.Errors[0].GetFailureCategory())
	assert.Equal(t, "exit-74", event.JobRunErrors.Errors[0].GetFailureSubcategory())
}

func TestCreateEventForCurrentState_WhenPodFailed_HintAppendedAfterReason(t *testing.T) {
	pod := makeTestPod(v1.PodFailed)
	pod.Status.ContainerStatuses = []v1.ContainerStatus{
		{
			Name: "main",
			State: v1.ContainerState{
				Terminated: &v1.ContainerStateTerminated{
					ExitCode: 74,
					Reason:   "Error",
					Message:  "raw runtime error from container",
				},
			},
		},
	}
	hint := "Operator-supplied actionable guidance"

	classifier, err := categorizer.NewClassifier(categorizer.ErrorCategoriesConfig{
		Categories: []categorizer.CategoryConfig{
			{
				Name: "custom-error",
				Rules: []categorizer.CategoryRule{
					{
						OnExitCodes: &errormatch.ExitCodeMatcher{Operator: errormatch.ExitCodeOperatorIn, Values: []int32{74}},
						Subcategory: "exit-74",
						Hint:        hint,
					},
				},
			},
		},
	})
	require.NoError(t, err)

	result, err := CreateEventForCurrentState(pod, "cluster1", classifier.ClassifyContainerError(pod), "")
	require.NoError(t, err)
	require.Len(t, result.Events, 1)
	event, ok := result.Events[0].Event.(*armadaevents.EventSequence_Event_JobRunErrors)
	require.True(t, ok)
	require.Len(t, event.JobRunErrors.Errors, 1)

	message := event.JobRunErrors.Errors[0].GetPodError().Message
	rawErrorIdx := strings.Index(message, "raw runtime error from container")
	hintIdx := strings.Index(message, hint)
	require.GreaterOrEqual(t, rawErrorIdx, 0, "raw container error must appear in message")
	require.GreaterOrEqual(t, hintIdx, 0, "hint must appear in message")
	assert.Greater(t, hintIdx, rawErrorIdx, "hint must come after the raw error, not before; defends against prepend regression")
}

func TestCreateEventForCurrentState_WhenPodFailed_NilClassifier(t *testing.T) {
	pod := makeTestPod(v1.PodFailed)
	pod.Status.ContainerStatuses = []v1.ContainerStatus{
		{
			Name: "main",
			State: v1.ContainerState{
				Terminated: &v1.ContainerStateTerminated{
					ExitCode: 1,
					Reason:   "Error",
				},
			},
		},
	}

	result, err := CreateEventForCurrentState(pod, "cluster1", categorizer.ClassifyResult{}, "")
	assert.NoError(t, err)
	require.Len(t, result.Events, 1)

	event, ok := result.Events[0].Event.(*armadaevents.EventSequence_Event_JobRunErrors)
	require.True(t, ok)
	require.Len(t, event.JobRunErrors.Errors, 1)

	assert.Empty(t, event.JobRunErrors.Errors[0].GetFailureCategory())
	assert.Empty(t, event.JobRunErrors.Errors[0].GetFailureSubcategory())
}

func TestCreateEventForCurrentState_WhenPodSucceeded(t *testing.T) {
	pod := makeTestPod(v1.PodSucceeded)
	finishedAt := time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC)
	pod.Status.ContainerStatuses = []v1.ContainerStatus{{
		State: v1.ContainerState{Terminated: &v1.ContainerStateTerminated{FinishedAt: metav1.NewTime(finishedAt)}},
	}}

	result, err := CreateEventForCurrentState(pod, "cluster1", categorizer.ClassifyResult{}, "")
	assert.Nil(t, err)

	assert.Len(t, result.Events, 1)
	succeeded, ok := result.Events[0].Event.(*armadaevents.EventSequence_Event_JobRunSucceeded)
	assert.True(t, ok)
	assert.Equal(t, protoutil.ToTimestamp(finishedAt), succeeded.JobRunSucceeded.FinishedAt)
}

func TestCreateEventForCurrentState_OmitsLifecycleTimestampsWithoutContainerTimes(t *testing.T) {
	for _, phase := range []v1.PodPhase{v1.PodRunning, v1.PodSucceeded, v1.PodFailed} {
		t.Run(string(phase), func(t *testing.T) {
			pod := makeTestPod(phase)

			result, err := CreateEventForCurrentState(pod, "cluster1", categorizer.ClassifyResult{}, "")

			require.NoError(t, err)
			switch event := result.Events[0].Event.(type) {
			case *armadaevents.EventSequence_Event_JobRunRunning:
				assert.Nil(t, event.JobRunRunning.StartedAt)
			case *armadaevents.EventSequence_Event_JobRunSucceeded:
				assert.Nil(t, event.JobRunSucceeded.FinishedAt)
			case *armadaevents.EventSequence_Event_JobRunErrors:
				assert.Nil(t, event.JobRunErrors.FinishedAt)
			default:
				t.Fatalf("unexpected event type %T", event)
			}
		})
	}
}

func TestCreateEventForCurrentState_ShouldError_WhenPodPhaseUnknown(t *testing.T) {
	pod := makeTestPod(v1.PodUnknown)

	_, err := CreateEventForCurrentState(pod, "cluster1", categorizer.ClassifyResult{}, "")
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
