package job

import (
	"testing"

	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	k8s_errors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/armadaproject/armada/internal/common/armadaerrors"
	"github.com/armadaproject/armada/internal/executor/configuration"
	"github.com/armadaproject/armada/internal/executor/fake/context"
)

const (
	AdmissionWebhookRegex  = "admission webhook"
	NamespaceNotFoundRegex = "namespaces \".*\" not found"
	HelloRegex             = "hello, [a-z]+!"
	podsResource           = "pods"
)

var testAppConfig = configuration.ApplicationConfiguration{ClusterId: "test", Pool: "pool"}

func TestIsRecoverable_ArbitraryErrorIsNotRecoverable(t *testing.T) {
	clusterContext := context.NewFakeClusterContext(testAppConfig, "kubernetes.io/hostname", []*context.NodeSpec{})
	submitter := NewSubmitter(clusterContext, &configuration.PodDefaults{}, 1, []string{
		AdmissionWebhookRegex,
		HelloRegex,
		NamespaceNotFoundRegex,
	}, []string{})

	recoverable := submitter.isRecoverable(newArbitraryError("some error"))
	assert.False(t, recoverable)
}

func TestIsRecoverable_KubernetesStatusInvalidIsUnrecoverable(t *testing.T) {
	clusterContext := context.NewFakeClusterContext(testAppConfig, "kubernetes.io/hostname", []*context.NodeSpec{})
	submitter := NewSubmitter(clusterContext, &configuration.PodDefaults{}, 1, []string{}, []string{})

	recoverable := submitter.isRecoverable(newK8sApiError("", metav1.StatusReasonInvalid))
	assert.False(t, recoverable)
}

func TestIsRecoverable_KubernetesStatusForbiddenIsUnrecoverable(t *testing.T) {
	clusterContext := context.NewFakeClusterContext(testAppConfig, "kubernetes.io/hostname", []*context.NodeSpec{})
	submitter := NewSubmitter(clusterContext, &configuration.PodDefaults{}, 1, []string{}, []string{})

	recoverable := submitter.isRecoverable(newK8sApiError("", metav1.StatusReasonForbidden))
	assert.False(t, recoverable)
}

func TestIsRecoverable_K8sApiMatchingRegexIsUnrecoverable(t *testing.T) {
	clusterContext := context.NewFakeClusterContext(testAppConfig, "kubernetes.io/hostname", []*context.NodeSpec{})
	submitter := NewSubmitter(clusterContext, &configuration.PodDefaults{}, 1, []string{
		AdmissionWebhookRegex,
		HelloRegex,
		NamespaceNotFoundRegex,
	}, []string{})

	recoverable := submitter.isRecoverable(newK8sApiError("admission webhook failure: some webhook failed validation", "other status"))
	assert.False(t, recoverable)

	recoverable = submitter.isRecoverable(newK8sApiError("Error: hello, john!", "other status"))
	assert.False(t, recoverable)

	recoverable = submitter.isRecoverable(newK8sApiError("namespaces \"test-1\" not found", "other status"))
	assert.False(t, recoverable)

	recoverable = submitter.isRecoverable(newK8sApiError("hello world!", "other status"))
	assert.True(t, recoverable)
}

func TestIsRecoverable_ArmadaErrCreateResourceIsRecoverable(t *testing.T) {
	clusterContext := context.NewFakeClusterContext(testAppConfig, "kubernetes.io/hostname", []*context.NodeSpec{})
	submitter := NewSubmitter(clusterContext, &configuration.PodDefaults{}, 1, []string{}, []string{})

	recoverable := submitter.isRecoverable(newArmadaErrCreateResource())
	assert.True(t, recoverable)
}

func TestSanitizePodResources(t *testing.T) {
	clusterContext := context.NewFakeClusterContext(testAppConfig, "kubernetes.io/hostname", []*context.NodeSpec{})
	submitter := NewSubmitter(clusterContext, &configuration.PodDefaults{}, 1, []string{}, []string{podsResource})

	initContainerResources := v1.ResourceRequirements{
		Requests: v1.ResourceList{
			v1.ResourceCPU:              resource.MustParse("100m"),
			v1.ResourceMemory:           resource.MustParse("100Mi"),
			v1.ResourceEphemeralStorage: resource.MustParse("100Mi"),
			podsResource:                resource.MustParse("1"),
		},
		Limits: v1.ResourceList{
			v1.ResourceCPU:              resource.MustParse("100m"),
			v1.ResourceMemory:           resource.MustParse("100Mi"),
			v1.ResourceEphemeralStorage: resource.MustParse("100Mi"),
			podsResource:                resource.MustParse("1"),
		},
	}

	containerResources := v1.ResourceRequirements{
		Requests: v1.ResourceList{
			v1.ResourceCPU:              resource.MustParse("100m"),
			v1.ResourceMemory:           resource.MustParse("100Mi"),
			v1.ResourceEphemeralStorage: resource.MustParse("100Mi"),
			podsResource:                resource.MustParse("1"),
		},
		Limits: v1.ResourceList{
			v1.ResourceCPU:              resource.MustParse("100m"),
			v1.ResourceMemory:           resource.MustParse("100Mi"),
			v1.ResourceEphemeralStorage: resource.MustParse("100Mi"),
			podsResource:                resource.MustParse("1"),
		},
	}

	pod := &v1.Pod{
		Spec: v1.PodSpec{
			InitContainers: []v1.Container{
				{
					Resources: initContainerResources,
				},
			},
			Containers: []v1.Container{
				{
					Resources: containerResources,
				},
			},
		},
	}

	expectedResources := v1.ResourceRequirements{
		Requests: v1.ResourceList{
			v1.ResourceCPU:              resource.MustParse("100m"),
			v1.ResourceMemory:           resource.MustParse("100Mi"),
			v1.ResourceEphemeralStorage: resource.MustParse("100Mi"),
		},
		Limits: v1.ResourceList{
			v1.ResourceCPU:              resource.MustParse("100m"),
			v1.ResourceMemory:           resource.MustParse("100Mi"),
			v1.ResourceEphemeralStorage: resource.MustParse("100Mi"),
		},
	}

	submitter.sanitizePodResources(pod)

	require.Equal(t, expectedResources, pod.Spec.InitContainers[0].Resources)
	require.Equal(t, expectedResources, pod.Spec.Containers[0].Resources)
}

func newK8sApiError(message string, reason metav1.StatusReason) *k8s_errors.StatusError {
	return &k8s_errors.StatusError{
		ErrStatus: metav1.Status{
			Message: message,
			Reason:  reason,
		},
	}
}

func newArbitraryError(message string) error {
	return errors.New(message)
}

func newArmadaErrCreateResource() error {
	return &armadaerrors.ErrCreateResource{}
}
