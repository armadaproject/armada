package job

import (
	"testing"

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
)

var testAppConfig = configuration.ApplicationConfiguration{ClusterId: "test", Pool: "pool"}

func TestIsRecoverable_ArbitraryErrorIsNotRecoverable(t *testing.T) {
	clusterContext := context.NewFakeClusterContext(testAppConfig, "kubernetes.io/hostname", []*context.NodeSpec{})
	submitter := NewSubmitter(clusterContext, &configuration.PodDefaults{}, 1, []string{
		AdmissionWebhookRegex,
		HelloRegex,
		NamespaceNotFoundRegex,
	}, false)

	recoverable := submitter.isRecoverable(newArbitraryError("some error"))
	assert.False(t, recoverable)
}

func TestIsRecoverable_KubernetesStatusInvalidIsUnrecoverable(t *testing.T) {
	clusterContext := context.NewFakeClusterContext(testAppConfig, "kubernetes.io/hostname", []*context.NodeSpec{})
	submitter := NewSubmitter(clusterContext, &configuration.PodDefaults{}, 1, []string{}, false)

	recoverable := submitter.isRecoverable(newK8sApiError("", metav1.StatusReasonInvalid))
	assert.False(t, recoverable)
}

func TestIsRecoverable_KubernetesStatusForbiddenIsUnrecoverable(t *testing.T) {
	clusterContext := context.NewFakeClusterContext(testAppConfig, "kubernetes.io/hostname", []*context.NodeSpec{})
	submitter := NewSubmitter(clusterContext, &configuration.PodDefaults{}, 1, []string{}, false)

	recoverable := submitter.isRecoverable(newK8sApiError("", metav1.StatusReasonForbidden))
	assert.False(t, recoverable)
}

func TestIsRecoverable_K8sApiMatchingRegexIsUnrecoverable(t *testing.T) {
	clusterContext := context.NewFakeClusterContext(testAppConfig, "kubernetes.io/hostname", []*context.NodeSpec{})
	submitter := NewSubmitter(clusterContext, &configuration.PodDefaults{}, 1, []string{
		AdmissionWebhookRegex,
		HelloRegex,
		NamespaceNotFoundRegex,
	}, false)

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
	submitter := NewSubmitter(clusterContext, &configuration.PodDefaults{}, 1, []string{}, false)

	recoverable := submitter.isRecoverable(newArmadaErrCreateResource())
	assert.True(t, recoverable)
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
