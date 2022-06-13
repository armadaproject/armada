package job

import (
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	k8s_errors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/G-Research/armada/internal/executor/configuration"
	"github.com/G-Research/armada/internal/executor/fake/context"
)

var testAppConfig = configuration.ApplicationConfiguration{ClusterId: "test", Pool: "pool"}

func TestIsRecoverable_ArbitraryErrorIsRecoverable(t *testing.T) {
	clusterContext := context.NewFakeClusterContext(testAppConfig, []*context.NodeSpec{})
	submitter := NewSubmitter(clusterContext, &configuration.PodDefaults{}, 1, []string{
		"admission webhook",
		"hello, [a-z]+!",
	})

	recoverable := submitter.isRecoverable(errors.New("some error"))
	assert.True(t, recoverable)
}

func TestIsRecoverable_KubernetesStatusInvalidIsUnrecoverable(t *testing.T) {
	clusterContext := context.NewFakeClusterContext(testAppConfig, []*context.NodeSpec{})
	submitter := NewSubmitter(clusterContext, &configuration.PodDefaults{}, 1, []string{})

	err := &k8s_errors.StatusError{ErrStatus: metav1.Status{
		Reason: metav1.StatusReasonInvalid,
	}}
	recoverable := submitter.isRecoverable(err)
	assert.False(t, recoverable)
}

func TestIsRecoverable_KubernetesStatusForbiddenIsUnrecoverable(t *testing.T) {
	clusterContext := context.NewFakeClusterContext(testAppConfig, []*context.NodeSpec{})
	submitter := NewSubmitter(clusterContext, &configuration.PodDefaults{}, 1, []string{})

	err := &k8s_errors.StatusError{ErrStatus: metav1.Status{
		Reason: metav1.StatusReasonForbidden,
	}}
	recoverable := submitter.isRecoverable(err)
	assert.False(t, recoverable)
}

func TestIsRecoverable_MatchingRegexIsUnrecoverable(t *testing.T) {
	clusterContext := context.NewFakeClusterContext(testAppConfig, []*context.NodeSpec{})
	submitter := NewSubmitter(clusterContext, &configuration.PodDefaults{}, 1, []string{
		"admission webhook",
		"hello, [a-z]+!",
	})

	recoverable := submitter.isRecoverable(errors.New("admission webhook failure: some webhook failed validation"))
	assert.False(t, recoverable)

	recoverable = submitter.isRecoverable(errors.New("Error: hello, john!"))
	assert.False(t, recoverable)

	recoverable = submitter.isRecoverable(errors.New("hello world!"))
	assert.True(t, recoverable)
}
