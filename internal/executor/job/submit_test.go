package job

import (
	stdcontext "context"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	k8s_errors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/armadaproject/armada/internal/common/armadaerrors"
	"github.com/armadaproject/armada/internal/executor/configuration"
	"github.com/armadaproject/armada/internal/executor/fake/context"
	"github.com/armadaproject/armada/internal/podwriter"
)

const (
	AdmissionWebhookRegex  = "admission webhook"
	NamespaceNotFoundRegex = "namespaces \".*\" not found"
	HelloRegex             = "hello, [a-z]+!"
)

var testAppConfig = configuration.ApplicationConfiguration{ClusterId: "test", Pool: "pool"}

func TestIsRecoverable_ArbitraryErrorIsNotRecoverable(t *testing.T) {
	clusterContext := context.NewFakeClusterContext(testAppConfig, "kubernetes.io/hostname", []*context.NodeSpec{})
	submitter := NewSubmitter(clusterContext, nil, nil, &configuration.PodDefaults{}, 1, []string{
		AdmissionWebhookRegex,
		HelloRegex,
		NamespaceNotFoundRegex,
	})

	recoverable := submitter.isRecoverable(newArbitraryError("some error"))
	assert.False(t, recoverable)
}

func TestIsRecoverable_KubernetesStatusInvalidIsUnrecoverable(t *testing.T) {
	clusterContext := context.NewFakeClusterContext(testAppConfig, "kubernetes.io/hostname", []*context.NodeSpec{})
	submitter := NewSubmitter(clusterContext, nil, nil, &configuration.PodDefaults{}, 1, []string{})

	recoverable := submitter.isRecoverable(newK8sApiError("", metav1.StatusReasonInvalid))
	assert.False(t, recoverable)
}

func TestIsRecoverable_KubernetesStatusForbiddenIsUnrecoverable(t *testing.T) {
	clusterContext := context.NewFakeClusterContext(testAppConfig, "kubernetes.io/hostname", []*context.NodeSpec{})
	submitter := NewSubmitter(clusterContext, nil, nil, &configuration.PodDefaults{}, 1, []string{})

	recoverable := submitter.isRecoverable(newK8sApiError("", metav1.StatusReasonForbidden))
	assert.False(t, recoverable)
}

func TestIsRecoverable_K8sApiMatchingRegexIsUnrecoverable(t *testing.T) {
	clusterContext := context.NewFakeClusterContext(testAppConfig, "kubernetes.io/hostname", []*context.NodeSpec{})
	submitter := NewSubmitter(clusterContext, nil, nil, &configuration.PodDefaults{}, 1, []string{
		AdmissionWebhookRegex,
		HelloRegex,
		NamespaceNotFoundRegex,
	})

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
	submitter := NewSubmitter(clusterContext, nil, nil, &configuration.PodDefaults{}, 1, []string{})

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

func TestSubmitJobsViaWriter_Created(t *testing.T) {
	clusterContext := context.NewFakeClusterContext(testAppConfig, "kubernetes.io/hostname", []*context.NodeSpec{})
	submitter := NewSubmitter(
		clusterContext,
		&fakePodWriter{status: podwriter.Created},
		nil,
		&configuration.PodDefaults{},
		1,
		nil,
	)

	failures := submitter.SubmitJobs([]*SubmitJob{testSubmitJob()})
	assert.Empty(t, failures)
}

func TestSubmitJobsViaWriter_TransientFailure(t *testing.T) {
	clusterContext := context.NewFakeClusterContext(testAppConfig, "kubernetes.io/hostname", []*context.NodeSpec{})
	submitter := NewSubmitter(
		clusterContext,
		&fakePodWriter{status: podwriter.Failed, transient: true},
		nil,
		&configuration.PodDefaults{},
		1,
		nil,
	)

	failures := submitter.SubmitJobs([]*SubmitJob{testSubmitJob()})
	assert.Len(t, failures, 1)
	assert.True(t, failures[0].Recoverable)
}

func TestSubmitJobsViaWriter_RejectedIsPermanent(t *testing.T) {
	clusterContext := context.NewFakeClusterContext(testAppConfig, "kubernetes.io/hostname", []*context.NodeSpec{})
	submitter := NewSubmitter(
		clusterContext,
		&fakePodWriter{status: podwriter.Rejected},
		nil,
		&configuration.PodDefaults{},
		1,
		nil,
	)

	failures := submitter.SubmitJobs([]*SubmitJob{testSubmitJob()})
	assert.Len(t, failures, 1)
	assert.False(t, failures[0].Recoverable)
}

func testSubmitJob() *SubmitJob {
	return &SubmitJob{
		Meta: SubmitJobMeta{
			RunMeta: &RunMeta{
				JobId:  "job-1",
				RunId:  "run-1",
				JobSet: "jobset-1",
				Queue:  "queue-1",
			},
			Owner: "user-1",
		},
		Pod: &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pod-1",
				Namespace: "default",
			},
		},
	}
}

type fakePodWriter struct {
	status    podwriter.Status
	transient bool
}

func (f *fakePodWriter) CreateBatch(_ stdcontext.Context, req podwriter.BatchRequest) podwriter.BatchResult {
	results := make([]podwriter.PodResult, len(req.Pods))
	for i := range req.Pods {
		results[i] = podwriter.PodResult{
			Name:      req.Pods[i].Pod.Name,
			Status:    f.status,
			Transient: f.transient,
		}
	}
	return podwriter.BatchResult{Results: results}
}
