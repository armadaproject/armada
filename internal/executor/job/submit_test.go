package job

import (
	"maps"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	networking "k8s.io/api/networking/v1"
	k8s_errors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	clientTesting "k8s.io/client-go/testing"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/armadaerrors"
	"github.com/armadaproject/armada/internal/executor/configuration"
	executorContext "github.com/armadaproject/armada/internal/executor/context"
	"github.com/armadaproject/armada/internal/executor/domain"
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

func TestSubmitService_SubmitJobs_ExistingObjects(t *testing.T) {
	tests := []struct {
		name                    string
		runScopedPodNames       bool
		existingPodRunId        string
		existingPodDeleting     bool
		createPodErr            error
		getPodErr               error
		existingServiceRunId    string
		existingServiceDeleting bool
		existingIngressRunId    string
		wantFailures            int
		wantPodDeleted          bool
		wantServiceRunId        string
		wantIngressRunId        string
	}{
		{
			name:              "run-scoped: a pod of the same run that exists is a success",
			runScopedPodNames: true,
			existingPodRunId:  "run-2",
			wantServiceRunId:  "run-2",
			wantIngressRunId:  "run-2",
		},
		{
			name:                "run-scoped: a pod of the same run that is being deleted fails and stays",
			runScopedPodNames:   true,
			existingPodRunId:    "run-2",
			existingPodDeleting: true,
			wantFailures:        1,
		},
		{
			name:              "run-scoped: a pod of another run that holds the name fails and stays",
			runScopedPodNames: true,
			existingPodRunId:  "run-1",
			wantFailures:      1,
		},
		{
			name:             "job-scoped: a pod that holds the name fails and is deleted by name",
			existingPodRunId: "run-1",
			wantFailures:     1,
			wantPodDeleted:   true,
		},
		{
			name:              "run-scoped: a pod with a name that exists and an owner that cannot be read stays",
			runScopedPodNames: true,
			existingPodRunId:  "run-1",
			getPodErr:         errors.New("connection refused"),
			wantFailures:      1,
		},
		{
			name:              "run-scoped: a pod create that fails for another reason deletes the pod of this run",
			runScopedPodNames: true,
			createPodErr:      k8s_errors.NewInternalError(errors.New("timeout")),
			wantFailures:      1,
			wantPodDeleted:    true,
		},
		{
			name:                 "run-scoped: a service and an ingress of the same run are a success",
			runScopedPodNames:    true,
			existingPodRunId:     "run-2",
			existingServiceRunId: "run-2",
			existingIngressRunId: "run-2",
			wantServiceRunId:     "run-2",
			wantIngressRunId:     "run-2",
		},
		{
			name:                    "run-scoped: a service of the same run that is being deleted fails",
			runScopedPodNames:       true,
			existingPodRunId:        "run-2",
			existingServiceRunId:    "run-2",
			existingServiceDeleting: true,
			wantFailures:            1,
			wantPodDeleted:          true,
			wantServiceRunId:        "run-2",
		},
		{
			name:                 "run-scoped: a service of an earlier run fails, and the pod of this run is deleted",
			runScopedPodNames:    true,
			existingServiceRunId: "run-1",
			wantFailures:         1,
			wantPodDeleted:       true,
			wantServiceRunId:     "run-1",
		},
		{
			name:                 "run-scoped: an ingress of an earlier run fails, and the pod of this run is deleted",
			runScopedPodNames:    true,
			existingIngressRunId: "run-1",
			wantFailures:         1,
			wantPodDeleted:       true,
			wantServiceRunId:     "run-2",
			wantIngressRunId:     "run-1",
		},
		{
			name:                 "job-scoped: a service that holds the name fails, and the pod is deleted",
			existingServiceRunId: "run-1",
			wantFailures:         1,
			wantPodDeleted:       true,
			wantServiceRunId:     "run-1",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			registerer := prometheus.DefaultRegisterer
			prometheus.DefaultRegisterer = prometheus.NewRegistry()
			t.Cleanup(func() { prometheus.DefaultRegisterer = registerer })
			client := fake.NewSimpleClientset()
			appConfig := configuration.ApplicationConfiguration{ClusterId: "test", Pool: "pool", DeleteConcurrencyLimit: 1}
			clusterContext := executorContext.NewClusterContext(appConfig, 2*time.Minute, &fakeClientProvider{client: client}, 5*time.Minute, tc.runScopedPodNames)
			t.Cleanup(clusterContext.Stop)
			submitter := NewSubmitter(clusterContext, &configuration.PodDefaults{}, 1, []string{}, tc.runScopedPodNames)

			job := submitJobForRun("run-2")
			if tc.existingPodRunId != "" {
				existing := job.Pod.DeepCopy()
				existing.Labels[domain.JobRunId] = tc.existingPodRunId
				if tc.existingPodDeleting {
					now := metav1.Now()
					existing.DeletionTimestamp = &now
				}
				_, err := client.CoreV1().Pods(existing.Namespace).Create(armadacontext.Background(), existing, metav1.CreateOptions{})
				require.NoError(t, err)
			}
			if tc.getPodErr != nil {
				client.PrependReactor("get", "pods", func(clientTesting.Action) (bool, runtime.Object, error) {
					return true, nil, tc.getPodErr
				})
			}
			if tc.createPodErr != nil {
				client.PrependReactor("create", "pods", func(clientTesting.Action) (bool, runtime.Object, error) {
					return true, nil, tc.createPodErr
				})
			}
			if tc.existingServiceRunId != "" {
				existing := job.Services[0].DeepCopy()
				existing.Labels[domain.JobRunId] = tc.existingServiceRunId
				if tc.existingServiceDeleting {
					now := metav1.Now()
					existing.DeletionTimestamp = &now
				}
				_, err := client.CoreV1().Services(existing.Namespace).Create(armadacontext.Background(), existing, metav1.CreateOptions{})
				require.NoError(t, err)
			}
			if tc.existingIngressRunId != "" {
				existing := job.Ingresses[0].DeepCopy()
				existing.Labels[domain.JobRunId] = tc.existingIngressRunId
				_, err := client.NetworkingV1().Ingresses(existing.Namespace).Create(armadacontext.Background(), existing, metav1.CreateOptions{})
				require.NoError(t, err)
			}
			client.Fake.ClearActions()

			failures := submitter.SubmitJobs([]*SubmitJob{job})
			clusterContext.ProcessPodsToDelete()

			require.Len(t, failures, tc.wantFailures)
			if tc.wantFailures > 0 {
				assert.True(t, failures[0].Recoverable)
			}
			assert.Equal(t, tc.wantPodDeleted, hasPodDeleteRequest(client, job.Pod.Name))
			assert.Equal(t, tc.wantServiceRunId, storedRunId(t, client, "services", job.Services[0].Name))
			assert.Equal(t, tc.wantIngressRunId, storedRunId(t, client, "ingresses", job.Ingresses[0].Name))
		})
	}
}

// storedRunId returns the run ID label of a stored service or ingress, or "" when it does not exist.
func storedRunId(t *testing.T, client *fake.Clientset, resource string, name string) string {
	var labels map[string]string
	var err error
	switch resource {
	case "services":
		var service *v1.Service
		service, err = client.CoreV1().Services("default").Get(armadacontext.Background(), name, metav1.GetOptions{})
		if err == nil {
			labels = service.Labels
		}
	case "ingresses":
		var ingress *networking.Ingress
		ingress, err = client.NetworkingV1().Ingresses("default").Get(armadacontext.Background(), name, metav1.GetOptions{})
		if err == nil {
			labels = ingress.Labels
		}
	}
	if k8s_errors.IsNotFound(err) {
		return ""
	}
	require.NoError(t, err)
	return labels[domain.JobRunId]
}

func submitJobForRun(runId string) *SubmitJob {
	labels := map[string]string{domain.JobId: "job-1", domain.JobRunId: runId, domain.Queue: "queue", domain.PodNumber: "0"}
	return &SubmitJob{
		Meta: SubmitJobMeta{RunMeta: &RunMeta{JobId: "job-1", RunId: runId, JobSet: "job-set", Queue: "queue"}, Owner: "user"},
		Pod: &v1.Pod{ObjectMeta: metav1.ObjectMeta{
			Name:      "pod",
			Namespace: "default",
			Labels:    maps.Clone(labels),
		}},
		Services: []*v1.Service{{ObjectMeta: metav1.ObjectMeta{
			Name:      "armada-job-1-0-service-0",
			Namespace: "default",
			Labels:    maps.Clone(labels),
		}}},
		Ingresses: []*networking.Ingress{{ObjectMeta: metav1.ObjectMeta{
			Name:      "armada-job-1-0-ingress-0",
			Namespace: "default",
			Labels:    maps.Clone(labels),
		}}},
	}
}

// hasPodDeleteRequest is true when the executor tried to delete the pod. The executor first marks the pod
// for deletion with a patch, so a pod that does not exist gets the patch and no delete.
func hasPodDeleteRequest(client *fake.Clientset, name string) bool {
	for _, action := range client.Fake.Actions() {
		if action.GetResource().Resource != "pods" {
			continue
		}
		if deleteAction, ok := action.(clientTesting.DeleteAction); ok && deleteAction.GetName() == name {
			return true
		}
		if patchAction, ok := action.(clientTesting.PatchAction); ok && patchAction.GetName() == name {
			return true
		}
	}
	return false
}

type fakeClientProvider struct {
	client *fake.Clientset
}

func (p *fakeClientProvider) ClientForUser(string, []string) (kubernetes.Interface, error) {
	return p.client, nil
}

func (p *fakeClientProvider) Client() kubernetes.Interface {
	return p.client
}

func (p *fakeClientProvider) ClientConfig() *rest.Config {
	return nil
}
