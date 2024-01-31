package context

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	networking "k8s.io/api/networking/v1"
	errors2 "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/clock"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	clientTesting "k8s.io/client-go/testing"
	"k8s.io/utils/pointer"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	util2 "github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/executor/configuration"
	"github.com/armadaproject/armada/internal/executor/domain"
	"github.com/armadaproject/armada/internal/executor/util"
)

func setupTest() (*KubernetesClusterContext, *fake.Clientset) {
	context, provider := setupTestWithMinRepeatedDeletePeriod(2 * time.Minute)
	return context, provider.FakeClient
}

func setupTestWithProvider() (*KubernetesClusterContext, *FakeClientProvider) {
	return setupTestWithMinRepeatedDeletePeriod(2 * time.Minute)
}

func setupTestWithMinRepeatedDeletePeriod(minRepeatedDeletePeriod time.Duration) (*KubernetesClusterContext, *FakeClientProvider) {
	prometheus.DefaultRegisterer = prometheus.NewRegistry()
	client := fake.NewSimpleClientset()
	clientProvider := &FakeClientProvider{FakeClient: client}
	clusterContext := NewClusterContext(
		configuration.ApplicationConfiguration{ClusterId: "test-cluster-1", Pool: "pool", DeleteConcurrencyLimit: 1},
		minRepeatedDeletePeriod,
		clientProvider,
		5*time.Minute,
	)
	return clusterContext, clientProvider
}

func TestKubernetesClusterContext_SubmitPod(t *testing.T) {
	clusterContext, client := setupTest()

	pod := createBatchPod()
	client.Fake.ClearActions()

	_, err := clusterContext.SubmitPod(pod, "user1", []string{})
	assert.Nil(t, err)

	assert.Equal(t, len(client.Fake.Actions()), 1)
	assert.True(t, client.Fake.Actions()[0].Matches("create", "pods"))

	createAction, ok := client.Fake.Actions()[0].(clientTesting.CreateAction)
	assert.True(t, ok)
	assert.Equal(t, createAction.GetObject(), pod)
}

func TestKubernetesClusterContext_ProcessPodsToDelete_DoesNotCallClient_WhenNoPodsMarkedForDeletion(t *testing.T) {
	clusterContext, client := setupTest()

	client.Fake.ClearActions()
	clusterContext.ProcessPodsToDelete()

	assert.Equal(t, len(client.Fake.Actions()), 0)
}

func TestKubernetesClusterContext_SubmitService(t *testing.T) {
	clusterContext, client := setupTest()

	service := createService()
	client.Fake.ClearActions()

	_, err := clusterContext.SubmitService(service)
	assert.Nil(t, err)

	assert.Equal(t, len(client.Fake.Actions()), 1)
	assert.True(t, client.Fake.Actions()[0].Matches("create", "services"))

	createAction, ok := client.Fake.Actions()[0].(clientTesting.CreateAction)
	assert.True(t, ok)
	assert.Equal(t, createAction.GetObject(), service)
}

func TestKubernetesClusterContext_DeleteService(t *testing.T) {
	clusterContext, client := setupTest()

	service := createService()

	_, err := clusterContext.SubmitService(service)
	assert.NoError(t, err)
	client.Fake.ClearActions()

	err = clusterContext.DeleteService(service)
	assert.NoError(t, err)

	assert.Equal(t, len(client.Fake.Actions()), 1)
	assert.True(t, client.Fake.Actions()[0].Matches("delete", "services"))

	deleteAction, ok := client.Fake.Actions()[0].(clientTesting.DeleteAction)
	assert.True(t, ok)
	assert.Equal(t, deleteAction.GetName(), service.Name)
}

func TestKubernetesClusterContext_DeleteService_NonExistent(t *testing.T) {
	clusterContext, _ := setupTest()
	service := createService()

	err := clusterContext.DeleteService(service)
	assert.NoError(t, err)
}

func TestKubernetesClusterContext_GetServices(t *testing.T) {
	clusterContext, client := setupTest()

	pod := createBatchPod()
	service := createService()
	service.ObjectMeta.Labels = util2.MergeMaps(service.ObjectMeta.Labels, pod.ObjectMeta.Labels)

	_, err := clusterContext.SubmitService(service)
	assert.NoError(t, err)
	waitForServiceContextSync(t, clusterContext, service)
	client.Fake.ClearActions()

	result, err := clusterContext.GetServices(pod)
	assert.NoError(t, err)

	assert.NotNil(t, result)
	assert.Equal(t, len(result), 1)
	assert.Equal(t, service.Name, result[0].Name)
}

func TestKubernetesClusterContext_GetServices_MissingPodLabels(t *testing.T) {
	clusterContext, client := setupTest()

	pod := createBatchPod()
	service := createService()
	service.ObjectMeta.Labels = util2.MergeMaps(service.ObjectMeta.Labels, pod.ObjectMeta.Labels)
	delete(pod.ObjectMeta.Labels, domain.PodNumber)

	_, err := clusterContext.SubmitService(service)
	assert.NoError(t, err)
	waitForServiceContextSync(t, clusterContext, service)
	client.Fake.ClearActions()

	result, err := clusterContext.GetServices(pod)
	assert.Error(t, err)
	assert.Equal(t, len(result), 0)
}

func TestKubernetesClusterContext_GetServices_NonExistent(t *testing.T) {
	clusterContext, _ := setupTest()
	pod := createBatchPod()

	result, err := clusterContext.GetServices(pod)
	assert.NoError(t, err)
	assert.Equal(t, len(result), 0)
}

func TestKubernetesClusterContext_SubmitIngress(t *testing.T) {
	clusterContext, client := setupTest()

	ingress := createIngress()
	client.Fake.ClearActions()

	_, err := clusterContext.SubmitIngress(ingress)
	assert.Nil(t, err)

	assert.Equal(t, len(client.Fake.Actions()), 1)
	assert.True(t, client.Fake.Actions()[0].Matches("create", "ingresses"))

	createAction, ok := client.Fake.Actions()[0].(clientTesting.CreateAction)
	assert.True(t, ok)
	assert.Equal(t, createAction.GetObject(), ingress)
}

func TestKubernetesClusterContext_DeleteIngress(t *testing.T) {
	clusterContext, client := setupTest()

	ingress := createIngress()

	_, err := clusterContext.SubmitIngress(ingress)
	assert.NoError(t, err)
	client.Fake.ClearActions()

	err = clusterContext.DeleteIngress(ingress)
	assert.NoError(t, err)

	assert.Equal(t, len(client.Fake.Actions()), 1)
	assert.True(t, client.Fake.Actions()[0].Matches("delete", "ingresses"))

	deleteAction, ok := client.Fake.Actions()[0].(clientTesting.DeleteAction)
	assert.True(t, ok)
	assert.Equal(t, deleteAction.GetName(), ingress.Name)
}

func TestKubernetesClusterContext_DeleteIngress_NonExistent(t *testing.T) {
	clusterContext, _ := setupTest()
	ingress := createIngress()

	err := clusterContext.DeleteIngress(ingress)
	assert.NoError(t, err)
}

func TestKubernetesClusterContext_GetIngresses(t *testing.T) {
	clusterContext, client := setupTest()

	pod := createBatchPod()
	ingress := createIngress()
	ingress.ObjectMeta.Labels = util2.MergeMaps(ingress.ObjectMeta.Labels, pod.ObjectMeta.Labels)

	_, err := clusterContext.SubmitIngress(ingress)
	assert.NoError(t, err)
	waitForIngressContextSync(t, clusterContext, ingress)
	client.Fake.ClearActions()

	result, err := clusterContext.GetIngresses(pod)
	assert.NoError(t, err)

	assert.NotNil(t, result)
	assert.Equal(t, len(result), 1)
	assert.Equal(t, ingress.Name, result[0].Name)
}

func TestKubernetesClusterContext_GetIngresses_MissingPodLabels(t *testing.T) {
	clusterContext, client := setupTest()

	pod := createBatchPod()
	ingress := createIngress()
	ingress.ObjectMeta.Labels = util2.MergeMaps(ingress.ObjectMeta.Labels, pod.ObjectMeta.Labels)
	delete(pod.ObjectMeta.Labels, domain.PodNumber)

	_, err := clusterContext.SubmitIngress(ingress)
	assert.NoError(t, err)
	waitForIngressContextSync(t, clusterContext, ingress)
	client.Fake.ClearActions()

	result, err := clusterContext.GetIngresses(pod)
	assert.Error(t, err)
	assert.Equal(t, len(result), 0)
}

func TestKubernetesClusterContext_GetIngresses_NonExistent(t *testing.T) {
	clusterContext, _ := setupTest()
	pod := createBatchPod()

	result, err := clusterContext.GetIngresses(pod)
	assert.NoError(t, err)
	assert.Equal(t, len(result), 0)
}

func TestKubernetesClusterContext_DeletePodWithCondition(t *testing.T) {
	clusterContext, client := setupTest()
	pod := createSubmittedBatchPod(t, clusterContext)

	client.Fake.ClearActions()

	err := clusterContext.DeletePodWithCondition(pod, func(pod *v1.Pod) bool {
		return true
	}, true)

	assert.NoError(t, err)
	assert.Equal(t, len(client.Fake.Actions()), 3)
	assert.True(t, client.Fake.Actions()[0].Matches("patch", "pods"))

	// Marks pod for deletion before deleting
	patchAction, ok := client.Fake.Actions()[0].(clientTesting.PatchAction)
	assert.True(t, ok)
	assert.Equal(t, patchAction.GetName(), pod.Name)
	patch := &domain.Patch{}
	err = json.Unmarshal(patchAction.GetPatch(), patch)
	assert.NoError(t, err)
	_, exists := patch.MetaData.Annotations[domain.MarkedForDeletion]
	assert.True(t, exists)

	// Calls delete
	assert.True(t, client.Fake.Actions()[2].Matches("delete", "pods"))
}

func TestKubernetesClusterContext_DeletePodWithCondition_DoesNotReapplyMarkForDeletion(t *testing.T) {
	clusterContext, client := setupTest()
	pod := createBatchPod()
	pod.ObjectMeta.Annotations = map[string]string{domain.MarkedForDeletion: "now"}
	submitPodsWithWait(t, clusterContext, pod)
	client.Fake.ClearActions()

	err := clusterContext.DeletePodWithCondition(pod, func(pod *v1.Pod) bool {
		return true
	}, true)

	assert.NoError(t, err)
	assert.Equal(t, len(client.Fake.Actions()), 1)
	assert.True(t, client.Fake.Actions()[0].Matches("delete", "pods"))
}

func TestKubernetesClusterContext_DeletePodWithCondition_WhenPodDoesNotExist(t *testing.T) {
	clusterContext, _ := setupTest()
	pod := createBatchPod() // Not submitted pod

	err := clusterContext.DeletePodWithCondition(pod, func(pod *v1.Pod) bool {
		return true
	}, true)

	assert.Error(t, err)
}

func TestKubernetesClusterContext_DeletePodWithCondition_ForceDeletesPod(t *testing.T) {
	clusterContext, client := setupTest()
	testClock := clock.NewFakeClock(time.Now())
	// create a pod with a deletion event 5 minutes in the past. This should be ignored
	pod := createBatchPod()
	pod.ObjectMeta.Annotations = map[string]string{domain.MarkedForDeletion: "now"}
	pod.DeletionGracePeriodSeconds = pointer.Int64(10)
	pod.DeletionTimestamp = &metav1.Time{Time: testClock.Now().Add(-300 * time.Second)}
	submitPodsWithWait(t, clusterContext, pod)
	client.Fake.ClearActions()

	err := clusterContext.DeletePodWithCondition(pod, func(pod *v1.Pod) bool {
		return true
	}, true)
	assert.NoError(t, err)
	assert.Equal(t, len(client.Fake.Actions()), 0)

	// create another pod with a deletion event 5 minutes and 10 seconds in the past. This should be force killed
	pod = createBatchPod()
	pod.ObjectMeta.Annotations = map[string]string{domain.MarkedForDeletion: "now"}
	pod.DeletionGracePeriodSeconds = pointer.Int64(10)
	pod.DeletionTimestamp = &metav1.Time{Time: testClock.Now().Add(-310 * time.Second)}
	submitPodsWithWait(t, clusterContext, pod)
	client.Fake.ClearActions()

	err = clusterContext.DeletePodWithCondition(pod, func(pod *v1.Pod) bool {
		return true
	}, true)
	assert.NoError(t, err)
	assert.Equal(t, len(client.Fake.Actions()), 1)
}

func TestKubernetesClusterContext_DeletePodWithCondition_DoesNotDelete_WhenPodDoesNotMatchCondition(t *testing.T) {
	clusterContext, _ := setupTest()

	pod := createSubmittedBatchPod(t, clusterContext)
	err := clusterContext.DeletePodWithCondition(pod, func(pod *v1.Pod) bool {
		return false
	}, true)

	assert.Error(t, err)
}

func TestKubernetesClusterContext_ProcessPodsToDelete_CallDeleteOnClient_WhenPodsMarkedForDeletion(t *testing.T) {
	clusterContext, client := setupTest()

	pod := createSubmittedBatchPod(t, clusterContext)

	client.Fake.ClearActions()
	clusterContext.DeletePods([]*v1.Pod{pod})
	clusterContext.ProcessPodsToDelete()

	assert.Equal(t, len(client.Fake.Actions()), 2)
	assert.True(t, client.Fake.Actions()[1].Matches("delete", "pods"))

	deleteAction, ok := client.Fake.Actions()[1].(clientTesting.DeleteAction)
	assert.True(t, ok)
	assert.Equal(t, deleteAction.GetName(), pod.Name)
}

func TestKubernetesClusterContext_ProcessPodsToDelete_CallPatchOnClient_WhenPodsMarkedForDeletion(t *testing.T) {
	clusterContext, client := setupTest()

	pod := createSubmittedBatchPod(t, clusterContext)

	client.Fake.ClearActions()
	clusterContext.DeletePods([]*v1.Pod{pod})
	clusterContext.ProcessPodsToDelete()

	assert.Equal(t, len(client.Fake.Actions()), 2)
	assert.True(t, client.Fake.Actions()[0].Matches("patch", "pods"))

	patchAction, ok := client.Fake.Actions()[0].(clientTesting.PatchAction)
	assert.True(t, ok)
	assert.Equal(t, patchAction.GetName(), pod.Name)

	patch := &domain.Patch{}
	err := json.Unmarshal(patchAction.GetPatch(), patch)
	assert.NoError(t, err)
	_, exists := patch.MetaData.Annotations[domain.MarkedForDeletion]
	assert.True(t, exists)
}

func TestKubernetesClusterContext_ProcessPodsToDelete_ForceKill(t *testing.T) {
	clusterContext, client := setupTest()
	testClock := clock.NewFakeClock(time.Now())

	// create a pod with a deletion event 5 minutes in the past. This should be ignored
	pod := createSubmittedBatchPod(t, clusterContext)
	pod.DeletionGracePeriodSeconds = pointer.Int64(10)
	pod.DeletionTimestamp = &metav1.Time{Time: testClock.Now().Add(-300 * time.Second)}
	client.Fake.ClearActions()

	clusterContext.DeletePods([]*v1.Pod{pod})
	clusterContext.ProcessPodsToDelete()
	assert.Equal(t, len(client.Fake.Actions()), 0)

	// create another pod with a deletion event 5 minutes and 10 seconds in the past. This should be force killed
	pod = createSubmittedBatchPod(t, clusterContext)
	pod.DeletionGracePeriodSeconds = pointer.Int64(10)
	pod.DeletionTimestamp = &metav1.Time{Time: testClock.Now().Add(-310 * time.Second)}
	client.Fake.ClearActions()

	// submitPodsWithWait(t, clusterContext, pod)
	client.Fake.ClearActions()
	clusterContext.DeletePods([]*v1.Pod{pod})
	clusterContext.ProcessPodsToDelete()
	assert.Equal(t, len(client.Fake.Actions()), 2)
}

func TestKubernetesClusterContext_ProcessPodsToDelete_PreventsRepeatedPatchCallsToClient_WhenPodAlreadyMarkedForDeletion(t *testing.T) {
	clusterContext, client := setupTest()

	pod := createSubmittedBatchPod(t, clusterContext)
	pod.ObjectMeta.Annotations = map[string]string{domain.MarkedForDeletion: "now"}

	client.Fake.ClearActions()
	clusterContext.DeletePods([]*v1.Pod{pod})
	clusterContext.ProcessPodsToDelete()

	assert.Equal(t, len(client.Fake.Actions()), 1)
	assert.True(t, client.Fake.Actions()[0].Matches("delete", "pods"))
}

func TestKubernetesClusterContext_ProcessPodsToDelete_PreventsRepeatedDeleteCallsToClient_OnDeleteClientSuccess(t *testing.T) {
	clusterContext, client := setupTest()

	pod := createSubmittedBatchPod(t, clusterContext)

	client.Fake.ClearActions()
	clusterContext.DeletePods([]*v1.Pod{pod})
	clusterContext.ProcessPodsToDelete()
	assert.Equal(t, len(client.Fake.Actions()), 2)

	client.Fake.ClearActions()
	clusterContext.DeletePods([]*v1.Pod{pod})
	clusterContext.ProcessPodsToDelete()
	assert.Equal(t, len(client.Fake.Actions()), 0)
}

func TestKubernetesClusterContext_ProcessPodsToDelete_PreventsRepeatedDeleteCallsToClient_OnPatchClientErrorNotFound(t *testing.T) {
	clusterContext, client := setupTest()
	client.Fake.PrependReactor("patch", "pods", func(action clientTesting.Action) (bool, runtime.Object, error) {
		notFound := errors2.StatusError{
			ErrStatus: metav1.Status{
				Reason: metav1.StatusReasonNotFound,
			},
		}
		return true, nil, &notFound
	})

	pod := createSubmittedBatchPod(t, clusterContext)

	client.Fake.ClearActions()
	clusterContext.DeletePods([]*v1.Pod{pod})
	clusterContext.ProcessPodsToDelete()
	assert.Equal(t, len(client.Fake.Actions()), 1)

	client.Fake.ClearActions()
	clusterContext.DeletePods([]*v1.Pod{pod})
	clusterContext.ProcessPodsToDelete()
	assert.Equal(t, len(client.Fake.Actions()), 0)
}

func TestKubernetesClusterContext_ProcessPodsToDelete_PreventsRepeatedDeleteCallsToClient_OnClientErrorNotFound(t *testing.T) {
	clusterContext, client := setupTest()
	client.Fake.PrependReactor("delete", "pods", func(action clientTesting.Action) (bool, runtime.Object, error) {
		notFound := errors2.StatusError{
			ErrStatus: metav1.Status{
				Reason: metav1.StatusReasonNotFound,
			},
		}
		return true, nil, &notFound
	})

	pod := createSubmittedBatchPod(t, clusterContext)

	client.Fake.ClearActions()
	clusterContext.DeletePods([]*v1.Pod{pod})
	clusterContext.ProcessPodsToDelete()
	assert.Equal(t, len(client.Fake.Actions()), 2)

	client.Fake.ClearActions()
	clusterContext.DeletePods([]*v1.Pod{pod})
	clusterContext.ProcessPodsToDelete()
	assert.Equal(t, len(client.Fake.Actions()), 0)
}

func TestKubernetesClusterContext_ProcessPodsToDelete_AllowsRepeatedDeleteCallToClient_OnClientError(t *testing.T) {
	clusterContext, client := setupTest()
	client.Fake.PrependReactor("delete", "pods", func(action clientTesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("server error")
	})

	pod := createSubmittedBatchPod(t, clusterContext)

	client.Fake.ClearActions()
	clusterContext.DeletePods([]*v1.Pod{pod})
	clusterContext.ProcessPodsToDelete()
	assert.Equal(t, len(client.Fake.Actions()), 2)

	client.Fake.ClearActions()
	clusterContext.DeletePods([]*v1.Pod{pod})
	clusterContext.ProcessPodsToDelete()
	assert.Equal(t, len(client.Fake.Actions()), 2)
}

func TestKubernetesClusterContext_ProcessPodsToDelete_AllowsRepeatedDeleteCallToClient_AfterMinimumDeletePeriodHasPassed(t *testing.T) {
	timeBetweenRepeatedDeleteCalls := 500 * time.Millisecond
	clusterContext, provider := setupTestWithMinRepeatedDeletePeriod(timeBetweenRepeatedDeleteCalls)
	client := provider.FakeClient

	pod := createSubmittedBatchPod(t, clusterContext)
	client.Fake.ClearActions()
	clusterContext.DeletePods([]*v1.Pod{pod})
	clusterContext.ProcessPodsToDelete()
	assert.Equal(t, len(client.Fake.Actions()), 2)

	// Wait time required between repeated delete calls
	time.Sleep(timeBetweenRepeatedDeleteCalls + 200*time.Millisecond)

	submitPodsWithWait(t, clusterContext, pod)
	client.Fake.ClearActions()
	clusterContext.DeletePods([]*v1.Pod{pod})
	clusterContext.ProcessPodsToDelete()
	assert.Equal(t, len(client.Fake.Actions()), 2)
}

func TestKubernetesClusterContext_AddAnnotation(t *testing.T) {
	clusterContext, _ := setupTest()
	pod := createSubmittedBatchPod(t, clusterContext)

	annotationsToAdd := map[string]string{"test": "annotation"}
	err := clusterContext.AddAnnotation(pod, annotationsToAdd)
	assert.Nil(t, err)

	updateOccurred := waitForCondition(func() bool {
		pods, err := clusterContext.GetActiveBatchPods()
		assert.Nil(t, err)
		return len(pods) > 0 && pods[0].Annotations["test"] == "annotation"
	})
	assert.True(t, updateOccurred)
}

func TestKubernetesClusterContext_AddAnnotation_ReturnsError_OnClientError(t *testing.T) {
	clusterContext, client := setupTest()
	client.Fake.PrependReactor("patch", "pods", func(action clientTesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("server error")
	})

	pod := createSubmittedBatchPod(t, clusterContext)

	annotationsToAdd := map[string]string{"test": "\\"}
	err := clusterContext.AddAnnotation(pod, annotationsToAdd)
	assert.NotNil(t, err)
}

func TestKubernetesClusterContext_GetAllPods(t *testing.T) {
	clusterContext, _ := setupTest()

	nonBatchPod := createPod()
	batchPod := createBatchPod()
	submitPodsWithWait(t, clusterContext, nonBatchPod, batchPod)

	clusterContext.Stop() // This prevents newly submitted pods being picked up by the informers
	transientBatchPod := createBatchPod()
	submitPod(t, clusterContext, transientBatchPod)

	allPods, err := clusterContext.GetAllPods()

	assert.Nil(t, err)
	assert.Equal(t, len(allPods), 3)

	// Confirm the pods returned are the ones created
	podSet := util2.StringListToSet(util.ExtractNames(allPods))
	assert.True(t, podSet[nonBatchPod.Name])
	assert.True(t, podSet[batchPod.Name])
	assert.True(t, podSet[transientBatchPod.Name])
}

func TestKubernetesClusterContext_GetAllPods_DeduplicatesTransientPods(t *testing.T) {
	clusterContext, _ := setupTest()

	batchPod := createSubmittedBatchPod(t, clusterContext)

	// Forcibly add pod back to cache, so now it exists in kubernetes + cache
	clusterContext.submittedPods.Add(batchPod)

	allPods, err := clusterContext.GetAllPods()

	assert.Nil(t, err)
	assert.Equal(t, len(allPods), 1)

	// Confirm the pods returned are the ones created
	podSet := util2.StringListToSet(util.ExtractNames(allPods))
	assert.True(t, podSet[batchPod.Name])
}

func TestKubernetesClusterContext_GetBatchPods_ReturnsOnlyBatchPods_IncludingTransient(t *testing.T) {
	clusterContext, _ := setupTest()

	nonBatchPod := createPod()
	batchPod := createBatchPod()
	submitPodsWithWait(t, clusterContext, nonBatchPod, batchPod)

	clusterContext.Stop() // This prevents newly submitted pods being picked up by the informers
	transientBatchPod := createBatchPod()
	submitPod(t, clusterContext, transientBatchPod)

	allPods, err := clusterContext.GetBatchPods()

	assert.Nil(t, err)
	assert.Equal(t, len(allPods), 2)

	// Confirm the pods returned are the ones created
	podSet := util2.StringListToSet(util.ExtractNames(allPods))
	assert.True(t, podSet[batchPod.Name])
	assert.True(t, podSet[transientBatchPod.Name])
}

func TestKubernetesClusterContext_GetBatchPods_DoesNotShowTransient_OnSubmitFailure(t *testing.T) {
	clusterContext, client := setupTest()
	client.Fake.PrependReactor("create", "pods", func(action clientTesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("server error")
	})

	batchPod := createBatchPod()
	_, err := clusterContext.SubmitPod(batchPod, "user", []string{})
	assert.NotNil(t, err)

	allPods, err := clusterContext.GetBatchPods()

	assert.Nil(t, err)
	assert.Equal(t, len(allPods), 0)
}

func TestKubernetesClusterContext_GetBatchPods_DeduplicatesTransientPods(t *testing.T) {
	clusterContext, _ := setupTest()

	batchPod := createSubmittedBatchPod(t, clusterContext)

	// Forcibly add pod back to cache, so now it exists in kubernetes + cache
	clusterContext.submittedPods.Add(batchPod)

	allPods, err := clusterContext.GetBatchPods()

	assert.Nil(t, err)
	assert.Equal(t, len(allPods), 1)

	// Confirm the pods returned are the ones created
	podSet := util2.StringListToSet(util.ExtractNames(allPods))
	assert.True(t, podSet[batchPod.Name])
}

func TestKubernetesClusterContext_GetActiveBatchPods_ReturnsOnlyBatchPods_ExcludingTransient(t *testing.T) {
	clusterContext, _ := setupTest()

	nonBatchPod := createPod()
	batchPod := createBatchPod()
	submitPodsWithWait(t, clusterContext, batchPod, nonBatchPod)

	clusterContext.Stop() // This prevents newly submitted pods being picked up by the informers
	transientBatchPod := createBatchPod()
	submitPod(t, clusterContext, transientBatchPod)

	allPods, err := clusterContext.GetActiveBatchPods()

	assert.Nil(t, err)
	assert.Equal(t, len(allPods), 1)

	podSet := util2.StringListToSet(util.ExtractNames(allPods))
	assert.True(t, podSet[batchPod.Name])
}

func TestKubernetesClusterContext_GetNodes(t *testing.T) {
	clusterContext, client := setupTest()

	node := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "Node1",
		},
	}

	_, err := client.CoreV1().Nodes().Create(armadacontext.Background(), node, metav1.CreateOptions{})
	assert.Nil(t, err)

	nodeFound := waitForCondition(func() bool {
		nodes, err := clusterContext.GetNodes()
		assert.Nil(t, err)
		return len(nodes) > 0 && nodes[0].Name == node.Name
	})
	assert.True(t, nodeFound)
}

func TestKubernetesClusterContext_Submit_UseUserSpecificClient(t *testing.T) {
	clusterContext, provider := setupTestWithProvider()

	_, err := clusterContext.SubmitPod(createBatchPod(), "user", []string{})
	assert.Nil(t, err)
	assert.Contains(t, provider.users, "user")
}

func submitPod(t *testing.T, context ClusterContext, pod *v1.Pod) {
	_, err := context.SubmitPod(pod, "user", []string{})
	assert.Nil(t, err)
}

func submitPodsWithWait(t *testing.T, context *KubernetesClusterContext, pods ...*v1.Pod) {
	for _, pod := range pods {
		submitPod(t, context, pod)
	}

	waitForContextSync(t, context, pods)
}

func waitForContextSync(t *testing.T, context *KubernetesClusterContext, pods []*v1.Pod) {
	synced := waitForCondition(func() bool {
		existingPods, err := context.podInformer.Lister().List(labels.Everything())
		assert.Nil(t, err)

		allSync := true
		existPodNamesMap := util2.StringListToSet(util.ExtractNames(existingPods))
		for _, pod := range pods {
			if _, present := existPodNamesMap[pod.Name]; !present {
				allSync = false
				break
			}
			if cachedPod := context.submittedPods.Get(util.ExtractPodKey(pod)); cachedPod != nil {
				allSync = false
				break
			}
		}
		return allSync
	})
	if !synced {
		t.Error("Timed out waiting for context sync. Submitted pods were never synced to context.")
		t.Fail()
	}
}

func waitForServiceContextSync(t *testing.T, context *KubernetesClusterContext, createdService *v1.Service) {
	synced := waitForCondition(func() bool {
		existingServices, err := context.serviceInformer.Lister().List(labels.Everything())
		assert.Nil(t, err)

		for _, service := range existingServices {
			if service.Name == createdService.Name {
				return true
			}
		}
		return false
	})
	if !synced {
		t.Error("Timed out waiting for context sync. Submitted service was never synced to context.")
		t.Fail()
	}
}

func waitForIngressContextSync(t *testing.T, context *KubernetesClusterContext, createdIngress *networking.Ingress) {
	synced := waitForCondition(func() bool {
		existingIngresses, err := context.ingressInformer.Lister().List(labels.Everything())
		assert.Nil(t, err)

		for _, ingress := range existingIngresses {
			if ingress.Name == createdIngress.Name {
				return true
			}
		}
		return false
	})
	if !synced {
		t.Error("Timed out waiting for context sync. Submitted ingress was never synced to context.")
		t.Fail()
	}
}

func waitForCondition(conditionCheck func() bool) bool {
	conditionMet := false
	for i := 0; i < 20; i++ {
		conditionMet = conditionCheck()
		if conditionMet {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	return conditionMet
}

func createSubmittedBatchPod(t *testing.T, context *KubernetesClusterContext) *v1.Pod {
	pod := createBatchPod()
	submitPodsWithWait(t, context, pod)
	return pod
}

func createBatchPod() *v1.Pod {
	pod := createPod()
	pod.ObjectMeta.Labels = map[string]string{
		domain.JobId:     "jobid" + util2.NewULID(),
		domain.Queue:     "test",
		domain.PodNumber: "0",
	}
	return pod
}

func createPod() *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      util2.NewULID(),
			UID:       types.UID(util2.NewULID()),
			Namespace: "default",
		},
	}
}

func createService() *v1.Service {
	return &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      util2.NewULID(),
			Namespace: "default",
		},
	}
}

func createIngress() *networking.Ingress {
	return &networking.Ingress{
		ObjectMeta: metav1.ObjectMeta{
			Name:      util2.NewULID(),
			Namespace: "default",
		},
	}
}

type FakeClientProvider struct {
	FakeClient *fake.Clientset
	users      []string
}

func (p *FakeClientProvider) ClientForUser(user string, groups []string) (kubernetes.Interface, error) {
	p.users = append(p.users, user)
	return p.FakeClient, nil
}

func (p *FakeClientProvider) Client() kubernetes.Interface {
	return p.FakeClient
}

func (p *FakeClientProvider) ClientConfig() *rest.Config {
	return nil
}
