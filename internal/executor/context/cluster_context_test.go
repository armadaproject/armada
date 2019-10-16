package context

import (
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	errors2 "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	clientTesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"

	util2 "github.com/G-Research/k8s-batch/internal/common/util"
	"github.com/G-Research/k8s-batch/internal/executor/domain"
	"github.com/G-Research/k8s-batch/internal/executor/util"
)

func setupTest() (*KubernetesClusterContext, *fake.Clientset) {
	return setupTestWithMinRepeatedDeletePeriod(2 * time.Minute)
}

func setupTestWithMinRepeatedDeletePeriod(minRepeatedDeletePeriod time.Duration) (*KubernetesClusterContext, *fake.Clientset) {
	prometheus.DefaultRegisterer = prometheus.NewRegistry()
	client := fake.NewSimpleClientset()

	clusterContext := NewClusterContext(
		"test-cluster-1",
		minRepeatedDeletePeriod,
		client,
	)

	return clusterContext, client
}

func TestKubernetesClusterContext_SubmitPod(t *testing.T) {
	clusterContext, client := setupTest()

	pod := createBatchPod()
	client.Fake.ClearActions()

	_, err := clusterContext.SubmitPod(pod)
	assert.Nil(t, err)

	assert.Equal(t, len(client.Fake.Actions()), 1)
	assert.True(t, client.Fake.Actions()[0].Matches("create", "pods"))

	createAction, ok := client.Fake.Actions()[0].(clientTesting.CreateAction)
	assert.True(t, ok)
	assert.Equal(t, createAction.GetObject(), pod)
}

func TestKubernetesClusterContext_DeletePods_AddsPodToDeleteCache(t *testing.T) {
	clusterContext, _ := setupTest()

	pod := createBatchPod()

	clusterContext.DeletePods([]*v1.Pod{pod})

	assert.True(t, clusterContext.podsToDelete.Exists(util.ExtractJobId(pod)))
}

func TestKubernetesClusterContext_DeletePods_DoesNotCallClient(t *testing.T) {
	clusterContext, client := setupTest()

	pod := createBatchPod()

	client.Fake.ClearActions()
	clusterContext.DeletePods([]*v1.Pod{pod})

	assert.Equal(t, len(client.Actions()), 0)
}

func TestKubernetesClusterContext_ProcessPodsToDelete_DoesNotCallClient_WhenNoPodsMarkedForDeletion(t *testing.T) {
	clusterContext, client := setupTest()

	client.Fake.ClearActions()
	clusterContext.ProcessPodsToDelete()

	assert.Equal(t, len(client.Fake.Actions()), 0)
}

func TestKubernetesClusterContext_ProcessPodsToDelete_CallDeleteOnClient_WhenPodsMarkedForDeletion(t *testing.T) {
	clusterContext, client := setupTest()

	pod := createSubmittedBatchPod(t, clusterContext)

	client.Fake.ClearActions()
	clusterContext.DeletePods([]*v1.Pod{pod})
	clusterContext.ProcessPodsToDelete()

	assert.Equal(t, len(client.Fake.Actions()), 1)
	assert.True(t, client.Fake.Actions()[0].Matches("delete", "pods"))

	deleteAction, ok := client.Fake.Actions()[0].(clientTesting.DeleteAction)
	assert.True(t, ok)
	assert.Equal(t, deleteAction.GetName(), pod.Name)
}

func TestKubernetesClusterContext_ProcessPodsToDelete_PreventsRepeatedDeleteCallsToClient_OnClientSuccess(t *testing.T) {
	clusterContext, client := setupTest()

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
	assert.Equal(t, len(client.Fake.Actions()), 1)

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
	assert.Equal(t, len(client.Fake.Actions()), 1)

	client.Fake.ClearActions()
	clusterContext.DeletePods([]*v1.Pod{pod})
	clusterContext.ProcessPodsToDelete()
	assert.Equal(t, len(client.Fake.Actions()), 1)
}

func TestKubernetesClusterContext_ProcessPodsToDelete_AllowsRepeatedDeleteCallToClient_AfterMinimumDeletePeriodHasPassed(t *testing.T) {
	timeBetweenRepeatedDeleteCalls := 500 * time.Millisecond
	clusterContext, client := setupTestWithMinRepeatedDeletePeriod(timeBetweenRepeatedDeleteCalls)

	pod := createSubmittedBatchPod(t, clusterContext)

	client.Fake.ClearActions()
	clusterContext.DeletePods([]*v1.Pod{pod})
	clusterContext.ProcessPodsToDelete()
	assert.Equal(t, len(client.Fake.Actions()), 1)

	//Wait time required between repeated delete calls
	time.Sleep(timeBetweenRepeatedDeleteCalls + 200*time.Millisecond)

	client.Fake.ClearActions()
	clusterContext.DeletePods([]*v1.Pod{pod})
	clusterContext.ProcessPodsToDelete()
	assert.Equal(t, len(client.Fake.Actions()), 1)
}

func TestKubernetesClusterContext_AddAnnotation(t *testing.T) {
	clusterContext, _ := setupTest()
	pod := createSubmittedBatchPod(t, clusterContext)

	annotationsToAdd := map[string]string{"test": "annotation"}
	err := clusterContext.AddAnnotation(pod, annotationsToAdd)
	assert.Nil(t, err)

	cache.WaitForCacheSync(make(chan struct{}), clusterContext.podInformer.Informer().HasSynced)

	allPods, err := clusterContext.GetActiveBatchPods()
	assert.Equal(t, allPods[0].Annotations, annotationsToAdd)
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

	submitPod(t, clusterContext, nonBatchPod)
	submitPod(t, clusterContext, batchPod)
	cache.WaitForCacheSync(make(chan struct{}), clusterContext.podInformer.Informer().HasSynced)

	transientBatchPod := createBatchPod()
	submitPod(t, clusterContext, transientBatchPod)

	allPods, err := clusterContext.GetAllPods()

	assert.Nil(t, err)
	assert.Equal(t, len(allPods), 3)

	//Confirm the pods returned are the ones created
	podSet := util2.StringListToSet(util.ExtractNames(allPods))
	assert.True(t, podSet[nonBatchPod.Name])
	assert.True(t, podSet[batchPod.Name])
	assert.True(t, podSet[transientBatchPod.Name])
}

func TestKubernetesClusterContext_GetAllPods_DeduplicatesTransientPods(t *testing.T) {
	clusterContext, _ := setupTest()

	batchPod := createBatchPod()
	submitPod(t, clusterContext, batchPod)
	cache.WaitForCacheSync(make(chan struct{}), clusterContext.podInformer.Informer().HasSynced)

	//Forcibly add pod back to cache, so now it exists in kubernetes + cache
	clusterContext.submittedPods.Add(batchPod)

	allPods, err := clusterContext.GetAllPods()

	assert.Nil(t, err)
	assert.Equal(t, len(allPods), 1)

	//Confirm the pods returned are the ones created
	podSet := util2.StringListToSet(util.ExtractNames(allPods))
	assert.True(t, podSet[batchPod.Name])
}

func TestKubernetesClusterContext_GetBatchPods_ReturnsOnlyBatchPods_IncludingTransient(t *testing.T) {
	clusterContext, _ := setupTest()

	nonBatchPod := createPod()
	batchPod := createBatchPod()

	submitPod(t, clusterContext, nonBatchPod)
	submitPod(t, clusterContext, batchPod)
	cache.WaitForCacheSync(make(chan struct{}), clusterContext.podInformer.Informer().HasSynced)

	transientBatchPod := createBatchPod()
	submitPod(t, clusterContext, transientBatchPod)

	allPods, err := clusterContext.GetBatchPods()

	assert.Nil(t, err)
	assert.Equal(t, len(allPods), 2)

	//Confirm the pods returned are the ones created
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
	_, err := clusterContext.SubmitPod(batchPod)
	assert.NotNil(t, err)

	allPods, err := clusterContext.GetBatchPods()

	assert.Nil(t, err)
	assert.Equal(t, len(allPods), 0)
}

func TestKubernetesClusterContext_GetBatchPods_DeduplicatesTransientPods(t *testing.T) {
	clusterContext, _ := setupTest()

	batchPod := createBatchPod()
	submitPod(t, clusterContext, batchPod)
	cache.WaitForCacheSync(make(chan struct{}), clusterContext.podInformer.Informer().HasSynced)

	//Forcibly add pod back to cache, so now it exists in kubernetes + cache
	clusterContext.submittedPods.Add(batchPod)

	allPods, err := clusterContext.GetBatchPods()

	assert.Nil(t, err)
	assert.Equal(t, len(allPods), 1)

	//Confirm the pods returned are the ones created
	podSet := util2.StringListToSet(util.ExtractNames(allPods))
	assert.True(t, podSet[batchPod.Name])
}

func TestKubernetesClusterContext_GetActiveBatchPods_ReturnsOnlyBatchPods_ExcludingTransient(t *testing.T) {
	clusterContext, _ := setupTest()

	nonBatchPod := createPod()
	batchPod := createBatchPod()

	submitPod(t, clusterContext, nonBatchPod)
	submitPod(t, clusterContext, batchPod)
	cache.WaitForCacheSync(make(chan struct{}), clusterContext.podInformer.Informer().HasSynced)

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

	_, err := client.CoreV1().Nodes().Create(node)
	assert.Nil(t, err)

	cache.WaitForCacheSync(make(chan struct{}), clusterContext.nodeInformer.Informer().HasSynced)
	nodes, err := clusterContext.GetNodes()

	assert.Nil(t, err)
	assert.Equal(t, len(nodes), 1)
	assert.Equal(t, nodes[0].Name, node.Name)
}

func submitPod(t *testing.T, context ClusterContext, pod *v1.Pod) {
	_, err := context.SubmitPod(pod)
	assert.Nil(t, err)
}

func createSubmittedBatchPod(t *testing.T, context ClusterContext) *v1.Pod {
	pod := createBatchPod()

	submitPod(t, context, pod)

	return pod
}

func createBatchPod() *v1.Pod {
	pod := createPod()
	pod.ObjectMeta.Labels = map[string]string{
		domain.JobId: "jobid",
	}
	return pod
}

func createPod() *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      util2.NewULID(),
			Namespace: "default",
		},
	}
}
