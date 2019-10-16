package context

import (
	"errors"
	"testing"
	"time"

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
	client := fake.NewSimpleClientset()

	clusterContext := NewClusterContext(
		"test-cluster-1",
		util.NewMapPodCache(time.Minute, time.Second, util2.NewULID()),
		util.NewMapPodCache(2*time.Minute, time.Second, util2.NewULID()),
		client,
	)

	return clusterContext, client
}

func TestKubernetesClusterContext_SubmitPod(t *testing.T) {
	clusterContext, _ := setupTest()

	pod := createBatchPod()
	_, err := clusterContext.SubmitPod(pod)

	assert.Nil(t, err)
	assert.True(t, clusterContext.submittedPods.Exists(util.ExtractJobId(pod)))

	//When informer cache syncs, it should receive an added event and remove the pod from the submitted cache
	cache.WaitForCacheSync(make(chan struct{}), clusterContext.podInformer.Informer().HasSynced)
	assert.False(t, clusterContext.submittedPods.Exists(util.ExtractJobId(pod)), "Pod was not cleared from cache by add event")
}

func TestKubernetesClusterContext_SubmitPod_RemovesPodFromSubmittedCache_OnClientError(t *testing.T) {
	clusterContext, client := setupTest()
	client.Fake.PrependReactor("create", "pods", func(action clientTesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("server error")
	})

	returned, err := clusterContext.SubmitPod(createBatchPod())

	assert.NotNil(t, err)
	assert.Nil(t, returned)

	submittedCache := clusterContext.submittedPods.GetAll()
	assert.Equal(t, len(submittedCache), 0)
}

func TestKubernetesClusterContext_DeletePods_AddsPodToDeleteCache(t *testing.T) {
	clusterContext, _ := setupTest()

	pod := createBatchPod()

	clusterContext.DeletePods([]*v1.Pod{pod})

	podsToDeleteCache := clusterContext.podsToDelete.GetAll()
	assert.Equal(t, len(podsToDeleteCache), 1)
	assert.Equal(t, podsToDeleteCache[0], pod)
}

func TestKubernetesClusterContext_DeletePods_DoesNotCallClient(t *testing.T) {
	clusterContext, client := setupTest()

	pod := createBatchPod()

	//Reset action count
	client.Fake.ClearActions()
	clusterContext.DeletePods([]*v1.Pod{pod})

	assert.Equal(t, len(client.Actions()), 0)
}

func TestKubernetesClusterContext_ProcessPodsToDelete_CallDeleteOnClient(t *testing.T) {
	clusterContext, client := setupTest()

	pod := createSubmittedPod(t, clusterContext, true)
	client.Fake.ClearActions()

	clusterContext.ProcessPodsToDelete()

	assert.Equal(t, len(client.Fake.Actions()), 1)
	assert.True(t, client.Fake.Actions()[0].Matches("delete", "pods"))

	deleteAction, ok := client.Fake.Actions()[0].(clientTesting.DeleteAction)
	assert.True(t, ok)
	assert.Equal(t, deleteAction.GetName(), pod.Name)
}

func TestKubernetesClusterContext_ProcessPodsToDelete_OnClientSuccess_MarkPodAsNilInCache(t *testing.T) {
	clusterContext, client := setupTest()

	pod := createSubmittedPod(t, clusterContext, true)
	client.Fake.ClearActions()

	clusterContext.ProcessPodsToDelete()

	assert.True(t, clusterContext.podsToDelete.Exists(util.ExtractJobId(pod)))
	assert.Nil(t, clusterContext.podsToDelete.Get(util.ExtractJobId(pod)))
}

func TestKubernetesClusterContext_ProcessPodsToDelete_OnClientErrorNotFound_MarkPodAsNilInCache(t *testing.T) {
	clusterContext, client := setupTest()
	client.Fake.PrependReactor("delete", "pods", func(action clientTesting.Action) (bool, runtime.Object, error) {
		notFound := errors2.StatusError{
			ErrStatus: metav1.Status{
				Reason: metav1.StatusReasonNotFound,
			},
		}
		return true, nil, &notFound
	})

	pod := createSubmittedPod(t, clusterContext, true)
	client.Fake.ClearActions()

	clusterContext.ProcessPodsToDelete()

	assert.True(t, clusterContext.podsToDelete.Exists(util.ExtractJobId(pod)))
	assert.Nil(t, clusterContext.podsToDelete.Get(util.ExtractJobId(pod)))
}

func TestKubernetesClusterContext_ProcessPodsToDelete_OnClientError_RemovePodFromDeleteCache(t *testing.T) {
	clusterContext, client := setupTest()
	client.Fake.PrependReactor("delete", "pods", func(action clientTesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("server error")
	})

	pod := createSubmittedPod(t, clusterContext, true)

	client.Fake.ClearActions()
	clusterContext.ProcessPodsToDelete()

	assert.False(t, clusterContext.podsToDelete.Exists(util.ExtractJobId(pod)))
}

func TestKubernetesClusterContext_ProcessPodsToDelete_OnRepeatedCall_DoesNotCallDeleteOnClientRepeatedly(t *testing.T) {
	clusterContext, client := setupTest()

	createSubmittedPod(t, clusterContext, true)

	client.Fake.ClearActions()
	clusterContext.ProcessPodsToDelete()
	assert.Equal(t, len(client.Fake.Actions()), 1)

	client.Fake.ClearActions()
	clusterContext.ProcessPodsToDelete()
	assert.Equal(t, len(client.Fake.Actions()), 0)
}

func TestKubernetesClusterContext_AddAnnotation(t *testing.T) {
	clusterContext, _ := setupTest()
	pod := createSubmittedPod(t, clusterContext, false)

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

	pod := createSubmittedPod(t, clusterContext, false)

	annotationsToAdd := map[string]string{"test": "\\"}
	err := clusterContext.AddAnnotation(pod, annotationsToAdd)
	assert.NotNil(t, err)
}

func TestKubernetesClusterContext_GetAllPods(t *testing.T) {
	clusterContext, _ := setupTest()

	nonBatchPod := createPod()
	batchPod := createBatchPod()

	clusterContext.SubmitPod(nonBatchPod)
	clusterContext.SubmitPod(batchPod)
	cache.WaitForCacheSync(make(chan struct{}), clusterContext.podInformer.Informer().HasSynced)

	transientBatchPod := createBatchPod()
	clusterContext.SubmitPod(transientBatchPod)

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
	clusterContext.SubmitPod(batchPod)
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

	clusterContext.SubmitPod(nonBatchPod)
	clusterContext.SubmitPod(batchPod)
	cache.WaitForCacheSync(make(chan struct{}), clusterContext.podInformer.Informer().HasSynced)

	transientBatchPod := createBatchPod()
	clusterContext.SubmitPod(transientBatchPod)

	allPods, err := clusterContext.GetBatchPods()

	assert.Nil(t, err)
	assert.Equal(t, len(allPods), 2)

	//Confirm the pods returned are the ones created
	podSet := util2.StringListToSet(util.ExtractNames(allPods))
	assert.True(t, podSet[batchPod.Name])
	assert.True(t, podSet[transientBatchPod.Name])
}

func TestKubernetesClusterContext_GetBatchPods_DeduplicatesTransientPods(t *testing.T) {
	clusterContext, _ := setupTest()

	batchPod := createBatchPod()
	clusterContext.SubmitPod(batchPod)
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

	clusterContext.SubmitPod(nonBatchPod)
	clusterContext.SubmitPod(batchPod)
	cache.WaitForCacheSync(make(chan struct{}), clusterContext.podInformer.Informer().HasSynced)

	transientBatchPod := createBatchPod()
	clusterContext.SubmitPod(transientBatchPod)

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

func createSubmittedPod(t *testing.T, context ClusterContext, markForDeletion bool) *v1.Pod {
	pod := createBatchPod()

	_, err := context.SubmitPod(pod)

	assert.Nil(t, err)

	if markForDeletion {
		context.DeletePods([]*v1.Pod{pod})
	}

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
