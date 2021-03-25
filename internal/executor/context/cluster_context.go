package context

import (
	ctx "context"
	"encoding/json"
	"time"

	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	informer "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/kubelet/pkg/apis/stats/v1alpha1"

	"github.com/G-Research/armada/internal/executor/cluster"
	"github.com/G-Research/armada/internal/executor/configuration"
	"github.com/G-Research/armada/internal/executor/domain"
	"github.com/G-Research/armada/internal/executor/util"
)

const podByUIDIndex = "podUID"

type ClusterContext interface {
	AddPodEventHandler(handler cache.ResourceEventHandlerFuncs)

	GetBatchPods() ([]*v1.Pod, error)
	GetAllPods() ([]*v1.Pod, error)
	GetActiveBatchPods() ([]*v1.Pod, error)
	GetNodes() ([]*v1.Node, error)
	GetNodeStatsSummary(*v1.Node) (*v1alpha1.Summary, error)
	GetPodEvents(pod *v1.Pod) ([]*v1.Event, error)

	SubmitPod(pod *v1.Pod, owner string) (*v1.Pod, error)
	AddAnnotation(pod *v1.Pod, annotations map[string]string) error
	DeletePods(pods []*v1.Pod)

	GetClusterId() string
	GetClusterPool() string

	Stop()
}

type KubernetesClusterContext struct {
	clusterId                string
	pool                     string
	submittedPods            util.PodCache
	podsToDelete             util.PodCache
	podInformer              informer.PodInformer
	nodeInformer             informer.NodeInformer
	stopper                  chan struct{}
	kubernetesClient         kubernetes.Interface
	kubernetesClientProvider cluster.KubernetesClientProvider
	eventInformer            informer.EventInformer
}

func (c *KubernetesClusterContext) GetClusterId() string {
	return c.clusterId
}

func (c *KubernetesClusterContext) GetClusterPool() string {
	return c.pool
}

func NewClusterContext(
	configuration configuration.ApplicationConfiguration,
	minTimeBetweenRepeatDeletionCalls time.Duration,
	kubernetesClientProvider cluster.KubernetesClientProvider) *KubernetesClusterContext {

	kubernetesClient := kubernetesClientProvider.Client()

	factory := informers.NewSharedInformerFactoryWithOptions(kubernetesClient, 0)

	context := &KubernetesClusterContext{
		clusterId:                configuration.ClusterId,
		pool:                     configuration.Pool,
		submittedPods:            util.NewTimeExpiringPodCache(time.Minute, time.Second, "submitted_job"),
		podsToDelete:             util.NewTimeExpiringPodCache(minTimeBetweenRepeatDeletionCalls, time.Second, "deleted_job"),
		stopper:                  make(chan struct{}),
		podInformer:              factory.Core().V1().Pods(),
		nodeInformer:             factory.Core().V1().Nodes(),
		eventInformer:            factory.Core().V1().Events(),
		kubernetesClient:         kubernetesClient,
		kubernetesClientProvider: kubernetesClientProvider,
	}

	context.AddPodEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			pod, ok := obj.(*v1.Pod)
			if !ok {
				log.Errorf("Failed to process pod event due to it being an unexpected type. Failed to process %+v", obj)
				return
			}
			context.submittedPods.Delete(util.ExtractJobId(pod))
		},
	})

	//Use node informer so it is initialized properly
	context.nodeInformer.Lister()

	err := context.eventInformer.Informer().AddIndexers(cache.Indexers{podByUIDIndex: indexPodByUID})
	if err != nil {
		panic(err)
	}

	factory.Start(context.stopper)
	factory.WaitForCacheSync(context.stopper)

	return context
}

func indexPodByUID(obj interface{}) (strings []string, err error) {
	event := obj.(*v1.Event)
	if event.InvolvedObject.Kind != "Pod" || event.InvolvedObject.UID == "" {
		return []string{}, nil
	}
	return []string{string(event.InvolvedObject.UID)}, nil
}

func (c *KubernetesClusterContext) AddPodEventHandler(handler cache.ResourceEventHandlerFuncs) {
	c.podInformer.Informer().AddEventHandler(handler)
}

func (c *KubernetesClusterContext) Stop() {
	close(c.stopper)
}

func (c *KubernetesClusterContext) GetActiveBatchPods() ([]*v1.Pod, error) {
	return c.podInformer.Lister().List(util.GetManagedPodSelector())
}

func (c *KubernetesClusterContext) GetBatchPods() ([]*v1.Pod, error) {
	podsInCluster, err := c.GetActiveBatchPods()
	if err != nil {
		return nil, err
	}
	allPods := podsInCluster
	allPods = util.MergePodList(allPods, c.submittedPods.GetAll())

	return allPods, nil
}

func (c *KubernetesClusterContext) GetAllPods() ([]*v1.Pod, error) {
	podsInCluster, err := c.podInformer.Lister().List(labels.Everything())
	if err != nil {
		return nil, err
	}
	allPods := podsInCluster
	allPods = util.MergePodList(allPods, c.submittedPods.GetAll())

	return allPods, nil
}

func (c *KubernetesClusterContext) GetPodEvents(pod *v1.Pod) ([]*v1.Event, error) {
	events, err := c.eventInformer.Informer().GetIndexer().ByIndex(podByUIDIndex, string(pod.UID))
	if err != nil {
		return nil, err
	}
	eventsTyped := []*v1.Event{}
	for _, untyped := range events {
		typed, ok := untyped.(*v1.Event)
		if ok {
			eventsTyped = append(eventsTyped, typed)
		}
	}
	return eventsTyped, nil
}

func (c *KubernetesClusterContext) GetNodes() ([]*v1.Node, error) {
	return c.nodeInformer.Lister().List(labels.Everything())
}

func (c *KubernetesClusterContext) GetNodeStatsSummary(node *v1.Node) (*v1alpha1.Summary, error) {
	request := c.kubernetesClient.
		CoreV1().
		RESTClient().
		Get().
		Resource("nodes").
		Name(node.Name).
		SubResource("proxy", "stats", "summary")

	res := request.Do(ctx.Background())
	rawJson, err := res.Raw()
	if err != nil {
		return nil, err
	}

	summary := &v1alpha1.Summary{}
	err = json.Unmarshal(rawJson, summary)
	if err != nil {
		return nil, err
	}
	return summary, nil
}

func (c *KubernetesClusterContext) SubmitPod(pod *v1.Pod, owner string) (*v1.Pod, error) {

	c.submittedPods.Add(pod)
	ownerClient, err := c.kubernetesClientProvider.ClientForUser(owner)
	if err != nil {
		return nil, err
	}

	returnedPod, err := ownerClient.CoreV1().Pods(pod.Namespace).Create(ctx.Background(), pod, metav1.CreateOptions{})

	if err != nil {
		c.submittedPods.Delete(util.ExtractJobId(pod))
	}
	return returnedPod, err
}

func (c *KubernetesClusterContext) AddAnnotation(pod *v1.Pod, annotations map[string]string) error {
	patch := &domain.Patch{
		MetaData: metav1.ObjectMeta{
			Annotations: annotations,
		},
	}
	patchBytes, err := json.Marshal(patch)
	if err != nil {
		return err
	}
	_, err = c.kubernetesClient.CoreV1().Pods(pod.Namespace).Patch(ctx.Background(), pod.Name, types.StrategicMergePatchType, patchBytes, metav1.PatchOptions{})
	if err != nil {
		return err
	}
	return nil
}

func (c *KubernetesClusterContext) DeletePods(pods []*v1.Pod) {
	for _, podToDelete := range pods {
		c.podsToDelete.AddIfNotExists(podToDelete)
	}
}

func (c *KubernetesClusterContext) ProcessPodsToDelete() {
	pods := c.podsToDelete.GetAll()

	deleteOptions := createPodDeletionDeleteOptions()
	for _, podToDelete := range pods {
		if podToDelete == nil {
			continue
		}
		err := c.kubernetesClient.CoreV1().Pods(podToDelete.Namespace).Delete(ctx.Background(), podToDelete.Name, deleteOptions)
		jobId := util.ExtractJobId(podToDelete)
		if err == nil || errors.IsNotFound(err) {
			c.podsToDelete.Update(jobId, nil)
		} else {
			log.Errorf("Failed to delete pod %s/%s because %s", podToDelete.Namespace, podToDelete.Name, err)
			c.podsToDelete.Delete(jobId)
		}
	}
}

func createPodDeletionDeleteOptions() metav1.DeleteOptions {
	gracePeriod := int64(0)
	deleteOptions := metav1.DeleteOptions{
		GracePeriodSeconds: &gracePeriod,
	}
	return deleteOptions
}
