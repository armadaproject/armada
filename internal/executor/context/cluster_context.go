package context

import (
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

	"github.com/G-Research/armada/internal/executor/domain"
	"github.com/G-Research/armada/internal/executor/util"
)

type ClusterContext interface {
	AddPodEventHandler(handler cache.ResourceEventHandlerFuncs)

	GetBatchPods() ([]*v1.Pod, error)
	GetAllPods() ([]*v1.Pod, error)
	GetActiveBatchPods() ([]*v1.Pod, error)
	GetNodes() ([]*v1.Node, error)

	SubmitPod(pod *v1.Pod) (*v1.Pod, error)
	AddAnnotation(pod *v1.Pod, annotations map[string]string) error
	DeletePods(pods []*v1.Pod)

	GetClusterId() string
}

type KubernetesClusterContext struct {
	clusterId        string
	submittedPods    util.PodCache
	podsToDelete     util.PodCache
	podInformer      informer.PodInformer
	nodeInformer     informer.NodeInformer
	stopper          chan struct{}
	kubernetesClient kubernetes.Interface
}

func (c *KubernetesClusterContext) GetClusterId() string {
	return c.clusterId
}

func NewClusterContext(
	clusterId string,
	minTimeBetweenRepeatDeletionCalls time.Duration,
	kubernetesClient kubernetes.Interface) *KubernetesClusterContext {

	factory := informers.NewSharedInformerFactoryWithOptions(kubernetesClient, 0)

	context := &KubernetesClusterContext{
		clusterId:        clusterId,
		submittedPods:    util.NewTimeExpiringPodCache(time.Minute, time.Second, "submitted_job"),
		podsToDelete:     util.NewTimeExpiringPodCache(minTimeBetweenRepeatDeletionCalls, time.Second, "deleted_job"),
		stopper:          make(chan struct{}),
		podInformer:      factory.Core().V1().Pods(),
		nodeInformer:     factory.Core().V1().Nodes(),
		kubernetesClient: kubernetesClient,
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

	factory.Start(context.stopper)
	factory.WaitForCacheSync(context.stopper)

	return context
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

func (c *KubernetesClusterContext) GetNodes() ([]*v1.Node, error) {
	return c.nodeInformer.Lister().List(labels.Everything())
}

func (c *KubernetesClusterContext) SubmitPod(pod *v1.Pod) (*v1.Pod, error) {

	c.submittedPods.Add(pod)
	returnedPod, err := c.kubernetesClient.CoreV1().Pods("default").Create(pod)

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
	_, err = c.kubernetesClient.CoreV1().Pods(pod.Namespace).Patch(pod.Name, types.StrategicMergePatchType, patchBytes)
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
		err := c.kubernetesClient.CoreV1().Pods(podToDelete.Namespace).Delete(podToDelete.Name, &deleteOptions)
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
