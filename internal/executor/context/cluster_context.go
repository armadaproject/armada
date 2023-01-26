package context

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	networking "k8s.io/api/networking/v1"
	k8s_errors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/clock"
	"k8s.io/client-go/informers"
	informer "k8s.io/client-go/informers/core/v1"
	network_informer "k8s.io/client-go/informers/networking/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/kubelet/pkg/apis/stats/v1alpha1"
	"k8s.io/utils/pointer"

	"github.com/armadaproject/armada/internal/common/armadaerrors"
	"github.com/armadaproject/armada/internal/common/cluster"
	util2 "github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/executor/configuration"
	"github.com/armadaproject/armada/internal/executor/domain"
	"github.com/armadaproject/armada/internal/executor/healthmonitor"
	"github.com/armadaproject/armada/internal/executor/util"
)

const podByUIDIndex = "podUID"

type ClusterIdentity interface {
	GetClusterId() string
	GetClusterPool() string
}

type ClusterContext interface {
	ClusterIdentity

	AddPodEventHandler(handler cache.ResourceEventHandlerFuncs)
	AddClusterEventEventHandler(handler cache.ResourceEventHandlerFuncs)
	GetBatchPods() ([]*v1.Pod, error)
	GetAllPods() ([]*v1.Pod, error)
	GetActiveBatchPods() ([]*v1.Pod, error)
	GetNodes() ([]*v1.Node, error)
	GetNode(nodeName string) (*v1.Node, error)
	GetNodeStatsSummary(context.Context, *v1.Node) (*v1alpha1.Summary, error)
	GetPodEvents(pod *v1.Pod) ([]*v1.Event, error)
	GetServices(pod *v1.Pod) ([]*v1.Service, error)
	GetIngresses(pod *v1.Pod) ([]*networking.Ingress, error)

	SubmitPod(pod *v1.Pod, owner string, ownerGroups []string) (*v1.Pod, error)
	SubmitService(service *v1.Service) (*v1.Service, error)
	SubmitIngress(ingress *networking.Ingress) (*networking.Ingress, error)
	DeletePodWithCondition(pod *v1.Pod, condition func(pod *v1.Pod) bool, pessimistic bool) error
	DeletePods(pods []*v1.Pod)
	DeleteService(service *v1.Service) error
	DeleteIngress(ingress *networking.Ingress) error

	AddAnnotation(pod *v1.Pod, annotations map[string]string) error
	AddClusterEventAnnotation(event *v1.Event, annotations map[string]string) error

	Stop()
}

type KubernetesClusterContext struct {
	clusterId                string
	pool                     string
	deleteThreadCount        int
	submittedPods            util.PodCache
	podsToDelete             util.PodCache
	podInformer              informer.PodInformer
	nodeInformer             informer.NodeInformer
	serviceInformer          informer.ServiceInformer
	ingressInformer          network_informer.IngressInformer
	stopper                  chan struct{}
	kubernetesClient         kubernetes.Interface
	kubernetesClientProvider cluster.KubernetesClientProvider
	eventInformer            informer.EventInformer
	// If provided, stops object creation while EtcdMaxFractionOfStorageInUse or more of etcd storage is full.
	etcdHealthMonitor healthmonitor.EtcdLimitHealthMonitor
	podKillTimeout    time.Duration
	clock             clock.Clock
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
	kubernetesClientProvider cluster.KubernetesClientProvider,
	etcdHealthMonitor healthmonitor.EtcdLimitHealthMonitor,
	killTimeout time.Duration,
) *KubernetesClusterContext {
	kubernetesClient := kubernetesClientProvider.Client()

	factory := informers.NewSharedInformerFactoryWithOptions(kubernetesClient, 0)

	context := &KubernetesClusterContext{
		clusterId:                configuration.ClusterId,
		pool:                     configuration.Pool,
		deleteThreadCount:        configuration.DeleteConcurrencyLimit,
		submittedPods:            util.NewTimeExpiringPodCache(time.Minute, time.Second, "submitted_job"),
		podsToDelete:             util.NewTimeExpiringPodCache(minTimeBetweenRepeatDeletionCalls, time.Second, "deleted_job"),
		stopper:                  make(chan struct{}),
		podInformer:              factory.Core().V1().Pods(),
		nodeInformer:             factory.Core().V1().Nodes(),
		eventInformer:            factory.Core().V1().Events(),
		serviceInformer:          factory.Core().V1().Services(),
		ingressInformer:          factory.Networking().V1().Ingresses(),
		kubernetesClient:         kubernetesClient,
		kubernetesClientProvider: kubernetesClientProvider,
		etcdHealthMonitor:        etcdHealthMonitor,
		podKillTimeout:           killTimeout,
		clock:                    clock.RealClock{},
	}

	context.AddPodEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			pod, ok := obj.(*v1.Pod)
			if !ok {
				log.Errorf("Failed to process pod event due to it being an unexpected type. Failed to process %+v", obj)
				return
			}
			context.submittedPods.Delete(util.ExtractPodKey(pod))
		},
	})

	// Use node informer so it is initialised properly
	context.nodeInformer.Lister()
	context.serviceInformer.Lister()
	context.ingressInformer.Lister()

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

func (c *KubernetesClusterContext) AddClusterEventEventHandler(handler cache.ResourceEventHandlerFuncs) {
	c.eventInformer.Informer().AddEventHandler(handler)
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
	var eventsTyped []*v1.Event
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

func (c *KubernetesClusterContext) GetNode(nodeName string) (*v1.Node, error) {
	return c.nodeInformer.Lister().Get(nodeName)
}

func (c *KubernetesClusterContext) GetNodeStatsSummary(ctx context.Context, node *v1.Node) (*v1alpha1.Summary, error) {
	request := c.kubernetesClient.
		CoreV1().
		RESTClient().
		Get().
		Resource("nodes").
		Name(node.Name).
		SubResource("proxy", "stats", "summary")

	res := request.Do(ctx)
	rawJson, err := res.Raw()
	if err != nil {
		return nil, fmt.Errorf("request error %s (body %s)", err, string(rawJson))
	}

	summary := &v1alpha1.Summary{}
	err = json.Unmarshal(rawJson, summary)
	if err != nil {
		return nil, fmt.Errorf("unable to unmarshal %s", err)
	}
	return summary, nil
}

func (c *KubernetesClusterContext) SubmitPod(pod *v1.Pod, owner string, ownerGroups []string) (*v1.Pod, error) {
	// If a health monitor is provided, reject pods when etcd is at its hard limit.
	if c.etcdHealthMonitor != nil && !c.etcdHealthMonitor.IsWithinHardHealthLimit() {
		err := errors.WithStack(&armadaerrors.ErrCreateResource{
			Type:    "pod",
			Name:    pod.Name,
			Message: fmt.Sprintf("etcd is at its hard heatlh limit and therefore not healthy to submit to"),
		})
		return nil, err
	}

	c.submittedPods.Add(pod)
	ownerClient, err := c.kubernetesClientProvider.ClientForUser(owner, ownerGroups)
	if err != nil {
		return nil, err
	}

	returnedPod, err := ownerClient.CoreV1().Pods(pod.Namespace).Create(context.Background(), pod, metav1.CreateOptions{})
	if err != nil {
		c.submittedPods.Delete(util.ExtractPodKey(pod))
	}
	return returnedPod, err
}

func (c *KubernetesClusterContext) SubmitService(service *v1.Service) (*v1.Service, error) {
	return c.kubernetesClient.CoreV1().Services(service.Namespace).Create(context.Background(), service, metav1.CreateOptions{})
}

func (c *KubernetesClusterContext) SubmitIngress(ingress *networking.Ingress) (*networking.Ingress, error) {
	return c.kubernetesClient.NetworkingV1().Ingresses(ingress.Namespace).Create(context.Background(), ingress, metav1.CreateOptions{})
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
	_, err = c.kubernetesClient.CoreV1().
		Pods(pod.Namespace).
		Patch(context.Background(), pod.Name, types.StrategicMergePatchType, patchBytes, metav1.PatchOptions{})
	if err != nil {
		return err
	}
	return nil
}

func (c *KubernetesClusterContext) AddClusterEventAnnotation(event *v1.Event, annotations map[string]string) error {
	patch := &domain.Patch{
		MetaData: metav1.ObjectMeta{
			Annotations: annotations,
		},
	}
	patchBytes, err := json.Marshal(patch)
	if err != nil {
		return err
	}
	_, err = c.kubernetesClient.CoreV1().
		Events(event.Namespace).
		Patch(context.Background(), event.Name, types.StrategicMergePatchType, patchBytes, metav1.PatchOptions{})
	if err != nil {
		return err
	}
	return nil
}

func (c *KubernetesClusterContext) DeletePodWithCondition(pod *v1.Pod, condition func(pod *v1.Pod) bool, pessimistic bool) error {
	if !condition(pod) {
		return fmt.Errorf("pod does not match provided condition")
	}

	currentPod := pod
	if !util.IsMarkedForDeletion(pod) {
		_, err := c.markForDeletion(pod)
		if err != nil {
			return err
		}
		currentPod, err = c.waitForPodUpdateInLocalCache(pod, time.Second*5)
		if err != nil {
			return err
		}
	}

	if !condition(currentPod) {
		return fmt.Errorf("pod does not match provided condition")
	}

	deleteOptions := metav1.DeleteOptions{GracePeriodSeconds: nil}
	if pessimistic {
		deleteOptions.Preconditions = &metav1.Preconditions{
			ResourceVersion: &currentPod.ResourceVersion,
		}
	}

	err := c.deletePod(currentPod, deleteOptions)
	if err != nil && k8s_errors.IsNotFound(err) {
		return nil
	}
	return err
}

func (c *KubernetesClusterContext) waitForPodUpdateInLocalCache(pod *v1.Pod, timeout time.Duration) (*v1.Pod, error) {
	timeoutContext, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	for {
		if timeoutContext.Err() != nil {
			return nil, fmt.Errorf("giving up waiting for pod informer cache sync for pod %s", pod.Name)
		}
		cachedPod, err := c.podInformer.Lister().Pods(pod.Namespace).Get(pod.Name)
		if err != nil {
			return nil, err
		}
		if cachedPod.ResourceVersion != pod.ResourceVersion {
			return cachedPod, nil
		}
		time.Sleep(time.Millisecond * 50)
	}
}

func (c *KubernetesClusterContext) DeletePods(pods []*v1.Pod) {
	for _, podToDelete := range pods {
		c.podsToDelete.AddIfNotExists(podToDelete)
	}
}

func (c *KubernetesClusterContext) DeleteService(service *v1.Service) error {
	deleteOptions := createDeleteOptions()
	err := c.kubernetesClient.CoreV1().Services(service.Namespace).Delete(context.Background(), service.Name, deleteOptions)
	if err != nil && k8s_errors.IsNotFound(err) {
		return nil
	}
	return err
}

func (c *KubernetesClusterContext) DeleteIngress(ingress *networking.Ingress) error {
	deleteOptions := createDeleteOptions()
	err := c.kubernetesClient.NetworkingV1().Ingresses(ingress.Namespace).Delete(context.Background(), ingress.Name, deleteOptions)
	if err != nil && k8s_errors.IsNotFound(err) {
		return nil
	}
	return err
}

func (c *KubernetesClusterContext) ProcessPodsToDelete() {
	pods := c.podsToDelete.GetAll()
	util.ProcessItemsWithThreadPool(context.Background(), c.deleteThreadCount, pods, func(podToDelete *v1.Pod) {
		if podToDelete == nil {
			return
		}
		if podToDelete.DeletionTimestamp == nil {
			// We've never tried to delete this pod before.  Delete using the grace period
			c.doDelete(podToDelete, false)
		} else {
			// we've tried to delete this pod before. If we're after the kill period then force delete
			// else it's a no-op
			killTime := podToDelete.DeletionTimestamp.
				Add(util.GetDeletionGracePeriodOrDefault(podToDelete)).
				Add(c.podKillTimeout)
			if c.clock.Now().After(killTime) {
				log.Infof("Pod %s/%s was requested deleted at %s, but is still present.  Force killing.", podToDelete.Namespace, podToDelete.Name, podToDelete.DeletionTimestamp)
				c.doDelete(podToDelete, true)
			} else {
				log.Debugf("Asked to delete pod %s/%s but this pod is already being deleted", podToDelete.Namespace, podToDelete.Name)
			}
		}
	})
}

func (c *KubernetesClusterContext) doDelete(pod *v1.Pod, force bool) {
	podId := util.ExtractPodKey(pod)
	var err error
	if !util.IsMarkedForDeletion(pod) {
		updatedPod, annotationErr := c.markForDeletion(pod)
		err = annotationErr
		if annotationErr == nil {
			pod = updatedPod
			c.podsToDelete.Update(podId, pod)
		}
	}

	if err == nil {
		deleteOptions := metav1.DeleteOptions{GracePeriodSeconds: nil}
		if force {
			deleteOptions.GracePeriodSeconds = pointer.Int64(0)
		}
		err = c.deletePod(pod, deleteOptions)
	}

	if err == nil || k8s_errors.IsNotFound(err) {
		c.podsToDelete.Update(podId, nil)
	} else {
		log.Errorf("Failed to delete pod %s/%s because %s", pod.Namespace, pod.Name, err)
		c.podsToDelete.Delete(podId)
	}
}

func (c *KubernetesClusterContext) deletePod(pod *v1.Pod, deleteOptions metav1.DeleteOptions) error {
	return c.kubernetesClient.CoreV1().Pods(pod.Namespace).Delete(context.Background(), pod.Name, deleteOptions)
}

func (c *KubernetesClusterContext) markForDeletion(pod *v1.Pod) (*v1.Pod, error) {
	annotations := make(map[string]string)
	annotationName := domain.MarkedForDeletion
	annotations[annotationName] = time.Now().String()

	err := c.AddAnnotation(pod, annotations)
	pod.Annotations = util2.MergeMaps(pod.Annotations, annotations)
	return pod, err
}

func (c *KubernetesClusterContext) GetServices(pod *v1.Pod) ([]*v1.Service, error) {
	podAssociationSelector, err := createPodAssociationSelector(pod)
	if err != nil {
		return []*v1.Service{}, err
	}
	services, err := c.serviceInformer.Lister().List(*podAssociationSelector)
	if err != nil && k8s_errors.IsNotFound(err) {
		return []*v1.Service{}, nil
	}
	if err == nil && services == nil {
		services = []*v1.Service{}
	}
	return services, err
}

func (c *KubernetesClusterContext) GetIngresses(pod *v1.Pod) ([]*networking.Ingress, error) {
	podAssociationSelector, err := createPodAssociationSelector(pod)
	if err != nil {
		return []*networking.Ingress{}, err
	}
	ingresses, err := c.ingressInformer.Lister().List(*podAssociationSelector)
	if err != nil && k8s_errors.IsNotFound(err) {
		return []*networking.Ingress{}, nil
	}
	if err == nil && ingresses == nil {
		ingresses = []*networking.Ingress{}
	}
	return ingresses, err
}

func createPodAssociationSelector(pod *v1.Pod) (*labels.Selector, error) {
	jobId, jobIdPresent := pod.Labels[domain.JobId]
	queue, queuePresent := pod.Labels[domain.Queue]
	podNumber, podNumberPresent := pod.Labels[domain.PodNumber]
	if !jobIdPresent || !queuePresent || !podNumberPresent {
		return nil, fmt.Errorf("Cannot create pod association selector as pod %s (%s) is missing Armada identifier labels", pod.Name, pod.Namespace)
	}
	jobIdMatchesSelector, err := labels.NewRequirement(domain.JobId, selection.Equals, []string{jobId})
	if err != nil {
		return nil, err
	}
	queueMatchesSelector, err := labels.NewRequirement(domain.Queue, selection.Equals, []string{queue})
	if err != nil {
		return nil, err
	}
	podNumberMatchesSelector, err := labels.NewRequirement(domain.PodNumber, selection.Equals, []string{podNumber})
	if err != nil {
		return nil, err
	}

	selector := labels.NewSelector().Add(*jobIdMatchesSelector, *queueMatchesSelector, *podNumberMatchesSelector)
	return &selector, nil
}

func createDeleteOptions() metav1.DeleteOptions {
	gracePeriod := int64(0)
	deleteOptions := metav1.DeleteOptions{
		GracePeriodSeconds: &gracePeriod,
	}
	return deleteOptions
}
