package fake

import (
	"errors"
	"fmt"

	"github.com/armadaproject/armada/internal/common/context"

	v1 "k8s.io/api/core/v1"
	discovery "k8s.io/api/discovery/v1"
	networking "k8s.io/api/networking/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/kubelet/pkg/apis/stats/v1alpha1"

	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/executor/domain"
)

type SyncFakeClusterContext struct {
	Pods                 map[string]*v1.Pod
	AnnotationsAdded     map[string]map[string]string
	podEventHandlers     []*cache.ResourceEventHandlerFuncs
	clusterEventHandlers []*cache.ResourceEventHandlerFuncs
}

func NewSyncFakeClusterContext() *SyncFakeClusterContext {
	c := &SyncFakeClusterContext{Pods: map[string]*v1.Pod{}, AnnotationsAdded: map[string]map[string]string{}}
	return c
}

func (*SyncFakeClusterContext) Stop() {}

func (c *SyncFakeClusterContext) AddPodEventHandler(handler cache.ResourceEventHandlerFuncs) {
	c.podEventHandlers = append(c.podEventHandlers, &handler)
}

func (c *SyncFakeClusterContext) AddClusterEventEventHandler(handler cache.ResourceEventHandlerFuncs) {
	c.clusterEventHandlers = append(c.clusterEventHandlers, &handler)
}

func (c *SyncFakeClusterContext) GetBatchPods() ([]*v1.Pod, error) {
	pods := make([]*v1.Pod, 0, len(c.Pods))
	for _, p := range c.Pods {
		pods = append(pods, p.DeepCopy())
	}
	return pods, nil
}

func (c *SyncFakeClusterContext) GetAllPods() ([]*v1.Pod, error) {
	return c.GetBatchPods()
}

func (c *SyncFakeClusterContext) GetActiveBatchPods() ([]*v1.Pod, error) {
	return c.GetBatchPods()
}

func (c *SyncFakeClusterContext) GetNodes() ([]*v1.Node, error) {
	return make([]*v1.Node, 0), nil
}

func (c *SyncFakeClusterContext) GetNode(nodeName string) (*v1.Node, error) {
	return nil, errors.New("GetNode Not implemented")
}

func (c *SyncFakeClusterContext) GetPodEvents(pod *v1.Pod) ([]*v1.Event, error) {
	return []*v1.Event{}, nil
}

func (c *SyncFakeClusterContext) SubmitService(service *v1.Service) (*v1.Service, error) {
	return nil, fmt.Errorf("Services not implemented in SyncFakeClusterContext")
}

func (c *SyncFakeClusterContext) GetServices(pod *v1.Pod) ([]*v1.Service, error) {
	return nil, fmt.Errorf("Services not implemented in SyncFakeClusterContext")
}

func (c *SyncFakeClusterContext) DeleteService(service *v1.Service) error {
	return fmt.Errorf("Services not implemented in SyncFakeClusterContext")
}

func (c *SyncFakeClusterContext) SubmitIngress(ingress *networking.Ingress) (*networking.Ingress, error) {
	return nil, fmt.Errorf("Ingresses not implemented in SyncFakeClusterContext")
}

func (c *SyncFakeClusterContext) GetIngresses(pod *v1.Pod) ([]*networking.Ingress, error) {
	return nil, fmt.Errorf("Ingresses not implemented in SyncFakeClusterContext")
}

func (c *SyncFakeClusterContext) GetEndpointSlices(namespace string, labelName string, labelValue string) ([]*discovery.EndpointSlice, error) {
	return nil, fmt.Errorf("EndpointSlices not implemented in SyncFakeClusterContext")
}

func (c *SyncFakeClusterContext) DeleteIngress(ingress *networking.Ingress) error {
	return fmt.Errorf("Ingresses not implemented in SyncFakeClusterContext")
}

func (c *SyncFakeClusterContext) SubmitPod(pod *v1.Pod, owner string, ownerGroups []string) (*v1.Pod, error) {
	c.Pods[pod.Labels[domain.JobId]] = pod
	return pod, nil
}

func (c *SyncFakeClusterContext) AddAnnotation(pod *v1.Pod, annotations map[string]string) error {
	pod.Annotations = util.MergeMaps(pod.Annotations, annotations)
	if c.AnnotationsAdded[pod.Labels[domain.JobId]] == nil {
		c.AnnotationsAdded[pod.Labels[domain.JobId]] = map[string]string{}
	}
	c.AnnotationsAdded[pod.Labels[domain.JobId]] = util.MergeMaps(c.AnnotationsAdded[pod.Labels[domain.JobId]], annotations)
	return nil
}

func (c *SyncFakeClusterContext) AddClusterEventAnnotation(event *v1.Event, annotations map[string]string) error {
	return nil
}

func (c *SyncFakeClusterContext) DeletePodWithCondition(pod *v1.Pod, condition func(pod *v1.Pod) bool, pessimistic bool) error {
	if condition(pod) {
		c.DeletePods([]*v1.Pod{pod})
	}
	return nil
}

func (c *SyncFakeClusterContext) DeletePods(pods []*v1.Pod) {
	for _, p := range pods {
		delete(c.Pods, p.Labels[domain.JobId])
	}
}

func (c *SyncFakeClusterContext) GetClusterId() string {
	return "cluster-id-1"
}

func (c *SyncFakeClusterContext) GetClusterPool() string {
	return "pool"
}

func (c *SyncFakeClusterContext) GetNodeStatsSummary(ctx *context.ArmadaContext, node *v1.Node) (*v1alpha1.Summary, error) {
	return &v1alpha1.Summary{}, nil
}

func (c *SyncFakeClusterContext) SimulateDeletionEvent(pod *v1.Pod) {
	for _, h := range c.podEventHandlers {
		if h.DeleteFunc != nil {
			h.DeleteFunc(pod)
		}
	}
}

func (c *SyncFakeClusterContext) SimulatePodAddEvent(pod *v1.Pod) {
	for _, h := range c.podEventHandlers {
		if h.AddFunc != nil {
			h.AddFunc(pod)
		}
	}
}

func (c *SyncFakeClusterContext) SimulateClusterAddEvent(clusterEvent *v1.Event) {
	for _, h := range c.clusterEventHandlers {
		if h.AddFunc != nil {
			h.AddFunc(clusterEvent)
		}
	}
}

type FakeClusterIdentity struct {
	clusterId   string
	clusterPool string
}

func NewFakeClusterIdentity(clusterId string, clusterPool string) *FakeClusterIdentity {
	return &FakeClusterIdentity{
		clusterId:   clusterId,
		clusterPool: clusterPool,
	}
}

func (f *FakeClusterIdentity) GetClusterId() string {
	return f.clusterId
}

func (f *FakeClusterIdentity) GetClusterPool() string {
	return f.clusterPool
}
