package fake

import (
	"fmt"

	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/kubelet/pkg/apis/stats/v1alpha1"

	"github.com/G-Research/armada/internal/executor/domain"
)

type SyncFakeClusterContext struct {
	Pods map[string]*v1.Pod
}

func NewSyncFakeClusterContext() *SyncFakeClusterContext {
	c := &SyncFakeClusterContext{Pods: map[string]*v1.Pod{}}
	return c
}

func (*SyncFakeClusterContext) Stop() {}

func (c *SyncFakeClusterContext) AddPodEventHandler(handler cache.ResourceEventHandlerFuncs) {}

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

func (c *SyncFakeClusterContext) GetPodEvents(pod *v1.Pod) ([]*v1.Event, error) {
	return []*v1.Event{}, nil
}

func (c *SyncFakeClusterContext) SubmitService(service *v1.Service) (*v1.Service, error) {
	return nil, fmt.Errorf("Services not implemented in SyncFakeClusterContext")
}

func (c *SyncFakeClusterContext) GetService(name string, namespace string) (*v1.Service, error) {
	return nil, fmt.Errorf("Services not implemented in SyncFakeClusterContext")
}

func (c *SyncFakeClusterContext) DeleteService(service *v1.Service) error {
	return fmt.Errorf("Services not implemented in SyncFakeClusterContext")
}

func (c *SyncFakeClusterContext) SubmitPod(pod *v1.Pod, owner string, ownerGroups []string) (*v1.Pod, error) {
	c.Pods[pod.Labels[domain.JobId]] = pod
	return pod, nil
}

func (c *SyncFakeClusterContext) AddAnnotation(pod *v1.Pod, annotations map[string]string) error {
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

func (c *SyncFakeClusterContext) GetNodeStatsSummary(node *v1.Node) (*v1alpha1.Summary, error) {
	return &v1alpha1.Summary{}, nil
}
