package context

import (
	"context"
	"math/rand"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	networking "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"
	"k8s.io/kubelet/pkg/apis/stats/v1alpha1"

	"github.com/armadaproject/armada/internal/common"
	"github.com/armadaproject/armada/internal/executor/configuration"
	cluster_context "github.com/armadaproject/armada/internal/executor/context"
)

type NodeSpec struct {
	Name        string
	Count       int
	Taints      []v1.Taint
	Labels      map[string]string
	Allocatable map[v1.ResourceName]resource.Quantity
}

var DefaultNodeSpec = []*NodeSpec{
	{
		Name:  "worker",
		Count: 500,
		Allocatable: map[v1.ResourceName]resource.Quantity{
			"cpu":               resource.MustParse("8"),
			"memory":            resource.MustParse("128Gi"),
			"ephemeral-storage": resource.MustParse("256Gi"),
		},
	},
}

type FakeClusterContext struct {
	clusterId             string
	pool                  string
	podEventHandlers      []*cache.ResourceEventHandlerFuncs
	clusterEventHandlers  []*cache.ResourceEventHandlerFuncs
	rwLock                sync.RWMutex
	pods                  map[string]*v1.Pod
	events                map[string]*v1.Event
	nodes                 []*v1.Node
	nodeAvailableResource map[string]common.ComputeResources
}

func NewFakeClusterContext(appConfig configuration.ApplicationConfiguration, nodeSpecs []*NodeSpec) cluster_context.ClusterContext {
	c := &FakeClusterContext{
		clusterId:             appConfig.ClusterId,
		pool:                  appConfig.Pool,
		pods:                  map[string]*v1.Pod{},
		nodeAvailableResource: map[string]common.ComputeResources{},
	}
	if nodeSpecs == nil {
		nodeSpecs = DefaultNodeSpec
	}
	c.addNodes(nodeSpecs)
	return c
}

func (*FakeClusterContext) Stop() {
}

func (c *FakeClusterContext) AddPodEventHandler(handler cache.ResourceEventHandlerFuncs) {
	c.podEventHandlers = append(c.podEventHandlers, &handler)
}

func (c *FakeClusterContext) AddClusterEventEventHandler(handler cache.ResourceEventHandlerFuncs) {
	c.clusterEventHandlers = append(c.clusterEventHandlers, &handler)
}

func (c *FakeClusterContext) GetBatchPods() ([]*v1.Pod, error) {
	c.rwLock.Lock()
	defer c.rwLock.Unlock()

	pods := []*v1.Pod{}
	for _, p := range c.pods {
		pods = append(pods, p.DeepCopy())
	}
	return pods, nil
}

func (c *FakeClusterContext) GetAllPods() ([]*v1.Pod, error) {
	return c.GetBatchPods()
}

func (c *FakeClusterContext) GetActiveBatchPods() ([]*v1.Pod, error) {
	return c.GetBatchPods()
}

func (c *FakeClusterContext) GetNodes() ([]*v1.Node, error) {
	return c.nodes, nil
}

func (c *FakeClusterContext) GetNode(nodeName string) (*v1.Node, error) {
	return c.nodes[0], nil
}

func (c *FakeClusterContext) GetPodEvents(pod *v1.Pod) ([]*v1.Event, error) {
	return []*v1.Event{}, nil
}

func (c *FakeClusterContext) SubmitPod(pod *v1.Pod, owner string, ownerGroups []string) (*v1.Pod, error) {
	saved := c.savePod(pod)

	for _, h := range c.podEventHandlers {
		if h.AddFunc != nil {
			h.AddFunc(pod)
		}
	}

	go func() {
		time.Sleep(time.Duration(rand.Float32()+1) * 100 * time.Millisecond)

		for {
			scheduled, removed := c.trySchedule(saved)
			if scheduled {
				break
			}
			if removed {
				return
			}
			time.Sleep(100 * time.Millisecond)
		}

		start := metav1.Now()
		c.updateStatus(saved, v1.PodRunning, v1.ContainerState{Running: &v1.ContainerStateRunning{
			StartedAt: start,
		}})

		runtime := c.extractSleepTime(saved)
		time.Sleep(time.Duration(runtime) * time.Second)

		c.deallocate(saved)

		c.updateStatus(saved, v1.PodSucceeded, v1.ContainerState{Terminated: &v1.ContainerStateTerminated{
			StartedAt:  start,
			FinishedAt: metav1.Now(),
			ExitCode:   0,
		}})
	}()

	return pod, nil
}

func (c *FakeClusterContext) savePod(pod *v1.Pod) *v1.Pod {
	c.rwLock.Lock()
	defer c.rwLock.Unlock()

	pod.Status.Phase = v1.PodPending
	pod.CreationTimestamp = metav1.Now()
	pod.UID = types.UID(uuid.New().String())
	saved := pod.DeepCopy()
	c.pods[pod.Name] = saved
	return saved
}

func (c *FakeClusterContext) SubmitService(service *v1.Service) (*v1.Service, error) {
	return nil, errors.Errorf("Services not implemented in FakeClusterContext")
}

func (c *FakeClusterContext) GetServices(pod *v1.Pod) ([]*v1.Service, error) {
	return nil, errors.Errorf("Services not implemented in FakeClusterContext")
}

func (c *FakeClusterContext) DeleteService(service *v1.Service) error {
	return errors.Errorf("Services not implemented in FakeClusterContext")
}

func (c *FakeClusterContext) SubmitIngress(ingress *networking.Ingress) (*networking.Ingress, error) {
	return nil, errors.Errorf("Ingresses not implemented in FakeClusterContext")
}

func (c *FakeClusterContext) GetIngresses(pod *v1.Pod) ([]*networking.Ingress, error) {
	return nil, errors.Errorf("Ingresses not implemented in FakeClusterContext")
}

func (c *FakeClusterContext) DeleteIngress(ingress *networking.Ingress) error {
	return errors.Errorf("Ingresses not implemented in FakeClusterContext")
}

func (c *FakeClusterContext) updateStatus(saved *v1.Pod, phase v1.PodPhase, state v1.ContainerState) (*v1.Pod, *v1.Pod) {
	c.rwLock.Lock()
	oldPod := saved.DeepCopy()
	saved.Status.Phase = phase
	containerStatuses := []v1.ContainerStatus{}
	for _, c := range saved.Spec.Containers {
		containerStatuses = append(containerStatuses, v1.ContainerStatus{
			Name:  c.Name,
			Ready: phase != v1.PodPending,
			State: state,
		})
	}

	saved.Status.ContainerStatuses = containerStatuses

	newPod := saved.DeepCopy()
	c.rwLock.Unlock()
	for _, h := range c.podEventHandlers {
		if h.UpdateFunc != nil {
			h.UpdateFunc(oldPod, newPod)
		}
	}
	return oldPod, newPod
}

func (c *FakeClusterContext) extractSleepTime(pod *v1.Pod) float32 {
	c.rwLock.Lock()
	defer c.rwLock.Unlock()

	command := append(pod.Spec.Containers[0].Command, pod.Spec.Containers[0].Args...)
	commandString := strings.Join(command, " ")

	// command needs to be in the form: sleep $(( (RANDOM % 60) + 100 ))
	r, e := regexp.Compile("\\(RANDOM *% *([0-9]+)\\) *\\+ *([0-9]+)")
	if e == nil {
		randomCallMatches := r.FindStringSubmatch(commandString)
		if len(randomCallMatches) == 3 {
			random, _ := strconv.Atoi(randomCallMatches[1])
			fixed, _ := strconv.Atoi(randomCallMatches[2])
			return rand.Float32()*float32(random) + float32(fixed)
		}
	}
	log.Errorf("Default sleep 1s, could not interpret command: %s", commandString)
	return 1
}

func (c *FakeClusterContext) AddAnnotation(pod *v1.Pod, annotations map[string]string) error {
	c.rwLock.Lock()
	defer c.rwLock.Unlock()

	p, found := c.pods[pod.Name]
	if !found {
		return errors.Errorf("missing pod to annotate: %s", pod.Name)
	}
	for k, v := range annotations {
		p.Annotations[k] = v
	}
	return nil
}

func (c *FakeClusterContext) AddClusterEventAnnotation(event *v1.Event, annotations map[string]string) error {
	c.rwLock.Lock()
	defer c.rwLock.Unlock()

	p, found := c.events[event.Name]
	if !found {
		return errors.Errorf("missing event to annotate: %s", event.Name)
	}
	for k, v := range annotations {
		p.Annotations[k] = v
	}
	return nil
}

func (c *FakeClusterContext) DeletePods(pods []*v1.Pod) {
	go func() {
		// wait a little before actual delete
		time.Sleep(100 * time.Millisecond)

		c.rwLock.Lock()
		defer c.rwLock.Unlock()

		for _, p := range pods {
			delete(c.pods, p.Name)
		}
	}()
}

func (c *FakeClusterContext) GetClusterId() string {
	return c.clusterId
}

func (c *FakeClusterContext) GetClusterPool() string {
	return c.pool
}

func (c *FakeClusterContext) GetNodeStatsSummary(ctx context.Context, node *v1.Node) (*v1alpha1.Summary, error) {
	return &v1alpha1.Summary{}, nil
}

func (c *FakeClusterContext) addNodes(specs []*NodeSpec) {
	for _, s := range specs {
		for i := 0; i < s.Count; i++ {
			node := &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:   c.clusterId + "-" + s.Name + "-" + strconv.Itoa(i),
					Labels: s.Labels,
				},
				Spec: v1.NodeSpec{
					Taints:        s.Taints,
					Unschedulable: false,
				},
				Status: v1.NodeStatus{
					Allocatable: s.Allocatable,
				},
			}
			c.nodes = append(c.nodes, node)
			c.nodeAvailableResource[node.Name] = common.FromResourceList(s.Allocatable)
		}
	}
}

func (c *FakeClusterContext) trySchedule(pod *v1.Pod) (scheduled bool, removed bool) {
	c.rwLock.Lock()
	defer c.rwLock.Unlock()

	if _, exists := c.pods[pod.Name]; !exists {
		return false, true
	}

	// fill more busy nodes first
	sort.Slice(c.nodes, func(i, j int) bool {
		node1 := c.nodes[i]
		node2 := c.nodes[j]
		node1Resource := c.nodeAvailableResource[node1.Name]
		node2Resource := c.nodeAvailableResource[node2.Name]

		// returns true if node1 should be considered before node2
		return node2Resource.Dominates(node1Resource)
	})

	for _, n := range c.nodes {
		if c.isSchedulableOn(pod, n) {
			resources := common.TotalPodResourceRequest(&pod.Spec)
			c.nodeAvailableResource[n.Name].Sub(resources)
			pod.Spec.NodeName = n.Name
			return true, false
		}
	}
	return false, false
}

func (c *FakeClusterContext) deallocate(pod *v1.Pod) {
	c.rwLock.Lock()
	defer c.rwLock.Unlock()

	resources := common.TotalPodResourceRequest(&pod.Spec)
	c.nodeAvailableResource[pod.Spec.NodeName].Add(resources)
}

func (c *FakeClusterContext) isSchedulableOn(pod *v1.Pod, n *v1.Node) bool {
	requiredResource := common.TotalPodResourceRequest(&pod.Spec)
	availableResource := c.nodeAvailableResource[n.Name].DeepCopy()
	availableResource.Sub(requiredResource)

	// resources
	if !availableResource.IsValid() {
		return false
	}

	// labels
	for k, v := range pod.Spec.NodeSelector {
		if n.Labels == nil {
			return false
		}
		nodeValue, exists := n.Labels[k]
		if !exists || nodeValue != v {
			return false
		}
	}

	// taints
	for _, t := range n.Spec.Taints {
		// check only hard constraints
		if t.Effect == v1.TaintEffectPreferNoSchedule {
			continue
		}
		if !tolerationsTolerateTaint(pod.Spec.Tolerations, &t) {
			return false
		}
	}
	return true
}

// https://github.com/kubernetes/kubernetes/blob/master/pkg/apis/core/v1/helper/helpers.go#L427
func tolerationsTolerateTaint(tolerations []v1.Toleration, taint *v1.Taint) bool {
	for i := range tolerations {
		if tolerations[i].ToleratesTaint(taint) {
			return true
		}
	}
	return false
}
