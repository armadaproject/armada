package fake

import (
	"fmt"
	"math/rand"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"
)

type fakeClusterContext struct {
	clusterId string
	handlers  []*cache.ResourceEventHandlerFuncs
	rwLock    sync.RWMutex
	pods      map[string]*v1.Pod
}

func (fakeClusterContext) Stop() {
}

func (c *fakeClusterContext) AddPodEventHandler(handler cache.ResourceEventHandlerFuncs) {
	c.handlers = append(c.handlers, &handler)
}

func (c *fakeClusterContext) GetBatchPods() ([]*v1.Pod, error) {
	c.rwLock.Lock()
	defer c.rwLock.Unlock()

	pods := []*v1.Pod{}
	for _, p := range c.pods {
		pods = append(pods, p.DeepCopy())
	}
	return pods, nil
}

func (c *fakeClusterContext) GetAllPods() ([]*v1.Pod, error) {
	return c.GetBatchPods()
}

func (c *fakeClusterContext) GetActiveBatchPods() ([]*v1.Pod, error) {
	return c.GetBatchPods()
}

func (c *fakeClusterContext) GetNodes() ([]*v1.Node, error) {
	return []*v1.Node{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: c.clusterId + "-mega-node",
			},
			Spec: v1.NodeSpec{
				Unschedulable: false,
			},
			Status: v1.NodeStatus{
				Allocatable: map[v1.ResourceName]resource.Quantity{
					"cpu":    resource.MustParse("1000"),
					"memory": resource.MustParse("10Ti"),
				},
			},
		},
	}, nil
}

func (c *fakeClusterContext) SubmitPod(pod *v1.Pod, owner string) (*v1.Pod, error) {
	c.rwLock.Lock()
	pod.Status.Phase = v1.PodPending
	pod.CreationTimestamp = metav1.Now()
	saved := pod.DeepCopy()
	c.pods[pod.Name] = saved
	c.rwLock.Unlock()

	for _, h := range c.handlers {
		h.AddFunc(pod)
	}

	go func() {
		time.Sleep(time.Duration(rand.Float32()+1) * 100 * time.Millisecond)

		c.rwLock.Lock()
		saved.Spec.NodeName = c.clusterId + "-mega-node"
		c.rwLock.Unlock()

		start := metav1.Now()
		c.updateStatus(saved, v1.PodRunning, v1.ContainerState{Running: &v1.ContainerStateRunning{
			StartedAt: start,
		}})

		c.rwLock.Lock()
		runtime := extractSleepTime(saved)
		c.rwLock.Unlock()
		time.Sleep(time.Duration(runtime) * time.Second)

		c.updateStatus(saved, v1.PodSucceeded, v1.ContainerState{Terminated: &v1.ContainerStateTerminated{
			StartedAt:  start,
			FinishedAt: metav1.Now(),
			ExitCode:   0,
		}})
	}()

	return pod, nil
}

func (c *fakeClusterContext) updateStatus(saved *v1.Pod, phase v1.PodPhase, state v1.ContainerState) (*v1.Pod, *v1.Pod) {
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
	for _, h := range c.handlers {
		h.UpdateFunc(oldPod, newPod)
	}
	return oldPod, newPod
}

func extractSleepTime(pod *v1.Pod) float32 {
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

func (c *fakeClusterContext) AddAnnotation(pod *v1.Pod, annotations map[string]string) error {
	c.rwLock.Lock()
	defer c.rwLock.Unlock()

	p, found := c.pods[pod.Name]
	if !found {
		return fmt.Errorf("Missing pod to annotate %v", pod.Name)
	}
	for k, v := range annotations {
		p.Annotations[k] = v
	}
	return nil
}

func (c *fakeClusterContext) DeletePods(pods []*v1.Pod) {
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

func (c *fakeClusterContext) GetClusterId() string {
	return c.clusterId
}
