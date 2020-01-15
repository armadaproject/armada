package fake

import (
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
		pods = append(pods, p)
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
	defer c.rwLock.Unlock()
	c.pods[pod.Name] = pod
	pod.Status.Phase = v1.PodPending

	for _, h := range c.handlers {
		h.AddFunc(pod)
	}
	go func() {
		time.Sleep(time.Duration(rand.Float32()+1) * 100 * time.Millisecond)
		oldPod := pod.DeepCopy()
		pod.Spec.NodeName = c.clusterId + "-mega-node"
		pod.Status.Phase = v1.PodRunning
		for _, h := range c.handlers {
			h.UpdateFunc(oldPod, pod)
		}

		runtime := extractSleepTime(pod)

		time.Sleep(time.Duration(runtime) * time.Second)
		oldPod = pod.DeepCopy()
		pod.Status.Phase = v1.PodSucceeded
		for _, h := range c.handlers {
			h.UpdateFunc(oldPod, pod)
		}
	}()

	return pod, nil
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

func (fakeClusterContext) AddAnnotation(pod *v1.Pod, annotations map[string]string) error {
	// just annotate previously returned object, this relies on not copying pods when returning them from context
	for k, v := range annotations {
		pod.Annotations[k] = v
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
