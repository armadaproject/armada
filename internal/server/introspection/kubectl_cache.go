package introspection

import (
	"context"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/kubectl/pkg/describe"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	log "github.com/armadaproject/armada/internal/common/logging"
)

type KubectlCache struct {
	kubeClient kubernetes.Interface
	interval   time.Duration
	mu         sync.RWMutex
	nodes      map[string]string
	pods       map[string]string
	updatedAt  time.Time
}

func NewKubectlCache(kubeClient kubernetes.Interface, interval time.Duration) *KubectlCache {
	return &KubectlCache{
		kubeClient: kubeClient,
		interval:   interval,
		nodes:      make(map[string]string),
		pods:       make(map[string]string),
	}
}

func (c *KubectlCache) Run(ctx *armadacontext.Context) error {
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()
	c.refresh(ctx)
	for {
		select {
		case <-ticker.C:
			c.refresh(ctx)
		case <-ctx.Done():
			return nil
		}
	}
}

func (c *KubectlCache) refresh(ctx context.Context) {
	nodes, err := c.kubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		log.WithError(err).Warn("kubectl cache: failed to list nodes")
		return
	}

	newNodes := make(map[string]string)
	nodeDescriber := &describe.NodeDescriber{Interface: c.kubeClient}
	for _, node := range nodes.Items {
		output, err := nodeDescriber.Describe("", node.Name, describe.DescriberSettings{ShowEvents: true})
		if err != nil {
			log.WithError(err).Warnf("kubectl cache: failed to describe node %s", node.Name)
			continue
		}
		newNodes[node.Name] = output
	}

	pods, err := c.kubeClient.CoreV1().Pods("default").List(ctx, metav1.ListOptions{})
	if err != nil {
		log.WithError(err).Warn("kubectl cache: failed to list pods")
		return
	}

	newPods := make(map[string]string)
	podDescriber := &describe.PodDescriber{Interface: c.kubeClient}
	for _, pod := range pods.Items {
		output, err := podDescriber.Describe("default", pod.Name, describe.DescriberSettings{ShowEvents: true})
		if err != nil {
			log.WithError(err).Warnf("kubectl cache: failed to describe pod %s", pod.Name)
			continue
		}
		newPods[pod.Name] = output
	}

	c.mu.Lock()
	c.nodes = newNodes
	c.pods = newPods
	c.updatedAt = time.Now()
	c.mu.Unlock()
}

func (c *KubectlCache) GetNode(name string) (string, time.Time, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	v, ok := c.nodes[name]
	return v, c.updatedAt, ok
}

func (c *KubectlCache) GetPod(name string) (string, time.Time, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	v, ok := c.pods[name]
	return v, c.updatedAt, ok
}

