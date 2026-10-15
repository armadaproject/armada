package kwok

import (
	"context"
	"fmt"
	"time"

	"golang.org/x/sync/errgroup"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	regattaconfig "github.com/armadaproject/armada/internal/regatta/config"
)

const (
	NodeAnnotation   = "kwok.x-k8s.io/node"
	NodeAnnotationOK = "fake"

	// TargetLabel names the target that created a fake node. The canary (readinessSelectsTarget) and the load of queues
	// with targets select on it; Armada's scheduler has no cluster affinity of its own.
	TargetLabel = regattaconfig.TargetLabel
)

// BuildFakeNode builds one fake node from profile, named by index. The KWOK taint and annotation are always added: the
// controller and the readiness check both key off them.
func BuildFakeNode(profile *regattaconfig.NodeProfile, index int, targetName string) *v1.Node {
	name := fmt.Sprintf("kwok-node-%s-%d", profile.Name, index)

	allocatable := v1.ResourceList{}
	for resourceName, quantity := range profile.Allocatable {
		allocatable[resourceName] = quantity
	}

	labels := map[string]string{
		"kubernetes.io/hostname": name,
		"kubernetes.io/os":       "linux",
		"type":                   "kwok",
		NodeAnnotation:           NodeAnnotationOK,
		TargetLabel:              targetName,
	}
	for k, v := range profile.Labels {
		labels[k] = v
	}

	taints := append([]v1.Taint{}, profile.Taints...)
	taints = append(taints, v1.Taint{
		Key:    NodeAnnotation,
		Value:  NodeAnnotationOK,
		Effect: v1.TaintEffectNoSchedule,
	})

	return &v1.Node{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "Node",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Annotations: map[string]string{
				"node.alpha.kubernetes.io/ttl": "0",
				NodeAnnotation:                 NodeAnnotationOK,
			},
			Labels: labels,
		},
		Spec: v1.NodeSpec{
			Taints: taints,
		},
		Status: v1.NodeStatus{
			Allocatable: allocatable,
			Capacity:    allocatable,
			NodeInfo: v1.NodeSystemInfo{
				Architecture:     "amd64",
				KubeProxyVersion: "fake",
				KubeletVersion:   "fake",
				OperatingSystem:  "linux",
			},
			Phase: v1.NodeRunning,
		},
	}
}

// ApplyFakeNodes creates count fake nodes from profile, up to concurrency at a time. A node that already exists is kept
// if this target created it.
func ApplyFakeNodes(ctx context.Context, client kubernetes.Interface, profile *regattaconfig.NodeProfile, count int, targetName string, concurrency int) error {
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(concurrency)
	for i := 0; i < count; i++ {
		i := i
		group.Go(func() error {
			node := BuildFakeNode(profile, i, targetName)
			_, err := client.CoreV1().Nodes().Create(groupCtx, node, metav1.CreateOptions{})
			if apierrors.IsAlreadyExists(err) {
				return checkNodeBelongsToTarget(groupCtx, client, node.Name, targetName)
			}
			if err != nil {
				return fmt.Errorf("creating fake node %s: %w", node.Name, err)
			}
			return nil
		})
	}
	return group.Wait()
}

// checkNodeBelongsToTarget accepts an existing node only if this target created it. Names come from the profile and an
// index, so another target's node under the same name means two targets share a cluster; reusing it would leave this
// target short of nodes.
func checkNodeBelongsToTarget(ctx context.Context, client kubernetes.Interface, nodeName, targetName string) error {
	existing, err := client.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil // deleted since the create; the next run creates it
	}
	if err != nil {
		return fmt.Errorf("checking existing fake node %s: %w", nodeName, err)
	}
	if owner := existing.Labels[TargetLabel]; owner != targetName {
		if owner == "" {
			return fmt.Errorf("a node named %s already exists and was not created by regatta; target %q cannot create its fake node", nodeName, targetName)
		}
		return fmt.Errorf("fake node %s already exists and belongs to target %q, not %q; two targets must not share a cluster", nodeName, owner, targetName)
	}
	return nil
}

// DeleteFakeNodes removes targetName's fake nodes, up to concurrency at a time. Real nodes and other targets' are left.
func DeleteFakeNodes(ctx context.Context, client kubernetes.Interface, targetName string, concurrency int) error {
	nodes, err := client.CoreV1().Nodes().List(ctx, metav1.ListOptions{
		LabelSelector: NodeAnnotation + "=" + NodeAnnotationOK + "," + TargetLabel + "=" + targetName,
	})
	if err != nil {
		return fmt.Errorf("listing fake nodes: %w", err)
	}

	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(concurrency)
	for _, node := range nodes.Items {
		name := node.Name
		group.Go(func() error {
			if err := client.CoreV1().Nodes().Delete(groupCtx, name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
				return fmt.Errorf("deleting fake node %s: %w", name, err)
			}
			return nil
		})
	}
	return group.Wait()
}

// readyPollInterval is how often WaitUntilReady looks at the nodes; a variable so tests can shorten it.
var readyPollInterval = time.Second

// WaitUntilReady polls targetName's fake nodes until all are Ready, ignoring other targets' nodes, and returns early if
// ctx is cancelled. It polls because kubectl wait is unreliable for objects that are Ready before it attaches.
func WaitUntilReady(ctx context.Context, client kubernetes.Interface, targetName string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("waiting for fake nodes to become ready: %w", err)
		}
		nodes, err := client.CoreV1().Nodes().List(ctx, metav1.ListOptions{
			LabelSelector: NodeAnnotation + "=" + NodeAnnotationOK + "," + TargetLabel + "=" + targetName,
		})
		if err != nil {
			lastErr = err
			sleepOrDone(ctx, readyPollInterval)
			continue
		}
		if len(nodes.Items) == 0 {
			lastErr = fmt.Errorf("no fake nodes found")
			sleepOrDone(ctx, readyPollInterval)
			continue
		}

		allReady := true
		for _, node := range nodes.Items {
			if !isNodeReady(&node) {
				allReady = false
				break
			}
		}
		if allReady {
			return nil
		}
		lastErr = fmt.Errorf("not all %d fake nodes are ready yet", len(nodes.Items))
		sleepOrDone(ctx, readyPollInterval)
	}
	return fmt.Errorf("timed out waiting for fake nodes to become ready: %w", lastErr)
}

// sleepOrDone waits for d or until ctx is done, whichever comes first.
func sleepOrDone(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}

func isNodeReady(node *v1.Node) bool {
	for _, cond := range node.Status.Conditions {
		if cond.Type == v1.NodeReady {
			return cond.Status == v1.ConditionTrue
		}
	}
	return false
}
