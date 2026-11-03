package kwok

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"
	v1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
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
	name := fmt.Sprintf("%s%d", nodeNamePrefix(profile), index)

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
// if this target created it and it still matches the profile; one that differs, or one beyond count that an earlier run
// left, is an error: reusing it would benchmark the old capacity. The fix is to tear the target down first.
func ApplyFakeNodes(ctx context.Context, client kubernetes.Interface, profile *regattaconfig.NodeProfile, count int, targetName string, concurrency int) error {
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(concurrency)
	for i := 0; i < count; i++ {
		i := i
		group.Go(func() error {
			node := BuildFakeNode(profile, i, targetName)
			_, err := client.CoreV1().Nodes().Create(groupCtx, node, metav1.CreateOptions{})
			if apierrors.IsAlreadyExists(err) {
				return checkExistingNode(groupCtx, client, node)
			}
			if err != nil {
				return fmt.Errorf("creating fake node %s: %w", node.Name, err)
			}
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return err
	}
	return checkNoSurplusNodes(ctx, client, profile, count, targetName)
}

// checkExistingNode accepts an existing node only if this target created it and it still matches want. Names come from
// the profile and an index, so another target's node under the same name means two targets share a cluster; reusing it
// would leave this target short of nodes.
func checkExistingNode(ctx context.Context, client kubernetes.Interface, want *v1.Node) error {
	targetName := want.Labels[TargetLabel]
	existing, err := client.CoreV1().Nodes().Get(ctx, want.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil // deleted since the create; the next run creates it
	}
	if err != nil {
		return fmt.Errorf("checking existing fake node %s: %w", want.Name, err)
	}
	if owner := existing.Labels[TargetLabel]; owner != targetName {
		if owner == "" {
			return fmt.Errorf("a node named %s already exists and was not created by regatta; target %q cannot create its fake node", want.Name, targetName)
		}
		return fmt.Errorf("fake node %s already exists and belongs to target %q, not %q; two targets must not share a cluster", want.Name, owner, targetName)
	}
	if !nodeMatches(existing, want) {
		return fmt.Errorf("fake node %s was left by an earlier run with a different profile (resources, labels or taints); tear target %q down before running again", want.Name, targetName)
	}
	return nil
}

// nodeMatches is whether existing has the capacity, labels and taints want asks for. Labels other than want's are
// ignored: the cluster may add its own.
func nodeMatches(existing, want *v1.Node) bool {
	if !apiequality.Semantic.DeepEqual(existing.Status.Allocatable, want.Status.Allocatable) ||
		!apiequality.Semantic.DeepEqual(existing.Spec.Taints, want.Spec.Taints) {
		return false
	}
	for k, v := range want.Labels {
		if existing.Labels[k] != v {
			return false
		}
	}
	return true
}

// checkNoSurplusNodes fails if the target still has nodes of profile numbered count or higher, left by an earlier run
// that asked for more: they would be benchmarked along with the requested ones.
func checkNoSurplusNodes(ctx context.Context, client kubernetes.Interface, profile *regattaconfig.NodeProfile, count int, targetName string) error {
	nodes, err := client.CoreV1().Nodes().List(ctx, metav1.ListOptions{
		LabelSelector: NodeAnnotation + "=" + NodeAnnotationOK + "," + TargetLabel + "=" + targetName,
	})
	if err != nil {
		return fmt.Errorf("listing fake nodes: %w", err)
	}
	prefix := nodeNamePrefix(profile)
	surplus := 0
	for _, node := range nodes.Items {
		// Only "<prefix><number>" counts, so profile "gpu" does not claim the nodes of profile "gpu-big".
		if index, err := strconv.Atoi(strings.TrimPrefix(node.Name, prefix)); strings.HasPrefix(node.Name, prefix) && err == nil && index >= count {
			surplus++
		}
	}
	if surplus > 0 {
		return fmt.Errorf("target %q has %d more %q fake nodes than the %d requested, left by an earlier run; tear it down before running again", targetName, surplus, profile.Name, count)
	}
	return nil
}

func nodeNamePrefix(profile *regattaconfig.NodeProfile) string {
	return fmt.Sprintf("kwok-node-%s-", profile.Name)
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
	parent := ctx
	// The deadline covers the requests too: a list call that never answers is cancelled when it passes.
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var lastErr error
	for {
		nodes, err := client.CoreV1().Nodes().List(ctx, metav1.ListOptions{
			LabelSelector: NodeAnnotation + "=" + NodeAnnotationOK + "," + TargetLabel + "=" + targetName,
		})
		switch {
		case err != nil:
			lastErr = err
		case len(nodes.Items) == 0:
			lastErr = fmt.Errorf("no fake nodes found")
		case allReady(nodes.Items):
			return nil
		default:
			lastErr = fmt.Errorf("not all %d fake nodes are ready yet", len(nodes.Items))
		}
		if parent.Err() != nil {
			return fmt.Errorf("waiting for fake nodes to become ready: %w", parent.Err())
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("timed out waiting for fake nodes to become ready: %w", lastErr)
		case <-time.After(readyPollInterval):
		}
	}
}

func allReady(nodes []v1.Node) bool {
	for i := range nodes {
		if !isNodeReady(&nodes[i]) {
			return false
		}
	}
	return true
}

func isNodeReady(node *v1.Node) bool {
	for _, cond := range node.Status.Conditions {
		if cond.Type == v1.NodeReady {
			return cond.Status == v1.ConditionTrue
		}
	}
	return false
}
