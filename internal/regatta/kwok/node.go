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

	// TargetLabel identifies which execution target created a fake node. The readiness check
	// overlays this onto its canary job's node selector (when the target's readinessSelectsTarget
	// is set) so a canary submitted for one target can't false-positive by landing on a different
	// target's fake nodes - Armada's scheduler has no cluster-affinity concept, so without this a
	// canary can land on any target's nodes that satisfy the generic fake-node taint/selector. Real, user-authored
	// load.jobs[] specs never carry this label and remain free to schedule on any target's nodes.
	TargetLabel = regattaconfig.TargetLabel
)

// BuildFakeNode constructs a single fake v1.Node shaped by profile, with name/hostname
// parameterized by index so multiple profiles/counts can coexist on one cluster. The
// KWOK fake-node taint/annotation is always added, on top of whatever the profile itself
// specifies, since KWOK's controller and the readiness check both key off of it.
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

// ApplyFakeNodes creates count fake nodes shaped by profile via the typed clientset, up to
// concurrency at a time. Idempotent: an already-existing node (same name/index) is left
// as-is.
func ApplyFakeNodes(ctx context.Context, client kubernetes.Interface, profile *regattaconfig.NodeProfile, count int, targetName string, concurrency int) error {
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(concurrency)
	for i := 0; i < count; i++ {
		i := i
		group.Go(func() error {
			node := BuildFakeNode(profile, i, targetName)
			_, err := client.CoreV1().Nodes().Create(groupCtx, node, metav1.CreateOptions{})
			if err != nil && !apierrors.IsAlreadyExists(err) {
				return fmt.Errorf("creating fake node %s: %w", node.Name, err)
			}
			return nil
		})
	}
	return group.Wait()
}

// DeleteFakeNodes removes targetName's fake v1.Node objects from the cluster, up to
// concurrency at a time, leaving real nodes and other targets' fake nodes (on a shared
// cluster) untouched.
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

// readyPollInterval is how often WaitUntilReady looks at the nodes. A variable so tests can shorten it.
var readyPollInterval = time.Second

// WaitUntilReady polls targetName's fake nodes until all are Ready, ignoring other targets' nodes (and any other
// fake nodes in a shared cluster), which may be in any state. It returns early if ctx is cancelled. kubectl wait
// --for=condition=ready is unreliable across many objects that are already Ready before the
// watch attaches (it only reliably catches a live transition, not pre-existing state), so this
// polls instead.
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
