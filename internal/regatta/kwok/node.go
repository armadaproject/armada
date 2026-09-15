package kwok

import (
	"context"
	"fmt"
	"strconv"
	"time"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const (
	NodeAnnotation   = "kwok.x-k8s.io/node"
	NodeAnnotationOK = "fake"
)

// NodeProfile describes one simulated node shape, modeled on a real hardware SKU so node count
// maps to a real deployable unit.
type NodeProfile struct {
	Name         string
	CPU          string
	Memory       string
	GPUCount     int
	InstanceType string
}

// GB200Slice models one AWS p6e-gb200.36xlarge, i.e. 1/18th of an NVL72 rack.
var GB200Slice = NodeProfile{
	Name:         "gb200-slice",
	CPU:          "144",
	Memory:       "960Gi",
	GPUCount:     4,
	InstanceType: "p6e-gb200.36xlarge",
}

// BuildFakeNode constructs a single fake v1.Node shaped by profile, with name/hostname
// parameterized by index so multiple profiles/counts can coexist on one cluster.
func BuildFakeNode(profile NodeProfile, index int) *v1.Node {
	name := fmt.Sprintf("kwok-node-%s-%d", profile.Name, index)

	resources := func() v1.ResourceList {
		rl := v1.ResourceList{
			v1.ResourceCPU:              resource.MustParse(profile.CPU),
			v1.ResourceMemory:           resource.MustParse(profile.Memory),
			v1.ResourceEphemeralStorage: resource.MustParse("256Gi"),
			v1.ResourcePods:             resource.MustParse("3000"),
		}
		if profile.GPUCount > 0 {
			rl["nvidia.com/gpu"] = resource.MustParse(strconv.Itoa(profile.GPUCount))
		}
		return rl
	}

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
			Labels: map[string]string{
				"kubernetes.io/hostname":           name,
				"kubernetes.io/os":                 "linux",
				"type":                             "kwok",
				"node.kubernetes.io/instance-type": profile.InstanceType,
				NodeAnnotation:                     NodeAnnotationOK,
			},
		},
		Spec: v1.NodeSpec{
			Taints: []v1.Taint{
				{
					Key:    NodeAnnotation,
					Value:  NodeAnnotationOK,
					Effect: v1.TaintEffectNoSchedule,
				},
			},
		},
		Status: v1.NodeStatus{
			Allocatable: resources(),
			Capacity:    resources(),
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

// ApplyFakeNodes creates count fake nodes shaped by profile via the typed clientset. Idempotent:
// an already-existing node (same name/index) is left as-is.
func ApplyFakeNodes(ctx context.Context, client kubernetes.Interface, profile NodeProfile, count int) error {
	for i := 0; i < count; i++ {
		node := BuildFakeNode(profile, i)
		_, err := client.CoreV1().Nodes().Create(ctx, node, metav1.CreateOptions{})
		if err != nil && !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("creating fake node %s: %w", node.Name, err)
		}
	}
	return nil
}

// DeleteFakeNodes removes the fake v1.Node objects from the cluster, leaving real nodes untouched.
func DeleteFakeNodes(ctx context.Context, client kubernetes.Interface) error {
	nodes, err := client.CoreV1().Nodes().List(ctx, metav1.ListOptions{
		LabelSelector: NodeAnnotation + "=" + NodeAnnotationOK,
	})
	if err != nil {
		return fmt.Errorf("listing fake nodes: %w", err)
	}
	for _, node := range nodes.Items {
		if err := client.CoreV1().Nodes().Delete(ctx, node.Name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("deleting fake node %s: %w", node.Name, err)
		}
	}
	return nil
}

// WaitUntilReady polls fake-annotated nodes until all are Ready. kubectl wait
// --for=condition=ready is unreliable across many objects that are already Ready before the
// watch attaches (it only reliably catches a live transition, not pre-existing state), so this
// polls instead.
func WaitUntilReady(ctx context.Context, client kubernetes.Interface, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		nodes, err := client.CoreV1().Nodes().List(ctx, metav1.ListOptions{
			LabelSelector: NodeAnnotation + "=" + NodeAnnotationOK,
		})
		if err != nil {
			lastErr = err
			time.Sleep(time.Second)
			continue
		}
		if len(nodes.Items) == 0 {
			lastErr = fmt.Errorf("no fake nodes found")
			time.Sleep(time.Second)
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
		time.Sleep(time.Second)
	}
	return fmt.Errorf("timed out waiting for fake nodes to become ready: %w", lastErr)
}

func isNodeReady(node *v1.Node) bool {
	for _, cond := range node.Status.Conditions {
		if cond.Type == v1.NodeReady {
			return cond.Status == v1.ConditionTrue
		}
	}
	return false
}
