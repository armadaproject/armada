package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"
)

const (
	KWOK_IMAGE          = "registry.k8s.io/kwok/cluster:v0.7.0-k8s.v1.28.15" // from _local/kwok/cluster.yaml
	KWOK_CONTAINER_NAME = "armada-kwok"
	KWOK_PORT           = "8888" // from _local/kwok/cluster.yaml; avoids colliding with Armada's own 8080-8084/9000-9008 ports
	KWOK_KUBECONFIG     = ".kube/kwok/config"
	KWOK_NODE_COUNT     = 500 // matches internal/executor/fake/context/context.go's DefaultNodeSpec
)

func kwokCheck() error {
	return dockerCheck()
}

// Idempotent: no-ops if a container named armada-kwok is already running.
func kwokInitCluster() error {
	out, err := dockerOutput("ps", "--filter", "name=^/"+KWOK_CONTAINER_NAME+"$", "--format", "{{.Names}}")
	if err != nil {
		return err
	}
	if strings.TrimSpace(out) == KWOK_CONTAINER_NAME {
		return nil
	}

	stagesPath, err := filepath.Abs("_local/kwok/stages.yaml")
	if err != nil {
		return err
	}

	// The all-in-one image's entrypoint runs `kwokctl create cluster "$@"`, so extra args
	// after the image name land on that command. -c/--config there is kwokctl's own flag
	// (not kubectl apply-able - Stage isn't a CRD in the apiserver, it's a kwok-controller
	// config construct) and gets merged into the cluster's generated kwok.yaml alongside the
	// built-in stages, which is how we override the default pod-complete's Job-ownership
	// requirement (see stages.yaml) for Armada's bare Pods.
	if err := dockerRun(
		"run", "--rm", "-d",
		"-p", KWOK_PORT+":8080",
		"--name", KWOK_CONTAINER_NAME,
		"-v", stagesPath+":/stages.yaml:ro",
		KWOK_IMAGE,
		"-c", "/stages.yaml",
	); err != nil {
		return err
	}

	if err := kwokWriteKubeConfig(); err != nil {
		return err
	}

	return kwokWaitForApiServer()
}

// Write kubeconfig to disk.
// The all-in-one image serves plain HTTP with no auth, so this is fully static.
func kwokWriteKubeConfig() error {
	kubeconfig := fmt.Sprintf(`apiVersion: v1
kind: Config
clusters:
  - name: kwok
    cluster:
      server: http://127.0.0.1:%s
contexts:
  - name: kwok
    context:
      cluster: kwok
current-context: kwok
`, KWOK_PORT)

	if err := os.MkdirAll(filepath.Dir(KWOK_KUBECONFIG), os.ModeDir|0o755); err != nil {
		return err
	}
	return os.WriteFile(KWOK_KUBECONFIG, []byte(kubeconfig), 0o644)
}

// The all-in-one container needs a moment to come up; retry until the apiserver
// answers. kubectl proxy (what the container fronts port 8080/8888 with) doesn't
// forward /healthz at the root, so probe a real resource list instead.
func kwokWaitForApiServer() error {
	var lastErr error
	for i := 0; i < 30; i++ {
		if _, lastErr = kubectlOutput("--kubeconfig", KWOK_KUBECONFIG, "get", "--raw", "/api/v1/namespaces"); lastErr == nil {
			return nil
		}
		time.Sleep(time.Second)
	}
	return fmt.Errorf("kwok apiserver did not become ready: %w", lastErr)
}

// buildFakeNode constructs a single fake v1.Node matching _local/kwok/nodes.yaml's shape,
// with name/hostname parameterized by index.
func buildFakeNode(index int) *v1.Node {
	name := fmt.Sprintf("kwok-node-%d", index)
	return &v1.Node{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "Node",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Annotations: map[string]string{
				"node.alpha.kubernetes.io/ttl": "0",
				"kwok.x-k8s.io/node":           "fake",
			},
			Labels: map[string]string{
				"kubernetes.io/hostname": name,
				"kubernetes.io/os":       "linux",
				"type":                   "kwok",
			},
		},
		Spec: v1.NodeSpec{
			Taints: []v1.Taint{
				{
					Key:    "kwok.x-k8s.io/node",
					Value:  "fake",
					Effect: v1.TaintEffectNoSchedule,
				},
			},
		},
		Status: v1.NodeStatus{
			Allocatable: v1.ResourceList{
				v1.ResourceCPU:              resource.MustParse("8"),
				v1.ResourceMemory:           resource.MustParse("128Gi"),
				v1.ResourceEphemeralStorage: resource.MustParse("256Gi"),
				v1.ResourcePods:             resource.MustParse("110"),
			},
			Capacity: v1.ResourceList{
				v1.ResourceCPU:              resource.MustParse("8"),
				v1.ResourceMemory:           resource.MustParse("128Gi"),
				v1.ResourceEphemeralStorage: resource.MustParse("256Gi"),
				v1.ResourcePods:             resource.MustParse("110"),
			},
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

// kwokApplyNodes expands the single-node seed (_local/kwok/nodes.yaml) to
// KWOK_NODE_COUNT nodes and applies them, mirroring kindSetup()'s apply step.
func kwokApplyNodes() error {
	var docs []string
	for i := 0; i < KWOK_NODE_COUNT; i++ {
		out, err := yaml.Marshal(buildFakeNode(i))
		if err != nil {
			return fmt.Errorf("error marshaling node %d: %w", i, err)
		}
		docs = append(docs, string(out))
	}
	content := strings.Join(docs, "---\n")

	f, err := os.CreateTemp("", "kwok-nodes-*.yaml")
	if err != nil {
		return fmt.Errorf("error creating temporary file: %w", err)
	}
	defer os.Remove(f.Name())
	if _, err := f.WriteString(content); err != nil {
		return fmt.Errorf("error writing temporary file: %w", err)
	}
	if err := f.Close(); err != nil {
		return err
	}

	return kubectlRun("--kubeconfig", KWOK_KUBECONFIG, "apply", "-f", f.Name())
}

// kwokApplyManifests applies the static, kind-analogous resources (namespace + RBAC for
// no-auth job submission, priority classes) that mage kind gets from kindSetup(). Unlike
// nodes, these don't need per-run generation, so they're committed manifests applied as-is.
// Stage resources (stages.yaml) aren't here - Stage isn't a CRD in the apiserver, it's a
// kwok-controller config construct loaded via -c at container-create time in kwokInitCluster.
func kwokApplyManifests() error {
	resources := []string{
		"_local/kwok/priorityclasses.yaml",
		"_local/kwok/namespace.yaml",
	}
	for _, f := range resources {
		if err := kubectlRun("--kubeconfig", KWOK_KUBECONFIG, "apply", "-f", f); err != nil {
			return err
		}
	}
	return nil
}

// kubectl wait --for=condition=ready node --all is unreliable across many
// objects that are already Ready before the watch attaches (it only reliably
// catches a live transition, not pre-existing state) — poll instead.
func kwokWaitUntilReady() error {
	var lastErr error
	for i := 0; i < 60; i++ {
		out, err := kubectlOutput("--kubeconfig", KWOK_KUBECONFIG, "get", "nodes", "-o", "json")
		if err != nil {
			lastErr = err
			time.Sleep(time.Second)
			continue
		}

		var nodeList v1.NodeList
		if err := json.Unmarshal([]byte(out), &nodeList); err != nil {
			return fmt.Errorf("error parsing node list: %w", err)
		}
		if len(nodeList.Items) == 0 {
			lastErr = fmt.Errorf("no nodes found")
			time.Sleep(time.Second)
			continue
		}

		allReady := true
		for _, node := range nodeList.Items {
			if !isNodeReady(&node) {
				allReady = false
				break
			}
		}
		if allReady {
			return nil
		}
		lastErr = fmt.Errorf("not all %d nodes are ready yet", len(nodeList.Items))
		time.Sleep(time.Second)
	}
	return fmt.Errorf("timed out waiting for nodes to become ready: %w", lastErr)
}

func isNodeReady(node *v1.Node) bool {
	for _, cond := range node.Status.Conditions {
		if cond.Type == v1.NodeReady {
			return cond.Status == v1.ConditionTrue
		}
	}
	return false
}

// No persistent volume in the all-in-one image, so removing the container is a
// complete teardown — no separate "delete cluster state" step needed.
func kwokTeardown() error {
	return dockerRun("rm", "-f", KWOK_CONTAINER_NAME)
}
