package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"
)

const (
	KWOK_CONTROLLER_IMAGE   = "registry.k8s.io/kwok/kwok:v0.7.0"
	KWOK_CONTROLLER_NAME    = "armada-kwok-controller"
	KWOK_CONTEXT            = "kind-" + KIND_NAME
	KWOK_NODE_ANNOTATION    = "kwok.x-k8s.io/node"
	KWOK_NODE_ANNOTATION_OK = "fake"

	// One GB200 NVL72 rack (72 GPU / 2,592 vCPU / 17,280 GiB) = 18 p6e-gb200.36xlarge slices.
	NODES_PER_GB200_RACK = 18

	// 80 racks. Sized so node capacity isn't the concurrency ceiling; override with KWOK_NODE_COUNT.
	KWOK_NODE_COUNT_DEFAULT = 80 * NODES_PER_GB200_RACK
)

// NodeProfile describes one simulated node shape, modeled on a real hardware SKU so node
// count maps to a real deployable unit. Multiple shapes can coexist on one cluster.
type NodeProfile struct {
	Name         string // e.g. "gb200-slice", "generic"
	CPU          string
	Memory       string
	GPUCount     int    // advertised as nvidia.com/gpu allocatable/capacity when > 0
	InstanceType string // node.kubernetes.io/instance-type label value
}

// One AWS p6e-gb200.36xlarge, i.e. 1/18th of an NVL72 rack.
var gb200Slice = NodeProfile{
	Name:         "gb200-slice",
	CPU:          "144",
	Memory:       "960Gi",
	GPUCount:     4,
	InstanceType: "p6e-gb200.36xlarge",
}

// KWOK_NODE_COUNT defaults to the local dev/CI tier (KWOK_NODE_COUNT_DEFAULT) and can be
// overridden with the KWOK_NODE_COUNT env var, without needing a code change to switch tiers.
var KWOK_NODE_COUNT = kwokNodeCountFromEnv()

func kwokNodeCountFromEnv() int {
	if v := os.Getenv("KWOK_NODE_COUNT"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			panic(fmt.Sprintf("invalid KWOK_NODE_COUNT %q: %s", v, err))
		}
		return n
	}
	return KWOK_NODE_COUNT_DEFAULT
}

func kwokCheck() error {
	return dockerCheck()
}

// kwokApplyStageCRD installs the Stage CRD (stages.kwok.x-k8s.io) into kind's cluster.
// Out-of-cluster mode needs this as a real CRD - unlike the all-in-one image, which reads
// Stage definitions from a local -c stages.yaml config file at container-create time.
func kwokApplyStageCRD() error {
	crdPath, err := filepath.Abs("_local/kwok/stage-crd.yaml")
	if err != nil {
		return err
	}
	return kubectlRun("apply", "-f", crdPath, "--context", KWOK_CONTEXT)
}

// kwokApplyStages applies the Stage resources (node-heartbeat plus the Pod-kind stage set
// carried over from the all-in-one image's stages.yaml - see that file's header comment on
// why the Pod-kind set must stay complete) as real objects in kind's cluster.
func kwokApplyStages() error {
	stagesPath, err := filepath.Abs("_local/kwok/stages.yaml")
	if err != nil {
		return err
	}
	return kubectlRun("apply", "-f", stagesPath, "--context", KWOK_CONTEXT)
}

// kwokControllerRun starts the standalone kwok-controller against kind's own kubeconfig,
// restricted to nodes carrying the kwok.x-k8s.io/node=fake annotation so kind's real nodes
// are never touched. Idempotent: no-op if already running.
// https://kwok.sigs.k8s.io/docs/user/kwok-out-cluster/
func kwokControllerRun() error {
	out, err := dockerOutput("ps", "--filter", "name=^/"+KWOK_CONTROLLER_NAME+"$", "--format", "{{.Names}}")
	if err != nil {
		return err
	}
	if strings.TrimSpace(out) == KWOK_CONTROLLER_NAME {
		return nil
	}

	kubeconfigPath, err := filepath.Abs(KIND_CONFIG_INTERNAL)
	if err != nil {
		return err
	}

	return dockerRun(
		"run", "--rm", "-d",
		"--name", KWOK_CONTROLLER_NAME,
		"--network", "kind",
		"-v", kubeconfigPath+":/kubeconfig:ro",
		KWOK_CONTROLLER_IMAGE,
		"--kubeconfig=/kubeconfig",
		"--manage-all-nodes=false",
		"--manage-nodes-with-annotation-selector="+KWOK_NODE_ANNOTATION+"="+KWOK_NODE_ANNOTATION_OK,
		// Without this, kwok-controller ignores our applied Stage CRD objects entirely and
		// falls back to its built-in pod stages (which is why pod-ready still worked - that's
		// the built-in default, not our stages.yaml object). Our custom pod-complete-armada
		// stage has no built-in equivalent, so without --enable-crds=Stage it never fires and
		// every pod sits in Running forever.
		"--enable-crds=Stage",
	)
}

// kwokControllerTeardown stops the standalone kwok-controller container. Fake nodes are real
// objects in kind's own etcd (not disposable with the container), so this doesn't remove
// them - see kwokDeleteFakeNodes.
func kwokControllerTeardown() error {
	return dockerRun("rm", "-f", KWOK_CONTROLLER_NAME)
}

// kwokDeleteFakeNodes removes the fake v1.Node objects from kind's cluster, leaving kind's
// real node(s) untouched.
func kwokDeleteFakeNodes() error {
	return kubectlRun(
		"delete", "nodes",
		"-l", KWOK_NODE_ANNOTATION+"="+KWOK_NODE_ANNOTATION_OK,
		"--context", KWOK_CONTEXT,
		"--ignore-not-found",
	)
}

// buildFakeNode constructs a single fake v1.Node shaped by profile, with name/hostname
// parameterized by index so multiple profiles/counts can coexist on one cluster.
func buildFakeNode(profile NodeProfile, index int) *v1.Node {
	name := fmt.Sprintf("kwok-node-%s-%d", profile.Name, index)

	// Called twice below so Allocatable and Capacity get separate maps rather than aliasing one.
	resources := func() v1.ResourceList {
		rl := v1.ResourceList{
			v1.ResourceCPU:              resource.MustParse(profile.CPU),
			v1.ResourceMemory:           resource.MustParse(profile.Memory),
			v1.ResourceEphemeralStorage: resource.MustParse("256Gi"),
			v1.ResourcePods:             resource.MustParse("3000"),
		}
		// Omitted rather than set to 0 when absent: nvidia.com/gpu is an indexedResource
		// (config/scheduler/config.yaml), so an explicit zero would index CPU-only nodes.
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
				KWOK_NODE_ANNOTATION:           KWOK_NODE_ANNOTATION_OK,
			},
			Labels: map[string]string{
				"kubernetes.io/hostname":           name,
				"kubernetes.io/os":                 "linux",
				"type":                             "kwok",
				"node.kubernetes.io/instance-type": profile.InstanceType,
				// kwok-controller's --manage-nodes-with-annotation-selector matches on the
				// annotation above; this label is what lets our own kubectl -l queries
				// (kwokWaitUntilReady, kwokDeleteFakeNodes) target only fake nodes.
				KWOK_NODE_ANNOTATION: KWOK_NODE_ANNOTATION_OK,
			},
		},
		Spec: v1.NodeSpec{
			Taints: []v1.Taint{
				{
					Key:    KWOK_NODE_ANNOTATION,
					Value:  KWOK_NODE_ANNOTATION_OK,
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

// kwokApplyFakeNodes expands the default node profile to KWOK_NODE_COUNT nodes and applies
// them to kind's own cluster (kind's real node(s) are untouched, fake nodes are additional
// objects, not a replacement).
func kwokApplyFakeNodes() error {
	var docs []string
	for i := 0; i < KWOK_NODE_COUNT; i++ {
		out, err := yaml.Marshal(buildFakeNode(gb200Slice, i))
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

	return kubectlRun("apply", "-f", f.Name(), "--context", KWOK_CONTEXT)
}

// kubectl wait --for=condition=ready node --all is unreliable across many
// objects that are already Ready before the watch attaches (it only reliably
// catches a live transition, not pre-existing state), poll instead. Only waits on
// fake-annotated nodes: kind's own real node(s) are already Ready long before this runs.
func kwokWaitUntilReady() error {
	var lastErr error
	for i := 0; i < 60; i++ {
		out, err := kubectlOutput(
			"get", "nodes",
			"-l", KWOK_NODE_ANNOTATION+"="+KWOK_NODE_ANNOTATION_OK,
			"--context", KWOK_CONTEXT,
			"-o", "json",
		)
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
			lastErr = fmt.Errorf("no fake nodes found")
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
		lastErr = fmt.Errorf("not all %d fake nodes are ready yet", len(nodeList.Items))
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
