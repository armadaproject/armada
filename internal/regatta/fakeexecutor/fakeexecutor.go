// Package fakeexecutor manages the armada-fakeexecutor process for a regatta run: it renders a
// config file from a shared node-profile plus regatta-file settings, starts the process
// (`go run ./cmd/fakeexecutor`, mirroring how the rest of the dev toolchain runs regatta itself),
// and tears it down on context cancellation. Unlike KWOK, no Kubernetes API is involved at all -
// armada-fakeexecutor registers as a real executor against the Armada scheduler over gRPC and
// simulates nodes/pods entirely in-process.
package fakeexecutor

import (
	"fmt"
	"os"
	"os/exec"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"sigs.k8s.io/yaml"

	"github.com/armadaproject/armada/internal/executor/fake/context"
	"github.com/armadaproject/armada/internal/regatta/config"
	"github.com/armadaproject/armada/pkg/client"
)

// generatedConfig mirrors the subset of internal/executor/configuration.ExecutorConfiguration
// (plus the executor's own top-level "nodes" key) that regatta needs to set; the rest of
// ExecutorConfiguration's many fields are left to its own defaults.
type generatedConfig struct {
	HttpPort              int                   `json:"httpPort"`
	ExecutorApiConnection executorApiConnection `json:"executorApiConnection"`
	Application           application           `json:"application"`
	Metric                metric                `json:"metric"`
	Kubernetes            kubernetesConfig      `json:"kubernetes"`
	Nodes                 []*context.NodeSpec   `json:"nodes"`
}

// kubernetesConfig sets Kubernetes.ToleratedTaints so the executor treats its own simulated
// fake-executor-taint nodes as usable capacity - without this, node.KubernetesNodeInfoService
// (internal/executor/node/node_group.go) excludes any NoSchedule-tainted node from
// GetAvailableClusterCapacity entirely, and the fake-executor reports zero free resource no
// matter how many simulated nodes it has.
//
// TrackedNodeLabels must include every label a submission's nodeSelector might use: cluster_
// utilisation.go's filterTrackedLabels strips any node label not in this list before the
// executor ever reports the node to the scheduler, regardless of what the scheduler's own
// indexedNodeLabels says - an untracked label looks identical to an unset one from the
// scheduler's point of view.
type kubernetesConfig struct {
	ToleratedTaints   []string `json:"toleratedTaints"`
	TrackedNodeLabels []string `json:"trackedNodeLabels"`
	NodeIdLabel       string   `json:"nodeIdLabel"`
}

type executorApiConnection struct {
	ArmadaUrl  string `json:"armadaUrl"`
	ForceNoTls bool   `json:"forceNoTls"`
}

type application struct {
	ClusterId string `json:"clusterId"`
	Pool      string `json:"pool"`
}

type metric struct {
	Port int `json:"port"`
}

// NodeAnnotation/NodeAnnotationOK mirror kwok.NodeAnnotation/NodeAnnotationOK: a taint always
// added to every simulated node on top of whatever the profile itself specifies, so a submission
// can target "any fake-executor node" via a single, stable toleration regardless of which
// node-profile file it came from.
const (
	NodeAnnotation   = "armadaproject.io/fake-executor"
	NodeAnnotationOK = "true"
)

// nodeIdLabel mirrors the executor config default (config/executor/config.yaml's
// kubernetes.nodeIdLabel) so it can be included in the generated trackedNodeLabels list without
// hardcoding the string twice.
const nodeIdLabel = "kubernetes.io/hostname"

// basePort/baseMetricPort plus a per-target index give each simultaneous fake-executor target
// its own HTTP/metrics ports, avoiding a bind collision when more than one is started at once.
const (
	basePort       = 8083
	baseMetricPort = 9101
)

// prebuiltBinary is where `mage build` (or an equivalent build step) places the compiled
// armada-fakeexecutor binary. Preferred over `go run` when present, since `go run` execs the
// compiled binary as a child of its own wrapper process - the wrapper's PID isn't the PID
// actually bound to the fake-executor's ports, which makes it useless for the caller to report.
const prebuiltBinary = "dist/armada-fakeexecutor"

// Process is a running armada-fakeexecutor subprocess.
type Process struct {
	cmd        *exec.Cmd
	configPath string
}

// PID returns the OS process ID of the running armada-fakeexecutor. If the prebuilt binary
// wasn't available and Start fell back to `go run`, this is the `go run` wrapper's PID, not the
// compiled binary's - `go run` execs the binary as a child rather than replacing itself.
func (p *Process) PID() int {
	return p.cmd.Process.Pid
}

// Start renders a config file from nodeGroup and target, and launches armada-fakeexecutor
// against it. Prefers the prebuilt dist/armada-fakeexecutor binary; falls back to
// `go run ./cmd/fakeexecutor` (assuming the process's own working directory is the repo root,
// i.e. `go run ./cmd/regatta run ...`) if that binary hasn't been built yet. The process's
// stdout/stderr are streamed to this process's own.
func Start(apiConnectionDetails *client.ApiConnectionDetails, nodeGroup []config.ResolvedNodeGroupMember, target config.FakeExecutorTarget, index int) (*Process, error) {
	configPath, err := writeConfig(apiConnectionDetails, nodeGroup, target, index)
	if err != nil {
		return nil, fmt.Errorf("writing fake-executor config: %w", err)
	}

	var cmd *exec.Cmd
	if _, err := os.Stat(prebuiltBinary); err == nil {
		cmd = exec.Command(prebuiltBinary, "--config", configPath)
	} else {
		cmd = exec.Command("go", "run", "./cmd/fakeexecutor", "--config", configPath)
	}
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		os.Remove(configPath)
		return nil, fmt.Errorf("starting armada-fakeexecutor: %w", err)
	}

	return &Process{cmd: cmd, configPath: configPath}, nil
}

// Stop terminates the armada-fakeexecutor process and removes its generated config file. Safe
// to call on a nil *Process.
func (p *Process) Stop() error {
	if p == nil {
		return nil
	}
	defer os.Remove(p.configPath)

	if p.cmd.Process == nil {
		return nil
	}
	if err := p.cmd.Process.Kill(); err != nil {
		return fmt.Errorf("stopping armada-fakeexecutor: %w", err)
	}
	_ = p.cmd.Wait()
	return nil
}

func writeConfig(apiConnectionDetails *client.ApiConnectionDetails, nodeGroup []config.ResolvedNodeGroupMember, target config.FakeExecutorTarget, index int) (string, error) {
	trackedLabels := map[string]bool{nodeIdLabel: true, NodeAnnotation: true}

	nodes := make([]*context.NodeSpec, 0, len(nodeGroup))
	for _, member := range nodeGroup {
		allocatable := make(map[v1.ResourceName]resource.Quantity, len(member.Profile.Allocatable))
		for name, qty := range member.Profile.Allocatable {
			allocatable[name] = qty
		}

		labels := map[string]string{
			NodeAnnotation: NodeAnnotationOK,
		}
		for k, v := range member.Profile.Labels {
			labels[k] = v
			trackedLabels[k] = true
		}

		taints := append([]v1.Taint{}, member.Profile.Taints...)
		taints = append(taints, v1.Taint{
			Key:    NodeAnnotation,
			Value:  NodeAnnotationOK,
			Effect: v1.TaintEffectNoSchedule,
		})

		nodes = append(nodes, &context.NodeSpec{
			Name:        member.Profile.Name,
			Count:       member.Count,
			Labels:      labels,
			Taints:      taints,
			Allocatable: allocatable,
		})
	}

	trackedNodeLabels := make([]string, 0, len(trackedLabels))
	for label := range trackedLabels {
		trackedNodeLabels = append(trackedNodeLabels, label)
	}

	cfg := generatedConfig{
		HttpPort: basePort + index,
		ExecutorApiConnection: executorApiConnection{
			ArmadaUrl:  target.SchedulerUrl,
			ForceNoTls: apiConnectionDetails.ForceNoTls,
		},
		Application: application{
			ClusterId: target.ClusterId,
			Pool:      target.Pool,
		},
		Metric: metric{Port: baseMetricPort + index},
		Kubernetes: kubernetesConfig{
			ToleratedTaints:   []string{NodeAnnotation},
			TrackedNodeLabels: trackedNodeLabels,
			NodeIdLabel:       nodeIdLabel,
		},
		Nodes: nodes,
	}

	data, err := yaml.Marshal(cfg)
	if err != nil {
		return "", fmt.Errorf("marshalling config: %w", err)
	}

	f, err := os.CreateTemp("", "regatta-fakeexecutor-config-*.yaml")
	if err != nil {
		return "", fmt.Errorf("creating fake-executor config file: %w", err)
	}
	defer f.Close()
	if _, err := f.Write(data); err != nil {
		return "", fmt.Errorf("writing %s: %w", f.Name(), err)
	}
	return f.Name(), nil
}
