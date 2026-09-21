// Package render generates the mage/goreman-facing config text (kind-cluster YAML, executor
// config YAML, Procfile) that N cluster-type execution targets need to run, driven entirely by
// an already-loaded Scenario. It only ever writes text - it never shells out to kind/kubectl
// itself, and never becomes a cluster-lifecycle owner. mage/goreman remain the sole actors that
// turn this output into running clusters/processes (see cmd/regatta/README.md).
package render

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"text/template"

	"github.com/armadaproject/armada/internal/regatta/config"
)

const (
	// basePort/baseMetricPort/baseClusterId match today's checked-in
	// cmd/regatta/config/armada/executor/config-regatta.yaml (target index 0). Each subsequent
	// cluster target offsets by portStep, mirroring config-regatta-2.yaml (target index 1).
	basePort       = 8082
	baseMetricPort = 9002
	portStep       = 10
)

var (
	kindConfigTmpl = template.Must(template.New("kind").Parse(`kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
name: {{ .KindClusterName }}
featureGates:
  "KubeletInUserNamespace": true
networking:
  apiServerAddress: 0.0.0.0
nodes:
- role: control-plane
  image: kindest/node:v1.33.12
`))

	executorConfigTmpl = template.Must(template.New("executor").Parse(`httpPort: {{ .HttpPort }}
executorApiConnection:
  armadaUrl: "localhost:50052"
  forceNoTls: true
observability:
  enabled: true
  exporter:
    endpoint: "http://localhost:4318"
    protocol: "http/protobuf"
  traces:
    sampler: "parent_based_trace_id_ratio"
    samplerArg: 1.0
metric:
  port: {{ .MetricPort }}
kubernetes:
  toleratedTaints:
    - node-tests
    - kwok.x-k8s.io/node
  trackedNodeLabels:
    - armadaproject.io/node-pool
    - kwok.x-k8s.io/node
    - armadaproject.io/regatta-target
application:
  clusterId: "{{ .ClusterId }}"
  pool: "default"
  errorCategories:
    enabled: true
    defaultCategory: uncategorized
    categories:
      # The action Delete makes the executor remove the failed pod. A retry
      # then reuses the name of the pod. The retry/ testcases need this.
      - name: oom
        action: Delete
        rules:
          - onConditions: ["OOMKilled"]
      - name: user_error
        action: Delete
        rules:
          - onExitCodes:
              operator: In
              values: [1, 2, 126, 127]
`))
)

// Target holds one cluster-type execution target's resolved render inputs.
type Target struct {
	Name            string
	Index           int
	KindClusterName string
	Kubeconfig      string
	HttpPort        int
	MetricPort      int
	ClusterId       string
}

// Plan is the full set of artifacts Render will write for a scenario's cluster targets.
type Plan struct {
	Targets []Target

	KindDir      string
	ExecutorDir  string
	ProcfilePath string
}

const (
	KindDir      = "cmd/regatta/config/armada/kind/.tmp"
	ExecutorDir  = "cmd/regatta/config/armada/executor/.tmp"
	ProcfileDir  = "cmd/regatta/config/armada/procfiles/.tmp"
	ProcfileName = "regatta.Procfile"
)

// BuildPlan derives one Target per cluster-type ExecutionTarget in scenario, in file order.
// KindClusterName defaults to the target's Name if the scenario left ClusterTarget.Name blank, so
// a scenario author isn't forced to restate the target name a second time.
func BuildPlan(scenario *config.Scenario) (*Plan, error) {
	plan := &Plan{
		KindDir:      KindDir,
		ExecutorDir:  ExecutorDir,
		ProcfilePath: filepath.Join(ProcfileDir, ProcfileName),
	}

	index := 0
	for _, target := range scenario.ExecutionTargets {
		if target.Type != config.TargetTypeCluster {
			continue
		}
		if target.Cluster.Kubeconfig == "" {
			return nil, fmt.Errorf("target %q: cluster.kubeconfig is required to render", target.Name)
		}

		kindClusterName := target.Cluster.Name
		if kindClusterName == "" {
			kindClusterName = target.Name
		}

		clusterId := "local-cluster"
		if index > 0 {
			clusterId = fmt.Sprintf("local-cluster-%d", index+1)
		}

		plan.Targets = append(plan.Targets, Target{
			Name:            target.Name,
			Index:           index,
			KindClusterName: kindClusterName,
			Kubeconfig:      target.Cluster.Kubeconfig,
			HttpPort:        basePort + index*portStep,
			MetricPort:      baseMetricPort + index*portStep,
			ClusterId:       clusterId,
		})
		index++
	}

	if len(plan.Targets) == 0 {
		return nil, fmt.Errorf("scenario has no %q targets to render", config.TargetTypeCluster)
	}

	return plan, nil
}

// executorConfigName returns the filename for a target's rendered executor config, e.g.
// "config-gpu-cluster.yaml".
func executorConfigName(target Target) string {
	return fmt.Sprintf("config-%s.yaml", target.Name)
}

// kindConfigName returns the filename for a target's rendered kind-cluster config, e.g.
// "gpu-cluster.yaml".
func kindConfigName(target Target) string {
	return fmt.Sprintf("%s.yaml", target.Name)
}

// executorBinaryName returns the goreman process/binary name for a target's index, matching
// today's checked-in Procfile convention: index 0 is "executor", every later index appends
// "N+1" ("executor2", "executor3", ...).
func executorBinaryName(index int) string {
	if index == 0 {
		return "executor"
	}
	return fmt.Sprintf("executor%d", index+1)
}

// Write renders every artifact in plan to disk, creating plan.KindDir/ExecutorDir/ProcfileDir as
// needed. Existing contents of those directories are overwritten, not merged - render is meant
// to be re-run whenever the source scenario file changes.
func Write(plan *Plan) error {
	if err := os.MkdirAll(plan.KindDir, 0o755); err != nil {
		return fmt.Errorf("creating %s: %w", plan.KindDir, err)
	}
	if err := os.MkdirAll(plan.ExecutorDir, 0o755); err != nil {
		return fmt.Errorf("creating %s: %w", plan.ExecutorDir, err)
	}
	if err := os.MkdirAll(filepath.Dir(plan.ProcfilePath), 0o755); err != nil {
		return fmt.Errorf("creating %s: %w", filepath.Dir(plan.ProcfilePath), err)
	}

	var procfileLines []string
	for _, target := range plan.Targets {
		kindPath := filepath.Join(plan.KindDir, kindConfigName(target))
		if err := renderToFile(kindPath, kindConfigTmpl, target); err != nil {
			return fmt.Errorf("target %q: rendering kind-cluster config: %w", target.Name, err)
		}

		executorConfigPath := filepath.Join(plan.ExecutorDir, executorConfigName(target))
		if err := renderToFile(executorConfigPath, executorConfigTmpl, target); err != nil {
			return fmt.Errorf("target %q: rendering executor config: %w", target.Name, err)
		}

		procfileLines = append(procfileLines, procfileLine(target, executorConfigPath))
	}

	procfileContent := strings.Join(procfileLines, "\n") + "\n"
	if err := os.WriteFile(plan.ProcfilePath, []byte(procfileContent), 0o644); err != nil {
		return fmt.Errorf("writing %s: %w", plan.ProcfilePath, err)
	}

	return nil
}

// procfileLine builds one goreman Procfile line for target, matching the shape of today's
// checked-in regatta.Procfile executor/executor2 lines (export-scoped KUBECONFIG, distinct
// binary output path per index, pointed at the target's rendered executor config).
func procfileLine(target Target, executorConfigPath string) string {
	binary := executorBinaryName(target.Index)
	return fmt.Sprintf(
		`%s: export KUBECONFIG=%s && ${GO_BIN:-go} build -gcflags="all=-N -l" -o ./dist/armada-%s ./cmd/executor/main.go && ./dist/armada-%s --config ./%s`,
		binary, target.Kubeconfig, binary, binary, executorConfigPath,
	)
}

func renderToFile(path string, tmpl *template.Template, data any) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("creating %s: %w", path, err)
	}
	defer f.Close()
	return tmpl.Execute(f, data)
}
