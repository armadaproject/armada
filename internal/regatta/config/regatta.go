// Package config defines the "scenario file" format: a manifest that points to the other files
// a benchmarking run needs (an .armadactl.yaml, kubeconfigs, node-profile files, job-spec files)
// and layers regatta-specific settings on top. It is deliberately not a load-test spec or a
// Broadside-style test-runner config - those embed everything inline; a scenario file mostly
// references other files.
package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/pkg/client/util"
)

const (
	TargetTypeCluster      = "cluster"
	TargetTypeFakeExecutor = "fake-executor"
)

// Scenario is the top-level manifest passed to `regatta run`.
type Scenario struct {
	// Armadactl is a path to an .armadactl.yaml file. Empty uses the same default resolution as
	// armadactl ($HOME/.armadactl.yaml).
	Armadactl string `json:"armadactl,omitempty"`

	ExecutionTargets []ExecutionTarget    `json:"executionTargets"`
	NodeGroups       map[string]NodeGroup `json:"nodeGroups,omitempty"`
	Load             Load                 `json:"load"`
}

// ExecutionTarget is a flat discriminated union keyed by Type ("cluster" or "fake-executor").
// Exactly one of Cluster/FakeExecutor is populated, matching Type.
type ExecutionTarget struct {
	Type string `json:"type"`

	// Name identifies this target for logging and per-target resource namespacing (kwok
	// controller container name/kubeconfig path, fake-executor ports). Optional: Load assigns
	// "<type>-<index>" (index counted per-type, file order) to any target left blank.
	Name string `json:"name,omitempty"`

	// NodeGroups names which top-level NodeGroups entries this target stands up. Omitted/nil
	// means ALL top-level NodeGroups apply. Referencing an unknown name is a Load-time error.
	NodeGroups []string `json:"nodeGroups,omitempty"`

	Cluster      *ClusterTarget      `json:"cluster,omitempty"`
	FakeExecutor *FakeExecutorTarget `json:"fakeExecutor,omitempty"`
}

// ClusterTarget (Type: "cluster") configures a KWOK fake-node target: real v1.Node objects
// created in a live cluster via KWOK, so Armada's real executor/scheduler path is exercised
// end-to-end against simulated hardware.
type ClusterTarget struct {
	Kubeconfig string `json:"kubeconfig"`

	// Name is a display label for the target's cluster, e.g. for logging. It is never used to
	// derive a kubeconfig context name - the target's own Kubeconfig file's current-context is
	// trusted directly. Defaults to the ExecutionTarget's own Name if left unset.
	Name string `json:"name,omitempty"`

	// InternalAPIServerAddress is the cluster's API server address as reachable from the
	// kwok-controller container's own network (e.g. a kind cluster's docker network), not the
	// host-facing address in Kubeconfig. Required for a kind-provisioned target; `regatta render`
	// auto-populates it. A hand-supplied non-kind cluster must set it directly.
	InternalAPIServerAddress string `json:"internalApiServerAddress,omitempty"`

	ProbeRetries int `json:"probeRetries,omitempty"`
	// ProbeDelay is a duration string (e.g. "5s"), parsed by Load into ProbeDelayDuration.
	ProbeDelay string `json:"probeDelay,omitempty"`

	// ProbeDelayDuration is ProbeDelay parsed by Load. Not part of the file format.
	ProbeDelayDuration time.Duration `json:"-"`

	// EvaluateReadiness controls whether a canary job is submitted to confirm the fake nodes are
	// actually schedulable before load is submitted (see kwok.WaitUntilSchedulable). Left unset,
	// it defaults to true when Name is set (a kind-provisioned target: the environment is fully
	// known/controlled, so the probe is meaningful and cheap) and false otherwise (an unknown,
	// externally-provided cluster is assumed already schedulable rather than probed).
	EvaluateReadiness *bool `json:"evaluateReadiness,omitempty"`
}

// ShouldEvaluateReadiness reports whether a canary-job readiness probe should run for this
// target, applying EvaluateReadiness's kind-provisioned-target default when unset.
func (c *ClusterTarget) ShouldEvaluateReadiness() bool {
	if c.EvaluateReadiness != nil {
		return *c.EvaluateReadiness
	}
	return c.Name != ""
}

// FakeExecutorTarget (Type: "fake-executor") configures an armada-fakeexecutor process that
// registers as a real executor against the scheduler but simulates nodes/pods in-process, no
// real cluster involved.
type FakeExecutorTarget struct {
	// SchedulerUrl is the armada-scheduler's executor-facing gRPC address (host:port), e.g.
	// "localhost:50052". This is deliberately separate from Armadactl's armadaUrl - that's the
	// submit-side API (armada-server, typically port 50051), a different service on a different
	// port than the scheduler's ExecutorApi that armada-fakeexecutor actually leases jobs from.
	SchedulerUrl string `json:"schedulerUrl"`
	Pool         string `json:"pool,omitempty"`
	ClusterId    string `json:"clusterId,omitempty"`
}

// Load describes the submission batch: which job-spec files to submit, how many of each, and
// how submission is paced. Queue/JobSetId/Mode live here (submission-batch concerns, matching
// Armada's own JobSubmitRequest shape) - not on the job-spec files, which stay plain PodSpecs.
type Load struct {
	Queue     string `json:"queue"`
	JobSetId  string `json:"jobSetId,omitempty"`
	Namespace string `json:"namespace,omitempty"`

	// Mode: "" or "one-shot" (default) or "ramp-up".
	Mode string `json:"mode,omitempty"`

	// Jobs lists job-spec files and how many of each to submit, mirroring how NodeGroups
	// reference node-profile files by path + count.
	Jobs []JobRef `json:"jobs"`

	RampUp *RampUpConfig `json:"rampUp,omitempty"`
}

const (
	LoadModeOneShot = "one-shot"
	LoadModeRampUp  = "ramp-up"
)

// JobRef points at a plain-PodSpec job-spec file and says how many jobs of that shape to submit.
type JobRef struct {
	JobSpec string `json:"jobSpec"`
	Count   int    `json:"count"`

	// ResolvedSpec is the loaded PodSpec, set by Load. Not part of the file format.
	ResolvedSpec *v1.PodSpec `json:"-"`
}

// RampUpConfig spreads the total Jobs[].Count across RampDuration in batches every
// StepInterval.
type RampUpConfig struct {
	RampDuration string `json:"rampDuration"`
	StepInterval string `json:"stepInterval,omitempty"`

	RampDurationParsed time.Duration `json:"-"`
	StepIntervalParsed time.Duration `json:"-"`
}

const defaultStepInterval = 5 * time.Second

// LoadScenario reads a Scenario from path and resolves every path field it contains (Armadactl,
// each target's Kubeconfig, each NodeGroup member's NodeProfile, each Load.Jobs[].JobSpec)
// relative to the scenario file's own directory, so a scenario file's relative paths behave the
// same regardless of the caller's working directory. It also validates the file and assigns
// auto-generated names to any ExecutionTarget left unnamed.
func LoadScenario(path string) (*Scenario, error) {
	scenario := &Scenario{}
	if err := util.BindJsonOrYaml(path, scenario); err != nil {
		return nil, err
	}

	dir := filepath.Dir(path)
	scenario.Armadactl = resolveRelative(dir, scenario.Armadactl)

	for name, group := range scenario.NodeGroups {
		resolveNodeGroup(dir, group)
		scenario.NodeGroups[name] = group
	}

	if len(scenario.ExecutionTargets) == 0 {
		return nil, fmt.Errorf("executionTargets must contain at least one target")
	}

	typeCounters := map[string]int{}
	names := map[string]bool{}
	var sharedType string
	for i := range scenario.ExecutionTargets {
		target := &scenario.ExecutionTargets[i]

		switch target.Type {
		case TargetTypeCluster:
			if target.Cluster == nil {
				return nil, fmt.Errorf("executionTargets[%d]: type is %q but cluster is not set", i, target.Type)
			}
			if target.FakeExecutor != nil {
				return nil, fmt.Errorf("executionTargets[%d]: type is %q but fakeExecutor is also set", i, target.Type)
			}
			target.Cluster.Kubeconfig = resolveRelative(dir, target.Cluster.Kubeconfig)
			if target.Cluster.ProbeDelay != "" {
				delay, err := time.ParseDuration(target.Cluster.ProbeDelay)
				if err != nil {
					return nil, fmt.Errorf("executionTargets[%d]: parsing cluster.probeDelay %q: %w", i, target.Cluster.ProbeDelay, err)
				}
				target.Cluster.ProbeDelayDuration = delay
			}
		case TargetTypeFakeExecutor:
			if target.FakeExecutor == nil {
				return nil, fmt.Errorf("executionTargets[%d]: type is %q but fakeExecutor is not set", i, target.Type)
			}
			if target.Cluster != nil {
				return nil, fmt.Errorf("executionTargets[%d]: type is %q but cluster is also set", i, target.Type)
			}
			if target.FakeExecutor.SchedulerUrl == "" {
				return nil, fmt.Errorf("executionTargets[%d]: fakeExecutor.schedulerUrl is required", i)
			}
		default:
			return nil, fmt.Errorf("executionTargets[%d]: unknown type %q, must be %q or %q", i, target.Type, TargetTypeCluster, TargetTypeFakeExecutor)
		}

		if sharedType == "" {
			sharedType = target.Type
		} else if target.Type != sharedType {
			return nil, fmt.Errorf("executionTargets[%d]: type %q cannot be mixed with type %q in the same scenario: cluster and fake-executor targets are mutually exclusive", i, target.Type, sharedType)
		}

		for _, groupName := range target.NodeGroups {
			if _, ok := scenario.NodeGroups[groupName]; !ok {
				return nil, fmt.Errorf("executionTargets[%d]: nodeGroups references unknown group %q", i, groupName)
			}
		}

		if target.Name == "" {
			target.Name = fmt.Sprintf("%s-%d", target.Type, typeCounters[target.Type])
		}
		typeCounters[target.Type]++

		if names[target.Name] {
			return nil, fmt.Errorf("executionTargets[%d]: duplicate name %q", i, target.Name)
		}
		names[target.Name] = true
	}

	if len(scenario.Load.Jobs) == 0 {
		return nil, fmt.Errorf("load.jobs must contain at least one entry")
	}
	for i := range scenario.Load.Jobs {
		job := &scenario.Load.Jobs[i]
		job.JobSpec = resolveRelative(dir, job.JobSpec)
		spec, err := loadPodSpec(job.JobSpec)
		if err != nil {
			return nil, fmt.Errorf("load.jobs[%d]: %w", i, err)
		}
		job.ResolvedSpec = spec
	}

	switch scenario.Load.Mode {
	case "", LoadModeOneShot:
	case LoadModeRampUp:
		if scenario.Load.RampUp == nil {
			return nil, fmt.Errorf("load.rampUp is required when load.mode is %q", LoadModeRampUp)
		}
		rampDuration, err := time.ParseDuration(scenario.Load.RampUp.RampDuration)
		if err != nil {
			return nil, fmt.Errorf("parsing load.rampUp.rampDuration %q: %w", scenario.Load.RampUp.RampDuration, err)
		}
		scenario.Load.RampUp.RampDurationParsed = rampDuration

		stepInterval := defaultStepInterval
		if scenario.Load.RampUp.StepInterval != "" {
			stepInterval, err = time.ParseDuration(scenario.Load.RampUp.StepInterval)
			if err != nil {
				return nil, fmt.Errorf("parsing load.rampUp.stepInterval %q: %w", scenario.Load.RampUp.StepInterval, err)
			}
		}
		scenario.Load.RampUp.StepIntervalParsed = stepInterval
	default:
		return nil, fmt.Errorf("load.mode must be %q or %q, got %q", LoadModeOneShot, LoadModeRampUp, scenario.Load.Mode)
	}

	return scenario, nil
}

func loadPodSpec(path string) (*v1.PodSpec, error) {
	spec := &v1.PodSpec{}
	if err := util.BindJsonOrYaml(path, spec); err != nil {
		return nil, fmt.Errorf("loading job spec %s: %w", path, err)
	}
	return spec, nil
}

func resolveNodeGroup(dir string, group NodeGroup) {
	for i := range group {
		group[i].NodeProfile = resolveRelative(dir, group[i].NodeProfile)
	}
}

func resolveRelative(dir, path string) string {
	if path == "" {
		return path
	}
	if strings.HasPrefix(path, "~/") {
		home, err := os.UserHomeDir()
		if err == nil {
			return filepath.Join(home, path[2:])
		}
	}
	if filepath.IsAbs(path) {
		return path
	}
	return filepath.Join(dir, path)
}
