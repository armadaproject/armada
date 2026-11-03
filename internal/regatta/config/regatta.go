// Package config defines the scenario file: a manifest that points to the other files a run needs
// (an .armadactl.yaml, kubeconfigs, node profiles, job specs) and adds regatta's own settings.
package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"

	log "github.com/armadaproject/armada/internal/common/logging"
	"github.com/armadaproject/armada/internal/regatta/metrics"
	"github.com/armadaproject/armada/pkg/client/util"
)

const TargetTypeCluster = "cluster"

// TargetLabel is the node label naming the execution target a fake node belongs to. Queue targets match on it.
const TargetLabel = "armadaproject.io/regatta-target"

// Scenario is the top-level manifest passed to `regatta run`.
type Scenario struct {
	Armadactl   string        `json:"armadactl,omitempty"`   // empty: $HOME/.armadactl.yaml
	AuthContext string        `json:"authContext,omitempty"` // empty: the armadactl file's currentContext; --context wins
	Metrics     MetricsConfig `json:"metrics,omitempty"`

	ExecutionTargets []ExecutionTarget    `json:"executionTargets"`
	NodeGroups       map[string]NodeGroup `json:"nodeGroups,omitempty"`
	Load             Load                 `json:"load"`
}

// MetricsConfig configures the metrics report (see internal/regatta/metrics).
type MetricsConfig struct {
	Prometheus   string `json:"prometheus,omitempty"`   // empty: http://localhost:9090
	PostRunDelay string `json:"postRunDelay,omitempty"` // wait after the drain for Prometheus to scrape; empty: metrics.DefaultSettleDelay
	ResultsPath  string `json:"resultsPath,omitempty"`  // relative to the scenario file; --metrics-results-path wins; empty: "."

	// ReportInterval is how often a continuous run writes a report; bounded runs ignore it. Empty: DefaultReportInterval.
	ReportInterval string `json:"reportInterval,omitempty"`

	// Parsed from PostRunDelay and ReportInterval by LoadScenario.
	PostRunDelayDuration   time.Duration `json:"-"`
	ReportIntervalDuration time.Duration `json:"-"`
}

// PrometheusURL is the Prometheus base URL to query.
func (s *Scenario) PrometheusURL() string {
	if s.Metrics.Prometheus != "" {
		return s.Metrics.Prometheus
	}
	return "http://localhost:9090"
}

// MetricsResultsDir is the directory the metrics report is written to.
func (s *Scenario) MetricsResultsDir() string {
	if s.Metrics.ResultsPath != "" {
		return s.Metrics.ResultsPath
	}
	return "."
}

// ExecutionTarget configures one "cluster" target.
type ExecutionTarget struct {
	Type       string         `json:"type"`
	Name       string         `json:"name,omitempty"`       // empty: "<type>-<index>"
	NodeGroups []string       `json:"nodeGroups,omitempty"` // empty: every top-level node group
	Cluster    *ClusterTarget `json:"cluster,omitempty"`
}

// ClusterTarget is a cluster that gets KWOK fake nodes: real v1.Node objects, so Armada's executor and scheduler are
// exercised against simulated hardware.
type ClusterTarget struct {
	Kubeconfig string `json:"kubeconfig"`
	Name       string `json:"name,omitempty"` // display label, and the kind cluster's name; empty: the target's name

	// Kind marks a kind cluster. The kwok-controller container cannot reach its API server at the kubeconfig's
	// address, so it joins the "kind" docker network and InternalAPIServerAddress defaults to
	// https://<Name>-control-plane:6443. Other clusters default to the kubeconfig's own address.
	Kind                     bool   `json:"kind,omitempty"`
	InternalAPIServerAddress string `json:"internalApiServerAddress,omitempty"`

	ReadinessRetries  int    `json:"readinessRetries,omitempty"`  // empty: DefaultReadinessRetries
	ReadinessDelay    string `json:"readinessDelay,omitempty"`    // first attempt's wait, doubled on each retry; empty: DefaultReadinessDelay
	EvaluateReadiness *bool  `json:"evaluateReadiness,omitempty"` // empty: true

	// ContinueOnReadinessFailure carries on when the readiness check gives up. Only that failure is tolerated, not a
	// failure to create the nodes. It is reported again at the end of the run and under readinessFailures in the report.
	ContinueOnReadinessFailure bool `json:"continueOnReadinessFailure,omitempty"`

	// ReadinessSelectsTarget pins the canary, and the load of queues with targets, to this target's own fake nodes by
	// TargetLabel. The target's executors must report the label (trackedNodeLabels). Empty: the value of Kind.
	ReadinessSelectsTarget *bool `json:"readinessSelectsTarget,omitempty"`

	Kubernetes      KubernetesClientConfiguration `json:"kubernetes,omitempty"`
	NodeConcurrency int                           `json:"nodeConcurrency,omitempty"` // empty: DefaultNodeConcurrency
	ReadyTimeout    string                        `json:"readyTimeout,omitempty"`    // empty: defaultReadyTimeout

	// StagesPath replaces kwok's default stage set (pod lifecycle timing and behaviour). A set missing the required
	// pod transitions only warns, so a scenario can model stuck pods.
	StagesPath string `json:"stagesPath,omitempty"`

	// Parsed from ReadinessDelay and ReadyTimeout by LoadScenario.
	ReadinessDelayDuration time.Duration `json:"-"`
	ReadyTimeoutDuration   time.Duration `json:"-"`
}

// KubernetesClientConfiguration is the rate limit of regatta's own Kubernetes client, which creates and deletes
// hundreds of fake nodes at once.
type KubernetesClientConfiguration struct {
	QPS   float32 `json:"qps,omitempty"`   // empty: 100
	Burst int     `json:"burst,omitempty"` // empty: 200
}

// Defaults for readinessRetries and readinessDelay: with the delay doubling, 5+10+20+40+80s = 155s in all.
const (
	DefaultReadinessRetries = 5
	DefaultReadinessDelay   = 5 * time.Second
)

func (c *ClusterTarget) ShouldEvaluateReadiness() bool {
	if c.EvaluateReadiness != nil {
		return *c.EvaluateReadiness
	}
	return true
}

func (c *ClusterTarget) ShouldReadinessSelectTarget() bool {
	if c.ReadinessSelectsTarget != nil {
		return *c.ReadinessSelectsTarget
	}
	return c.Kind
}

func (c *ClusterTarget) EffectiveReadinessRetries() int {
	if c.ReadinessRetries > 0 {
		return c.ReadinessRetries
	}
	return DefaultReadinessRetries
}

func (c *ClusterTarget) EffectiveReadinessDelay() time.Duration {
	if c.ReadinessDelayDuration > 0 {
		return c.ReadinessDelayDuration
	}
	return DefaultReadinessDelay
}

// defaultReadyTimeout leaves room for several targets creating nodes at once to compete for the same host CPU.
const defaultReadyTimeout = 5 * time.Minute

const DefaultReportInterval = 120 * time.Second

const DefaultNodeConcurrency = 50

func (c *ClusterTarget) EffectiveNodeConcurrency() int {
	if c.NodeConcurrency != 0 {
		return c.NodeConcurrency
	}
	return DefaultNodeConcurrency
}

// LoadScenario reads and validates a scenario. Every path in it is resolved relative to the scenario file.
func LoadScenario(path string) (*Scenario, error) {
	return loadScenario(path, true)
}

// LoadScenarioTargets is LoadScenario without the load section, so the job-spec files are not opened. Teardown uses
// it: it has to work after the job templates are gone.
func LoadScenarioTargets(path string) (*Scenario, error) {
	return loadScenario(path, false)
}

func loadScenario(path string, withLoad bool) (*Scenario, error) {
	scenario := &Scenario{}
	if err := util.BindJsonOrYaml(path, scenario); err != nil {
		return nil, err
	}

	dir := filepath.Dir(path)
	scenario.Armadactl = resolveRelative(dir, scenario.Armadactl)
	scenario.Metrics.ResultsPath = resolveRelative(dir, scenario.Metrics.ResultsPath)

	if scenario.Metrics.PostRunDelay != "" {
		delay, err := time.ParseDuration(scenario.Metrics.PostRunDelay)
		if err != nil {
			return nil, fmt.Errorf("parsing metrics.postRunDelay %q: %w", scenario.Metrics.PostRunDelay, err)
		}
		scenario.Metrics.PostRunDelayDuration = delay
	} else {
		scenario.Metrics.PostRunDelayDuration = metrics.DefaultSettleDelay
	}

	if scenario.Metrics.ReportInterval != "" {
		interval, err := time.ParseDuration(scenario.Metrics.ReportInterval)
		if err != nil {
			return nil, fmt.Errorf("parsing metrics.reportInterval %q: %w", scenario.Metrics.ReportInterval, err)
		}
		if interval <= 0 {
			return nil, fmt.Errorf("metrics.reportInterval must be positive, got %q", scenario.Metrics.ReportInterval)
		}
		scenario.Metrics.ReportIntervalDuration = interval
	} else {
		scenario.Metrics.ReportIntervalDuration = DefaultReportInterval
	}

	for name, group := range scenario.NodeGroups {
		resolveNodeGroup(dir, group)
		scenario.NodeGroups[name] = group
	}

	if len(scenario.ExecutionTargets) == 0 {
		return nil, fmt.Errorf("executionTargets must contain at least one target")
	}

	names := map[string]bool{}
	clusters := map[string]string{} // kubeconfig path or cluster name -> the target that uses it
	for i := range scenario.ExecutionTargets {
		target := &scenario.ExecutionTargets[i]

		if target.Type != TargetTypeCluster {
			return nil, fmt.Errorf("executionTargets[%d]: unknown type %q, must be %q", i, target.Type, TargetTypeCluster)
		}
		if target.Cluster == nil {
			return nil, fmt.Errorf("executionTargets[%d]: type is %q but cluster is not set", i, target.Type)
		}
		target.Cluster.Kubeconfig = resolveRelative(dir, target.Cluster.Kubeconfig)
		target.Cluster.StagesPath = resolveRelative(dir, target.Cluster.StagesPath)
		if target.Cluster.ReadinessDelay != "" {
			delay, err := time.ParseDuration(target.Cluster.ReadinessDelay)
			if err != nil {
				return nil, fmt.Errorf("executionTargets[%d]: parsing cluster.readinessDelay %q: %w", i, target.Cluster.ReadinessDelay, err)
			}
			target.Cluster.ReadinessDelayDuration = delay
		}
		if !target.Cluster.ShouldEvaluateReadiness() && (target.Cluster.ReadinessRetries != 0 || target.Cluster.ReadinessDelay != "") {
			log.Warnf("executionTargets[%d]: cluster.readinessRetries/readinessDelay are set but cluster.evaluateReadiness is false, so the readiness check will not run", i)
		}
		if target.Cluster.ReadyTimeout != "" {
			timeout, err := time.ParseDuration(target.Cluster.ReadyTimeout)
			if err != nil {
				return nil, fmt.Errorf("executionTargets[%d]: parsing cluster.readyTimeout %q: %w", i, target.Cluster.ReadyTimeout, err)
			}
			target.Cluster.ReadyTimeoutDuration = timeout
		} else {
			target.Cluster.ReadyTimeoutDuration = defaultReadyTimeout
		}

		for _, groupName := range target.NodeGroups {
			if _, ok := scenario.NodeGroups[groupName]; !ok {
				return nil, fmt.Errorf("executionTargets[%d]: nodeGroups references unknown group %q", i, groupName)
			}
		}

		if target.Name == "" {
			target.Name = fmt.Sprintf("%s-%d", target.Type, i)
		}

		if names[target.Name] {
			return nil, fmt.Errorf("executionTargets[%d]: duplicate name %q", i, target.Name)
		}
		names[target.Name] = true

		if target.Cluster.Name == "" {
			target.Cluster.Name = target.Name
		}
		// Fake nodes are named after their profile and labelled with their target, so two targets sharing a
		// cluster would collide on node names and overwrite each other's labels.
		for _, identity := range []string{"kubeconfig " + filepath.Clean(target.Cluster.Kubeconfig), "cluster name " + target.Cluster.Name} {
			if other, shared := clusters[identity]; shared {
				return nil, fmt.Errorf("executionTargets[%d] (%q) uses the same cluster as target %q (same %s); give each target its own cluster", i, target.Name, other, identity)
			}
			clusters[identity] = target.Name
		}
	}

	if !withLoad {
		return scenario, nil
	}

	if err := scenario.Load.normalize(dir, names); err != nil {
		return nil, err
	}

	// A queue's targets are enforced by a node affinity on the target label, which the scheduler can only match
	// if the target's executors report that label (the same condition as cluster.readinessSelectsTarget).
	targetsByName := map[string]*ExecutionTarget{}
	for i := range scenario.ExecutionTargets {
		targetsByName[scenario.ExecutionTargets[i].Name] = &scenario.ExecutionTargets[i]
	}
	for _, queue := range scenario.Load.Queues {
		for _, name := range queue.Targets {
			if !targetsByName[name].Cluster.ShouldReadinessSelectTarget() {
				return nil, fmt.Errorf(
					"queue %q: targets lists %q, but that target's executors are not set up to report the %s node label "+
						"(cluster.readinessSelectsTarget is false); set it to true once they track the label, or drop the queue's targets",
					queue.Name, name, TargetLabel,
				)
			}
		}
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
