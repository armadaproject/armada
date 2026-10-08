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

	log "github.com/armadaproject/armada/internal/common/logging"
	"github.com/armadaproject/armada/internal/regatta/metrics"
	"github.com/armadaproject/armada/pkg/client/util"
)

const TargetTypeCluster = "cluster"

// TargetLabel is the node label that names the execution target a fake node belongs to. It is what a queue's
// targets are matched against and what the readiness canary can select on.
const TargetLabel = "armadaproject.io/regatta-target"

// Scenario is the top-level manifest passed to `regatta run`.
type Scenario struct {
	// Armadactl is a path to an .armadactl.yaml file. Empty uses the same default resolution as
	// armadactl ($HOME/.armadactl.yaml).
	Armadactl string `json:"armadactl,omitempty"`

	// AuthContext names the context, within the .armadactl.yaml file above, that selects which
	// Armada instance (and credentials) this scenario runs against - the same contexts
	// `armadactl config get-contexts` lists. Empty uses that file's own currentContext. The
	// --context flag on `regatta run`, when passed, takes precedence over this field.
	AuthContext string `json:"authContext,omitempty"`

	// Metrics configures the post-run Prometheus metrics report - see MetricsConfig.
	Metrics MetricsConfig `json:"metrics,omitempty"`

	ExecutionTargets []ExecutionTarget    `json:"executionTargets"`
	NodeGroups       map[string]NodeGroup `json:"nodeGroups,omitempty"`
	Load             Load                 `json:"load"`
}

// MetricsConfig configures the post-run Prometheus metrics report (see internal/regatta/metrics).
type MetricsConfig struct {
	// Prometheus is the base URL of a running Prometheus server to query for a post-run metrics
	// report. Empty defaults to http://localhost:9090 (Prometheus's own default, and the address
	// `mage dev:up ...,prometheus` exposes it at) - see Scenario.PrometheusURL.
	Prometheus string `json:"prometheus,omitempty"`

	// PostRunDelay is how long `regatta run` waits after the run's queue drains (see
	// metrics.WaitForQueueDrain) before collecting the metrics report, so Prometheus's own scrape
	// interval has time to catch up to the drain moment. Duration string (e.g. "90s"); empty
	// defaults to metrics.DefaultSettleDelay (90s).
	PostRunDelay string `json:"postRunDelay,omitempty"`

	// ResultsPath is the directory to write the post-run metrics report JSON into (the filename
	// itself is generated, e.g. regatta-result-20060102-150405.json - see cmd/regatta/cmd/run.go).
	// Resolved by LoadScenario relative to the scenario file's own directory, matching Armadactl/
	// Kubeconfig/JobSpec/NodeProfile. The --metrics-results-path CLI flag overrides this when
	// explicitly passed. Empty defaults to "." - see Scenario.MetricsResultsDir.
	ResultsPath string `json:"resultsPath,omitempty"`

	// PostRunDelayDuration is PostRunDelay parsed by LoadScenario. Not part of the file format.
	PostRunDelayDuration time.Duration `json:"-"`

	// ReportInterval is how often a continuous run writes a report: each one covers the time since the
	// previous one. Duration string (e.g. "120s"); empty defaults to DefaultReportInterval. Bounded runs
	// write a single report at the end and ignore it.
	ReportInterval string `json:"reportInterval,omitempty"`

	// ReportIntervalDuration is ReportInterval parsed by LoadScenario. Not part of the file format.
	ReportIntervalDuration time.Duration `json:"-"`
}

// PrometheusURL reports the Prometheus base URL to query, applying Prometheus's default-address
// fallback when the scenario leaves it unset.
func (s *Scenario) PrometheusURL() string {
	if s.Metrics.Prometheus != "" {
		return s.Metrics.Prometheus
	}
	return "http://localhost:9090"
}

// MetricsResultsDir reports the directory to write the post-run metrics report into, applying
// the "." fallback when the scenario leaves it unset.
func (s *Scenario) MetricsResultsDir() string {
	if s.Metrics.ResultsPath != "" {
		return s.Metrics.ResultsPath
	}
	return "."
}

// ExecutionTarget configures one "cluster" target (Type is currently always "cluster").
type ExecutionTarget struct {
	Type string `json:"type"`

	// Name identifies this target for logging and per-target resource namespacing (kwok
	// controller container name/kubeconfig path). Optional: Load assigns "<type>-<index>" (index
	// counted per-type, file order) to any target left blank.
	Name string `json:"name,omitempty"`

	// NodeGroups names which top-level NodeGroups entries this target stands up. Omitted/nil
	// means ALL top-level NodeGroups apply. Referencing an unknown name is a Load-time error.
	NodeGroups []string `json:"nodeGroups,omitempty"`

	Cluster *ClusterTarget `json:"cluster,omitempty"`
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

	// Kind marks this target as a kind-provisioned cluster (created via `mage kind:multiCluster`).
	// A kind cluster's API server isn't reachable at its host-facing Kubeconfig address from the
	// kwok-controller's own docker container, so Kind additionally opts into two kind-only
	// behaviors: InternalAPIServerAddress is auto-derived from Name (kind's own internal-DNS
	// convention, https://<name>-control-plane:6443) when left unset, and the kwok-controller
	// container joins the "kind" docker network to reach it. A real cluster (e.g. EKS) reached
	// over a normal network needs neither: InternalAPIServerAddress falls back to the address
	// already in Kubeconfig, and the controller container uses docker's default network. Defaults
	// to false.
	Kind bool `json:"kind,omitempty"`

	// InternalAPIServerAddress is the cluster's API server address as reachable from the
	// kwok-controller container. Left unset, it's auto-derived: from Name using kind's own
	// internal-DNS convention when Kind is true, otherwise from Kubeconfig's own server address
	// (see Kind's doc comment). Set this explicitly to override either default - e.g. a
	// non-default-network remote cluster, or a kind cluster reached some other way.
	InternalAPIServerAddress string `json:"internalApiServerAddress,omitempty"`

	// ReadinessRetries is how many canary-job attempts the readiness check makes before giving up.
	// Defaults to DefaultReadinessRetries when left unset (zero).
	ReadinessRetries int `json:"readinessRetries,omitempty"`
	// ReadinessDelay is a duration string (e.g. "5s"): how long the readiness check's first attempt waits for its
	// canary job to start running. Each retry doubles it, so the defaults wait about 2.5 minutes in
	// total. Parsed by Load into ReadinessDelayDuration. Defaults to DefaultReadinessDelay when left unset.
	ReadinessDelay string `json:"readinessDelay,omitempty"`

	// ReadinessDelayDuration is ReadinessDelay parsed by Load. Not part of the file format.
	ReadinessDelayDuration time.Duration `json:"-"`

	// EvaluateReadiness controls whether a canary job is submitted to confirm the fake nodes are
	// actually schedulable before load is submitted (see kwok.WaitUntilSchedulable). The scheduler
	// takes a while to learn about freshly created nodes, so without the readiness check load starts before
	// it can place anything and the first stretch of the run measures that warm-up. Left unset it
	// defaults to true for every target; set it to false to skip the wait.
	EvaluateReadiness *bool `json:"evaluateReadiness,omitempty"`

	// ContinueOnReadinessFailure lets the run carry on when the readiness check gives up, instead
	// of failing setup. For environments that are known to be faulty, where the test is still
	// wanted: the failure is logged as a warning when it happens and again at the end of the run,
	// and recorded under readinessFailures in the metrics report. The fake nodes are kept. Only a
	// failed check is tolerated - not a failure to create the nodes - and it does nothing when
	// EvaluateReadiness is false. Defaults to false.
	ContinueOnReadinessFailure bool `json:"continueOnReadinessFailure,omitempty"`

	// ReadinessSelectsTarget makes the readiness canary also select on this target's
	// armadaproject.io/regatta-target node label, so it can only land on this target's own fake
	// nodes. That matters when several targets share one Armada, and it requires the executors to
	// report that label (trackedNodeLabels). Left unset it defaults to Kind: the kind quickstart's
	// executors are configured for it, while an external cluster's executors may not be, and there
	// the canary selects on the KWOK annotation alone.
	ReadinessSelectsTarget *bool `json:"readinessSelectsTarget,omitempty"`

	// Kubernetes configures this target's Kubernetes API client, notably its request rate limit -
	// see KubernetesClientConfiguration. Left unset, every field defaults as documented there.
	Kubernetes KubernetesClientConfiguration `json:"kubernetes,omitempty"`

	// NodeConcurrency caps how many fake-node create/delete calls are in flight at once for this
	// target. Defaults to 50 when left unset (zero).
	NodeConcurrency int `json:"nodeConcurrency,omitempty"`

	// ReadyTimeout is how long to wait for this target's fake nodes to report Ready before giving
	// up. Duration string (e.g. "5m"); empty defaults to 5 minutes.
	ReadyTimeout string `json:"readyTimeout,omitempty"`

	// ReadyTimeoutDuration is ReadyTimeout parsed by LoadScenario. Not part of the file format.
	ReadyTimeoutDuration time.Duration `json:"-"`

	// StagesPath overrides the kwok Stage-set YAML applied to this target (see kwok.ApplyStages),
	// resolved relative to the scenario file like every other path field. Left unset, kwok's
	// embedded default stage set is used. This is a deliberate extension point for varying pod
	// lifecycle timing/behavior per scenario (e.g. a slower-completing GPU-job stage) - not just a
	// path convenience, since a custom set can also be used to model stuck/never-completing pods
	// and observe how Armada reacts to them, so kwok only warns (never errors) if a custom set
	// looks incomplete against the required Pod-kind transitions - see kwok.ApplyStages.
	StagesPath string `json:"stagesPath,omitempty"`
}

// KubernetesClientConfiguration controls the rate limit regatta's own Kubernetes client applies
// against this target's API server - notably when creating/deleting the fake Node objects
// themselves (see kwok.ApplyFakeNodes/DeleteFakeNodes), which fan hundreds of requests out
// concurrently and will bottleneck on client-go's conservative built-in defaults otherwise.
// Mirrors the same QPS/Burst knobs internal/executor and internal/binoculars already expose for
// their own Kubernetes clients.
type KubernetesClientConfiguration struct {
	// QPS is the max steady-state number of Kubernetes API requests per second. Defaults to 100
	// when left unset (zero) - well above client-go's own default of 5, which meaningfully
	// throttles a several-hundred-node target.
	QPS float32 `json:"qps,omitempty"`
	// Burst is the max number of requests allowed to burst above QPS momentarily. Defaults to
	// 200 when left unset (zero) - client-go's own default is 10.
	Burst int `json:"burst,omitempty"`
}

// Readiness check defaults, used when a target leaves readinessRetries/readinessDelay unset. With the
// check doubling its delay each retry they wait 5+10+20+40+80s = 155s in total.
const (
	DefaultReadinessRetries = 5
	DefaultReadinessDelay   = 5 * time.Second
)

// ShouldEvaluateReadiness reports whether the canary-job readiness check should run for this
// target: always, unless EvaluateReadiness is explicitly false.
func (c *ClusterTarget) ShouldEvaluateReadiness() bool {
	if c.EvaluateReadiness != nil {
		return *c.EvaluateReadiness
	}
	return true
}

// ShouldReadinessSelectTarget reports whether the readiness canary should also select on this
// target's regatta-target node label, applying ReadinessSelectsTarget's Kind default when unset.
func (c *ClusterTarget) ShouldReadinessSelectTarget() bool {
	if c.ReadinessSelectsTarget != nil {
		return *c.ReadinessSelectsTarget
	}
	return c.Kind
}

// EffectiveReadinessRetries returns ReadinessRetries, or DefaultReadinessRetries when unset.
func (c *ClusterTarget) EffectiveReadinessRetries() int {
	if c.ReadinessRetries > 0 {
		return c.ReadinessRetries
	}
	return DefaultReadinessRetries
}

// EffectiveReadinessDelay returns the parsed ReadinessDelay, or DefaultReadinessDelay when unset.
func (c *ClusterTarget) EffectiveReadinessDelay() time.Duration {
	if c.ReadinessDelayDuration > 0 {
		return c.ReadinessDelayDuration
	}
	return DefaultReadinessDelay
}

// defaultReadyTimeout has headroom above what even a large (300+ node) target needs on an idle
// machine, since several targets' worth of concurrent node creates/kwok-controller reconciliation
// compete for the same host CPU when multiple execution targets are set up at once (see
// orchestrate.Setup) - observed flakiness right around a 60s value at 10 concurrent targets was
// host contention, not a stuck controller.
const defaultReadyTimeout = 5 * time.Minute

// DefaultReportInterval is how often a continuous run writes a report when metrics.reportInterval is unset.
const DefaultReportInterval = 120 * time.Second

// DefaultNodeConcurrency caps how many fake-node create/delete calls are in flight at once -
// plenty to turn hundreds of nodes from a multi-second sequential slog into a sub-second burst,
// without hammering the API server harder than KubernetesClientConfiguration's QPS/Burst allow.
// Applies whenever NodeConcurrency is left unset (zero) - see ClusterTarget.EffectiveNodeConcurrency.
const DefaultNodeConcurrency = 50

// EffectiveNodeConcurrency reports NodeConcurrency, applying DefaultNodeConcurrency when unset.
func (c *ClusterTarget) EffectiveNodeConcurrency() int {
	if c.NodeConcurrency != 0 {
		return c.NodeConcurrency
	}
	return DefaultNodeConcurrency
}

// LoadScenario reads a Scenario from path and resolves every path field it contains (Armadactl,
// each target's Kubeconfig, each NodeGroup member's NodeProfile, each queue's job-spec files)
// relative to the scenario file's own directory, so a scenario file's relative paths behave the
// same regardless of the caller's working directory. It also validates the file and assigns
// auto-generated names to any ExecutionTarget left unnamed.
func LoadScenario(path string) (*Scenario, error) {
	return loadScenario(path, true)
}

// LoadScenarioTargets reads a scenario like LoadScenario but stops after the execution targets: the load section
// is not expanded, so the job-spec files it names are not opened. It is for commands that only need the targets,
// such as teardown, which must work after the job templates have been moved or deleted.
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
				return nil, fmt.Errorf("queue %q: targets lists %q, but that target's executors are not set up to report the %s node label (cluster.readinessSelectsTarget is false); set it to true once they track the label, or drop the queue's targets", queue.Name, name, TargetLabel)
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
