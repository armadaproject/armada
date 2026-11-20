package replay

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"slices"
	"time"

	"github.com/spf13/viper"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"sigs.k8s.io/yaml"

	commonconfig "github.com/armadaproject/armada/internal/common/config"
	"github.com/armadaproject/armada/internal/scheduler/configuration"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/pkg/api"
)

const maxScenarioInputBytes = 4 << 20

type scenario struct {
	Version       int         `json:"version"`
	CandidateID   string      `json:"candidate_id"`
	Workload      string      `json:"workload"`
	Farm          string      `json:"farm"`
	Config        string      `json:"scheduler_config"`
	Seed          int64       `json:"seed"`
	Window        Window      `json:"window"`
	BucketUS      int64       `json:"bucket_us"`
	ScratchBytes  int64       `json:"scratch_bytes"`
	AcceptBudgets bool        `json:"acknowledge_unbounded_rounds"`
	Queues        []queueSpec `json:"queues"`
}

type queueSpec struct {
	Name     string                                      `json:"name"`
	Weight   float64                                     `json:"weight"`
	Cordoned bool                                        `json:"cordoned,omitempty"`
	Limits   map[string]*api.PriorityClassResourceLimits `json:"priority_class_limits,omitempty"`
}

type farmSpec struct {
	SchemaVersion int           `json:"schemaVersion"`
	Clusters      []clusterSpec `json:"clusters"`
}

type clusterSpec struct {
	Name          string         `json:"name"`
	Pool          string         `json:"pool"`
	NodeTemplates []nodeTemplate `json:"nodeTemplates"`
}

type nodeTemplate struct {
	Number         int64             `json:"number"`
	Labels         map[string]string `json:"labels"`
	Taints         []*v1.Taint       `json:"taints"`
	TotalResources struct {
		Resources map[string]string `json:"resources"`
	} `json:"totalResources"`
}

type runManifest struct {
	ResultVersion       int               `json:"result_version"`
	Status              string            `json:"status"`
	Failure             string            `json:"failure,omitempty"`
	Scenario            *scenario         `json:"scenario,omitempty"`
	Workload            *workloadManifest `json:"workload,omitempty"`
	InputSHA256         map[string]string `json:"input_sha256"`
	OutputSHA256        map[string]string `json:"output_sha256,omitempty"`
	Build               *debug.BuildInfo  `json:"build,omitempty"`
	BinarySHA256        string            `json:"binary_sha256,omitempty"`
	SourcePatchSHA256   string            `json:"source_patch_sha256,omitempty"`
	GOOS                string            `json:"goos"`
	GOARCH              string            `json:"goarch"`
	Profile             string            `json:"profile"`
	Model               string            `json:"model"`
	SourceConfig        string            `json:"source_scheduler_config,omitempty"`
	EffectiveSoftUS     int64             `json:"effective_soft_round_us"`
	EffectivePerQueueUS int64             `json:"effective_soft_queue_us"`
	HardRoundUS         int64             `json:"effective_hard_round_us"`
	SourceBudgetsUS     map[string]int64  `json:"source_round_budgets_us,omitempty"`
	StartedAt           time.Time         `json:"host_started_at"`
	WallUS              int64             `json:"host_wall_us"`
	CPUUS               *int64            `json:"host_cpu_us,omitempty"`
	PeakRSSBytes        *uint64           `json:"host_peak_rss_bytes,omitempty"`
	Warnings            []string          `json:"warnings"`
}

// RunScenario runs the local recorded replay command. It claims a new output
// directory and publishes complete status only after every output is closed
// and fingerprinted. An optional source patch identifies a dirty build.
func RunScenario(ctx context.Context, scenarioPath, outputDir, sourcePatch string) (result error) {
	started := time.Now()
	startCPU, _ := hostUsage()
	manifest := runManifest{
		ResultVersion: 1, Status: "incomplete", StartedAt: started.UTC(),
		InputSHA256: make(map[string]string), GOOS: runtime.GOOS, GOARCH: runtime.GOARCH,
		Profile: "synchronous-static-farm-unbounded-rounds-v1", Model: "fixed-useful-work-v1",
		Warnings: []string{
			"Round duration budgets are disabled. Parent cancellation and host deadlines remain active.",
			"Runs start with empty scheduler state. Executor acknowledgment is immediate. Startup occupies the allocated pod.",
			"Resource-time accounts for requests and modeled progress.",
		},
	}
	raw, err := readLocal(scenarioPath)
	if err != nil {
		return fmt.Errorf("scenario: %w", err)
	}
	var sc scenario
	if err := strictJSON(raw, &sc); err != nil {
		return fmt.Errorf("scenario: %w", err)
	}
	if sc.Version != 1 || sc.CandidateID == "" || sc.BucketUS <= 0 ||
		sc.BucketUS > maxDurationUS || sc.ScratchBytes <= 0 || !sc.AcceptBudgets {
		return errors.New("scenario requires version 1, candidate ID, positive bucket and scratch sizes, and acknowledge_unbounded_rounds=true")
	}
	if err := sc.Window.validate(); err != nil {
		return err
	}
	if sc.Window.WarmupUS < -maxDurationUS || sc.Window.EndUS > maxDurationUS {
		return errors.New("replay window exceeds Armada nanosecond timestamp range")
	}
	base := filepath.Dir(scenarioPath)
	resolve := func(path string) string {
		if filepath.IsAbs(path) {
			return path
		}
		return filepath.Join(base, path)
	}
	for _, path := range []string{sc.Workload, sc.Farm, sc.Config} {
		if path == "" {
			return errors.New("scenario requires workload, farm and scheduler_config paths")
		}
	}
	workloadDir := resolve(sc.Workload)
	if err := checkOutputPath(workloadDir, outputDir); err != nil {
		return err
	}
	if err := os.Mkdir(outputDir, 0o755); err != nil {
		return fmt.Errorf("create a new replay output directory: %w", err)
	}
	manifest.Scenario = &sc
	manifest.InputSHA256["scenario"] = fmt.Sprintf("%x", sha256.Sum256(raw))
	if err := writeRunManifest(outputDir, manifest); err != nil {
		return err
	}
	finished := false
	defer func() {
		manifest.WallUS = time.Since(started).Microseconds()
		if cpu, peak := hostUsage(); peak != 0 {
			elapsed := cpu - startCPU
			manifest.CPUUS, manifest.PeakRSSBytes = &elapsed, &peak
		}
		if finished && result == nil {
			manifest.Status = "complete"
		} else if result != nil {
			manifest.Failure = result.Error()
		} else {
			manifest.Failure = "run terminated before successful finalization"
		}
		result = errors.Join(result, writeRunManifest(outputDir, manifest))
	}()
	manifest.Build, _ = debug.ReadBuildInfo()
	executable, err := os.Executable()
	if err != nil {
		return err
	}
	manifest.BinarySHA256, err = hashOutput(ctx, executable)
	if err != nil {
		return fmt.Errorf("hash executable: %w", err)
	}
	if sourcePatch != "" {
		manifest.SourcePatchSHA256, err = hashOutput(ctx, sourcePatch)
		if err != nil {
			return fmt.Errorf("hash supplied source patch: %w", err)
		}
	} else if manifest.Build != nil && slices.Contains(manifest.Build.Settings, debug.BuildSetting{Key: "vcs.modified", Value: "true"}) {
		manifest.Warnings = append(manifest.Warnings, "Binary build reports a dirty tree. No source patch was supplied.")
	}
	w, err := openWorkload(workloadDir)
	if err != nil {
		return fmt.Errorf("open workload: %w", err)
	}
	defer func() { result = errors.Join(result, w.root.Close()) }()
	manifest.Workload = &w.manifest
	manifest.InputSHA256["workload_manifest"] = w.hash

	config, configData, err := readSchedulerConfig(resolve(sc.Config))
	if err != nil {
		return fmt.Errorf("read scheduler config: %w", err)
	}
	manifest.InputSHA256["scheduler_config"] = fmt.Sprintf("%x", sha256.Sum256(configData))
	manifest.SourceConfig = string(configData)
	manifest.SourceBudgetsUS = map[string]int64{
		"hard":           config.MaxSchedulingDuration.Microseconds(),
		"soft":           config.MaxNewJobSchedulingDuration.Microseconds(),
		"soft_per_queue": config.MaxNewJobSchedulingDurationPerQueue.Microseconds(),
	}
	executors, capacity, farmHash, err := readFarm(resolve(sc.Farm), config)
	if err != nil {
		return fmt.Errorf("read farm: %w", err)
	}
	manifest.InputSHA256["farm"] = farmHash
	queues := make([]*api.Queue, 0, len(sc.Queues))
	for _, q := range sc.Queues {
		factor := 1 / q.Weight
		if math.IsNaN(factor) || math.IsInf(factor, 0) || factor <= 0 {
			return fmt.Errorf("queue %q weight must have a positive finite inverse", q.Name)
		}
		queues = append(queues, &api.Queue{
			Name: q.Name, PriorityFactor: factor, Cordoned: q.Cordoned, ResourceLimitsByPriorityClassName: q.Limits,
		})
	}
	scheduler, err := NewProductionScheduler(ctx, sc.Window.WarmupUS, config, executors, queues)
	if err != nil {
		return err
	}
	if err := w.validate(ctx, outputDir, sc.ScratchBytes, func(job Job) error {
		_, err := scheduler.makeJob(job)
		return err
	}); err != nil {
		return fmt.Errorf("validate workload: %w", err)
	}

	scratch, err := os.MkdirTemp(outputDir, ".replay-series-")
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, os.RemoveAll(scratch)) }()
	spool, err := openSeriesSpool(filepath.Join(scratch, "observations"), sc.ScratchBytes)
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, spool.file.Close()) }()
	artifacts, err := openArtifacts(outputDir, sc.Window, w.manifest.WorkEstimationMethod, spool)
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, artifacts.logical.close(), artifacts.attempts.close()) }()
	stream := workloadStream{ctx: ctx, input: w}
	defer func() { result = errors.Join(result, stream.close()) }()
	stats, err := Run(ctx, sc.Window, stream.next, scheduler, artifacts)
	if err != nil {
		return err
	}
	if err := artifacts.finish(ctx, sc.BucketUS, stats, capacity); err != nil {
		return err
	}
	manifest.OutputSHA256 = make(map[string]string)
	for _, name := range []string{logicalFile, attemptFile, seriesFile, summaryFile} {
		digest, err := hashOutput(ctx, filepath.Join(outputDir, name))
		if err != nil {
			return fmt.Errorf("checksum %s: %w", name, err)
		}
		manifest.OutputSHA256[name] = digest
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	finished = true
	return nil
}

func checkOutputPath(workload, output string) error {
	source, err := filepath.EvalSymlinks(workload)
	if err != nil {
		return fmt.Errorf("resolve workload directory: %w", err)
	}
	parent, err := filepath.EvalSymlinks(filepath.Dir(output))
	if err != nil {
		return fmt.Errorf("resolve output parent: %w", err)
	}
	source, err = filepath.Abs(source)
	if err != nil {
		return err
	}
	target, err := filepath.Abs(filepath.Join(parent, filepath.Base(output)))
	if err != nil {
		return err
	}
	relative, err := filepath.Rel(source, target)
	if err != nil {
		return err
	}
	if filepath.IsLocal(relative) {
		return errors.New("replay output directory must be outside the input workload directory")
	}
	return nil
}

func readLocal(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	raw, err := readBounded(f, maxScenarioInputBytes)
	return raw, errors.Join(err, f.Close())
}

func readSchedulerConfig(path string) (configuration.SchedulingConfig, []byte, error) {
	var config configuration.SchedulingConfig
	raw, err := readLocal(path)
	if err != nil {
		return config, nil, err
	}
	if _, err := yaml.YAMLToJSONStrict(raw); err != nil {
		return config, nil, err
	}
	v := viper.NewWithOptions(viper.KeyDelimiter("::"))
	v.SetConfigType("yaml")
	if err := v.ReadConfig(bytes.NewReader(raw)); err != nil {
		return config, nil, err
	}
	if err := v.UnmarshalExact(&config, commonconfig.CustomHooks...); err != nil {
		return config, nil, err
	}
	return config, raw, nil
}

func readFarm(path string, cfg configuration.SchedulingConfig) ([]*schedulerobjects.Executor, map[string]v1.ResourceList, string, error) {
	raw, err := readLocal(path)
	if err != nil {
		return nil, nil, "", err
	}
	var farm farmSpec
	if err := yaml.UnmarshalStrict(raw, &farm); err != nil {
		return nil, nil, "", err
	}
	if farm.SchemaVersion != 1 || farm.Clusters == nil {
		return nil, nil, "", errors.New("expected farm schemaVersion 1 with explicit clusters")
	}
	var executors []*schedulerobjects.Executor
	capacity := make(map[string]v1.ResourceList)
	for _, cluster := range farm.Clusters {
		executor := &schedulerobjects.Executor{Id: cluster.Name, Pool: cluster.Pool}
		if capacity[cluster.Pool] == nil {
			capacity[cluster.Pool] = make(v1.ResourceList)
		}
		for templateIndex, template := range cluster.NodeTemplates {
			if template.Number <= 0 || template.Number > 100_000-int64(len(executor.Nodes)) || len(template.TotalResources.Resources) == 0 {
				return nil, nil, "", fmt.Errorf("cluster %q template %d has invalid number or missing resources", cluster.Name, templateIndex+1)
			}
			resources := make(map[string]*resource.Quantity, len(template.TotalResources.Resources))
			for name, text := range template.TotalResources.Resources {
				quantity, err := exactQuantity(text)
				if err != nil {
					return nil, nil, "", fmt.Errorf("cluster %q template %d resource %q: %w", cluster.Name, templateIndex+1, name, err)
				}
				resources[name] = &quantity
			}
			for range template.Number {
				id := fmt.Sprintf("%s-%d-%d", cluster.Name, templateIndex, len(executor.Nodes))
				labels := maps.Clone(template.Labels)
				if labels == nil {
					labels = make(map[string]string)
				}
				labels["armadaproject.io/clusterName"] = cluster.Name
				labels[cfg.NodeIdLabel] = id
				for name, quantity := range resources {
					name := v1.ResourceName(name)
					total := capacity[cluster.Pool][name]
					total.Add(*quantity)
					capacity[cluster.Pool][name] = total
				}
				executor.Nodes = append(executor.Nodes, &schedulerobjects.Node{
					Id: id, Name: id, Executor: executor.Id, Pool: executor.Pool, Labels: labels,
					Taints: template.Taints, TotalResources: &schedulerobjects.ResourceList{Resources: resources},
				})
			}
		}
		executors = append(executors, executor)
	}
	return executors, capacity, fmt.Sprintf("%x", sha256.Sum256(raw)), nil
}

func hashBytes(r io.Reader) (string, error) {
	hash := sha256.New()
	if _, err := io.Copy(hash, r); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", hash.Sum(nil)), nil
}
