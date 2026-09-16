// Package config defines the "regatta file" format: a manifest that points to the other files
// a benchmarking run needs (an .armadactl.yaml, a kubeconfig, node-profile files, a submission
// spec) and layers regatta-specific settings on top. It is deliberately not a load-test spec or
// a Broadside-style test-runner config - those embed everything inline; a regatta file mostly
// references other files.
package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/armadaproject/armada/pkg/client/util"
)

// RegattaFile is the top-level manifest passed to `regatta run`.
type RegattaFile struct {
	// Armadactl is a path to an .armadactl.yaml file. Empty uses the same default resolution as
	// armadactl ($HOME/.armadactl.yaml).
	Armadactl string `json:"armadactl,omitempty"`
	// Kubeconfig is a path to a kubeconfig file. Empty uses standard KUBECONFIG/$HOME/.kube/config
	// resolution. Only consulted if Kwok.Enabled is true.
	Kubeconfig string `json:"kubeconfig,omitempty"`

	Kwok         KwokTarget         `json:"kwok,omitempty"`
	FakeExecutor FakeExecutorTarget `json:"fakeExecutor,omitempty"`

	// Submission is a path to a submission spec file (see internal/regatta/submit).
	Submission string `json:"submission"`
}

// KwokTarget configures regatta's KWOK fake-node target: real v1.Node objects created in a
// live cluster via KWOK, so Armada's real executor/scheduler path is exercised end-to-end.
type KwokTarget struct {
	Enabled         bool      `json:"enabled"`
	NodeGroup       NodeGroup `json:"nodeGroup"`
	KindClusterName string    `json:"kindClusterName,omitempty"`
	ProbeRetries    int       `json:"probeRetries,omitempty"`
	// ProbeDelay is a duration string (e.g. "5s"), parsed by Load into ProbeDelayDuration.
	// A plain string field, not time.Duration, since encoding/json (which BindJsonOrYaml
	// round-trips YAML through) has no built-in support for duration strings.
	ProbeDelay string `json:"probeDelay,omitempty"`

	// ProbeDelayDuration is ProbeDelay parsed by Load. Not part of the file format.
	ProbeDelayDuration time.Duration `json:"-"`
}

// FakeExecutorTarget configures regatta's fake-executor target: an armada-fakeexecutor process
// that registers as a real executor against the scheduler but simulates nodes/pods in-process,
// no real cluster involved.
type FakeExecutorTarget struct {
	Enabled   bool      `json:"enabled"`
	NodeGroup NodeGroup `json:"nodeGroup"`
	// SchedulerUrl is the armada-scheduler's executor-facing gRPC address (host:port), e.g.
	// "localhost:50052". This is deliberately separate from Armadactl's armadaUrl - that's the
	// submit-side API (armada-server, typically port 50051), a different service on a different
	// port than the scheduler's ExecutorApi that armada-fakeexecutor actually leases jobs from.
	SchedulerUrl string `json:"schedulerUrl"`
	Pool         string `json:"pool,omitempty"`
	ClusterId    string `json:"clusterId,omitempty"`
}

// Load reads a RegattaFile from path and resolves every path field it contains (Armadactl,
// Kubeconfig, each Kwok.NodeGroup/FakeExecutor.NodeGroup member's NodeProfile, Submission)
// relative to the regatta file's own directory, so a regatta file's relative paths behave the
// same regardless of the caller's working directory.
func Load(path string) (*RegattaFile, error) {
	file := &RegattaFile{}
	if err := util.BindJsonOrYaml(path, file); err != nil {
		return nil, err
	}

	dir := filepath.Dir(path)
	file.Armadactl = resolveRelative(dir, file.Armadactl)
	file.Kubeconfig = resolveRelative(dir, file.Kubeconfig)
	resolveNodeGroup(dir, file.Kwok.NodeGroup)
	resolveNodeGroup(dir, file.FakeExecutor.NodeGroup)
	file.Submission = resolveRelative(dir, file.Submission)

	if file.Kwok.ProbeDelay != "" {
		delay, err := time.ParseDuration(file.Kwok.ProbeDelay)
		if err != nil {
			return nil, fmt.Errorf("parsing kwok.probeDelay %q: %w", file.Kwok.ProbeDelay, err)
		}
		file.Kwok.ProbeDelayDuration = delay
	}

	if file.FakeExecutor.Enabled && file.FakeExecutor.SchedulerUrl == "" {
		return nil, fmt.Errorf("fakeExecutor.schedulerUrl is required when fakeExecutor.enabled is true")
	}

	if file.Kwok.Enabled && file.FakeExecutor.Enabled {
		return nil, fmt.Errorf("kwok.enabled and fakeExecutor.enabled cannot both be true: fakeExecutor simulates nodes in place of a real cluster, it is not a target to run alongside one")
	}

	return file, nil
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
