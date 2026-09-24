// Package kwok stands up and tears down KWOK-simulated fake Kubernetes nodes for a regatta run.
// It is regatta's own responsibility, not a mage/dev-environment concern: applying the Stage
// CRD, Stage objects, and fake nodes happens here as part of `regatta run`, against whatever
// cluster the ambient kubeconfig points at.
package kwok

import (
	"context"
	"fmt"
	"time"

	"k8s.io/client-go/kubernetes"

	"github.com/armadaproject/armada/internal/regatta/config"
	"github.com/armadaproject/armada/pkg/client"
)

// readyTimeout has headroom above what even a large (300+ node) target needs on an idle machine,
// since several targets' worth of concurrent node creates/kwok-controller reconciliation compete
// for the same host CPU when multiple execution targets are set up at once (see orchestrate.Setup)
// - observed flakiness right around the old 60s value at 10 concurrent targets was host
// contention, not a stuck controller.
const readyTimeout = 5 * time.Minute

// Config describes one KWOK setup: which fixture files to apply and how many fake nodes of
// which shape(s) to create.
type Config struct {
	Name                     string
	Kind                     bool
	KubeconfigPath           string
	InternalAPIServerAddress string
	StageCRDPath             string
	StagesPath               string
	NodeGroup                []config.ResolvedNodeGroupMember
	ApiConnectionDetails     *client.ApiConnectionDetails
	SchedulableProbe         ProbeConfig
	EvaluateReadiness        bool
}

// Setup applies the Stage CRD, Stage objects, starts the kwok-controller, creates the fake
// nodes, and waits for them to report Ready and become schedulable.
func Setup(ctx context.Context, kubeClient kubernetes.Interface, cfg Config) error {
	kubeconfig, err := ResolveKubeconfig(cfg.KubeconfigPath)
	if err != nil {
		return fmt.Errorf("resolving kubeconfig: %w", err)
	}

	if err := ApplyStageCRD(ctx, kubeconfig, cfg.StageCRDPath); err != nil {
		return fmt.Errorf("applying Stage CRD: %w", err)
	}
	if err := ApplyStages(ctx, kubeconfig, cfg.StagesPath); err != nil {
		return fmt.Errorf("applying Stages: %w", err)
	}
	if err := RunController(ctx, kubeconfig, cfg.InternalAPIServerAddress, cfg.Name, cfg.Kind); err != nil {
		return fmt.Errorf("starting kwok-controller: %w", err)
	}
	for _, member := range cfg.NodeGroup {
		if err := ApplyFakeNodes(ctx, kubeClient, member.Profile, member.Count, cfg.Name); err != nil {
			return fmt.Errorf("applying fake nodes: %w", err)
		}
	}
	if err := WaitUntilReady(ctx, kubeClient, readyTimeout); err != nil {
		return fmt.Errorf("waiting for fake nodes: %w", err)
	}
	if cfg.EvaluateReadiness {
		if err := WaitUntilSchedulable(ctx, kubeClient, cfg.ApiConnectionDetails, cfg.SchedulableProbe, cfg.Name); err != nil {
			return fmt.Errorf("waiting for fake nodes to become schedulable: %w", err)
		}
	}
	return nil
}

// Teardown stops the kwok-controller and deletes the fake nodes. Safe to call even if Setup
// never ran or only partially completed.
func Teardown(ctx context.Context, kubeClient kubernetes.Interface, targetName string) error {
	if err := TeardownController(ctx, targetName); err != nil {
		return fmt.Errorf("stopping kwok-controller: %w", err)
	}
	if err := DeleteFakeNodes(ctx, kubeClient, targetName); err != nil {
		return fmt.Errorf("deleting fake nodes: %w", err)
	}
	return nil
}
