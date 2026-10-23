// Package kwok sets up and tears down KWOK fake nodes for a regatta run: the Stage CRD and Stages, the controller
// container, and the nodes.
package kwok

import (
	"context"
	"errors"
	"fmt"
	"time"

	"k8s.io/client-go/kubernetes"

	log "github.com/armadaproject/armada/internal/common/logging"
	"github.com/armadaproject/armada/internal/regatta/config"
	"github.com/armadaproject/armada/pkg/client"
)

// Config describes one KWOK setup.
type Config struct {
	Name                     string
	Kind                     bool
	KubeconfigPath           string
	InternalAPIServerAddress string
	NodeGroup                []config.ResolvedNodeGroupMember
	ApiConnectionDetails     *client.ApiConnectionDetails
	Readiness                ReadinessConfig
	// ReadinessQueue is the queue the canary is submitted to; with none there is nothing to check.
	ReadinessQueue    string
	EvaluateReadiness bool
	NodeConcurrency   int
	ReadyTimeout      time.Duration
	StagesYAML        []byte
}

// ReadinessError is what Setup returns when everything was created and only the readiness check failed. The nodes exist,
// so a caller may continue (cluster.continueOnReadinessFailure); after any other error the target is unusable.
type ReadinessError struct {
	Err error
}

func (e *ReadinessError) Error() string {
	return fmt.Sprintf("waiting for fake nodes to become schedulable: %s", e.Err)
}

func (e *ReadinessError) Unwrap() error { return e.Err }

// Setup applies the Stage CRD and Stages, starts the controller, creates the nodes, and waits for them to be Ready and
// schedulable.
func Setup(ctx context.Context, kubeClient kubernetes.Interface, cfg Config) error {
	setupStart := time.Now()
	kubeconfig, err := ResolveKubeconfig(cfg.KubeconfigPath)
	if err != nil {
		return fmt.Errorf("resolving kubeconfig: %w", err)
	}

	if err := timedPhase(cfg.Name, "applied Stage CRD and Stages", func() error {
		if err := ApplyStageCRD(ctx, kubeconfig); err != nil {
			return fmt.Errorf("applying Stage CRD: %w", err)
		}
		if err := WaitForStageCRD(ctx, kubeconfig); err != nil {
			return err
		}
		if err := ApplyStages(ctx, kubeconfig, cfg.StagesYAML); err != nil {
			return fmt.Errorf("applying Stages: %w", err)
		}
		return nil
	}); err != nil {
		return err
	}
	if err := timedPhase(cfg.Name, "started kwok-controller", func() error {
		if err := RunController(ctx, kubeconfig, cfg.InternalAPIServerAddress, cfg.Name, cfg.Kind); err != nil {
			return fmt.Errorf("starting kwok-controller: %w", err)
		}
		return nil
	}); err != nil {
		return err
	}

	totalNodes := 0
	for _, member := range cfg.NodeGroup {
		totalNodes += member.Count
	}
	if err := timedPhase(cfg.Name, fmt.Sprintf("created %d fake nodes (concurrency %d)", totalNodes, cfg.NodeConcurrency), func() error {
		for _, member := range cfg.NodeGroup {
			if err := ApplyFakeNodes(ctx, kubeClient, member.Profile, member.Count, cfg.Name, cfg.NodeConcurrency); err != nil {
				return fmt.Errorf("applying fake nodes: %w", err)
			}
		}
		return nil
	}); err != nil {
		return err
	}
	if err := timedPhase(cfg.Name, "fake nodes Ready", func() error {
		if err := WaitUntilReady(ctx, kubeClient, cfg.Name, cfg.ReadyTimeout); err != nil {
			return fmt.Errorf("waiting for fake nodes: %w", err)
		}
		return nil
	}); err != nil {
		return err
	}

	if cfg.EvaluateReadiness {
		if err := WaitUntilSchedulable(ctx, cfg.ApiConnectionDetails, cfg.Readiness, cfg.Name, cfg.ReadinessQueue); err != nil {
			if ctx.Err() != nil {
				return err // the run was interrupted: that is not a readiness failure a target may opt to continue past
			}
			return &ReadinessError{Err: err}
		}
	} else {
		log.Infof("target %q: readiness check skipped (evaluateReadiness is false), load may start before the scheduler knows the nodes", cfg.Name)
	}
	log.Infof("target %q: setup complete in %s", cfg.Name, time.Since(setupStart).Round(time.Millisecond))
	return nil
}

// timedPhase runs fn and logs how long it took; a failure is returned unlogged.
func timedPhase(target, what string, fn func() error) error {
	start := time.Now()
	if err := fn(); err != nil {
		return err
	}
	log.Infof("target %q: %s in %s", target, what, time.Since(start).Round(time.Millisecond))
	return nil
}

// Teardown stops the controller and deletes the fake nodes. Safe if Setup never ran or only partly completed.
func Teardown(ctx context.Context, kubeClient kubernetes.Interface, targetName string, nodeConcurrency int) error {
	// Both steps are attempted even if the first fails.
	var errs []error
	if err := TeardownController(ctx, targetName); err != nil {
		errs = append(errs, fmt.Errorf("stopping kwok-controller: %w", err))
	}
	if err := DeleteFakeNodes(ctx, kubeClient, targetName, nodeConcurrency); err != nil {
		errs = append(errs, fmt.Errorf("deleting fake nodes: %w", err))
	}
	return errors.Join(errs...)
}
