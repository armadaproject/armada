// Package orchestrate fans a Scenario's ExecutionTargets out into running kwok cluster targets
// and back down again. It exists separately from cmd/regatta/cmd/run.go so the fan-out logic
// (per-target naming/indexing, best-effort-all teardown) is testable independent of cobra
// plumbing.
package orchestrate

import (
	"context"
	"fmt"
	"sync"

	"golang.org/x/sync/errgroup"

	log "github.com/armadaproject/armada/internal/common/logging"
	"github.com/armadaproject/armada/internal/regatta/config"
	"github.com/armadaproject/armada/internal/regatta/kwok"
	"github.com/armadaproject/armada/pkg/client"
)

const (
	stageCRDPath = "cmd/regatta/kwok/stage-crd.yaml"
	stagesPath   = "cmd/regatta/kwok/stages.yaml"
)

// Setup stands up every target in scenario.ExecutionTargets concurrently, since each target is
// an independent cluster/process with no cross-target dependency - running them sequentially
// meant one target's slow schedulability probe (WaitUntilSchedulable can retry for minutes)
// starved every later target of even starting. Returns a teardown func that reverses every
// target that successfully started, even if another target's Setup failed (best-effort-all, not
// strict-LIFO-on-success-only) - any failure still tears down everything that did come up.
func Setup(ctx context.Context, scenario *config.Scenario, apiConnectionDetails *client.ApiConnectionDetails) (func(context.Context), error) {
	var (
		mu        sync.Mutex
		teardowns []func(context.Context)
	)
	teardownAll := func(ctx context.Context) {
		for i := len(teardowns) - 1; i >= 0; i-- {
			teardowns[i](ctx)
		}
	}

	group, groupCtx := errgroup.WithContext(ctx)
	for _, target := range scenario.ExecutionTargets {
		target := target
		group.Go(func() error {
			nodeGroup, err := config.ResolveTargetNodeGroups(target, scenario.NodeGroups)
			if err != nil {
				return fmt.Errorf("target %q: resolving node groups: %w", target.Name, err)
			}

			teardown, err := setupCluster(groupCtx, target, nodeGroup, apiConnectionDetails)
			if err != nil {
				return fmt.Errorf("target %q: %w", target.Name, err)
			}

			mu.Lock()
			teardowns = append(teardowns, teardown)
			mu.Unlock()
			return nil
		})
	}

	if err := group.Wait(); err != nil {
		teardownAll(ctx)
		return nil, err
	}

	return teardownAll, nil
}

// Teardown tears down every cluster target in scenario.ExecutionTargets, best-effort - a failure
// on one target is logged but doesn't stop the rest from being torn down.
func Teardown(ctx context.Context, scenario *config.Scenario) {
	for _, target := range scenario.ExecutionTargets {
		kubeClient, err := kwok.NewClientset(target.Cluster.Kubeconfig, target.Cluster.Kubernetes)
		if err != nil {
			log.Errorf("target %q: could not build kubernetes client: %s", target.Name, err)
			continue
		}
		log.Infof("target %q: tearing down KWOK fake nodes", target.Name)
		if err := kwok.Teardown(ctx, kubeClient, target.Name); err != nil {
			log.Errorf("target %q: KWOK teardown failed: %s", target.Name, err)
		}
	}
}

func setupCluster(ctx context.Context, target config.ExecutionTarget, nodeGroup []config.ResolvedNodeGroupMember, apiConnectionDetails *client.ApiConnectionDetails) (func(context.Context), error) {
	kubeClient, err := kwok.NewClientset(target.Cluster.Kubeconfig, target.Cluster.Kubernetes)
	if err != nil {
		return nil, fmt.Errorf("building kubernetes client: %w", err)
	}

	internalAPIServerAddress := target.Cluster.InternalAPIServerAddress
	if internalAPIServerAddress == "" {
		var err error
		if target.Cluster.Kind {
			// Default to kind's own internal-DNS convention for a control-plane-only cluster on
			// its own docker network, matching what `kind get kubeconfig --internal` used to
			// produce.
			internalAPIServerAddress = fmt.Sprintf("https://%s-control-plane:6443", target.Cluster.Name)
		} else {
			// A real cluster reached over a normal network (e.g. EKS) has no kind-style internal
			// address to derive - the kwok-controller container isn't confined to a private
			// docker network for these, so it can just reuse Kubeconfig's own server address.
			internalAPIServerAddress, err = kwok.KubeconfigServerAddress(target.Cluster.Kubeconfig)
			if err != nil {
				return nil, fmt.Errorf("resolving default internal API server address: %w", err)
			}
		}
	}

	cfg := kwok.Config{
		Name:                     target.Name,
		Kind:                     target.Cluster.Kind,
		KubeconfigPath:           target.Cluster.Kubeconfig,
		InternalAPIServerAddress: internalAPIServerAddress,
		StageCRDPath:             stageCRDPath,
		StagesPath:               stagesPath,
		NodeGroup:                nodeGroup,
		ApiConnectionDetails:     apiConnectionDetails,
		SchedulableProbe: kwok.ProbeConfig{
			Retries:      target.Cluster.ProbeRetries,
			InitialDelay: target.Cluster.ProbeDelayDuration,
		},
		EvaluateReadiness: target.Cluster.ShouldEvaluateReadiness(),
	}

	log.Infof("target %q: setting up KWOK fake nodes", target.Name)
	if err := kwok.Setup(ctx, kubeClient, cfg); err != nil {
		return nil, fmt.Errorf("KWOK setup failed: %w", err)
	}

	return func(ctx context.Context) {
		log.Infof("target %q: tearing down KWOK fake nodes", target.Name)
		if err := kwok.Teardown(ctx, kubeClient, target.Name); err != nil {
			log.Errorf("target %q: KWOK teardown failed: %s", target.Name, err)
		}
	}, nil
}
