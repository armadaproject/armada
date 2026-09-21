// Package orchestrate fans a Scenario's ExecutionTargets out into running kwok/fake-executor
// targets and back down again. It exists separately from cmd/regatta/cmd/run.go so the fan-out
// logic (per-type naming/indexing, best-effort-all teardown) is testable independent of cobra
// plumbing.
package orchestrate

import (
	"context"
	"fmt"
	"sync"

	"golang.org/x/sync/errgroup"

	log "github.com/armadaproject/armada/internal/common/logging"
	"github.com/armadaproject/armada/internal/regatta/config"
	"github.com/armadaproject/armada/internal/regatta/fakeexecutor"
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
	for i, target := range scenario.ExecutionTargets {
		target := target
		fakeExecutorIndex := i
		group.Go(func() error {
			nodeGroup, err := config.ResolveTargetNodeGroups(target, scenario.NodeGroups)
			if err != nil {
				return fmt.Errorf("target %q: resolving node groups: %w", target.Name, err)
			}

			var teardown func(context.Context)
			switch target.Type {
			case config.TargetTypeCluster:
				teardown, err = setupCluster(groupCtx, target, nodeGroup, apiConnectionDetails)
			case config.TargetTypeFakeExecutor:
				teardown, err = setupFakeExecutor(groupCtx, target, nodeGroup, apiConnectionDetails, fakeExecutorIndex)
			}
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
// on one target is logged but doesn't stop the rest from being torn down. Fake-executor targets
// are skipped: they're a process regatta itself would have started, and by the time a separate
// `regatta teardown` invocation runs there's no PID left to reconnect to.
func Teardown(ctx context.Context, scenario *config.Scenario) {
	for _, target := range scenario.ExecutionTargets {
		if target.Type != config.TargetTypeCluster {
			continue
		}

		kubeClient, err := kwok.NewClientset(target.Cluster.Kubeconfig)
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
	kubeClient, err := kwok.NewClientset(target.Cluster.Kubeconfig)
	if err != nil {
		return nil, fmt.Errorf("building kubernetes client: %w", err)
	}

	internalAPIServerAddress := target.Cluster.InternalAPIServerAddress
	if internalAPIServerAddress == "" && target.Cluster.Name != "" {
		// Default to kind's own internal-DNS convention for a control-plane-only cluster on its
		// own docker network, matching what `kind get kubeconfig --internal` used to produce.
		// A hand-supplied non-kind cluster has no Cluster.Name-derived default and must set
		// InternalAPIServerAddress explicitly.
		internalAPIServerAddress = fmt.Sprintf("https://%s-control-plane:6443", target.Cluster.Name)
	}

	cfg := kwok.Config{
		Name:                     target.Name,
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

func setupFakeExecutor(ctx context.Context, target config.ExecutionTarget, nodeGroup []config.ResolvedNodeGroupMember, apiConnectionDetails *client.ApiConnectionDetails, index int) (func(context.Context), error) {
	log.Infof("target %q: starting armada-fakeexecutor", target.Name)
	process, err := fakeexecutor.Start(apiConnectionDetails, nodeGroup, *target.FakeExecutor, index)
	if err != nil {
		return nil, fmt.Errorf("starting armada-fakeexecutor failed: %w", err)
	}
	log.Infof("target %q: armada-fakeexecutor started, pid %d", target.Name, process.PID())

	log.Infof("target %q: waiting for armada-fakeexecutor to become schedulable", target.Name)
	probeCfg := fakeexecutor.ProbeConfig{
		Retries:      target.FakeExecutor.ProbeRetries,
		InitialDelay: target.FakeExecutor.ProbeDelayDuration,
	}
	if err := fakeexecutor.WaitUntilSchedulable(ctx, apiConnectionDetails, probeCfg, target.Name); err != nil {
		_ = process.Stop()
		return nil, fmt.Errorf("waiting for armada-fakeexecutor to become schedulable: %w", err)
	}

	return func(context.Context) {
		log.Infof("target %q: stopping armada-fakeexecutor", target.Name)
		if err := process.Stop(); err != nil {
			log.Errorf("target %q: stopping armada-fakeexecutor failed: %s", target.Name, err)
		}
	}, nil
}
