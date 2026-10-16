// Package orchestrate fans a scenario's execution targets out into running kwok targets, and back down.
package orchestrate

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	log "github.com/armadaproject/armada/internal/common/logging"
	"github.com/armadaproject/armada/internal/regatta/config"
	"github.com/armadaproject/armada/internal/regatta/kwok"
	"github.com/armadaproject/armada/internal/regatta/metrics"
	"github.com/armadaproject/armada/pkg/client"
)

// failureCleanupTimeout bounds the cleanup after a failed or interrupted Setup. It runs on a fresh context: the run's
// own is usually cancelled (Ctrl+C), which would fail every docker and Kubernetes call in the cleanup.
const failureCleanupTimeout = 2 * time.Minute

// Setup stands up every target concurrently. It returns a teardown that reverses every target that started, the one
// that failed included, since it may already have a controller and nodes; a failure tears down everything that came up.
// A target that sets cluster.continueOnReadinessFailure does not fail Setup when only its readiness check fails: the
// failure is returned in the second result, in target order, for the report.
func Setup(ctx context.Context, scenario *config.Scenario, apiConnectionDetails *client.ApiConnectionDetails) (func(context.Context), []metrics.ReadinessFailure, error) {
	return setupTargets(ctx, scenario.ExecutionTargets, func(ctx context.Context, target config.ExecutionTarget) (func(context.Context), error) {
		nodeGroup, err := config.ResolveTargetNodeGroups(target, scenario.NodeGroups)
		if err != nil {
			return nil, fmt.Errorf("resolving node groups: %w", err)
		}
		return setupCluster(ctx, target, nodeGroup, scenario.Load, apiConnectionDetails)
	})
}

// setupTargets runs setup for every target concurrently. setup returns the teardown for whatever it created, on
// failure too (nil when nothing was created); every non-nil teardown is kept.
func setupTargets(
	ctx context.Context,
	targets []config.ExecutionTarget,
	setup func(ctx context.Context, target config.ExecutionTarget) (func(context.Context), error),
) (func(context.Context), []metrics.ReadinessFailure, error) {
	var (
		mu        sync.Mutex
		teardowns []func(context.Context)
	)
	failures := make([]*metrics.ReadinessFailure, len(targets))
	teardownAll := func(ctx context.Context) {
		for i := len(teardowns) - 1; i >= 0; i-- {
			teardowns[i](ctx)
		}
	}

	group, groupCtx := errgroup.WithContext(ctx)
	for i, target := range targets {
		i, target := i, target
		group.Go(func() error {
			teardown, err := setup(groupCtx, target)
			if teardown != nil {
				mu.Lock()
				teardowns = append(teardowns, teardown)
				mu.Unlock()
			}
			// An interrupted run (or a failed sibling) is not a readiness failure to continue past, whatever the opt-in: setup
			// must fail so the cleanup runs.
			if failure, tolerated := toleratedReadinessFailure(target, err); tolerated && groupCtx.Err() == nil {
				log.Warnf("target %q: readiness check failed, continuing anyway because cluster.continueOnReadinessFailure is set: %s", target.Name, failure.Error)
				failures[i] = failure
			} else if err != nil {
				return fmt.Errorf("target %q: %w", target.Name, err)
			}
			return nil
		})
	}

	if err := group.Wait(); err != nil {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), failureCleanupTimeout)
		defer cancel()
		teardownAll(cleanupCtx)
		return nil, nil, err
	}

	var readinessFailures []metrics.ReadinessFailure
	for _, failure := range failures {
		if failure != nil {
			readinessFailures = append(readinessFailures, *failure)
		}
	}
	return teardownAll, readinessFailures, nil
}

// toleratedReadinessFailure reports whether err is a readiness failure the target opted to continue past, and the
// failure to record. Only that qualifies: the nodes exist by then, whereas any other error leaves the target unusable.
func toleratedReadinessFailure(target config.ExecutionTarget, err error) (*metrics.ReadinessFailure, bool) {
	var readinessErr *kwok.ReadinessError
	if err == nil || target.Cluster == nil || !target.Cluster.ContinueOnReadinessFailure || !errors.As(err, &readinessErr) {
		return nil, false
	}
	return &metrics.ReadinessFailure{Target: target.Name, Error: readinessErr.Err.Error()}, true
}

// Teardown tears down every target, best-effort: a failure does not stop the rest. It returns all the failures together.
func Teardown(ctx context.Context, scenario *config.Scenario) error {
	return teardownTargets(ctx, scenario.ExecutionTargets, func(ctx context.Context, target config.ExecutionTarget) error {
		kubeClient, err := kwok.NewClientset(target.Cluster.Kubeconfig, target.Cluster.Kubernetes)
		if err != nil {
			return fmt.Errorf("could not build kubernetes client: %w", err)
		}
		log.Infof("target %q: tearing down KWOK fake nodes", target.Name)
		return kwok.Teardown(ctx, kubeClient, target.Name, target.Cluster.EffectiveNodeConcurrency())
	})
}

// teardownTargets runs teardown for every target, whatever happens to the others, and joins the errors.
func teardownTargets(ctx context.Context, targets []config.ExecutionTarget, teardown func(ctx context.Context, target config.ExecutionTarget) error) error {
	var errs []error
	for _, target := range targets {
		if err := teardown(ctx, target); err != nil {
			log.Errorf("target %q: teardown failed: %s", target.Name, err)
			errs = append(errs, fmt.Errorf("target %q: %w", target.Name, err))
		}
	}
	return errors.Join(errs...)
}

func setupCluster(ctx context.Context, target config.ExecutionTarget, nodeGroup []config.ResolvedNodeGroupMember, load config.Load, apiConnectionDetails *client.ApiConnectionDetails) (func(context.Context), error) {
	kubeClient, err := kwok.NewClientset(target.Cluster.Kubeconfig, target.Cluster.Kubernetes)
	if err != nil {
		return nil, fmt.Errorf("building kubernetes client: %w", err)
	}

	internalAPIServerAddress := target.Cluster.InternalAPIServerAddress
	if internalAPIServerAddress == "" {
		var err error
		if target.Cluster.Kind {
			// kind's internal DNS name of the control-plane container, on the "kind" docker network.
			internalAPIServerAddress = fmt.Sprintf("https://%s-control-plane:6443", target.Cluster.Name)
		} else {
			// A cluster on a normal network is reached at the kubeconfig's own server address.
			internalAPIServerAddress, err = kwok.KubeconfigServerAddress(target.Cluster.Kubeconfig)
			if err != nil {
				return nil, fmt.Errorf("resolving default internal API server address: %w", err)
			}
		}
	}

	stagesYAML, err := kwok.ResolveStagesYAML(target.Cluster.StagesPath)
	if err != nil {
		return nil, fmt.Errorf("resolving stages: %w", err)
	}

	// The canary runs in the first queue that may use this target, which also proves that queue is visible and schedulable.
	readinessQueue := ""
	if queues := load.QueuesForTarget(target.Name); len(queues) > 0 {
		readinessQueue = queues[0].Name
	}
	evaluateReadiness := target.Cluster.ShouldEvaluateReadiness()
	if evaluateReadiness && readinessQueue == "" {
		log.Infof("target %q: no queue submits to this target, so there is nothing to check readiness with", target.Name)
		evaluateReadiness = false
	}

	cfg := kwok.Config{
		Name:                     target.Name,
		Kind:                     target.Cluster.Kind,
		KubeconfigPath:           target.Cluster.Kubeconfig,
		InternalAPIServerAddress: internalAPIServerAddress,
		NodeGroup:                nodeGroup,
		ApiConnectionDetails:     apiConnectionDetails,
		Readiness: kwok.ReadinessConfig{
			Retries:      target.Cluster.EffectiveReadinessRetries(),
			InitialDelay: target.Cluster.EffectiveReadinessDelay(),
			SelectTarget: target.Cluster.ShouldReadinessSelectTarget(),
		},
		ReadinessQueue:    readinessQueue,
		EvaluateReadiness: evaluateReadiness,
		NodeConcurrency:   target.Cluster.EffectiveNodeConcurrency(),
		ReadyTimeout:      target.Cluster.ReadyTimeoutDuration,
		StagesYAML:        stagesYAML,
	}

	teardown := func(ctx context.Context) {
		log.Infof("target %q: tearing down KWOK fake nodes", target.Name)
		if err := kwok.Teardown(ctx, kubeClient, target.Name, cfg.NodeConcurrency); err != nil {
			log.Errorf("target %q: KWOK teardown failed: %s", target.Name, err)
		}
	}

	if contextName, server, err := kwok.DescribeKubeconfig(target.Cluster.Kubeconfig); err != nil {
		log.Warnf("target %q: could not describe kubeconfig %s: %s", target.Name, target.Cluster.Kubeconfig, err)
	} else {
		log.Infof("target %q: cluster context %q, API server %s (kind: %t)", target.Name, contextName, server, target.Cluster.Kind)
	}
	log.Infof("target %q: setting up KWOK fake nodes", target.Name)
	if err := kwok.Setup(ctx, kubeClient, cfg); err != nil {
		// The controller and some nodes may exist whatever step failed, so the teardown comes back with the error (it is safe to
		// run for a step that never happened). A caller that continues after a readiness failure keeps using it.
		return teardown, fmt.Errorf("KWOK setup failed: %w", err)
	}

	return teardown, nil
}
