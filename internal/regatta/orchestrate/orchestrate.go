// Package orchestrate fans a Scenario's ExecutionTargets out into running kwok cluster targets
// and back down again. It exists separately from cmd/regatta/cmd/run.go so the fan-out logic
// (per-target naming/indexing, best-effort-all teardown) is testable independent of cobra
// plumbing.
package orchestrate

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"golang.org/x/sync/errgroup"

	log "github.com/armadaproject/armada/internal/common/logging"
	"github.com/armadaproject/armada/internal/regatta/config"
	"github.com/armadaproject/armada/internal/regatta/kwok"
	"github.com/armadaproject/armada/internal/regatta/metrics"
	"github.com/armadaproject/armada/pkg/client"
)

// Setup stands up every target in scenario.ExecutionTargets concurrently, since each target is
// an independent cluster/process with no cross-target dependency - running them sequentially
// meant one target's slow readiness check (WaitUntilSchedulable can retry for minutes)
// starved every later target of even starting. Returns a teardown func that reverses every
// target that successfully started, even if another target's Setup failed (best-effort-all, not
// strict-LIFO-on-success-only) - any failure still tears down everything that did come up.
//
// A target whose readiness check fails does not fail Setup if it sets
// cluster.continueOnReadinessFailure: it is logged and returned in the second result instead, in
// scenario target order, for the caller to put in the metrics report.
func Setup(ctx context.Context, scenario *config.Scenario, apiConnectionDetails *client.ApiConnectionDetails) (func(context.Context), []metrics.ReadinessFailure, error) {
	var (
		mu        sync.Mutex
		teardowns []func(context.Context)
	)
	failures := make([]*metrics.ReadinessFailure, len(scenario.ExecutionTargets))
	teardownAll := func(ctx context.Context) {
		for i := len(teardowns) - 1; i >= 0; i-- {
			teardowns[i](ctx)
		}
	}

	group, groupCtx := errgroup.WithContext(ctx)
	for i, target := range scenario.ExecutionTargets {
		i, target := i, target
		group.Go(func() error {
			nodeGroup, err := config.ResolveTargetNodeGroups(target, scenario.NodeGroups)
			if err != nil {
				return fmt.Errorf("target %q: resolving node groups: %w", target.Name, err)
			}

			teardown, err := setupCluster(groupCtx, target, nodeGroup, scenario.Load, apiConnectionDetails)
			if failure, tolerated := toleratedReadinessFailure(target, err); tolerated {
				log.Warnf("target %q: readiness check failed, continuing anyway because cluster.continueOnReadinessFailure is set: %s", target.Name, failure.Error)
				failures[i] = failure
			} else if err != nil {
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

// toleratedReadinessFailure reports whether err is a failed readiness check the target has opted
// to continue past, and if so the failure to record. Only a readiness failure qualifies: by then
// the nodes exist and setupCluster has returned a teardown for them, whereas any other setup error
// means the target is not usable.
func toleratedReadinessFailure(target config.ExecutionTarget, err error) (*metrics.ReadinessFailure, bool) {
	var readinessErr *kwok.ReadinessError
	if err == nil || target.Cluster == nil || !target.Cluster.ContinueOnReadinessFailure || !errors.As(err, &readinessErr) {
		return nil, false
	}
	return &metrics.ReadinessFailure{Target: target.Name, Error: readinessErr.Err.Error()}, true
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
		if err := kwok.Teardown(ctx, kubeClient, target.Name, target.Cluster.EffectiveNodeConcurrency()); err != nil {
			log.Errorf("target %q: KWOK teardown failed: %s", target.Name, err)
		}
	}
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

	stagesYAML, err := kwok.ResolveStagesYAML(target.Cluster.StagesPath)
	if err != nil {
		return nil, fmt.Errorf("resolving stages: %w", err)
	}

	// The readiness canary runs in the first queue whose jobs may land on this target, so it also
	// proves that queue can be seen and scheduled; a target no queue uses has nothing to check.
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
		var readinessErr *kwok.ReadinessError
		if errors.As(err, &readinessErr) {
			// Everything was created and only the readiness check gave up, so the fake nodes
			// exist: hand back their teardown alongside the error for a caller that carries on.
			return teardown, fmt.Errorf("KWOK setup failed: %w", err)
		}
		return nil, fmt.Errorf("KWOK setup failed: %w", err)
	}

	return teardown, nil
}
