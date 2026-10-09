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
	"time"

	"golang.org/x/sync/errgroup"

	log "github.com/armadaproject/armada/internal/common/logging"
	"github.com/armadaproject/armada/internal/regatta/config"
	"github.com/armadaproject/armada/internal/regatta/kwok"
	"github.com/armadaproject/armada/internal/regatta/metrics"
	"github.com/armadaproject/armada/pkg/client"
)

// failureCleanupTimeout bounds the cleanup that follows a failed or interrupted Setup. The cleanup runs on a fresh
// context, because the run's own is typically the reason for the failure (Ctrl+C cancels it) and a cancelled
// context would make every docker and Kubernetes call in the cleanup fail at once.
const failureCleanupTimeout = 2 * time.Minute

// Setup stands up every target in scenario.ExecutionTargets concurrently, since each target is
// an independent cluster/process with no cross-target dependency - running them sequentially
// meant one target's slow readiness check (WaitUntilSchedulable can retry for minutes)
// starved every later target of even starting. Returns a teardown func that reverses every
// target that started, even if another target's Setup failed (best-effort-all, not
// strict-LIFO-on-success-only) - any failure still tears down everything that did come up,
// including the target that failed itself, since by then it may have a controller and nodes.
//
// A target whose readiness check fails does not fail Setup if it sets
// cluster.continueOnReadinessFailure: it is logged and returned in the second result instead, in
// scenario target order, for the caller to put in the metrics report.
func Setup(ctx context.Context, scenario *config.Scenario, apiConnectionDetails *client.ApiConnectionDetails) (func(context.Context), []metrics.ReadinessFailure, error) {
	return setupTargets(ctx, scenario.ExecutionTargets, func(ctx context.Context, target config.ExecutionTarget) (func(context.Context), error) {
		nodeGroup, err := config.ResolveTargetNodeGroups(target, scenario.NodeGroups)
		if err != nil {
			return nil, fmt.Errorf("resolving node groups: %w", err)
		}
		return setupCluster(ctx, target, nodeGroup, scenario.Load, apiConnectionDetails)
	})
}

// setupTargets runs setup for every target concurrently. setup returns the teardown for whatever it created,
// which it must do on failure too (nil when nothing was created); every non-nil teardown is kept.
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
			// A run that was interrupted (or whose sibling failed) is not a readiness failure to continue past,
			// whatever the target's opt-in: it has to fail setup so that the failure cleanup runs.
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
// on one target doesn't stop the rest from being torn down. It returns the failures of all targets together, so
// a caller can tell that something was left behind.
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
		// Whatever step failed, the controller and some of the nodes may already exist, so the teardown is
		// handed back with the error (it is safe to run for a step that never happened). A caller that carries
		// on after a readiness failure keeps using it too.
		return teardown, fmt.Errorf("KWOK setup failed: %w", err)
	}

	return teardown, nil
}
