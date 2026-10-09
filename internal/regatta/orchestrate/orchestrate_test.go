package orchestrate

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/internal/regatta/config"
	"github.com/armadaproject/armada/internal/regatta/kwok"
)

func TestToleratedReadinessFailure(t *testing.T) {
	readinessErr := &kwok.ReadinessError{Err: errors.New("canary job was not running within 5s, last event JobQueuedEvent")}
	target := func(continueOnFailure bool) config.ExecutionTarget {
		return config.ExecutionTarget{
			Name:    "dev",
			Cluster: &config.ClusterTarget{ContinueOnReadinessFailure: continueOnFailure},
		}
	}

	t.Run("readiness failure the target opted to continue past is recorded", func(t *testing.T) {
		failure, tolerated := toleratedReadinessFailure(target(true), readinessErr)
		require.True(t, tolerated)
		require.Equal(t, "dev", failure.Target)
		require.Equal(t, "canary job was not running within 5s, last event JobQueuedEvent", failure.Error)
	})

	t.Run("a wrapped readiness failure is still recognised", func(t *testing.T) {
		failure, tolerated := toleratedReadinessFailure(target(true), fmt.Errorf("KWOK setup failed: %w", readinessErr))
		require.True(t, tolerated)
		require.Equal(t, "dev", failure.Target)
	})

	t.Run("readiness failure without the opt-in fails setup", func(t *testing.T) {
		failure, tolerated := toleratedReadinessFailure(target(false), readinessErr)
		require.False(t, tolerated)
		require.Nil(t, failure)
	})

	t.Run("any other setup error is never tolerated", func(t *testing.T) {
		failure, tolerated := toleratedReadinessFailure(target(true), errors.New("applying Stage CRD: connection refused"))
		require.False(t, tolerated)
		require.Nil(t, failure)
	})

	t.Run("no error is not a failure", func(t *testing.T) {
		failure, tolerated := toleratedReadinessFailure(target(true), nil)
		require.False(t, tolerated)
		require.Nil(t, failure)
	})

	t.Run("a target with no cluster config is not tolerated", func(t *testing.T) {
		failure, tolerated := toleratedReadinessFailure(config.ExecutionTarget{Name: "x"}, readinessErr)
		require.False(t, tolerated)
		require.Nil(t, failure)
	})
}

// teardownRecorder is a fake setup step: each target gets a teardown that records the context it was run with.
type teardownRecorder struct {
	mu     sync.Mutex
	calls  []string
	ctxErr map[string]error
}

func (r *teardownRecorder) teardownFor(name string) func(context.Context) {
	return func(ctx context.Context) {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.calls = append(r.calls, name)
		if r.ctxErr == nil {
			r.ctxErr = map[string]error{}
		}
		r.ctxErr[name] = ctx.Err()
	}
}

func targetsNamed(names ...string) []config.ExecutionTarget {
	var targets []config.ExecutionTarget
	for _, name := range names {
		targets = append(targets, config.ExecutionTarget{Name: name, Cluster: &config.ClusterTarget{}})
	}
	return targets
}

func TestSetupTargets_AFailedTargetIsTornDownTooAndSoAreItsSiblings(t *testing.T) {
	rec := &teardownRecorder{}
	setup := func(_ context.Context, target config.ExecutionTarget) (func(context.Context), error) {
		if target.Name == "bad" {
			// the controller and nodes were created before this step failed
			return rec.teardownFor(target.Name), errors.New("waiting for fake nodes: timed out")
		}
		return rec.teardownFor(target.Name), nil
	}

	teardown, failures, err := setupTargets(context.Background(), targetsNamed("good", "bad"), setup)

	require.ErrorContains(t, err, `target "bad"`)
	require.Nil(t, teardown)
	require.Nil(t, failures)
	require.ElementsMatch(t, []string{"good", "bad"}, rec.calls, "the failed target's resources are not leaked")
}

func TestSetupTargets_NothingToTearDownWhenASetupStepCreatedNothing(t *testing.T) {
	rec := &teardownRecorder{}
	setup := func(_ context.Context, target config.ExecutionTarget) (func(context.Context), error) {
		if target.Name == "bad" {
			return nil, errors.New("resolving node groups")
		}
		return rec.teardownFor(target.Name), nil
	}

	_, _, err := setupTargets(context.Background(), targetsNamed("good", "bad"), setup)

	require.Error(t, err)
	require.Equal(t, []string{"good"}, rec.calls)
}

func TestSetupTargets_FailureCleanupRunsOnALiveContextEvenWhenTheRunWasCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	rec := &teardownRecorder{}
	setup := func(ctx context.Context, target config.ExecutionTarget) (func(context.Context), error) {
		cancel() // Ctrl+C while the target is being set up
		return rec.teardownFor(target.Name), ctx.Err()
	}

	_, _, err := setupTargets(ctx, targetsNamed("t"), setup)

	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, []string{"t"}, rec.calls)
	require.NoError(t, rec.ctxErr["t"], "the cleanup does not inherit the cancelled run context")
}

func TestSetupTargets_ToleratedReadinessFailureKeepsTheTeardownAndTheRun(t *testing.T) {
	rec := &teardownRecorder{}
	readinessErr := &kwok.ReadinessError{Err: errors.New("canary never ran")}
	targets := targetsNamed("ok", "flaky")
	targets[1].Cluster.ContinueOnReadinessFailure = true
	setup := func(_ context.Context, target config.ExecutionTarget) (func(context.Context), error) {
		if target.Name == "flaky" {
			return rec.teardownFor(target.Name), fmt.Errorf("KWOK setup failed: %w", readinessErr)
		}
		return rec.teardownFor(target.Name), nil
	}

	teardown, failures, err := setupTargets(context.Background(), targets, setup)

	require.NoError(t, err)
	require.Len(t, failures, 1)
	require.Equal(t, "flaky", failures[0].Target)
	require.Empty(t, rec.calls, "a successful setup tears nothing down yet")
	teardown(context.Background())
	require.ElementsMatch(t, []string{"ok", "flaky"}, rec.calls, "both targets, the one with the failed check included, come down with the run")
}

func TestSetupTargets_AnInterruptedReadinessWaitIsNotToleratedEvenWithTheOptIn(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	rec := &teardownRecorder{}
	targets := targetsNamed("t")
	targets[0].Cluster.ContinueOnReadinessFailure = true
	setup := func(ctx context.Context, target config.ExecutionTarget) (func(context.Context), error) {
		cancel() // Ctrl+C during the readiness wait
		return rec.teardownFor(target.Name), &kwok.ReadinessError{Err: ctx.Err()}
	}

	teardown, failures, err := setupTargets(ctx, targets, setup)

	require.ErrorIs(t, err, context.Canceled, "setup fails instead of carrying on with a half-started target")
	require.Nil(t, teardown)
	require.Nil(t, failures)
	require.Equal(t, []string{"t"}, rec.calls, "so the failure cleanup runs")
}

func TestTeardownTargets_ReportsEveryFailureAndStillTriesEveryTarget(t *testing.T) {
	var attempted []string
	teardown := func(_ context.Context, target config.ExecutionTarget) error {
		attempted = append(attempted, target.Name)
		if target.Name == "a" || target.Name == "c" {
			return errors.New("docker rm failed")
		}
		return nil
	}

	err := teardownTargets(context.Background(), targetsNamed("a", "b", "c"), teardown)

	require.Equal(t, []string{"a", "b", "c"}, attempted, "a failure does not stop the rest")
	require.ErrorContains(t, err, `target "a"`)
	require.ErrorContains(t, err, `target "c"`)
	require.NotContains(t, err.Error(), `target "b"`)
	require.NoError(t, teardownTargets(context.Background(), targetsNamed("a"), func(context.Context, config.ExecutionTarget) error { return nil }))
}
