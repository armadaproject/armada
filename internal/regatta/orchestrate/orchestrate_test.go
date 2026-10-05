package orchestrate

import (
	"errors"
	"fmt"
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
