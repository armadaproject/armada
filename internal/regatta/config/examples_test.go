package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"sigs.k8s.io/yaml"
)

// The example scenarios are what the docs point people at, so they must load with the real parser.
func exampleScenario(t *testing.T, name string) *Scenario {
	t.Helper()
	path := filepath.Join("..", "..", "..", "cmd", "regatta", "config", "scenarios", name)
	scenario, err := LoadScenario(path)
	require.NoError(t, err, name)
	return scenario
}

func TestExample_TwoClusterIsTenEvenQueues(t *testing.T) {
	s := exampleScenario(t, "two-cluster.example.yaml")

	names := s.Load.Names()
	require.Len(t, names, 10)
	require.Equal(t, "regatta-01", names[0], "zero-padded to the width of the count")
	require.Equal(t, "regatta-10", names[9])
	require.Equal(t, 1000, s.Load.TotalJobs())
	for _, q := range s.Load.Queues {
		require.Equal(t, 100, q.TotalJobs(), "no distribution means every queue gets the same")
		require.Equal(t, 10, q.Jobs[0].Count, "a 10% share of the queue's 100 jobs")
		require.Equal(t, 90, q.Jobs[1].Count)
		require.Equal(t, "regatta-"+q.Name, q.JobSetId)
		require.Zero(t, q.Schedule.Duration, "duration 0: everything at once")
	}
	require.Len(t, s.Load.QueuesForTarget("gpu-cluster"), 10)
	require.Len(t, s.Load.QueuesForTarget("cpu-cluster"), 10)
}

func TestExample_TwoClusterContinuous(t *testing.T) {
	s := exampleScenario(t, "two-cluster.continuous.example.yaml")

	require.True(t, s.Load.Continuous())
	require.Len(t, s.Load.Queues, 100)
	perStep := 0.0
	for _, q := range s.Load.Queues {
		require.Equal(t, time.Minute, q.Continuous.Step)
		for _, r := range q.Continuous.Rates {
			perStep += r.PerStep
		}
	}
	require.InDelta(t, 600, perStep, 1e-6, "the queues share jobsPerStep")
	require.Equal(t, 2*time.Minute, s.Metrics.ReportIntervalDuration)
}

func TestExample_TwoClusterLognormal(t *testing.T) {
	s := exampleScenario(t, "two-cluster.lognormal.example.yaml")

	require.False(t, s.Load.Continuous())
	require.Len(t, s.Load.Queues, 221)
	require.Equal(t, 22300, s.Load.TotalJobs())

	tenants := s.Load.Queues[:200]
	busiest, min := 0, tenants[0].TotalJobs()
	for i, q := range tenants {
		require.GreaterOrEqual(t, q.TotalJobs(), 5, "minJobsPerQueue")
		if q.TotalJobs() > tenants[busiest].TotalJobs() {
			busiest = i
		}
		if q.TotalJobs() < min {
			min = q.TotalJobs()
		}
	}
	require.Less(t, busiest, 20, "the lognormal peaks among the low queue numbers")
	require.Greater(t, tenants[busiest].TotalJobs(), 10*min, "a long tail")

	require.Len(t, s.Load.QueuesForTarget("gpu-cluster"), 221)
	require.Len(t, s.Load.QueuesForTarget("cpu-cluster"), 201, "batch queues are GPU only")
}

// The parser ignores keys it does not know, so a misplaced or misspelled setting would silently do
// nothing. Decode every example strictly to catch that.
func TestExamples_HaveNoUnknownKeys(t *testing.T) {
	paths, err := filepath.Glob(filepath.Join("..", "..", "..", "cmd", "regatta", "config", "scenarios", "*.yaml"))
	require.NoError(t, err)
	require.NotEmpty(t, paths)
	for _, path := range paths {
		raw, err := os.ReadFile(path)
		require.NoError(t, err)
		require.NoError(t, yaml.UnmarshalStrict(raw, &Scenario{}), path)
	}
}
