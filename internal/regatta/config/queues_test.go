package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/internal/regatta/load"
)

func writeJobSpec(t *testing.T, dir, name string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	require.NoError(t, os.WriteFile(path, []byte("containers:\n  - name: c\n    image: alpine\n"), 0o600))
	return name
}

func ptr(v float64) *float64 { return &v }

func TestNormalise_SingleQueueEntry(t *testing.T) {
	dir := t.TempDir()
	spec := writeJobSpec(t, dir, "sleep.yaml")
	l := Load{Entries: []QueueEntry{{Prefix: "team-a-", TotalJobs: 10, Jobs: []JobRef{{JobSpec: spec, Share: 1}}}}}

	require.NoError(t, l.normalize(dir, map[string]bool{"gpu": true}))

	require.Len(t, l.Queues, 1)
	q := l.Queues[0]
	require.Equal(t, "team-a-1", q.Name, "a count of 1 still gets a number: there is no special single-queue form")
	require.Equal(t, 1.0, q.PriorityFactor)
	require.Equal(t, "regatta-team-a-1", q.JobSetId)
	require.Equal(t, "team-a-", q.Entry)
	require.Equal(t, load.Schedule{}, q.Schedule, "no arrival means everything at once")
	require.Equal(t, 10, q.Jobs[0].Count, "a share of 1 is every job")
	require.NotNil(t, q.Jobs[0].ResolvedSpec)
	require.Equal(t, filepath.Join(dir, spec), q.Jobs[0].JobSpec, "job-spec paths resolve against the scenario directory")
	require.Equal(t, 10, l.TotalJobs())
	require.Equal(t, []string{"team-a-1"}, l.Names())
}

func TestNormalise_EntryExpandsToExactCounts(t *testing.T) {
	dir := t.TempDir()
	sleep, gpu := writeJobSpec(t, dir, "sleep.yaml"), writeJobSpec(t, dir, "gpu.yaml")
	l := Load{Entries: []QueueEntry{{
		Prefix: "tenant-", Count: 1000, TotalJobs: 100000,
		Distribution: load.Dist{Type: load.LogNormal, Mu: ptr(4.6), Sigma: ptr(1.0)},
		Targets:      []string{"gpu"},
		Jobs:         []JobRef{{JobSpec: sleep, Share: 0.9}, {JobSpec: gpu, Share: 0.1}},
	}}}

	require.NoError(t, l.normalize(dir, map[string]bool{"gpu": true}))

	require.Len(t, l.Queues, 1000)
	require.Equal(t, 100000, l.TotalJobs(), "counts sum exactly to totalJobs")
	require.Equal(t, "tenant-0001", l.Queues[0].Name, "zero-padded to the width of the count")
	require.Equal(t, "tenant-1000", l.Queues[999].Name)
	seen := map[string]bool{}
	for _, q := range l.Queues {
		require.False(t, seen[q.Name])
		seen[q.Name] = true
		require.Equal(t, "tenant-", q.Entry)
		require.Equal(t, []string{"gpu"}, q.Targets)
		require.Equal(t, "regatta-"+q.Name, q.JobSetId)
		require.GreaterOrEqual(t, q.TotalJobs(), 1, "minJobsPerQueue defaults to 1")
	}
	busiest := l.Queues[36] // mu = ln(100): the mode sits around queue 37
	require.Greater(t, busiest.TotalJobs(), l.Queues[999].TotalJobs())
	require.Len(t, busiest.Jobs, 2, "the 90/10 mix shows up in a big queue")
	require.InDelta(t, 0.9*float64(busiest.TotalJobs()), busiest.Jobs[0].Count, 1)
}

func TestNormalise_SmallQueuesKeepAtLeastOneJobEntry(t *testing.T) {
	dir := t.TempDir()
	sleep, gpu := writeJobSpec(t, dir, "sleep.yaml"), writeJobSpec(t, dir, "gpu.yaml")
	zero := 0
	l := Load{Entries: []QueueEntry{{
		Prefix: "q", Count: 3, TotalJobs: 3, MinJobsPerQueue: &zero,
		Jobs: []JobRef{{JobSpec: sleep, Share: 0.5}, {JobSpec: gpu, Share: 0.5}},
	}}}
	require.NoError(t, l.normalize(dir, nil))
	require.Equal(t, "q1", l.Queues[0].Name, "a count under 10 pads to one digit")
	require.Equal(t, 3, l.TotalJobs())
	for _, q := range l.Queues {
		require.NotEmpty(t, q.Jobs, "a queue with one job keeps one job entry, never an empty mix")
		require.Equal(t, 1, q.TotalJobs())
	}
}

func TestNormalise_QueueNamesAreUniqueAcrossEntries(t *testing.T) {
	dir := t.TempDir()
	spec := writeJobSpec(t, dir, "sleep.yaml")
	jobs := []JobRef{{JobSpec: spec, Share: 1}}
	l := Load{Entries: []QueueEntry{
		{Prefix: "tenant-", Count: 10, TotalJobs: 10, Jobs: jobs}, // tenant-01 ... tenant-10
		{Prefix: "tenant-0", Count: 1, TotalJobs: 1, Jobs: jobs},  // tenant-01
	}}
	require.ErrorContains(t, l.normalize(dir, nil), `"tenant-01" is declared more than once`)
}

func TestNormalise_Validation(t *testing.T) {
	dir := t.TempDir()
	spec := writeJobSpec(t, dir, "sleep.yaml")
	entry := func(mutate func(e *QueueEntry)) Load {
		e := QueueEntry{Prefix: "p-", Count: 4, TotalJobs: 40, Jobs: []JobRef{{JobSpec: spec, Share: 1}}}
		if mutate != nil {
			mutate(&e)
		}
		return Load{Entries: []QueueEntry{e}}
	}
	tests := map[string]struct {
		load    Load
		wantErr string
	}{
		"no entries":                {Load{}, "at least one entry"},
		"no prefix":                 {entry(func(e *QueueEntry) { e.Prefix = "" }), "prefix is required"},
		"negative count":            {entry(func(e *QueueEntry) { e.Count = -1 }), "count must be at least 1"},
		"totalJobs zero":            {entry(func(e *QueueEntry) { e.TotalJobs = 0 }), "totalJobs must be at least 1"},
		"total below the minimum":   {entry(func(e *QueueEntry) { e.TotalJobs = 3 }), "cannot give each"},
		"no jobs":                   {entry(func(e *QueueEntry) { e.Jobs = nil }), "at least one entry"},
		"job without a share":       {entry(func(e *QueueEntry) { e.Jobs[0].Share = 0 }), "share greater than 0"},
		"shares not summing to one": {entry(func(e *QueueEntry) { e.Jobs[0].Share = 0.7 }), "must sum to 1"},
		"missing job spec":          {entry(func(e *QueueEntry) { e.Jobs[0].JobSpec = "missing.yaml" }), "missing.yaml"},
		"unknown distribution":      {entry(func(e *QueueEntry) { e.Distribution = load.Dist{Type: "zipf"} }), "unknown distribution"},
		"lognormal without sigma":   {entry(func(e *QueueEntry) { e.Distribution = load.Dist{Type: load.LogNormal, Mu: ptr(1.0)} }), "mu and sigma"},
		"distribution on one queue": {entry(func(e *QueueEntry) { e.Count = 1; e.Distribution = load.Dist{Type: load.Gaussian} }), "nothing to split"},
		"negative minJobsPerQueue":  {entry(func(e *QueueEntry) { n := -1; e.MinJobsPerQueue = &n }), "must not be negative"},
		"negative priorityFactor":   {entry(func(e *QueueEntry) { e.PriorityFactor = -1 }), "priorityFactor"},
		"invalid generated name":    {entry(func(e *QueueEntry) { e.Prefix = "has space-" }), "not a valid queue name"},
		"unknown target":            {entry(func(e *QueueEntry) { e.Targets = []string{"nope"} }), "not an execution target"},
		"duration -1 alone":         {entry(func(e *QueueEntry) { e.Arrival = &Arrival{Duration: "-1"} }), "both totalJobs and arrival.duration to be -1"},
		"totalJobs -1 alone":        {entry(func(e *QueueEntry) { e.TotalJobs = -1 }), "both totalJobs and arrival.duration to be -1"},
		"lognormal over time":       {entry(func(e *QueueEntry) { e.Arrival = &Arrival{Shape: "lognormal", Duration: "1m"} }), "only available over queues"},
		"unknown arrival shape":     {entry(func(e *QueueEntry) { e.Arrival = &Arrival{Shape: "zipf", Duration: "1m"} }), "unknown shape"},
		"gaussian needs a duration": {entry(func(e *QueueEntry) { e.Arrival = &Arrival{Shape: "gaussian"} }), "needs a duration"},
		"bad duration":              {entry(func(e *QueueEntry) { e.Arrival = &Arrival{Duration: "soon"} }), "duration"},
		"zero step":                 {entry(func(e *QueueEntry) { e.Arrival = &Arrival{Duration: "1m", Step: "0s"} }), "step must be positive"},
		"mean on a uniform arrival": {entry(func(e *QueueEntry) { e.Arrival = &Arrival{Duration: "1m", Mean: "30s"} }), "only applies to shape gaussian"},
		"mean outside the duration": {entry(func(e *QueueEntry) { e.Arrival = &Arrival{Shape: "gaussian", Duration: "1m", Mean: "5m"} }), "outside the range"},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			l := tc.load
			err := l.normalize(dir, map[string]bool{"gpu": true})
			require.Error(t, err)
			require.ErrorContains(t, err, tc.wantErr)
		})
	}
}

func TestArrival_Resolve(t *testing.T) {
	t.Run("nil submits everything at once", func(t *testing.T) {
		s, err := (*Arrival)(nil).resolve()
		require.NoError(t, err)
		require.Equal(t, load.Schedule{}, s)
	})

	t.Run("duration zero", func(t *testing.T) {
		s, err := (&Arrival{Duration: "0"}).resolve()
		require.NoError(t, err)
		require.Equal(t, time.Duration(0), s.Duration)
	})

	t.Run("uniform with the default step", func(t *testing.T) {
		s, err := (&Arrival{Duration: "10m"}).resolve()
		require.NoError(t, err)
		require.Equal(t, 10*time.Minute, s.Duration)
		require.Equal(t, 10*time.Second, s.Step)
		require.Equal(t, load.Uniform, s.Dist.EffectiveType())
	})

	t.Run("gaussian mean and stddev become seconds", func(t *testing.T) {
		s, err := (&Arrival{Shape: "gaussian", Duration: "10m", Step: "5s", Mean: "2m", StdDev: "30s"}).resolve()
		require.NoError(t, err)
		require.Equal(t, 5*time.Second, s.Step)
		require.Equal(t, 120.0, *s.Dist.Mean)
		require.Equal(t, 30.0, *s.Dist.StdDev)
	})
}

func TestLoad_QueuesForTarget(t *testing.T) {
	l := Load{Queues: []QueueLoad{
		{Name: "everywhere"},
		{Name: "gpu-only", Targets: []string{"gpu"}},
		{Name: "cpu-only", Targets: []string{"cpu"}},
	}}
	names := func(qs []QueueLoad) []string {
		out := []string{}
		for _, q := range qs {
			out = append(out, q.Name)
		}
		return out
	}
	require.Equal(t, []string{"everywhere", "gpu-only"}, names(l.QueuesForTarget("gpu")))
	require.Equal(t, []string{"everywhere", "cpu-only"}, names(l.QueuesForTarget("cpu")))
	require.Equal(t, []string{"everywhere"}, names(l.QueuesForTarget("other")))
}

func continuousEntry(spec string, mutate func(e *QueueEntry)) Load {
	e := QueueEntry{
		Prefix: "steady-", Count: 4, TotalJobs: -1, JobsPerStep: 10,
		Arrival: &Arrival{Duration: "-1"},
		Jobs:    []JobRef{{JobSpec: spec, Share: 0.75}, {JobSpec: spec, Share: 0.25}},
	}
	if mutate != nil {
		mutate(&e)
	}
	return Load{Entries: []QueueEntry{e}}
}

func TestNormalise_ContinuousEntryBecomesRatesThatAddUpToJobsPerStep(t *testing.T) {
	dir := t.TempDir()
	spec := writeJobSpec(t, dir, "sleep.yaml")
	l := continuousEntry(spec, func(e *QueueEntry) {
		e.Distribution = load.Dist{Type: load.Gaussian}
	})

	require.NoError(t, l.normalize(dir, nil))

	require.True(t, l.Continuous())
	require.Equal(t, 0, l.TotalJobs(), "continuous queues add nothing to the bounded total")
	require.Len(t, l.Queues, 4)
	total, perJobShape := 0.0, [2]float64{}
	for _, q := range l.Queues {
		require.NotNil(t, q.Continuous)
		require.Empty(t, q.Jobs, "a continuous queue has rates, not counts")
		require.Equal(t, time.Minute, q.Continuous.Step, "the default step for continuous submission is a minute")
		require.Len(t, q.Continuous.Rates, 2)
		for i, r := range q.Continuous.Rates {
			require.NotNil(t, r.ResolvedSpec)
			total += r.PerStep
			perJobShape[i] += r.PerStep
		}
	}
	require.InDelta(t, 10.0, total, 1e-9, "the queues' rates add up to the entry's jobsPerStep")
	require.InDelta(t, 7.5, perJobShape[0], 1e-9, "and split 75/25 between the job shapes")
	require.InDelta(t, 2.5, perJobShape[1], 1e-9)
	require.Greater(t, l.Queues[1].Continuous.Rates[0].PerStep, l.Queues[0].Continuous.Rates[0].PerStep,
		"a gaussian over 4 queues puts more on the middle ones")
}

func TestNormalise_ContinuousStepIsConfigurable(t *testing.T) {
	dir := t.TempDir()
	spec := writeJobSpec(t, dir, "sleep.yaml")
	l := continuousEntry(spec, func(e *QueueEntry) { e.Arrival.Step = "30s" })
	require.NoError(t, l.normalize(dir, nil))
	require.Equal(t, 30*time.Second, l.Queues[0].Continuous.Step)
}

func TestNormalise_ContinuousValidation(t *testing.T) {
	dir := t.TempDir()
	spec := writeJobSpec(t, dir, "sleep.yaml")
	tests := map[string]struct {
		load    Load
		wantErr string
	}{
		"totalJobs -1 without a duration of -1": {continuousEntry(spec, func(e *QueueEntry) { e.Arrival = nil }), "both totalJobs and arrival.duration to be -1"},
		"duration -1 with a real totalJobs":     {continuousEntry(spec, func(e *QueueEntry) { e.TotalJobs = 100 }), "both totalJobs and arrival.duration to be -1"},
		"no jobsPerStep":                        {continuousEntry(spec, func(e *QueueEntry) { e.JobsPerStep = 0 }), "jobsPerStep greater than 0"},
		"negative jobsPerStep":                  {continuousEntry(spec, func(e *QueueEntry) { e.JobsPerStep = -2 }), "jobsPerStep greater than 0"},
		"minJobsPerQueue":                       {continuousEntry(spec, func(e *QueueEntry) { n := 1; e.MinJobsPerQueue = &n }), "minJobsPerQueue does not apply"},
		"gaussian shape":                        {continuousEntry(spec, func(e *QueueEntry) { e.Arrival.Shape = "gaussian" }), "uniform only"},
		"a mean":                                {continuousEntry(spec, func(e *QueueEntry) { e.Arrival.Mean = "1m" }), "only to shape gaussian"},
		"a zero step":                           {continuousEntry(spec, func(e *QueueEntry) { e.Arrival.Step = "0s" }), "step must be positive"},
		"an unparseable step":                   {continuousEntry(spec, func(e *QueueEntry) { e.Arrival.Step = "soon" }), "step"},
		"jobsPerStep on a bounded entry": {Load{Entries: []QueueEntry{{
			Prefix: "p-", TotalJobs: 10, JobsPerStep: 5, Jobs: []JobRef{{JobSpec: spec, Share: 1}},
		}}}, "jobsPerStep only applies to continuous"},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			l := tc.load
			err := l.normalize(dir, nil)
			require.Error(t, err)
			require.ErrorContains(t, err, tc.wantErr)
		})
	}
}

func TestNormalise_BoundedAndContinuousEntriesCanShareAFile(t *testing.T) {
	dir := t.TempDir()
	spec := writeJobSpec(t, dir, "sleep.yaml")
	l := continuousEntry(spec, nil)
	l.Entries = append(l.Entries, QueueEntry{Prefix: "batch-", TotalJobs: 50, Jobs: []JobRef{{JobSpec: spec, Share: 1}}})

	require.NoError(t, l.normalize(dir, nil))
	require.Len(t, l.Queues, 5)
	require.True(t, l.Continuous())
	require.Equal(t, 50, l.TotalJobs(), "only the bounded entry counts")
	require.Nil(t, l.Queues[4].Continuous)
	require.Equal(t, "batch-1", l.Queues[4].Name)
}

func TestLoadScenario_ReportInterval(t *testing.T) {
	dir := t.TempDir()
	writeJobSpec(t, dir, "sleep.yaml")
	scenario := func(metrics string) string {
		path := filepath.Join(dir, "scenario.yaml")
		body := "metrics:\n" + metrics + "\nexecutionTargets:\n  - name: t\n    type: cluster\n    cluster:\n      kubeconfig: kube\n" +
			"load:\n  queues:\n    - prefix: q-\n      totalJobs: 1\n      jobs:\n        - jobSpec: sleep.yaml\n          share: 1\n"
		require.NoError(t, os.WriteFile(path, []byte(body), 0o600))
		return path
	}

	s, err := LoadScenario(scenario("  prometheus: http://localhost:9090"))
	require.NoError(t, err)
	require.Equal(t, DefaultReportInterval, s.Metrics.ReportIntervalDuration, "defaults to 120s")
	require.Equal(t, 120*time.Second, DefaultReportInterval)

	s, err = LoadScenario(scenario("  reportInterval: 45s"))
	require.NoError(t, err)
	require.Equal(t, 45*time.Second, s.Metrics.ReportIntervalDuration)

	_, err = LoadScenario(scenario("  reportInterval: soon"))
	require.ErrorContains(t, err, "metrics.reportInterval")
	_, err = LoadScenario(scenario("  reportInterval: 0s"))
	require.ErrorContains(t, err, "must be positive")
}

func TestLoadScenario_TargetsAndClusters(t *testing.T) {
	dir := t.TempDir()
	writeJobSpec(t, dir, "sleep.yaml")
	scenario := func(targets, queueTargets string) string {
		path := filepath.Join(dir, "scenario.yaml")
		body := "executionTargets:\n" + targets + "load:\n  queues:\n    - prefix: q-\n      totalJobs: 1\n" + queueTargets +
			"      jobs:\n        - jobSpec: sleep.yaml\n          share: 1\n"
		require.NoError(t, os.WriteFile(path, []byte(body), 0o600))
		return path
	}
	target := func(name, clusterFields string) string {
		return "  - name: " + name + "\n    type: cluster\n    cluster:\n" + clusterFields
	}
	const kindA = "      kubeconfig: kube-a\n      kind: true\n"
	const kindB = "      kubeconfig: kube-b\n      kind: true\n"

	t.Run("a cluster name defaults to the target's name", func(t *testing.T) {
		s, err := LoadScenario(scenario(target("gpu-cluster", kindA), ""))
		require.NoError(t, err)
		require.Equal(t, "gpu-cluster", s.ExecutionTargets[0].Cluster.Name)
	})
	t.Run("an explicit cluster name is kept", func(t *testing.T) {
		s, err := LoadScenario(scenario(target("gpu", kindA+"      name: armada-cluster-1\n"), ""))
		require.NoError(t, err)
		require.Equal(t, "armada-cluster-1", s.ExecutionTargets[0].Cluster.Name)
	})
	t.Run("two targets cannot share a kubeconfig", func(t *testing.T) {
		_, err := LoadScenario(scenario(target("a", kindA)+target("b", kindA+"      name: other\n"), ""))
		require.ErrorContains(t, err, `uses the same cluster as target "a"`)
		require.ErrorContains(t, err, "kubeconfig")
	})
	t.Run("two targets cannot share a cluster name", func(t *testing.T) {
		_, err := LoadScenario(scenario(target("a", kindA+"      name: shared\n")+target("b", kindB+"      name: shared\n"), ""))
		require.ErrorContains(t, err, `uses the same cluster as target "a"`)
		require.ErrorContains(t, err, "cluster name shared")
	})
	t.Run("targets on different clusters are accepted", func(t *testing.T) {
		_, err := LoadScenario(scenario(target("a", kindA)+target("b", kindB), "      targets:\n        - a\n"))
		require.NoError(t, err)
	})
	t.Run("a queue's targets need executors that report the target label", func(t *testing.T) {
		external := "      kubeconfig: kube-ext\n"
		_, err := LoadScenario(scenario(target("ext", external), "      targets:\n        - ext\n"))
		require.ErrorContains(t, err, "readinessSelectsTarget")

		_, err = LoadScenario(scenario(target("ext", external+"      readinessSelectsTarget: true\n"), "      targets:\n        - ext\n"))
		require.NoError(t, err)

		_, err = LoadScenario(scenario(target("ext", external), ""))
		require.NoError(t, err, "a queue with no targets needs nothing")
	})
}

func TestLoadScenarioTargets_DoesNotNeedTheJobSpecFiles(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "scenario.yaml")
	body := "executionTargets:\n  - name: gpu\n    type: cluster\n    cluster:\n      kubeconfig: kube-a\n" +
		"load:\n  queues:\n    - prefix: q-\n      totalJobs: 1\n      jobs:\n        - jobSpec: moved-away.yaml\n          share: 1\n"
	require.NoError(t, os.WriteFile(path, []byte(body), 0o600))

	_, err := LoadScenario(path)
	require.ErrorContains(t, err, "moved-away.yaml", "loading the whole scenario needs the job specs")

	scenario, err := LoadScenarioTargets(path)
	require.NoError(t, err, "teardown still works after the job templates are gone")
	require.Equal(t, "gpu", scenario.ExecutionTargets[0].Name)
	require.Equal(t, filepath.Join(dir, "kube-a"), scenario.ExecutionTargets[0].Cluster.Kubeconfig, "paths are resolved as usual")
	require.Equal(t, "gpu", scenario.ExecutionTargets[0].Cluster.Name)
}
