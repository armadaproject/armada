package replay

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCompareCapacityAndCordon(t *testing.T) {
	window := Window{EndUS: 180_000_000, CycleUS: 1_000_000}
	for _, tc := range []struct {
		name   string
		farm   string
		window Window
		cordon bool
	}{
		{name: "capacity", farm: "farm-two.yaml", window: window},
		{name: "cordon", farm: "farm-one.yaml", window: Window{EndUS: 90_000_000, CycleUS: 1_000_000}, cordon: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base := filepath.Join("testdata", "compare")
			input, err := loadWorkload(filepath.Join(base, "workload.json"), tc.window)
			require.NoError(t, err)
			if tc.cordon {
				input.Jobs[0].Queue = "priority"
				input.Jobs[0].PriorityClass = "high"
			}
			exe, err := os.Executable()
			require.NoError(t, err)
			spec := candidate{Binary: exe, Farm: "farm-one.yaml", Config: "scheduler.yaml", Queues: []queueSpec{{Name: "shared", Weight: 1}}}
			if tc.cordon {
				spec.Queues = append(spec.Queues, queueSpec{Name: "priority", Weight: 1})
			}
			a := runTestCandidate(t, base, "a", spec, input)
			spec.Farm = tc.farm
			if tc.cordon {
				spec.Queues[1].Cordoned = true
			}
			b := runTestCandidate(t, base, "b", spec, input)
			report := compareResults(tc.name, nil, a, b)
			require.Len(t, report.Jobs, 2)
			if tc.cordon {
				require.Equal(t, values(360, 240), report.Allocated)
				require.Equal(t, values(360, 360), report.Available)
				require.Equal(t, values(1, 2.0/3), report.Utilisation)
				require.Equal(t, values(60_000_000, 0), report.Jobs[1].Delay)
				require.Nil(t, report.Jobs[0].Delay.B)
				require.Nil(t, report.Jobs[0].Delay.Delta)
				require.Equal(t, values(1, 0), report.Groups[0].Completed)
				require.Equal(t, values(0, 1), report.Groups[0].QueuedAtCutoff)
				require.Equal(t, values(0, 1), report.Groups[1].Completed)
				require.Equal(t, values(1, 0), report.Groups[1].RunningAtCutoff)
				return
			}
			require.Equal(t, values(480, 480), report.Allocated)
			require.Equal(t, values(720, 1440), report.Available)
			require.Equal(t, values(2.0/3, 1.0/3), report.Utilisation)
			require.Equal(t, values(0, 0), report.Jobs[0].Delay)
			require.Equal(t, values(60_000_000, 0), report.Jobs[1].Delay)
			require.Equal(t, values(2, 2), report.Groups[0].Completed)

		})
	}
}

func runTestCandidate(t *testing.T, base, id string, spec candidate, input candidateInput) *runResult {
	t.Helper()
	c, err := prepareCandidate(t.Context(), base, id, spec, input)
	require.NoError(t, err)
	raw, err := json.Marshal(c.input)
	require.NoError(t, err)
	dir := t.TempDir()
	inputPath, outputPath := filepath.Join(dir, "input.json"), filepath.Join(dir, "run.json")
	require.NoError(t, os.WriteFile(inputPath, raw, 0o644))
	require.NoError(t, RunComparisonCandidate(t.Context(), inputPath, outputPath))
	resultRaw, err := os.ReadFile(outputPath)
	require.NoError(t, err)
	var result runResult
	require.NoError(t, strictJSON(resultRaw, &result))
	require.NoError(t, result.validate(c, digest(raw)))
	broken := result
	broken.Outcomes = broken.Outcomes[:1]
	require.ErrorContains(t, broken.validate(c, digest(raw)), "missing 1 logical outcomes")
	broken = result
	broken.Window.EndUS++
	require.ErrorContains(t, broken.validate(c, digest(raw)), "incompatible run")
	return &result
}

func TestValidateChangeRejectsUndeclaredInputs(t *testing.T) {
	a := candidateRun{binarySHA256: "binary-a", input: candidateInput{
		Config: []byte("same-config"), Farm: []byte("farm-a"), Queues: []queueSpec{{Name: "q", Weight: 1}},
	}}
	b := a
	b.input.Farm = []byte("farm-b")
	require.NoError(t, validateChange("capacity", nil, a, b))
	b.binarySHA256 = "binary-b"
	require.ErrorContains(t, validateChange("capacity", nil, a, b), "do not match")
	b.input.Farm = a.input.Farm
	require.ErrorContains(t, validateChange("binary", &codeChange{Path: "short-job penalties", Behavior: "penalise"}, a, b), "supported path")
	require.NoError(t, validateChange("binary", &codeChange{Path: orderingPath, Behavior: "reverse priorities"}, a, b))
	b.input.Config = []byte("other-config")
	require.ErrorContains(t, validateChange("binary", &codeChange{Path: orderingPath, Behavior: "reverse priorities"}, a, b), "scheduler_config differs")
}

func TestCandidateProcessFailuresKeepDiagnostics(t *testing.T) {
	for _, tc := range []struct {
		name, script, message string
		timeout               time.Duration
	}{
		{"exit", "echo solver-failed >&2; exit 17", "exit status 17", time.Second},
		{"deadline", "echo solver-failed >&2; exec sleep 5", "context deadline exceeded", 200 * time.Millisecond},
		{"malformed", "echo solver-failed >&2; echo '{not JSON}' > \"$3\"", "candidate a result", time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			exe := filepath.Join(dir, "candidate")
			require.NoError(t, os.WriteFile(exe, []byte("#!/bin/sh\n"+tc.script+"\n"), 0o755))
			c := candidateRun{binary: exe, input: candidateInput{ID: "a"}}
			_, err := c.execute(context.Background(), dir, tc.timeout)
			require.ErrorContains(t, err, tc.message)
			log, err := os.ReadFile(filepath.Join(dir, "a", "process.log"))
			require.NoError(t, err)
			require.Contains(t, string(log), "solver-failed")
		})
	}
}
