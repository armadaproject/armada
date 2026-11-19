package replay

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
)

func TestCompareCapacityAndCordon(t *testing.T) {
	window := Window{EndSeconds: 180, CycleSeconds: 1}
	for _, tc := range []struct {
		name   string
		farm   string
		window Window
		cordon bool
	}{
		{name: "capacity", farm: "farm-two.yaml", window: window},
		{name: "cordon", farm: "farm-one.yaml", window: Window{EndSeconds: 90, CycleSeconds: 1}, cordon: true},
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
			a, aAttempts, aQueue := runTestCandidate(t, base, "a", spec, input)
			spec.Farm = tc.farm
			if tc.cordon {
				spec.Queues[1].Cordoned = true
			}
			b, bAttempts, bQueue := runTestCandidate(t, base, "b", spec, input)
			report := compareResults(tc.name, a, b)
			require.Equal(t, 1, report.Version)
			checkAttemptCPU(t, aAttempts, *report.Allocated.A)
			checkAttemptCPU(t, bAttempts, *report.Allocated.B)
			require.Equal(t, []queueIntervalRow{{JobID: "first"}, {JobID: "second", EndSeconds: 60}}, aQueue)

			if tc.cordon {
				require.Equal(t, values(360, 240), report.Allocated)
				require.Equal(t, []queueIntervalRow{{JobID: "first", EndSeconds: 90, Censored: true}, {JobID: "second"}}, bQueue)
				require.Len(t, bAttempts, 1)
				require.Equal(t, "second", bAttempts[0].JobID)
				require.Equal(t, values(360, 360), report.Available)
				require.Equal(t, values(1, 2.0/3), report.Utilisation)
				require.Equal(t, waitValues(60, 0), report.Groups[1].FirstAllocationWaitSeconds.P50)
				zero := int64(0)
				require.Equal(t, difference(&zero, nil), report.Groups[0].FirstAllocationWaitSeconds.P95)
				require.Equal(t, values(1, 0), report.Groups[0].Allocated)
				require.Equal(t, values(1, 0), report.Groups[0].Completed)
				require.Equal(t, values(0, 1), report.Groups[0].QueuedAtCutoff)
				require.Equal(t, values(0, 1), report.Groups[1].Completed)
				require.Equal(t, values(1, 0), report.Groups[1].RunningAtCutoff)
				return
			}
			require.Equal(t, values(480, 480), report.Allocated)
			require.Equal(t, []queueIntervalRow{{JobID: "first"}, {JobID: "second"}}, bQueue)
			require.Len(t, aAttempts, 2)
			require.Equal(t, int64(60), aAttempts[1].AllocatedSeconds)
			require.Equal(t, "second", aAttempts[1].JobID)
			require.Equal(t, values(720, 1440), report.Available)
			require.Equal(t, values(2.0/3, 1.0/3), report.Utilisation)
			require.Equal(t, waitPercentiles{P50: waitValues(0, 0), P95: waitValues(60, 0), P99: waitValues(60, 0)}, report.Groups[0].FirstAllocationWaitSeconds)
			require.Equal(t, values(2, 2), report.Groups[0].Completed)
		})
	}
}

func runTestCandidate(t *testing.T, base, id string, spec candidate, input candidateInput) (*runResult, []attemptRow, []queueIntervalRow) {
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
	broken.Version = 2
	require.ErrorContains(t, broken.validate(c, digest(raw)), "worker result protocol version 2 is incompatible with version 1")
	broken = result
	broken.Outcomes = broken.Outcomes[:1]
	require.ErrorContains(t, broken.validate(c, digest(raw)), "missing 1 logical outcomes")
	broken = result
	broken.Window.EndSeconds++
	require.ErrorContains(t, broken.validate(c, digest(raw)), "incompatible run")
	broken = result
	broken.AllocatedCPUSeconds = nil
	require.ErrorContains(t, broken.validate(c, digest(raw)), "invalid CPU accounting")
	require.NoError(t, verifyObservations(dir))
	attempts := readTestParquet[attemptRow](t, filepath.Join(dir, attemptsFile))
	queue := readTestParquet[queueIntervalRow](t, filepath.Join(dir, queueIntervalsFile))
	slices.SortFunc(queue, func(a, b queueIntervalRow) int { return strings.Compare(a.JobID, b.JobID) })
	return &result, attempts, queue
}

func readTestParquet[T any](t *testing.T, path string) []T {
	t.Helper()
	f, err := local.NewLocalFileReader(path)
	require.NoError(t, err)
	defer f.Close()
	p, err := reader.NewParquetReader(f, new(T), 1)
	require.NoError(t, err)
	defer p.ReadStop()
	rows := make([]T, p.GetNumRows())
	require.NoError(t, p.Read(&rows))
	return rows
}

func checkAttemptCPU(t *testing.T, attempts []attemptRow, expected float64) {
	t.Helper()
	var allocated float64
	for _, attempt := range attempts {
		require.JSONEq(t, `{"cpu":"4"}`, attempt.Requests)
		require.NotEmpty(t, attempt.RunID)
		require.NotEmpty(t, attempt.Node)
		require.Equal(t, int64(1), attempt.Ordinal)
		allocated += 4 * float64(attempt.EndSeconds-attempt.AllocatedSeconds)
	}
	require.Equal(t, expected, allocated)
}

func TestEmptyAttemptsStillWriteReadableParquet(t *testing.T) {
	path := filepath.Join(t.TempDir(), attemptsFile)
	output, err := createParquet(path, new(attemptRow))
	require.NoError(t, err)
	require.NoError(t, output.close())
	require.Empty(t, readTestParquet[attemptRow](t, path))
}

func TestVerifyObservationsRequiresBothReadableParquetFiles(t *testing.T) {
	dir := t.TempDir()
	attemptPath := filepath.Join(dir, attemptsFile)
	attempts, err := createParquet(attemptPath, new(attemptRow))
	require.NoError(t, err)
	require.NoError(t, attempts.close())
	require.ErrorIs(t, verifyObservations(dir), os.ErrNotExist)
	queuePath := filepath.Join(dir, queueIntervalsFile)
	queue, err := createParquet(queuePath, new(queueIntervalRow))
	require.NoError(t, err)
	require.NoError(t, queue.close())
	require.NoError(t, verifyObservations(dir))
	require.NoError(t, os.WriteFile(attemptPath, []byte("PAR1X\x01\x00\x00\x00PAR1"), 0o644))
	require.ErrorContains(t, verifyObservations(dir), "attempts.parquet: invalid Parquet footer")
}

func TestObservationSchemaAcceptsReorderingAndExtraFields(t *testing.T) {
	type reorderedQueueRow struct {
		Annotations struct {
			Note string `parquet:"name=note, type=BYTE_ARRAY, convertedtype=UTF8"`
		} `parquet:"name=annotations"`
		Censored bool   `parquet:"name=censored, type=BOOLEAN"`
		End      int64  `parquet:"name=end_at_s, type=INT64"`
		Queued   int64  `parquet:"name=queued_at_s, type=INT64"`
		JobID    string `parquet:"name=logical_job_id, type=BYTE_ARRAY, convertedtype=UTF8"`
	}
	path := filepath.Join(t.TempDir(), queueIntervalsFile)
	out, err := createParquet(path, new(reorderedQueueRow))
	require.NoError(t, err)
	require.NoError(t, out.close())
	require.NoError(t, checkParquetSchema(path, new(queueIntervalRow)))
}

func TestObservationSchemaRejectsWrongPhysicalType(t *testing.T) {
	type queueWithFractionalTimes struct {
		JobID    string  `parquet:"name=logical_job_id, type=BYTE_ARRAY, convertedtype=UTF8"`
		Queued   float64 `parquet:"name=queued_at_s, type=DOUBLE"`
		End      int64   `parquet:"name=end_at_s, type=INT64"`
		Censored bool    `parquet:"name=censored, type=BOOLEAN"`
	}
	path := filepath.Join(t.TempDir(), queueIntervalsFile)
	out, err := createParquet(path, new(queueWithFractionalTimes))
	require.NoError(t, err)
	require.NoError(t, out.close())
	require.ErrorContains(t, checkParquetSchema(path, new(queueIntervalRow)), `Parquet column "queued_at_s" has type DOUBLE, expected INT64`)
}

func TestObservationsRejectLegacyParquetColumns(t *testing.T) {
	type legacyQueueRow struct {
		JobID    string `parquet:"name=logical_job_id, type=BYTE_ARRAY, convertedtype=UTF8"`
		Queued   int64  `parquet:"name=queued_at_us, type=INT64"`
		End      int64  `parquet:"name=end_at_us, type=INT64"`
		Censored bool   `parquet:"name=censored, type=BOOLEAN"`
	}
	dir := t.TempDir()
	attempts, err := createParquet(filepath.Join(dir, attemptsFile), new(attemptRow))
	require.NoError(t, err)
	require.NoError(t, attempts.close())
	queue, err := createParquet(filepath.Join(dir, queueIntervalsFile), new(legacyQueueRow))
	require.NoError(t, err)
	require.NoError(t, queue.close())
	require.ErrorContains(t, verifyObservations(dir), `missing Parquet column "queued_at_s"`)
}

func TestValidateChangeRejectsUndeclaredInputs(t *testing.T) {
	a := candidateRun{binarySHA256: "binary-a", input: candidateInput{
		Config: []byte("same-config"), Farm: []byte("farm-a"), Queues: []queueSpec{{Name: "q", Weight: 1}},
	}}
	b := a
	b.input.Farm = []byte("farm-b")
	require.NoError(t, validateChange("capacity", a, b))
	require.ErrorContains(t, validateChange("none", a, b), "do not match")
	binaryB := b
	binaryB.binarySHA256 = "binary-b"
	for _, mode := range []string{"none", "capacity", "queue_cordon"} {
		require.ErrorContains(t, validateChange(mode, a, binaryB), "requires the same executable")
	}
	require.ErrorContains(t, validateChange("binary", a, a), "unknown experiment change")
	b.input.Config = []byte("other-config")
	require.ErrorContains(t, validateChange("capacity", a, b), "scheduler_config differs")
}

func TestCandidateProcessFailuresKeepDiagnostics(t *testing.T) {
	for _, tc := range []struct {
		name, script, message string
		timeout               time.Duration
	}{
		{"exit", "echo solver-failed >&2; exit 17", "exit status 17", time.Second},
		{"deadline", "echo solver-failed >&2; exec sleep 5", "context deadline exceeded", 200 * time.Millisecond},
		{"fifo", "echo solver-failed >&2; mkfifo \"$3\"", "expected a regular file", time.Second},
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

func TestStrictJSONRejectsAmbiguousWindow(t *testing.T) {
	var window Window
	require.ErrorContains(t, strictJSON([]byte(`{"end_s":180,"END_S":90}`), &window), "unknown field")
	require.ErrorContains(t, strictJSON([]byte(`{"end_s":180,"end_s":90}`), &window), "duplicate field")
}

func TestWaitPercentilesUseAllocatedArrivalsDuringMeasurement(t *testing.T) {
	zero := int64(0)
	arrivals := []outcome{
		{Queue: "q", SubmittedSeconds: -10, ObservedSeconds: 300, FirstAllocationSeconds: &zero, State: Completed},
		{Queue: "q", SubmittedSeconds: 0, ObservedSeconds: 500, State: Queued},
	}
	for wait := int64(100); wait >= 1; wait-- {
		arrivals = append(arrivals, outcome{
			Queue: "q", SubmittedSeconds: 0, ObservedSeconds: 300, FirstAllocationSeconds: &wait, State: Completed,
		})
	}
	cpu := float64(1)
	run := &runResult{Window: Window{EndSeconds: 500}, Outcomes: arrivals, AllocatedCPUSeconds: &cpu, AvailableCPUSeconds: &cpu}
	group := compareResults("none", run, run).Groups[0]
	require.Equal(t, values(101, 101), group.Arrivals)
	require.Equal(t, values(100, 100), group.Allocated)
	require.Equal(t, values(1, 1), group.Unallocated)
	require.Equal(t, values(1, 1), group.CarryIn)
	require.Equal(t, waitPercentiles{P50: waitValues(50, 50), P95: waitValues(95, 95), P99: waitValues(99, 99)}, group.FirstAllocationWaitSeconds)
}

func TestReportKeepsExactSeconds(t *testing.T) {
	a, b := maxDurationSeconds-2, maxDurationSeconds-1
	cpu := float64(1)
	run := func(wait *int64) *runResult {
		return &runResult{
			Window: Window{EndSeconds: b + 1}, AllocatedCPUSeconds: &cpu, AvailableCPUSeconds: &cpu,
			Outcomes: []outcome{{FirstAllocationSeconds: wait, ObservedSeconds: b, State: Completed}},
		}
	}
	report := compareResults("capacity", run(&a), run(&b))
	raw, err := json.Marshal(report)
	require.NoError(t, err)
	require.Contains(t, string(raw), `"p50":{"a":9223372034,"b":9223372035,"delta":1}`)
	var table bytes.Buffer
	require.NoError(t, report.print(&table))
	require.Contains(t, table.String(), "9223372034")
	require.Contains(t, table.String(), "9223372035")
}

func waitValues(a, b int64) metric[int64] {
	return difference(&a, &b)
}

func TestWorkerRejectsUnsupportedInputProtocol(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "input.json")
	result := filepath.Join(dir, "run.json")
	require.NoError(t, os.WriteFile(input, []byte(`{"version":2,"candidate_id":"a"}`), 0o644))
	require.ErrorContains(t, RunComparisonCandidate(t.Context(), input, result), "worker input protocol version 2 is incompatible with version 1")
	require.NoFileExists(t, result)
}

func TestComparisonRequiresWholeSecondFields(t *testing.T) {
	var window Window
	require.Error(t, strictJSON([]byte(`{"end_s":1.5}`), &window))
	require.ErrorContains(t, strictJSON([]byte(`{"end_us":1000000}`), &window), "unknown field")

	var job workloadJob
	require.Error(t, strictJSON([]byte(`{"useful_s":0.5}`), &job))
	require.ErrorContains(t, strictJSON([]byte(`{"useful_us":1000000}`), &job), "unknown field")
}
