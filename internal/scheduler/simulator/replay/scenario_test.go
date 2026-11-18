package replay_test

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xitongsys/parquet-go-source/local"
	parquetreader "github.com/xitongsys/parquet-go/reader"
	parquetwriter "github.com/xitongsys/parquet-go/writer"

	"github.com/armadaproject/armada/internal/scheduler/simulator/replay"
)

const (
	mixedScenario    = "testdata/mixed/scenario.json"
	measurementStart = int64(1735689600000000)
	minuteUS         = int64(60_000_000)
)

type recordedJob struct {
	ID          string `parquet:"name=logical_job_id, type=BYTE_ARRAY, convertedtype=UTF8"`
	SubmittedUS int64  `parquet:"name=submitted_at_us, type=INT64"`
	Queue       string `parquet:"name=queue, type=BYTE_ARRAY, convertedtype=UTF8"`
	JobSet      string `parquet:"name=job_set, type=BYTE_ARRAY, convertedtype=UTF8"`
	Priority    int64  `parquet:"name=queue_priority, type=INT64"`
	Class       string `parquet:"name=priority_class, type=BYTE_ARRAY, convertedtype=UTF8"`
	UsefulUS    int64  `parquet:"name=useful_work_us, type=INT64"`
	StartupUS   int64  `parquet:"name=startup_us, type=INT64"`
	Checkpoint  *int64 `parquet:"name=checkpoint_interval_us, type=INT64"`
	Requests    string `parquet:"name=requirements_json, type=BYTE_ARRAY, convertedtype=UTF8"`
}

type checkedLogical struct {
	ID          string `parquet:"name=logical_job_id, type=BYTE_ARRAY, convertedtype=UTF8"`
	State       string `parquet:"name=state, type=BYTE_ARRAY, convertedtype=UTF8"`
	ObservedUS  int64  `parquet:"name=observed_at_us, type=INT64"`
	UsefulUS    int64  `parquet:"name=useful_work_us, type=INT64"`
	RetryWaitUS int64  `parquet:"name=retry_wait_us, type=INT64"`
	Reason      string `parquet:"name=reason, type=BYTE_ARRAY, convertedtype=UTF8"`
	Censored    bool   `parquet:"name=censored, type=BOOLEAN"`
}

type checkedAttempt struct {
	Pool        string `parquet:"name=pool, type=BYTE_ARRAY, convertedtype=UTF8"`
	Node        string `parquet:"name=node, type=BYTE_ARRAY, convertedtype=UTF8"`
	ID          string `parquet:"name=logical_job_id, type=BYTE_ARRAY, convertedtype=UTF8"`
	Ordinal     int64  `parquet:"name=attempt, type=INT64"`
	AllocatedUS int64  `parquet:"name=allocated_at_us, type=INT64"`
	EndUS       int64  `parquet:"name=end_at_us, type=INT64"`
	RunID       string `parquet:"name=run_id, type=BYTE_ARRAY, convertedtype=UTF8"`
}

type manifestStatus struct {
	Status  string            `json:"status"`
	Failure string            `json:"failure"`
	Hashes  map[string]string `json:"output_sha256"`
}

func TestRecordedReplayReadsDuckDBAndPreservesRunSemantics(t *testing.T) {
	dir := t.TempDir()
	var baseline string
	var normalized []checkedAttempt
	for i := range 3 {
		run := filepath.Join(dir, fmt.Sprintf("run-%d", i))
		require.NoError(t, replay.RunScenario(t.Context(), mixedScenario, run, ""))
		status := readJSON[manifestStatus](t, filepath.Join(run, "run.json"))
		require.Equal(t, "complete", status.Status)
		require.Len(t, status.Hashes, 4)
		for name, want := range status.Hashes {
			data, err := os.ReadFile(filepath.Join(run, name))
			require.NoError(t, err)
			require.Equal(t, want, fmt.Sprintf("%x", sha256.Sum256(data)))
		}
		rows := readParquet[checkedLogical](t, filepath.Join(run, "logical_jobs.parquet"))
		require.Equal(t, []checkedLogical{
			{ID: "preemptor", State: "completed", ObservedUS: measurementStart + 40*minuteUS, UsefulUS: 10 * minuteUS},
			{ID: "gpu", State: "completed", ObservedUS: measurementStart + 205*minuteUS, UsefulUS: 180 * minuteUS, RetryWaitUS: 10 * minuteUS},
			{ID: "cpu", State: "running", ObservedUS: measurementStart + 210*minuteUS, UsefulUS: 180 * minuteUS, RetryWaitUS: 10 * minuteUS, Censored: true},
		}, rows)
		attempts := readParquet[checkedAttempt](t, filepath.Join(run, "attempts.parquet"))
		require.Len(t, attempts, 5)
		runIDs := make(map[string]bool)
		for j := range attempts {
			id := attempts[j].RunID
			require.NotEmpty(t, id)
			require.False(t, runIDs[id], "run IDs must be unique within one run")
			runIDs[id] = true
			// Production attaches opaque run IDs in unspecified order within a round.
			attempts[j].RunID = ""
		}
		bytes, err := os.ReadFile(filepath.Join(run, "summary.json"))
		require.NoError(t, err)
		if i == 0 {
			baseline, normalized = string(bytes), attempts
		} else {
			require.JSONEq(t, baseline, string(bytes))
			require.Equal(t, normalized, attempts)
		}
		summary := readJSON[struct {
			Work []struct {
				Resource    string `json:"resource"`
				Priority    string `json:"priority_class"`
				Allocation  string `json:"allocated_resource_us"`
				Startup     string `json:"startup_resource_us"`
				Retained    string `json:"retained_resource_us"`
				Lost        string `json:"lost_resource_us"`
				Provisional string `json:"provisional_resource_us"`
			} `json:"window_resource_time"`
		}](t, filepath.Join(run, "summary.json"))
		for _, row := range summary.Work {
			switch {
			case row.Priority == "low" && row.Resource == "nvidia.com/gpu":
				require.Equal(t, []string{"11700000000", "2700000000", "8100000000", "900000000", "0"},
					[]string{row.Allocation, row.Startup, row.Retained, row.Lost, row.Provisional})
			case row.Priority == "low" && row.Resource == "cpu":
				require.Equal(t, []string{"12000000000", "2700000000", "0", "1800000000", "7500000000"},
					[]string{row.Allocation, row.Startup, row.Retained, row.Lost, row.Provisional})
			}
		}
		require.Len(t, summary.Work, 5)
	}
}

func TestCapacityChangesFeasibilityWithoutDeletingOfferedWork(t *testing.T) {
	for _, tc := range []struct {
		name       string
		nodeCount  int
		nodeCPU    string
		queueLimit bool
		wantState  string
		wantReason string
	}{
		{"no matching node", 1, "4", false, "queued", "cannot place the job"},
		{"matching node", 1, "8", false, "completed", ""},
		{"queue cap below request", 8, "8", true, "queued", "exceeds queue/priority resource limit"},
		{"queue cap above request", 16, "8", true, "completed", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := copyScenario(t)
			writeInput(t, dir, [][]recordedJob{{cpuJob("fixed-demand", measurementStart)}})
			farm := fmt.Sprintf("schemaVersion: 1\nclusters:\n  - name: cap\n    pool: test-pool\n    nodeTemplates:\n      - number: %d\n        totalResources: { resources: { cpu: %q } }\n", tc.nodeCount, tc.nodeCPU)
			require.NoError(t, os.WriteFile(filepath.Join(dir, "farm.yaml"), []byte(farm), 0o644))
			if tc.queueLimit {
				updateScenario(t, dir, func(s map[string]any) {
					s["queues"] = []any{map[string]any{
						"name": "offered", "weight": 1,
						"priority_class_limits": map[string]any{"low": map[string]any{
							"maximumResourceFraction": map[string]float64{"cpu": 0.1},
						}},
					}}
				})
			}
			out := filepath.Join(dir, "out")
			require.NoError(t, replay.RunScenario(t.Context(), filepath.Join(dir, "scenario.json"), out, ""))
			rows := readParquet[checkedLogical](t, filepath.Join(out, "logical_jobs.parquet"))
			require.Len(t, rows, 1)
			require.Equal(t, "fixed-demand", rows[0].ID)
			require.Equal(t, tc.wantState, rows[0].State)
			require.Equal(t, 120*minuteUS, rows[0].UsefulUS)
			require.Equal(t, tc.wantState == "queued", rows[0].Censored)
			if tc.wantReason != "" {
				require.Contains(t, rows[0].Reason, tc.wantReason)
			} else {
				require.Empty(t, rows[0].Reason)
			}
		})
	}
}

func TestInvalidInputsAndCancellationLeaveIncompleteStatus(t *testing.T) {
	tests := []struct {
		name   string
		edit   func(*testing.T, string)
		cancel bool
		want   string
	}{
		{"late cross-partition duplicate", func(t *testing.T, dir string) {
			writeInput(t, dir, [][]recordedJob{{cpuJob("duplicate", measurementStart)}, {cpuJob("duplicate", measurementStart+minuteUS)}})
		}, false, "duplicate logical job \"duplicate\""},
		{"malformed job after cutoff", func(t *testing.T, dir string) {
			row := cpuJob("future", measurementStart+211*minuteUS)
			row.Class = "unknown"
			writeInput(t, dir, [][]recordedJob{{row}})
		}, false, "unknown priority class"},
		{"null queue policy", func(t *testing.T, dir string) {
			updateScenario(t, dir, func(s map[string]any) {
				s["queues"] = []any{map[string]any{"name": "offered", "weight": 1, "priority_class_limits": map[string]any{"low": nil}}}
			})
		}, false, "null limits"},
		{"malformed late page with matching digest", func(t *testing.T, dir string) {
			manifest := filepath.Join(dir, "workload", "manifest.json")
			m := readJSON[map[string]any](t, manifest)
			entry := m["partitions"].([]any)[2].(map[string]any)
			name := filepath.Join(dir, "workload", entry["path"].(string))
			data, err := os.ReadFile(name)
			require.NoError(t, err)
			// The first column page of the last partition starts after PAR1.
			for i := 4; i < 12; i++ {
				data[i] = 0xff
			}
			require.NoError(t, os.WriteFile(name, data, 0o644))
			entry["sha256"] = fmt.Sprintf("%x", sha256.Sum256(data))
			writeJSONFixture(t, manifest, m)
		}, false, "row group 1"},
		{"scratch budget exhausted", func(t *testing.T, dir string) {
			updateScenario(t, dir, func(s map[string]any) { s["scratch_bytes"] = 1 })
		}, false, "needs more than 1 bytes of scratch space"},
		{"absent executor timeout", func(t *testing.T, dir string) {
			editSchedulerConfig(t, dir, func(config string) string {
				return strings.ReplaceAll(config, "executorTimeout: 15m\n", "")
			})
		}, false, "executorTimeout must be positive"},
		{"timed default toleration", func(t *testing.T, dir string) {
			editSchedulerConfig(t, dir, func(config string) string {
				return strings.Replace(config, "  - name: test-pool\n", "  - name: test-pool\n    jobDefaults:\n      tolerations:\n        - {key: temporary, operator: Exists, effect: NoExecute, tolerationSeconds: 1}\n", 1)
			})
		}, false, "timed tolerations"},
		{"overlapping away pools", func(t *testing.T, dir string) {
			editSchedulerConfig(t, dir, func(config string) string {
				return strings.Replace(config, "  - name: test-pool\n", "  - name: test-pool\n    awayPools: [other]\n  - name: other\n", 1)
			})
		}, false, "away-pool placement"},
		{"oversize page rows", func(t *testing.T, dir string) {
			rows := make([]recordedJob, 1025)
			for i := range rows {
				rows[i] = cpuJob(fmt.Sprintf("j-%04d", i), measurementStart)
			}
			writeInput(t, dir, [][]recordedJob{rows})
		}, false, "invalid page value count 1025"},
		{"already canceled", nil, true, "context canceled"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir := copyScenario(t)
			if tc.edit != nil {
				tc.edit(t, dir)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if tc.cancel {
				cancel()
			}
			out := filepath.Join(dir, "out")
			err := replay.RunScenario(ctx, filepath.Join(dir, "scenario.json"), out, "")
			require.ErrorContains(t, err, tc.want)
			status := readJSON[manifestStatus](t, filepath.Join(out, "run.json"))
			require.Equal(t, "incomplete", status.Status)
			require.Contains(t, status.Failure, tc.want)
			require.Empty(t, status.Hashes)
		})
	}
}

func cpuJob(id string, submitted int64) recordedJob {
	return recordedJob{
		ID: id, SubmittedUS: submitted, Queue: "offered", Class: "low", UsefulUS: 120 * minuteUS,
		Requests: `{"resourceRequirements":{"requests":{"cpu":"8"}}}`,
	}
}

func copyScenario(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, os.CopyFS(dir, os.DirFS("testdata/mixed")))
	return dir
}

func writeInput(t *testing.T, dir string, partitions [][]recordedJob) {
	t.Helper()
	entries := make([]any, 0, len(partitions))
	for i, rows := range partitions {
		name := fmt.Sprintf("go-%d.parquet", i)
		path := filepath.Join(dir, "workload", name)
		file, err := os.Create(path)
		require.NoError(t, err)
		pw, err := parquetwriter.NewParquetWriterFromWriter(file, new(recordedJob), 1)
		require.NoError(t, err)
		for _, row := range rows {
			require.NoError(t, pw.Write(row))
		}
		require.NoError(t, pw.WriteStop())
		require.NoError(t, file.Close())
		data, err := os.ReadFile(path)
		require.NoError(t, err)
		entry := map[string]any{"path": name, "rows": len(rows), "sha256": fmt.Sprintf("%x", sha256.Sum256(data))}
		if len(rows) != 0 {
			entry["min_submitted_at_us"] = rows[0].SubmittedUS
			entry["max_submitted_at_us"] = rows[len(rows)-1].SubmittedUS
		}
		entries = append(entries, entry)
	}
	writeJSONFixture(t, filepath.Join(dir, "workload", "manifest.json"), map[string]any{
		"schema_version": 1, "execution_model": "fixed-useful-work-v1", "dataset_id": "go-generated",
		"converter_version": "xitongsys", "work_estimation_method": "synthetic", "quality_counts": map[string]int{},
		"partitions": entries,
	})
}

func editSchedulerConfig(t *testing.T, dir string, edit func(string) string) {
	t.Helper()
	path := filepath.Join(dir, "scheduler.yaml")
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, []byte(edit(string(raw))), 0o644))
}

func updateScenario(t *testing.T, dir string, change func(map[string]any)) {
	t.Helper()
	path := filepath.Join(dir, "scenario.json")
	sc := readJSON[map[string]any](t, path)
	change(sc)
	writeJSONFixture(t, path, sc)
}

func writeJSONFixture(t *testing.T, path string, v any) {
	t.Helper()
	raw, err := json.Marshal(v)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, raw, 0o644))
}

func readJSON[T any](t *testing.T, path string) T {
	t.Helper()
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	var out T
	require.NoError(t, json.Unmarshal(raw, &out))
	return out
}

func readParquet[T any](t *testing.T, path string) []T {
	t.Helper()
	file, err := local.NewLocalFileReader(path)
	require.NoError(t, err)
	defer func() { require.NoError(t, file.Close()) }()
	r, err := parquetreader.NewParquetReader(file, new(T), 1)
	require.NoError(t, err)
	defer r.ReadStop()
	rows := make([]T, int(r.GetNumRows()))
	require.NoError(t, r.Read(&rows))
	return rows
}

func TestReplayTimeAloneRefillsRateLimiters(t *testing.T) {
	dir := copyScenario(t)
	source := filepath.Join(dir, "scheduler.yaml")
	raw, err := os.ReadFile(source)
	require.NoError(t, err)
	rates := strings.NewReplacer(
		`maximumSchedulingRate: "+Inf"`, "maximumSchedulingRate: 0.1",
		"maximumSchedulingBurst: 1000", "maximumSchedulingBurst: 1",
		`maximumPerQueueSchedulingRate: "+Inf"`, "maximumPerQueueSchedulingRate: 0.05",
		"maximumPerQueueSchedulingBurst: 1000", "maximumPerQueueSchedulingBurst: 1",
	)
	require.NoError(t, os.WriteFile(source, []byte(rates.Replace(string(raw))), 0o644))
	updateScenario(t, dir, func(s map[string]any) { s["window"].(map[string]any)["cycle_us"] = 10_000_000 })
	first := cpuJob("a", measurementStart)
	first.Requests = `{"resourceRequirements":{"requests":{"cpu":"500m"}}}`
	second := first
	second.ID = "b"
	writeInput(t, dir, [][]recordedJob{{first, second}})
	run := filepath.Join(dir, "out")
	require.NoError(t, replay.RunScenario(t.Context(), filepath.Join(dir, "scenario.json"), run, ""))
	rows := readParquet[checkedAttempt](t, filepath.Join(run, "attempts.parquet"))
	require.Len(t, rows, 2)
	require.Equal(t, []int64{measurementStart, measurementStart + 20_000_000}, []int64{rows[0].AllocatedUS, rows[1].AllocatedUS})
}

func TestReplayEmptyWorkloadProducesReadableOutputs(t *testing.T) {
	dir := copyScenario(t)
	writeInput(t, dir, [][]recordedJob{{}})
	out := filepath.Join(dir, "out")
	require.NoError(t, replay.RunScenario(t.Context(), filepath.Join(dir, "scenario.json"), out, ""))
	require.Equal(t, "complete", readJSON[manifestStatus](t, filepath.Join(out, "run.json")).Status)
	require.Empty(t, readParquet[checkedLogical](t, filepath.Join(out, "logical_jobs.parquet")))
	require.Empty(t, readParquet[checkedAttempt](t, filepath.Join(out, "attempts.parquet")))
}

func TestDuckDBShortNullablePage(t *testing.T) {
	dir := copyScenario(t)
	input, err := filepath.Abs("testdata/duckdb-nullable")
	require.NoError(t, err)
	updateScenario(t, dir, func(s map[string]any) { s["workload"] = input })
	out := filepath.Join(dir, "out")
	require.NoError(t, replay.RunScenario(t.Context(), filepath.Join(dir, "scenario.json"), out, ""))
	require.Len(t, readParquet[checkedLogical](t, filepath.Join(out, "logical_jobs.parquet")), 4)
}

func TestReplayPlacementRepeatsAcrossInputPartitions(t *testing.T) {
	dir := copyScenario(t)
	editSchedulerConfig(t, dir, func(config string) string {
		return strings.Replace(config, "  - name: test-pool\n", "  - name: test-pool\n  - name: other\n", 1)
	})
	farm := `schemaVersion: 1
clusters:
  - name: east
    pool: test-pool
    nodeTemplates:
      - number: 1
        labels: {site: east}
        totalResources: {resources: {cpu: "8"}}
  - name: west
    pool: other
    nodeTemplates:
      - number: 1
        labels: {site: west}
        taints: [{key: dedicated, value: west, effect: NoSchedule}]
        totalResources: {resources: {cpu: "8"}}
`
	require.NoError(t, os.WriteFile(filepath.Join(dir, "farm.yaml"), []byte(farm), 0o644))
	east := cpuJob("east", measurementStart)
	east.Requests = `{"resourceRequirements":{"requests":{"cpu":"8"}},"nodeSelector":{"site":"east"}}`
	west := cpuJob("west", measurementStart)
	west.Requests = `{
		"resourceRequirements":{"requests":{"cpu":"8"}},
		"affinity":{"nodeAffinity":{"requiredDuringSchedulingIgnoredDuringExecution":{
			"nodeSelectorTerms":[{"matchExpressions":[{"key":"site","operator":"In","values":["west"]}]}]
		}}},
		"tolerations":[{"key":"dedicated","operator":"Equal","value":"west","effect":"NoSchedule"}]
	}`
	var previous []checkedAttempt
	for i, input := range [][][]recordedJob{{{east, west}}, {{east}, {west}}, {{east, west}}} {
		writeInput(t, dir, input)
		out := filepath.Join(dir, fmt.Sprintf("out-%d", i))
		require.NoError(t, replay.RunScenario(t.Context(), filepath.Join(dir, "scenario.json"), out, ""))
		actual := readParquet[checkedAttempt](t, filepath.Join(out, "attempts.parquet"))
		require.Len(t, actual, 2)
		require.Equal(t, "test-pool", actual[0].Pool)
		require.Equal(t, "other", actual[1].Pool)
		for j := range actual {
			actual[j].RunID = ""
		}
		if i > 0 {
			require.Equal(t, previous, actual)
		}
		previous = actual
	}
}
