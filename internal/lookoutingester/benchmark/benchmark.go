package benchmark

import (
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"gonum.org/v1/gonum/stat"
	"k8s.io/utils/pointer"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/database"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/lookout/schema"
	"github.com/armadaproject/armada/internal/lookoutingester/configuration"
	"github.com/armadaproject/armada/internal/lookoutingester/lookoutdb"
	"github.com/armadaproject/armada/internal/lookoutingester/metrics"
	"github.com/armadaproject/armada/internal/lookoutingester/model"
)

// BenchmarkSummary is the top-level JSON output written to outPath.
type BenchmarkSummary struct {
	Generated  string                      `json:"generated"`
	Iterations int                         `json:"iterations"`
	Benchmarks map[string]*BenchmarkResult `json:"benchmarks"`
}

// BenchmarkResult holds stats for a single benchmark across all iterations.
type BenchmarkResult struct {
	MeanMs    float64   `json:"mean_ms"`
	StddevMs  float64   `json:"stddev_ms"`
	Discarded int       `json:"discarded"`
	Total     int       `json:"total"`
	RawMs     []float64 `json:"raw_ms"`
}

func computeStats(rawMs []float64) BenchmarkResult {
	mean := stat.Mean(rawMs, nil)
	stddev := stat.StdDev(rawMs, nil)
	lower := mean - 2*stddev
	upper := mean + 2*stddev

	var filtered []float64
	var discarded int
	for _, v := range rawMs {
		if v >= lower && v <= upper {
			filtered = append(filtered, v)
		} else {
			discarded++
		}
	}

	filteredMean := stat.Mean(filtered, nil)
	filteredStddev := stat.StdDev(filtered, nil)

	return BenchmarkResult{
		MeanMs:    math.Round(filteredMean*1000) / 1000,
		StddevMs:  math.Round(filteredStddev*1000) / 1000,
		Discarded: discarded,
		Total:     len(rawMs),
		RawMs:     rawMs,
	}
}

func withDbBenchmark(b *testing.B, config configuration.LookoutIngesterConfiguration, action func(b *testing.B, db *pgxpool.Pool)) {
	migrations, err := schema.LookoutMigrations()
	if err != nil {
		panic(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		err := database.WithTestDbCustom(migrations, config.Postgres, func(db *pgxpool.Pool) error {
			action(b, db)
			return nil
		})
		if err != nil {
			panic(err)
		}
	}
}

func benchmarkSubmissions1000(b *testing.B, config configuration.LookoutIngesterConfiguration) {
	const n = 1000
	jobIds := makeUlids(n)
	jobsToCreate := createJobInstructions(jobIds, n)
	instructions := &model.InstructionSet{
		JobsToCreate: jobsToCreate,
	}
	withDbBenchmark(b, config, func(b *testing.B, db *pgxpool.Pool) {
		ldb := lookoutdb.NewLookoutDb(db, nil, metrics.Get(), 10, 10)
		b.StartTimer()
		err := ldb.Store(armadacontext.TODO(), instructions)
		if err != nil {
			panic(err)
		}
		b.StopTimer()
	})
}

func benchmarkSubmissions10000(b *testing.B, config configuration.LookoutIngesterConfiguration) {
	const n = 10000
	jobIds := makeUlids(n)
	jobsToCreate := createJobInstructions(jobIds, n)
	instructions := &model.InstructionSet{
		JobsToCreate: jobsToCreate,
	}
	withDbBenchmark(b, config, func(b *testing.B, db *pgxpool.Pool) {
		ldb := lookoutdb.NewLookoutDb(db, nil, metrics.Get(), 10, 10)
		b.StartTimer()
		err := ldb.Store(armadacontext.TODO(), instructions)
		if err != nil {
			panic(err)
		}
		b.StopTimer()
	})
}

func benchmarkUpdates1000(b *testing.B, config configuration.LookoutIngesterConfiguration) {
	const n = 1000
	const updatesPerJob = 5
	const runsPerJob = 3
	const updatesPerRun = 5
	const percentErrorRunUpdates = 0.05

	jobIds := makeUlids(n)
	jobRunIds := makeUuids(runsPerJob * n)

	jobsToCreate := createJobInstructions(jobIds, n)
	initialInstructions := &model.InstructionSet{
		JobsToCreate: jobsToCreate,
	}

	instructions := &model.InstructionSet{
		JobsToUpdate:    updateJobInstructions(updatesPerJob*n, jobIds),
		JobRunsToCreate: createJobRunInstructions(runsPerJob*n, jobRunIds),
		JobRunsToUpdate: updateJobRunInstructions(updatesPerRun*runsPerJob*n, jobRunIds, percentErrorRunUpdates),
	}

	withDbBenchmark(b, config, func(b *testing.B, db *pgxpool.Pool) {
		ldb := lookoutdb.NewLookoutDb(db, nil, metrics.Get(), 10, 10)
		err := ldb.Store(armadacontext.TODO(), initialInstructions)
		if err != nil {
			panic(err)
		}
		b.StartTimer()
		err = ldb.Store(armadacontext.TODO(), instructions)
		if err != nil {
			panic(err)
		}
		b.StopTimer()
	})
}

func benchmarkUpdates10000(b *testing.B, config configuration.LookoutIngesterConfiguration) {
	const n = 10000
	const updatesPerJob = 5
	const runsPerJob = 3
	const updatesPerRun = 5
	const percentErrorRunUpdates = 0.05

	jobIds := makeUlids(n)
	jobRunIds := makeUuids(runsPerJob * n)

	jobsToCreate := createJobInstructions(jobIds, n)
	initialInstructions := &model.InstructionSet{
		JobsToCreate: jobsToCreate,
	}

	instructions := &model.InstructionSet{
		JobsToUpdate:    updateJobInstructions(updatesPerJob*n, jobIds),
		JobRunsToCreate: createJobRunInstructions(runsPerJob*n, jobRunIds),
		JobRunsToUpdate: updateJobRunInstructions(updatesPerRun*runsPerJob*n, jobRunIds, percentErrorRunUpdates),
	}

	withDbBenchmark(b, config, func(b *testing.B, db *pgxpool.Pool) {
		ldb := lookoutdb.NewLookoutDb(db, nil, metrics.Get(), 10, 10)
		err := ldb.Store(armadacontext.TODO(), initialInstructions)
		if err != nil {
			panic(err)
		}
		b.StartTimer()
		err = ldb.Store(armadacontext.TODO(), instructions)
		if err != nil {
			panic(err)
		}
		b.StopTimer()
	})
}

func makeUlids(n int) []string {
	ulids := make([]string, n)
	for i := 0; i < n; i++ {
		ulids[i] = util.NewULID()
	}
	return ulids
}

func makeUuids(n int) []string {
	uuids := make([]string, n)
	for i := 0; i < n; i++ {
		uuids[i] = uuid.NewString()
	}
	return uuids
}

func createJobInstructions(jobIds []string, numJobs int) []*model.CreateJobInstruction {
	createJobInstructions := make([]*model.CreateJobInstruction, numJobs)
	jobBytes := make([]byte, 10000, 10000)
	rand.Read(jobBytes)
	for i := 0; i < numJobs; i++ {
		createJobInstructions[i] = &model.CreateJobInstruction{
			JobId:                     jobIds[i%len(jobIds)],
			Queue:                     uuid.NewString(),
			Owner:                     uuid.NewString(),
			Namespace:                 uuid.NewString(),
			JobSet:                    uuid.NewString(),
			Cpu:                       rand.Int63(),
			Memory:                    rand.Int63(),
			EphemeralStorage:          rand.Int63(),
			Gpu:                       rand.Int63(),
			Priority:                  rand.Int63(),
			Submitted:                 time.Now(),
			State:                     int32(rand.Intn(10)),
			LastTransitionTime:        time.Now(),
			LastTransitionTimeSeconds: rand.Int63(),
			JobProto:                  jobBytes,
			PriorityClass:             pointer.String(uuid.NewString()),
			Annotations:               make(map[string]string),
		}
	}
	return createJobInstructions
}

func createJobRunInstructions(n int, runIds []string) []*model.CreateJobRunInstruction {
	instructions := make([]*model.CreateJobRunInstruction, n)
	for i := 0; i < n; i++ {
		instructions[i] = &model.CreateJobRunInstruction{
			RunId:       runIds[i%len(runIds)],
			JobId:       util.NewULID(),
			Cluster:     uuid.NewString(),
			Pending:     pointerTime(time.Now()),
			JobRunState: int32(rand.Intn(10)),
		}
	}
	return instructions
}

func updateJobInstructions(n int, jobIds []string) []*model.UpdateJobInstruction {
	instructions := make([]*model.UpdateJobInstruction, n)
	for i := 0; i < n; i++ {
		instructions[i] = &model.UpdateJobInstruction{
			JobId:                     jobIds[i%len(jobIds)],
			Priority:                  pointer.Int64(rand.Int63()),
			State:                     pointer.Int32(int32(rand.Intn(10))),
			Cancelled:                 pointerTime(time.Now()),
			LastTransitionTime:        pointerTime(time.Now()),
			LastTransitionTimeSeconds: pointer.Int64(rand.Int63()),
			Duplicate:                 pointer.Bool(false),
			LatestRunId:               pointer.String(uuid.NewString()),
		}
	}
	return instructions
}

func updateJobRunInstructions(n int, jobRunIds []string, percentError float64) []*model.UpdateJobRunInstruction {
	instructions := make([]*model.UpdateJobRunInstruction, n)
	errorBytes := make([]byte, 10000, 10000)
	rand.Read(errorBytes)
	totalErrors := int(math.Floor(float64(n) * percentError))
	totalPreemptions := int(math.Floor(float64(n) * 0.05))
	for i := 0; i < n; i++ {
		var jobRunErr []byte
		if i > (n - totalErrors) {
			jobRunErr = errorBytes
		}
		var schedulerTerminationReason map[string]any
		if i < totalPreemptions {
			schedulerTerminationReason = map[string]any{
				"reason": "Preempted by scheduler using fair share preemption - preempting job some-long-job-id-string",
				"args":   map[string]any{"preemptingRunId": uuid.NewString()},
			}
		}
		instructions[i] = &model.UpdateJobRunInstruction{
			RunId:                      jobRunIds[i%len(jobRunIds)],
			Node:                       pointer.String(uuid.NewString()),
			Started:                    pointerTime(time.Now()),
			Finished:                   pointerTime(time.Now()),
			JobRunState:                pointer.Int32(int32(rand.Intn(10))),
			Error:                      jobRunErr,
			ExitCode:                   pointer.Int32(rand.Int31()),
			SchedulerTerminationReason: schedulerTerminationReason,
		}
	}
	return instructions
}

func pointerTime(t time.Time) *time.Time {
	return &t
}

func apply(f func(b *testing.B, config configuration.LookoutIngesterConfiguration), config configuration.LookoutIngesterConfiguration) func(b *testing.B) {
	return func(b *testing.B) {
		f(b, config)
	}
}

// RunBenchmark executes each benchmark n-iterations times, computes summary statistics,
// prints results to stdout, and writes a JSON summary to outPath.
func RunBenchmark(config configuration.LookoutIngesterConfiguration, iterations int, outPath string) {
	benchmarkFns := map[string]func(b *testing.B){
		"benchmarkSubmissions1000":  apply(benchmarkSubmissions1000, config),
		"benchmarkSubmissions10000": apply(benchmarkSubmissions10000, config),
		"benchmarkUpdates1000":      apply(benchmarkUpdates1000, config),
		"benchmarkUpdates10000":     apply(benchmarkUpdates10000, config),
	}

	// Collect raw runtime-per-execution (ms) across iterations for each benchmark.
	rawResults := make(map[string][]float64)
	for benchmarkName, benchmarkFn := range benchmarkFns {
		fmt.Println(benchmarkName)
		for i := 0; i < iterations; i++ {
			res := testing.Benchmark(benchmarkFn)
			msPerExec := float64(res.T.Milliseconds()) / float64(res.N)
			rawResults[benchmarkName] = append(rawResults[benchmarkName], msPerExec)
			printIterationResult(benchmarkName, i+1, iterations, msPerExec, res)
		}
	}

	// Compute stats and build summary.
	summary := BenchmarkSummary{
		Generated:  time.Now().UTC().Format(time.RFC3339),
		Iterations: iterations,
		Benchmarks: make(map[string]*BenchmarkResult),
	}
	for name, raw := range rawResults {
		result := computeStats(raw)
		summary.Benchmarks[name] = &result
		printBenchmarkResult(name, result)
	}

	// Write JSON output.
	data, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		panic(err)
	}
	if err := os.MkdirAll(getDirPath(outPath), 0755); err != nil {
		panic(err)
	}
	if err := os.WriteFile(outPath, data, 0644); err != nil {
		panic(err)
	}
	fmt.Printf("\nSummary written to %s\n", outPath)
}

func printIterationResult(benchmarkName string, iteration, iterations int, msPerExec float64, res testing.BenchmarkResult) {
	fmt.Printf("  iteration %d/%d: %v (%d executions, %.3fms/exec)\n",
		iteration, iterations, res.T, res.N, msPerExec)
}

func printBenchmarkResult(name string, result BenchmarkResult) {
	fmt.Printf("\n# %s\n  mean: %.3fms  stddev: %.3fms  (discarded %d/%d)\n",
		name, result.MeanMs, result.StddevMs, result.Discarded, result.Total)
}

func getDirPath(path string) string {
	for i := len(path) - 1; i >= 0; i-- {
		if path[i] == '/' {
			return path[:i]
		}
	}
	return "."
}
