package benchmark

import (
	"fmt"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"k8s.io/utils/pointer"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/database"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/lookoutingesterv2/configuration"
	"github.com/armadaproject/armada/internal/lookoutingesterv2/lookoutdb"
	"github.com/armadaproject/armada/internal/lookoutingesterv2/metrics"
	"github.com/armadaproject/armada/internal/lookoutingesterv2/model"
	"github.com/armadaproject/armada/internal/lookoutv2/schema"
)

func withDbBenchmark(b *testing.B, config configuration.LookoutIngesterV2Configuration, action func(b *testing.B, db *pgxpool.Pool)) {
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

func benchmarkSubmissions1000(b *testing.B, config configuration.LookoutIngesterV2Configuration) {
	const n = 1000
	jobIds := makeUlids(n)
	instructions := &model.InstructionSet{
		JobsToCreate:            createJobInstructions(n, jobIds),
		UserAnnotationsToCreate: createUserAnnotationInstructions(10*n, jobIds),
	}
	withDbBenchmark(b, config, func(b *testing.B, db *pgxpool.Pool) {
		ldb := lookoutdb.NewLookoutDb(db, metrics.Get(), 2, 10)
		b.StartTimer()
		err := ldb.Store(armadacontext.TODO(), instructions)
		if err != nil {
			panic(err)
		}
		b.StopTimer()
	})
}

func benchmarkSubmissions10000(b *testing.B, config configuration.LookoutIngesterV2Configuration) {
	const n = 10000
	jobIds := makeUlids(n)
	instructions := &model.InstructionSet{
		JobsToCreate:            createJobInstructions(n, jobIds),
		UserAnnotationsToCreate: createUserAnnotationInstructions(10*n, jobIds),
	}
	withDbBenchmark(b, config, func(b *testing.B, db *pgxpool.Pool) {
		ldb := lookoutdb.NewLookoutDb(db, metrics.Get(), 2, 10)
		b.StartTimer()
		err := ldb.Store(armadacontext.TODO(), instructions)
		if err != nil {
			panic(err)
		}
		b.StopTimer()
	})
}

func benchmarkUpdates1000(b *testing.B, config configuration.LookoutIngesterV2Configuration) {
	const n = 1000
	const updatesPerJob = 5
	const runsPerJob = 3
	const updatesPerRun = 5
	const percentErrorRunUpdates = 0.05

	jobIds := makeUlids(n)
	jobRunIds := makeUuids(runsPerJob * n)

	initialInstructions := &model.InstructionSet{
		JobsToCreate: createJobInstructions(n, jobIds),
	}

	instructions := &model.InstructionSet{
		JobsToUpdate:    updateJobInstructions(updatesPerJob*n, jobIds),
		JobRunsToCreate: createJobRunInstructions(runsPerJob*n, jobRunIds),
		JobRunsToUpdate: updateJobRunInstructions(updatesPerRun*runsPerJob*n, jobRunIds, percentErrorRunUpdates),
	}

	withDbBenchmark(b, config, func(b *testing.B, db *pgxpool.Pool) {
		ldb := lookoutdb.NewLookoutDb(db, metrics.Get(), 2, 10)
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

func benchmarkUpdates10000(b *testing.B, config configuration.LookoutIngesterV2Configuration) {
	const n = 10000
	const updatesPerJob = 5
	const runsPerJob = 3
	const updatesPerRun = 5
	const percentErrorRunUpdates = 0.05

	jobIds := makeUlids(n)
	jobRunIds := makeUuids(runsPerJob * n)

	initialInstructions := &model.InstructionSet{
		JobsToCreate: createJobInstructions(n, jobIds),
	}

	instructions := &model.InstructionSet{
		JobsToUpdate:    updateJobInstructions(updatesPerJob*n, jobIds),
		JobRunsToCreate: createJobRunInstructions(runsPerJob*n, jobRunIds),
		JobRunsToUpdate: updateJobRunInstructions(updatesPerRun*runsPerJob*n, jobRunIds, percentErrorRunUpdates),
	}

	withDbBenchmark(b, config, func(b *testing.B, db *pgxpool.Pool) {
		ldb := lookoutdb.NewLookoutDb(db, metrics.Get(), 2, 10)
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

func createJobInstructions(n int, jobIds []string) []*model.CreateJobInstruction {
	instructions := make([]*model.CreateJobInstruction, n)
	jobBytes := make([]byte, 10000, 10000)
	rand.Read(jobBytes)
	for i := 0; i < n; i++ {
		instructions[i] = &model.CreateJobInstruction{
			JobId:                     jobIds[i%len(jobIds)],
			Queue:                     uuid.NewString(),
			Owner:                     uuid.NewString(),
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
		}
	}
	return instructions
}

func createUserAnnotationInstructions(n int, jobIds []string) []*model.CreateUserAnnotationInstruction {
	instructions := make([]*model.CreateUserAnnotationInstruction, n)
	for i := 0; i < n; i++ {
		instructions[i] = &model.CreateUserAnnotationInstruction{
			JobId:  jobIds[i%len(jobIds)],
			Key:    uuid.NewString(),
			Value:  uuid.NewString(),
			Queue:  uuid.NewString(),
			Jobset: uuid.NewString(),
		}
	}
	return instructions
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
	for i := 0; i < n; i++ {
		var jobRunErr []byte
		if i > (n - totalErrors) {
			jobRunErr = errorBytes
		}
		instructions[i] = &model.UpdateJobRunInstruction{
			RunId:       jobRunIds[i%len(jobRunIds)],
			Node:        pointer.String(uuid.NewString()),
			Started:     pointerTime(time.Now()),
			Finished:    pointerTime(time.Now()),
			JobRunState: pointer.Int32(int32(rand.Intn(10))),
			Error:       jobRunErr,
			ExitCode:    pointer.Int32(rand.Int31()),
		}
	}
	return instructions
}

func pointerTime(time time.Time) *time.Time {
	return &time
}

func printBenchmarkResults(result testing.BenchmarkResult) {
	fmt.Println(result)
	fmt.Printf(
		"total time: %v - total executions: %d - runtime per execution: %v\n",
		result.T,
		result.N,
		result.T/time.Duration(result.N),
	)
}

func apply(f func(b *testing.B, config configuration.LookoutIngesterV2Configuration), config configuration.LookoutIngesterV2Configuration) func(b *testing.B) {
	return func(b *testing.B) {
		f(b, config)
	}
}

// RunBenchmark executes benchmarking functions defined above for LookoutIngesterV2 database saving logic
func RunBenchmark(config configuration.LookoutIngesterV2Configuration) {
	benchmarkFns := map[string]func(b *testing.B){
		"benchmarkSubmissions1000":  apply(benchmarkSubmissions1000, config),
		"benchmarkSubmissions10000": apply(benchmarkSubmissions10000, config),
		"benchmarkUpdates1000":      apply(benchmarkUpdates1000, config),
		"benchmarkUpdates10000":     apply(benchmarkUpdates10000, config),
	}

	for benchmarkName, benchmarkFn := range benchmarkFns {
		fmt.Println(benchmarkName)
		res := testing.Benchmark(benchmarkFn)
		printBenchmarkResults(res)
	}
}
