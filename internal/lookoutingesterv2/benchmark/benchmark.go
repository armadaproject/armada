package main

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v4/pgxpool"
	"k8s.io/utils/pointer"

	"github.com/G-Research/armada/internal/common"
	"github.com/G-Research/armada/internal/common/database"
	"github.com/G-Research/armada/internal/common/util"
	"github.com/G-Research/armada/internal/lookoutingesterv2/configuration"
	"github.com/G-Research/armada/internal/lookoutingesterv2/lookoutdb"
	"github.com/G-Research/armada/internal/lookoutingesterv2/metrics"
	"github.com/G-Research/armada/internal/lookoutingesterv2/model"
	"github.com/G-Research/armada/internal/lookoutv2/schema/statik"
)

func benchmarkSubmissions1000(b *testing.B) {
	// Initialize
	n := 1000

	config := loadDefaultConfig()

	migrations, err := database.GetMigrations(statik.Lookoutv2Sql)
	if err != nil {
		panic(err)
	}
	instructions := createJobInstructions(n)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()

		err := database.WithTestDb(migrations, &config.Postgres, func(db *pgxpool.Pool) error {
			if err != nil {
				return err
			}
			ldb := lookoutdb.New(db, metrics.Get(), 2, 10)

			b.StartTimer()
			ldb.CreateJobs(context.TODO(), instructions)

			b.StopTimer()
			return nil
		})
		if err != nil {
			panic(err)
		}

	}
}

func loadDefaultConfig() configuration.LookoutIngesterV2Configuration {
	var config configuration.LookoutIngesterV2Configuration
	common.LoadConfig(&config, "./config/lookoutingesterv2", []string{})
	return config
}

func createJobInstructions(n int) []*model.CreateJobInstruction {
	instructions := make([]*model.CreateJobInstruction, n)

	jobBytes := make([]byte, 10000, 10000)
	rand.Read(jobBytes)

	for i := 0; i < n; i++ {
		instructions[i] = &model.CreateJobInstruction{
			JobId:                     util.NewULID(),
			Queue:                     uuid.NewString(),
			Owner:                     uuid.NewString(),
			JobSet:                    uuid.NewString(),
			Cpu:                       rand.Int63(),
			Memory:                    rand.Int63(),
			EphemeralStorage:          rand.Int63(),
			Gpu:                       rand.Int63(),
			Priority:                  rand.Uint32(),
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

func main() {
	benchmarkFns := []func(b *testing.B){
		benchmarkSubmissions1000,
	}

	for _, benchmarkFn := range benchmarkFns {
		res := testing.Benchmark(benchmarkFn)
		printBenchmarkResults(res)
	}
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
