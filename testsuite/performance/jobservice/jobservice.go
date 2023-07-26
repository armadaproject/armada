package main

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/armadaproject/armada/internal/jobservice"
	"github.com/armadaproject/armada/internal/jobservice/configuration"
	jsgrpc "github.com/armadaproject/armada/pkg/api/jobservice"
	"github.com/armadaproject/armada/pkg/client"
	types "github.com/gogo/protobuf/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	fmt.Printf("Num CPUs: %d\n", runtime.NumCPU())
	old := runtime.GOMAXPROCS(runtime.NumCPU())
	fmt.Printf("Old GOMAXPROCS: %d\n", old)

	var wg sync.WaitGroup

	// TODO: Break this out into its own process/main.
	wg.Add(1)
	go func() {
		err := ServePerformanceTestArmadaServer(1337)
		if err != nil {
			fmt.Println(err.Error())
		}
		wg.Done()
	}()

	ctx := context.Background()

	js := jobservice.New()
	wg.Add(1)
	go func() {
		// os.Setenv("JOBSERVICE_DEBUG", "TRUE")
		js.StartUp(ctx, &configuration.JobServiceConfiguration{
			GrpcPort:     2000,
			MetricsPort:  2001,
			HttpPort:     2002,
			DatabaseType: "postgres",
			ApiConnection: client.ApiConnectionDetails{
				ArmadaUrl:  "localhost:1337",
				ForceNoTls: true,
			},
			PostgresConfig: configuration.PostgresConfig{
				PoolMaxOpenConns:    50,
				PoolMaxIdleConns:    10,
				PoolMaxConnLifetime: time.Duration(time.Minute * 30),
				Connection: map[string]string{
					"host":     "localhost",
					"port":     "5432",
					"user":     "postgres",
					"password": "psw",
					"dbname":   "postgres",
					"sslmode":  "disable",
				},
			},
		})
		wg.Done()
	}()

	time.Sleep(time.Duration(1 * time.Second))

	// Launch a jobservice client to query jobservice about a jobset
	conn, err := grpc.Dial("localhost:2000", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	client := jsgrpc.NewJobServiceClient(conn)
	healthResp, err := client.Health(ctx, &types.Empty{})
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Println(healthResp.Status.String())

	// Try to query about a jobset...
	for i := 0; i < 1; i++ {
		resp, err := client.GetJobStatus(ctx, &jsgrpc.JobServiceRequest{
			JobId:    "fake_job_id",
			JobSetId: fmt.Sprintf("fake_job_set_id_%d", i),
			Queue:    "fake_queue",
		})
		if err != nil {
			fmt.Println(err.Error())
			return
		}

		fmt.Println(resp.State.String())
	}

	wg.Wait()
}
