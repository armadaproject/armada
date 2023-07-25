package main

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/armadaproject/armada/internal/jobservice"
	"github.com/armadaproject/armada/internal/jobservice/configuration"
	"github.com/armadaproject/armada/pkg/client"
)

func main() {
	var wg sync.WaitGroup

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
		os.Setenv("JOBSERVICE_DEBUG", "TRUE")
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

	wg.Wait()
}
