package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/armadaproject/armada/internal/jobservice"
	"github.com/armadaproject/armada/internal/jobservice/configuration"
	"github.com/armadaproject/armada/pkg/client"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/encoding/gzip"

	_ "net/http/pprof"
)

func main() {
	var wg sync.WaitGroup

	ctx, cancel := context.WithCancel(context.Background())

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)

	go func() {
		select {
		case <-ctx.Done():
			return
		case <-signalChan:
			fmt.Println("Got interrupt, stopping...")
			cancel()
			return
		}
	}()

	go func() {
		http.ListenAndServe("localhost:6060", nil)
	}()

	comp := encoding.GetCompressor(gzip.Name)
	encoding.RegisterCompressor(comp)

	outfile, err := os.Create("jobservice.profile")
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	defer outfile.Close()

	/*
		err = pprof.StartCPUProfile(outfile)
		if err != nil {
			fmt.Println(err.Error())
			return
		}*/

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

	// pprof.StopCPUProfile()
}
