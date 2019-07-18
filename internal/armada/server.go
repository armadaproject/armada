package armada

import (
	"github.com/G-Research/k8s-batch/internal/armada/api"
	"github.com/G-Research/k8s-batch/internal/armada/configuration"
	"github.com/G-Research/k8s-batch/internal/armada/repository"
	"github.com/G-Research/k8s-batch/internal/armada/service"
	"github.com/go-redis/redis"
	"google.golang.org/grpc"
	"log"
	"net"
	"sync"
)

func Serve(config *configuration.ArmadaConfig) (*grpc.Server, *sync.WaitGroup) {
	wg := &sync.WaitGroup{}
	wg.Add(1)
	server := grpc.NewServer()
	go func () {
		log.Printf("Grpc listening on %s", config.GrpcPort)
		defer log.Println("Stopping server.")

		db := redis.NewClient(&redis.Options{
			Addr:     config.Redis.Addr,
			Password: config.Redis.Password,
			DB:       config.Redis.Db,
		})

		jobRepository := &repository.RedisJobRepository{ Db: db }
		submitServer := &service.SubmitServer{ JobRepository: jobRepository }
		aggregatedQueueServer := &service.AggregatedQueueServer{ JobRepository: jobRepository }

		lis, err := net.Listen("tcp", config.GrpcPort)
		if err != nil {
			log.Fatalf("failed to listen: %v", err)
		}

		api.RegisterSubmitServer(server, submitServer)
		api.RegisterAggregatedQueueServer(server, aggregatedQueueServer)

		if err := server.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}

		wg.Done()
	} ()
	return server, wg
}
