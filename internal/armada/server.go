package armada

import (
	"context"
	"github.com/G-Research/k8s-batch/internal/armada/api"
	"github.com/G-Research/k8s-batch/internal/armada/configuration"
	"github.com/G-Research/k8s-batch/internal/armada/repository"
	"github.com/G-Research/k8s-batch/internal/armada/server"
	"github.com/G-Research/k8s-batch/internal/armada/service"
	"github.com/go-redis/redis"
	"google.golang.org/grpc"
	"log"
	"net"
	"sync"
	"time"
)

func Serve(config *configuration.ArmadaConfig) (*grpc.Server, *sync.WaitGroup) {
	wg := &sync.WaitGroup{}
	wg.Add(1)
	grpcServer := createServer(config)
	go func() {
		log.Printf("Grpc listening on %s", config.GrpcPort)
		defer log.Println("Stopping server.")

		db := redis.NewClient(&redis.Options{
			Addr:     config.Redis.Addr,
			Password: config.Redis.Password,
			DB:       config.Redis.Db,
		})

		eventsDb := redis.NewClient(&redis.Options{
			Addr:     config.Redis.Addr,
			Password: config.Redis.Password,
			DB:       config.Redis.Db,
		})

		jobRepository := repository.NewRedisJobRepository(db)
		usageRepository := repository.NewRedisUsageRepository(db)
		queueRepository := repository.NewRedisQueueRepository(db)

		eventRepository := repository.NewRedisEventRepository(eventsDb)

		submitServer := server.NewSubmitServer(jobRepository, queueRepository, eventRepository)
		usageServer := server.NewUsageServer(time.Minute, usageRepository)
		aggregatedQueueServer := server.NewAggregatedQueueServer(jobRepository, usageRepository, queueRepository)
		eventServer := server.NewEventServer(eventRepository)

		lis, err := net.Listen("tcp", config.GrpcPort)
		if err != nil {
			log.Fatalf("failed to listen: %v", err)
		}

		api.RegisterSubmitServer(grpcServer, submitServer)
		api.RegisterUsageServer(grpcServer, usageServer)
		api.RegisterAggregatedQueueServer(grpcServer, aggregatedQueueServer)
		api.RegisterEventServer(grpcServer, eventServer)

		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}

		wg.Done()
	}()
	return grpcServer, wg
}

func createServer(config *configuration.ArmadaConfig) *grpc.Server {
	if config.Authentication.EnableAuthentication {
		authService := service.BasicAuthAuthorizeService{config.Authentication.Users}
		unaryInterceptor, streamInterceptor := createInterceptors(authService)

		return grpc.NewServer(
			grpc.StreamInterceptor(streamInterceptor),
			grpc.UnaryInterceptor(unaryInterceptor),
		)
	} else {
		return grpc.NewServer()
	}
}

func createInterceptors(authService service.AuthorizeService) (grpc.UnaryServerInterceptor, grpc.StreamServerInterceptor) {
	unaryInterceptor := func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if err := authService.Authorize(ctx); err != nil {
			return nil, err
		}

		return handler(ctx, req)
	}
	streamInterceptor := func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if err := authService.Authorize(stream.Context()); err != nil {
			return err
		}

		return handler(srv, stream)
	}

	return unaryInterceptor, streamInterceptor
}
