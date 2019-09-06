package armada

import (
	"context"
	"github.com/G-Research/k8s-batch/internal/armada/api"
	"github.com/G-Research/k8s-batch/internal/armada/configuration"
	"github.com/G-Research/k8s-batch/internal/armada/metrics"
	"github.com/G-Research/k8s-batch/internal/armada/repository"
	"github.com/G-Research/k8s-batch/internal/armada/server"
	"github.com/G-Research/k8s-batch/internal/armada/service"
	"github.com/go-redis/redis"
	"github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"net"
	"sync"
)

func Serve(config *configuration.ArmadaConfig) (*grpc.Server, *sync.WaitGroup) {
	wg := &sync.WaitGroup{}
	wg.Add(1)
	grpcServer := createServer(config)
	go func() {
		defer log.Println("Stopping server.")

		db := createRedisClient(config.Redis)
		eventsDb := createRedisClient(config.EventsRedis)

		jobRepository := repository.NewRedisJobRepository(db)
		usageRepository := repository.NewRedisUsageRepository(db)
		queueRepository := repository.NewRedisQueueRepository(db)

		eventRepository := repository.NewRedisEventRepository(eventsDb)

		metricsRecorder := metrics.ExposeDataMetrics(queueRepository, jobRepository)

		submitServer := server.NewSubmitServer(jobRepository, queueRepository, eventRepository)
		usageServer := server.NewUsageServer(config.PriorityHalfTime, usageRepository)
		aggregatedQueueServer := server.NewAggregatedQueueServer(jobRepository, usageRepository, queueRepository, eventRepository, metricsRecorder)
		eventServer := server.NewEventServer(eventRepository)

		lis, err := net.Listen("tcp", config.GrpcPort)
		if err != nil {
			log.Fatalf("failed to listen: %v", err)
		}

		api.RegisterSubmitServer(grpcServer, submitServer)
		api.RegisterUsageServer(grpcServer, usageServer)
		api.RegisterAggregatedQueueServer(grpcServer, aggregatedQueueServer)
		api.RegisterEventServer(grpcServer, eventServer)

		grpc_prometheus.Register(grpcServer)

		log.Printf("Grpc listening on %s", config.GrpcPort)
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}

		wg.Done()
	}()
	return grpcServer, wg
}

func createRedisClient(config configuration.RedisConfig) *redis.Client {
	if config.MasterName != "" && len(config.SentinelAddresses) > 0 {
		log.Infof("Connecting to redis HA with master %s and sentinel addresses %s", config.MasterName, config.SentinelAddresses)
		return redis.NewFailoverClient(&redis.FailoverOptions{
			MasterName:    config.MasterName,
			SentinelAddrs: config.SentinelAddresses,
			Password:      config.Password,
			DB:            config.Db,
			PoolSize:      1000,
		})
	} else {
		log.Infof("Connecting to redis with address %s", config.Addr)
		return redis.NewClient(&redis.Options{
			Addr:     config.Addr,
			Password: config.Password,
			DB:       config.Db,
			PoolSize: 1000,
		})
	}
}

func createServer(config *configuration.ArmadaConfig) *grpc.Server {

	unaryInterceptors := []grpc.UnaryServerInterceptor{}
	streamInterceptors := []grpc.StreamServerInterceptor{}

	if config.Authentication.EnableAuthentication {
		authService := service.NewBasicAuthAuthorizeService(config.Authentication.Users)
		authUnaryInterceptor, authStreamInterceptor := createInterceptors(authService)

		unaryInterceptors = append(unaryInterceptors, authUnaryInterceptor)
		streamInterceptors = append(streamInterceptors, authStreamInterceptor)
	}

	grpc_prometheus.EnableHandlingTimeHistogram()

	unaryInterceptors = append(unaryInterceptors, grpc_prometheus.UnaryServerInterceptor)
	streamInterceptors = append(streamInterceptors, grpc_prometheus.StreamServerInterceptor)

	return grpc.NewServer(
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(streamInterceptors...)),
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(unaryInterceptors...)))
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
