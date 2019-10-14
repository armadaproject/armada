package armada

import (
	"context"
	"net"
	"sync"
	"time"

	"github.com/go-redis/redis"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	"github.com/G-Research/k8s-batch/internal/armada/api"
	"github.com/G-Research/k8s-batch/internal/armada/authorization"
	"github.com/G-Research/k8s-batch/internal/armada/configuration"
	"github.com/G-Research/k8s-batch/internal/armada/metrics"
	"github.com/G-Research/k8s-batch/internal/armada/repository"
	"github.com/G-Research/k8s-batch/internal/armada/server"
	"github.com/G-Research/k8s-batch/internal/armada/service"
)

func Serve(config *configuration.ArmadaConfig) (*grpc.Server, *sync.WaitGroup) {
	wg := &sync.WaitGroup{}
	wg.Add(1)
	grpcServer := createServer(config)
	go func() {
		defer log.Println("Stopping server.")

		db := createRedisClient(&config.Redis)
		eventsDb := createRedisClient(&config.EventsRedis)

		jobRepository := repository.NewRedisJobRepository(db)
		usageRepository := repository.NewRedisUsageRepository(db)
		queueRepository := repository.NewRedisQueueRepository(db)

		eventRepository := repository.NewRedisEventRepository(eventsDb)

		metricsRecorder := metrics.ExposeDataMetrics(queueRepository, jobRepository)

		usageService := service.NewMultiClusterPriorityService(usageRepository, queueRepository, metricsRecorder)

		submitServer := server.NewSubmitServer(jobRepository, queueRepository, eventRepository)
		usageServer := server.NewUsageServer(config.PriorityHalfTime, usageRepository)
		aggregatedQueueServer := server.NewAggregatedQueueServer(usageService, jobRepository, eventRepository)
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

func createRedisClient(config *redis.UniversalOptions) redis.UniversalClient {
	return redis.NewUniversalClient(config)
}

func createServer(config *configuration.ArmadaConfig) *grpc.Server {

	unaryInterceptors := []grpc.UnaryServerInterceptor{}
	streamInterceptors := []grpc.StreamServerInterceptor{}

	authServices := []authorization.AuthorizeService{}

	if config.BasicAuth.EnableAuthentication {
		authServices = append(authServices,
			authorization.NewBasicAuthorizeService(config.BasicAuth.Users))
	}

	if config.OpenIdAuth.ProviderUrl != "" {
		openIdAuthService, err := authorization.NewOpenIdAuthorizeServiceForProvider(context.Background(), &config.OpenIdAuth)
		if err != nil {
			panic(err)
		}
		authServices = append(authServices, openIdAuthService)
	}

	if len(authServices) > 0 {
		authFunction := authorization.CreateMiddlewareAuthFunction(authServices)
		unaryInterceptors = append(unaryInterceptors, grpc_auth.UnaryServerInterceptor(authFunction))
		streamInterceptors = append(streamInterceptors, grpc_auth.StreamServerInterceptor(authFunction))
	}

	grpc_prometheus.EnableHandlingTimeHistogram()

	unaryInterceptors = append(unaryInterceptors, grpc_prometheus.UnaryServerInterceptor)
	streamInterceptors = append(streamInterceptors, grpc_prometheus.StreamServerInterceptor)

	return grpc.NewServer(
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle: 5 * time.Minute,
		}),
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(streamInterceptors...)),
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(unaryInterceptors...)))
}
