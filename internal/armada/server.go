package armada

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"

	"github.com/G-Research/armada/internal/armada/authorization"
	"github.com/G-Research/armada/internal/armada/configuration"
	"github.com/G-Research/armada/internal/armada/metrics"
	"github.com/G-Research/armada/internal/armada/repository"
	"github.com/G-Research/armada/internal/armada/scheduling"
	"github.com/G-Research/armada/internal/armada/server"
	grpcCommon "github.com/G-Research/armada/internal/common/grpc"
	stan_util "github.com/G-Research/armada/internal/common/stan-util"
	"github.com/G-Research/armada/internal/common/task"
	"github.com/G-Research/armada/internal/common/util"
	"github.com/G-Research/armada/pkg/api"
)

func Serve(config *configuration.ArmadaConfig) (func(), *sync.WaitGroup) {
	wg := &sync.WaitGroup{}
	wg.Add(1)
	grpcServer := createServer(config)

	taskManager := task.NewBackgroundTaskManager(metrics.MetricPrefix)

	db := createRedisClient(&config.Redis)
	eventsDb := createRedisClient(&config.EventsRedis)

	jobRepository := repository.NewRedisJobRepository(db, config.Scheduling.DefaultJobLimits)
	usageRepository := repository.NewRedisUsageRepository(db)
	queueRepository := repository.NewRedisQueueRepository(db)
	schedulingInfoRepository := repository.NewRedisSchedulingInfoRepository(db)

	redisEventRepository := repository.NewRedisEventRepository(eventsDb, config.EventRetention)
	var eventStore repository.EventStore

	// TODO: move this to task manager
	stopSubscription := func() {}
	if len(config.EventsKafka.Brokers) > 0 {
		log.Infof("Using Kafka for events (%+v)", config.EventsKafka)
		writer := kafka.NewWriter(kafka.WriterConfig{
			Brokers: config.EventsKafka.Brokers,
			Topic:   config.EventsKafka.Topic,
		})
		reader := kafka.NewReader(kafka.ReaderConfig{
			Brokers:  config.EventsKafka.Brokers,
			GroupID:  config.EventsKafka.ConsumerGroupID,
			Topic:    config.EventsKafka.Topic,
			MaxWait:  500 * time.Millisecond,
			MinBytes: 0,    // 10KB
			MaxBytes: 10e6, // 10MB
		})

		eventStore = repository.NewKafkaEventStore(writer)
		eventProcessor := repository.NewKafkaEventRedisProcessor(reader, redisEventRepository)

		//TODO: Remove this metric, and add one to track event delay
		taskManager.Register(eventProcessor.ProcessEvents, 100*time.Millisecond, "kafka_redis_processor")

	} else if len(config.EventsNats.Servers) > 0 {

		conn, err := stan_util.DurableConnect(
			config.EventsNats.ClusterID,
			"armada-server-"+util.NewULID(),
			strings.Join(config.EventsNats.Servers, ","),
		)
		if err != nil {
			panic(err)
		}
		eventStore = repository.NewNatsEventStore(conn, config.EventsNats.Subject)
		eventProcessor := repository.NewNatsEventRedisProcessor(conn, redisEventRepository, config.EventsNats.Subject, config.EventsNats.QueueGroup)
		eventProcessor.Start()

		stopSubscription = func() {
			err := conn.Close()
			if err != nil {
				log.Errorf("failed to close nats connection: %v", err)
			}
		}

	} else {
		eventStore = redisEventRepository
	}

	permissions := authorization.NewPrincipalPermissionChecker(config.PermissionGroupMapping, config.PermissionScopeMapping)

	submitServer := server.NewSubmitServer(permissions, jobRepository, queueRepository, eventStore, schedulingInfoRepository, &config.QueueManagement)
	usageServer := server.NewUsageServer(permissions, config.PriorityHalfTime, &config.Scheduling, usageRepository, queueRepository)
	aggregatedQueueServer := server.NewAggregatedQueueServer(permissions, config.Scheduling, jobRepository, queueRepository, usageRepository, eventStore, schedulingInfoRepository)
	eventServer := server.NewEventServer(permissions, redisEventRepository, eventStore)
	leaseManager := scheduling.NewLeaseManager(jobRepository, queueRepository, eventStore, config.Scheduling.Lease.ExpireAfter)

	taskManager.Register(leaseManager.ExpireLeases, config.Scheduling.Lease.ExpiryLoopInterval, "lease_expiry")

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", config.GrpcPort))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	collector := metrics.ExposeDataMetrics(queueRepository, jobRepository, usageRepository, schedulingInfoRepository)
	taskManager.Register(collector.RefreshMetrics, config.Metrics.RefreshInterval, "refresh_metrics")

	api.RegisterSubmitServer(grpcServer, submitServer)
	api.RegisterUsageServer(grpcServer, usageServer)
	api.RegisterAggregatedQueueServer(grpcServer, aggregatedQueueServer)
	api.RegisterEventServer(grpcServer, eventServer)

	grpc_prometheus.Register(grpcServer)

	go func() {
		defer log.Println("Stopping server.")

		log.Printf("Grpc listening on %d", config.GrpcPort)
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}

		wg.Done()
	}()

	return func() {
		stopSubscription()
		taskManager.StopAll(time.Second * 2)
		grpcServer.GracefulStop()
	}, wg
}

func createRedisClient(config *redis.UniversalOptions) redis.UniversalClient {
	return redis.NewUniversalClient(config)
}

func createServer(config *configuration.ArmadaConfig) *grpc.Server {

	authServices := []authorization.AuthService{}

	if len(config.BasicAuth.Users) > 0 {
		authServices = append(authServices,
			authorization.NewBasicAuthService(config.BasicAuth.Users))
	}

	if config.OpenIdAuth.ProviderUrl != "" {
		openIdAuthService, err := authorization.NewOpenIdAuthServiceForProvider(context.Background(), &config.OpenIdAuth)
		if err != nil {
			panic(err)
		}
		authServices = append(authServices, openIdAuthService)
	}

	if config.AnonymousAuth {
		authServices = append(authServices, &authorization.AnonymousAuthService{})
	}

	// Kerberos should be the last service as it is adding WWW-Authenticate header for unauthenticated response
	if config.Kerberos.KeytabLocation != "" {
		kerberosAuthService, err := authorization.NewKerberosAuthService(&config.Kerberos)
		if err != nil {
			panic(err)
		}
		authServices = append(authServices, kerberosAuthService)
	}

	return grpcCommon.CreateGrpcServer(authServices)
}
