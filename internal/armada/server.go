package armada

import (
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"

	"github.com/G-Research/armada/internal/armada/cache"
	"github.com/G-Research/armada/internal/armada/configuration"
	"github.com/G-Research/armada/internal/armada/metrics"
	"github.com/G-Research/armada/internal/armada/repository"
	"github.com/G-Research/armada/internal/armada/scheduling"
	"github.com/G-Research/armada/internal/armada/server"
	"github.com/G-Research/armada/internal/common/auth"
	"github.com/G-Research/armada/internal/common/auth/authorization"
	grpcCommon "github.com/G-Research/armada/internal/common/grpc"
	stan_util "github.com/G-Research/armada/internal/common/stan-util"
	"github.com/G-Research/armada/internal/common/task"
	"github.com/G-Research/armada/internal/common/util"
	"github.com/G-Research/armada/pkg/api"
)

func Serve(config *configuration.ArmadaConfig) (func(), *sync.WaitGroup) {
	wg := &sync.WaitGroup{}
	wg.Add(1)

	grpcServer := grpcCommon.CreateGrpcServer(auth.ConfigureAuth(config.Auth))

	taskManager := task.NewBackgroundTaskManager(metrics.MetricPrefix)

	db := createRedisClient(&config.Redis)
	eventsDb := createRedisClient(&config.EventsRedis)

	jobRepository := repository.NewRedisJobRepository(db, config.Scheduling.DefaultJobLimits)
	usageRepository := repository.NewRedisUsageRepository(db)
	queueRepository := repository.NewRedisQueueRepository(db)
	schedulingInfoRepository := repository.NewRedisSchedulingInfoRepository(db)

	queueCache := cache.NewQueueCache(queueRepository, jobRepository, schedulingInfoRepository)
	taskManager.Register(queueCache.Refresh, config.Metrics.RefreshInterval, "refresh_queue_cache")

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
		jobStatusReader := kafka.NewReader(kafka.ReaderConfig{
			Brokers:  config.EventsKafka.Brokers,
			GroupID:  config.EventsKafka.JobStatusConsumerGroupID,
			Topic:    config.EventsKafka.Topic,
			MaxWait:  500 * time.Millisecond,
			MinBytes: 0,    // 10KB
			MaxBytes: 10e6, // 10MB
		})

		eventStore = repository.NewKafkaEventStore(writer)
		eventProcessor := repository.NewKafkaEventRedisProcessor(reader, redisEventRepository)
		jobStatusEventProcessor := repository.NewKafkaJobStatusProcessor(jobStatusReader, jobRepository)

		//TODO: Remove this metric, and add one to track event delay
		taskManager.Register(eventProcessor.ProcessEvents, 100*time.Millisecond, "kafka_redis_processor")
		taskManager.Register(jobStatusEventProcessor.ProcessEvents, 100*time.Millisecond, "kafka_job_status_processor")

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
		jobStatusProcessor := repository.NewNatsEventJobStatusProcessor(conn, jobRepository, config.EventsNats.Subject, config.EventsNats.JobStatusGroup)
		jobStatusProcessor.Start()

		stopSubscription = func() {
			err := conn.Close()
			if err != nil {
				log.Errorf("failed to close nats connection: %v", err)
			}
		}

	} else {
		eventStore = redisEventRepository
	}

	permissions := authorization.NewPrincipalPermissionChecker(config.Auth.PermissionGroupMapping, config.Auth.PermissionScopeMapping, config.Auth.PermissionClaimMapping)

	submitServer := server.NewSubmitServer(permissions, jobRepository, queueRepository, eventStore, schedulingInfoRepository, &config.QueueManagement)
	usageServer := server.NewUsageServer(permissions, config.PriorityHalfTime, &config.Scheduling, usageRepository, queueRepository)
	aggregatedQueueServer := server.NewAggregatedQueueServer(permissions, config.Scheduling, jobRepository, queueCache, queueRepository, usageRepository, eventStore, schedulingInfoRepository)
	eventServer := server.NewEventServer(permissions, redisEventRepository, eventStore)
	leaseManager := scheduling.NewLeaseManager(jobRepository, queueRepository, eventStore, config.Scheduling.Lease.ExpireAfter)

	taskManager.Register(leaseManager.ExpireLeases, config.Scheduling.Lease.ExpiryLoopInterval, "lease_expiry")

	metrics.ExposeDataMetrics(queueRepository, jobRepository, usageRepository, schedulingInfoRepository, queueCache)

	api.RegisterSubmitServer(grpcServer, submitServer)
	api.RegisterUsageServer(grpcServer, usageServer)
	api.RegisterAggregatedQueueServer(grpcServer, aggregatedQueueServer)
	api.RegisterEventServer(grpcServer, eventServer)

	grpc_prometheus.Register(grpcServer)

	grpcCommon.Listen(config.GrpcPort, grpcServer, wg)

	return func() {
		stopSubscription()
		taskManager.StopAll(time.Second * 2)
		grpcServer.GracefulStop()
	}, wg
}

func createRedisClient(config *redis.UniversalOptions) redis.UniversalClient {
	return redis.NewUniversalClient(config)
}
