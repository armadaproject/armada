package armada

import (
	"sync"
	"time"

	"github.com/go-redis/redis"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	log "github.com/sirupsen/logrus"

	"github.com/G-Research/armada/internal/armada/cache"
	"github.com/G-Research/armada/internal/armada/configuration"
	"github.com/G-Research/armada/internal/armada/metrics"
	"github.com/G-Research/armada/internal/armada/repository"
	"github.com/G-Research/armada/internal/armada/scheduling"
	"github.com/G-Research/armada/internal/armada/server"
	"github.com/G-Research/armada/internal/common/auth"
	"github.com/G-Research/armada/internal/common/auth/authorization"
	"github.com/G-Research/armada/internal/common/eventstream"
	grpcCommon "github.com/G-Research/armada/internal/common/grpc"
	"github.com/G-Research/armada/internal/common/health"
	"github.com/G-Research/armada/internal/common/task"
	"github.com/G-Research/armada/internal/common/util"
	"github.com/G-Research/armada/pkg/api"
)

func Serve(config *configuration.ArmadaConfig, healthChecks *health.MultiChecker) (func(), *sync.WaitGroup) {
	wg := &sync.WaitGroup{}
	wg.Add(1)

	grpcServer := grpcCommon.CreateGrpcServer(auth.ConfigureAuth(config.Auth))

	taskManager := task.NewBackgroundTaskManager(metrics.MetricPrefix)

	db := createRedisClient(&config.Redis)
	eventsDb := createRedisClient(&config.EventsRedis)

	jobRepository := repository.NewRedisJobRepository(db, config.Scheduling.DefaultJobLimits, config.Scheduling.DefaultJobTolerations)
	usageRepository := repository.NewRedisUsageRepository(db)
	queueRepository := repository.NewRedisQueueRepository(db)
	schedulingInfoRepository := repository.NewRedisSchedulingInfoRepository(db)
	healthChecks.Add(repository.NewRedisHealth(db))

	queueCache := cache.NewQueueCache(queueRepository, jobRepository, schedulingInfoRepository)
	taskManager.Register(queueCache.Refresh, config.Metrics.RefreshInterval, "refresh_queue_cache")

	redisEventRepository := repository.NewRedisEventRepository(eventsDb, config.EventRetention)
	var eventStore repository.EventStore

	// TODO: move this to task manager
	stopSubscription := func() {}
	if len(config.EventsNats.Servers) > 0 {
		stanClient, err := eventstream.NewStanClientConnection(
			config.EventsNats.ClusterID,
			"armada-server-"+util.NewULID(),
			config.EventsNats.Servers)

		stream := eventstream.NewStanEventStream(
			config.EventsNats.Subject,
			config.EventsNats.QueueGroup,
			stanClient)
		if err != nil {
			panic(err)
		}
		eventStore = repository.NewEventStore(stream)
		eventProcessor := repository.NewEventRedisProcessor(stream, redisEventRepository)
		eventProcessor.Start()
		jobStatusProcessor := repository.NewEventJobStatusProcessor(stream, jobRepository)
		jobStatusProcessor.Start()

		stopSubscription = func() {
			err := stream.Close()
			if err != nil {
				log.Errorf("failed to close nats connection: %v", err)
			}
		}

		healthChecks.Add(stanClient)
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
