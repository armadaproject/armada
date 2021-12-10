package armada

import (
	"fmt"
	"sync"
	"time"

	"github.com/go-redis/redis"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/nats-io/jsm.go"
	"github.com/nats-io/stan.go"
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

	// TODO Using an error group would be better.
	// Since we want to shut down everything in a controlled manner in case of an irrecoverable error.
	// Doesn't look like wg.Done() is called anywhere in this method.
	// Either it's not called (we could be exiting using panic/os.Exit),
	// or it's called in a sub-call.
	wg := &sync.WaitGroup{}
	wg.Add(1)

	// TODO Return an error, don't panic.
	err := validateArmadaConfig(config)
	if err != nil {
		panic(fmt.Errorf("configuration validation error: %v", err))
	}

	// We support multiple simultaneous authentication services (e.g., username/password  OpenId).
	// For each gRPC request, we try them all until one succeeds, at which point the process is
	// short-circuited.
	authServices := auth.ConfigureAuth(config.Auth)
	grpcServer := grpcCommon.CreateGrpcServer(authServices)

	taskManager := task.NewBackgroundTaskManager(metrics.MetricPrefix)

	// TODO Redis setup code. Move into a separate function.
	db := createRedisClient(&config.Redis)
	eventsDb := createRedisClient(&config.EventsRedis)

	jobRepository := repository.NewRedisJobRepository(db, config.DatabaseRetention)
	usageRepository := repository.NewRedisUsageRepository(db)
	queueRepository := repository.NewRedisQueueRepository(db)
	schedulingInfoRepository := repository.NewRedisSchedulingInfoRepository(db)
	healthChecks.Add(repository.NewRedisHealth(db))

	queueCache := cache.NewQueueCache(queueRepository, jobRepository, schedulingInfoRepository)
	taskManager.Register(queueCache.Refresh, config.Metrics.RefreshInterval, "refresh_queue_cache")

	redisEventRepository := repository.NewRedisEventRepository(eventsDb, config.EventRetention)
	var eventStore repository.EventStore
	var eventStream eventstream.EventStream

	// TODO It looks like multiple backends can be provided.
	// We should ensure that only 1 system is provided.
	// TODO Return an error, don't panic.
	if len(config.EventsNats.Servers) > 0 {
		stanClient, err := eventstream.NewStanClientConnection(
			config.EventsNats.ClusterID,
			"armada-server-"+util.NewULID(),
			config.EventsNats.Servers)
		if err != nil {
			panic(err)
		}

		eventStream = eventstream.NewStanEventStream(
			config.EventsNats.Subject,
			stanClient,
			stan.SetManualAckMode(),
			stan.StartWithLastReceived())

		healthChecks.Add(stanClient)
	} else if len(config.EventsJetstream.Servers) > 0 {
		stream, err := eventstream.NewJetstreamEventStream(
			&config.EventsJetstream,
			jsm.SamplePercent(100),
			jsm.StartWithLastReceived())
		if err != nil {
			panic(err)
		}
		eventStream = stream

		healthChecks.Add(stream)
	}

	// TODO: move this to task manager
	eventstreamTeardown := func() {}
	if eventStream != nil {
		eventStore = repository.NewEventStore(eventStream)

		eventRepoBatcher := eventstream.NewTimedEventBatcher(config.Events.ProcessorBatchSize, config.Events.ProcessorMaxTimeBetweenBatches, config.Events.ProcessorTimeout)
		eventProcessor := repository.NewEventRedisProcessor(config.Events.StoreQueue, redisEventRepository, eventStream, eventRepoBatcher)
		eventProcessor.Start()

		jobStatusBatcher := eventstream.NewTimedEventBatcher(config.Events.ProcessorBatchSize, config.Events.ProcessorMaxTimeBetweenBatches, config.Events.ProcessorTimeout)
		jobStatusProcessor := repository.NewEventJobStatusProcessor(config.Events.JobStatusQueue, jobRepository, eventStream, jobStatusBatcher)
		jobStatusProcessor.Start()

		// TODO Teardown functions should return an error that can be logged/whatever by the caller.
		// Not use log internally.
		eventstreamTeardown = func() {
			err := eventRepoBatcher.Stop()
			if err != nil {
				log.Errorf("failed to flush event processor buffer for redis")
			}
			err = jobStatusBatcher.Stop()
			if err != nil {
				log.Errorf("failed to flush job status batcher processor")
			}
			err = eventStream.Close()
			if err != nil {
				log.Errorf("failed to close event stream connection: %v", err)
			}
		}
	} else {
		eventStore = redisEventRepository
	}

	permissions := authorization.NewPrincipalPermissionChecker(
		config.Auth.PermissionGroupMapping,
		config.Auth.PermissionScopeMapping,
		config.Auth.PermissionClaimMapping)

	submitServer := server.NewSubmitServer(
		permissions,
		jobRepository,
		queueRepository,
		eventStore,
		schedulingInfoRepository,
		config.CancelJobsBatchSize,
		&config.QueueManagement,
		&config.Scheduling)
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

	// TODO grpcCommon.Listen is supposed to call wg.Done() internally.
	// Except, that it calls log.Fatalf first, so wg.Done() is never called.
	// There's no clean way for grpcCommon.Listen to return an error.
	grpcCommon.Listen(config.GrpcPort, grpcServer, wg)

	teardown := func() {
		eventstreamTeardown()
		taskManager.StopAll(time.Second * 2)
		grpcServer.GracefulStop()
	}
	return teardown, wg
}

func createRedisClient(config *redis.UniversalOptions) redis.UniversalClient {
	return redis.NewUniversalClient(config)
}

// TODO Is this all validation that needs to be done?
func validateArmadaConfig(config *configuration.ArmadaConfig) error {
	if config.CancelJobsBatchSize <= 0 {
		return fmt.Errorf("cancel jobs batch should be greater than 0: is %d", config.CancelJobsBatchSize)
	}
	return nil
}
