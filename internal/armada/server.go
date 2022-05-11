package armada

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/go-redis/redis"
	"github.com/google/uuid"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/nats-io/jsm.go"
	"github.com/nats-io/stan.go"
	log "github.com/sirupsen/logrus"

	"github.com/G-Research/armada/internal/armada/cache"
	"github.com/G-Research/armada/internal/armada/configuration"
	"github.com/G-Research/armada/internal/armada/metrics"
	"github.com/G-Research/armada/internal/armada/processor"
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
	"github.com/G-Research/armada/internal/lookout/postgres"
	"github.com/G-Research/armada/internal/pgkeyvalue"
	"github.com/G-Research/armada/internal/pulsarutils"
	"github.com/G-Research/armada/pkg/api"
)

func Serve(config *configuration.ArmadaConfig, healthChecks *health.MultiChecker) (func(), *sync.WaitGroup) {

	// TODO Using an error group would be better.
	// Since we want to shut down everything in a controlled manner in case of an irrecoverable error.
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

	// Allows for registering functions to be run periodically in the background
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
	var streamEventStore *processor.StreamEventStore
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
	var eventProcessor *processor.RedisEventProcessor
	if eventStream != nil {
		streamEventStore = processor.NewEventStore(eventStream)
		eventStore = streamEventStore

		eventRepoBatcher := eventstream.NewTimedEventBatcher(config.Events.ProcessorBatchSize, config.Events.ProcessorMaxTimeBetweenBatches, config.Events.ProcessorTimeout)
		eventProcessor = processor.NewEventRedisProcessor(config.Events.StoreQueue, redisEventRepository, eventStream, eventRepoBatcher)
		eventProcessor.Start()

		jobStatusBatcher := eventstream.NewTimedEventBatcher(config.Events.ProcessorBatchSize, config.Events.ProcessorMaxTimeBetweenBatches, config.Events.ProcessorTimeout)
		jobStatusProcessor := processor.NewEventJobStatusProcessor(config.Events.JobStatusQueue, jobRepository, eventStream, jobStatusBatcher)
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
		config.Auth.PermissionClaimMapping,
	)

	submitServer := server.NewSubmitServer(
		permissions,
		jobRepository,
		queueRepository,
		eventStore,
		schedulingInfoRepository,
		config.CancelJobsBatchSize,
		&config.QueueManagement,
		&config.Scheduling,
	)
	var submitServerToRegister api.SubmitServer
	submitServerToRegister = submitServer

	// If Pulsar is enabled, use the Pulsar submit endpoints.
	// Store a list of all Pulsar components to use during cleanup later.
	var pulsarComponents []interface{ Close() }
	pulsarContext, pulsarCancel := context.WithCancel(context.Background())
	if config.Pulsar.Enabled {
		serverId := uuid.New()

		// API endpoints that generate Pulsar messages.
		log.Info("Pulsar config provided; using Pulsar submit endpoints.")
		pulsarClient, err := pulsarutils.NewPulsarClient(&config.Pulsar)
		if err != nil {
			panic(err)
		}
		pulsarComponents = append(pulsarComponents, pulsarClient)

		compressionType, err := pulsarutils.ParsePulsarCompressionType(config.Pulsar.CompressionType)
		if err != nil {
			panic(err)
		}
		compressionLevel, err := pulsarutils.ParsePulsarCompressionLevel(config.Pulsar.CompressionLevel)
		if err != nil {
			panic(err)
		}
		producer, err := pulsarClient.CreateProducer(pulsar.ProducerOptions{
			Name:             fmt.Sprintf("armada-server-%s", serverId),
			CompressionType:  compressionType,
			CompressionLevel: compressionLevel,
			Topic:            config.Pulsar.JobsetEventsTopic,
		})
		if err != nil {
			panic(err)
		}
		pulsarComponents = append(pulsarComponents, producer)
		pulsarSubmitServer := &server.PulsarSubmitServer{
			Producer:        producer,
			QueueRepository: queueRepository,
			Permissions:     permissions,
			SubmitServer:    submitServer,
		}
		submitServerToRegister = pulsarSubmitServer

		// If postgres details were provided, enable deduplication.
		if config.Pulsar.DedupTable != "" {
			log.Info("Pulsar submit API deduplication enabled")
			db, err := postgres.OpenPgxPool(config.Postgres)
			if err != nil {
				panic(err)
			}
			store, err := pgkeyvalue.New(db, 1000000, config.Pulsar.DedupTable)
			if err != nil {
				panic(err)
			}
			pulsarSubmitServer.KVStore = store

			// Automatically clean up keys after two weeks.
			store.PeriodicCleanup(pulsarContext, time.Hour, 14*24*time.Hour)
		} else {
			log.Info("Pulsar submit API deduplication disabled")
		}

		// If there's a streamEventProcessor,
		// insert the PulsarSubmitServer so it can publish state transitions to Pulsar too.
		if streamEventStore != nil {
			streamEventStore.PulsarSubmitServer = pulsarSubmitServer
		}

		// Service that consumes Pulsar messages and writes to Redis and Nats.
		consumer, err := pulsarClient.Subscribe(pulsar.ConsumerOptions{
			Topic:            config.Pulsar.JobsetEventsTopic,
			SubscriptionName: config.Pulsar.RedisFromPulsarSubscription,
			Type:             pulsar.KeyShared,
		})
		if err != nil {
			panic(err)
		}
		pulsarComponents = append(pulsarComponents, consumer)

		submitFromLog := server.SubmitFromLog{
			Consumer:     consumer,
			SubmitServer: submitServer,
		}
		go submitFromLog.Run(pulsarContext)

		// Service that reads from Pulsar and submits messages back into Pulsar.
		// E.g., needed to automatically publish JobSucceeded after a JobRunSucceeded.
		consumer, err = pulsarClient.Subscribe(pulsar.ConsumerOptions{
			Topic:            config.Pulsar.JobsetEventsTopic,
			SubscriptionName: config.Pulsar.PulsarFromPulsarSubscription,
			Type:             pulsar.KeyShared,
		})
		if err != nil {
			panic(err)
		}
		pulsarComponents = append(pulsarComponents, consumer)

		// Create a new producer for this service.
		producer, err = pulsarClient.CreateProducer(pulsar.ProducerOptions{
			Name:             fmt.Sprintf("armada-pulsar-to-pulsar-%s", serverId),
			CompressionType:  compressionType,
			CompressionLevel: compressionLevel,
			Topic:            config.Pulsar.JobsetEventsTopic,
		})
		if err != nil {
			panic(err)
		}
		pulsarComponents = append(pulsarComponents, producer)

		pulsarFromPulsar := server.PulsarFromPulsar{
			Consumer: consumer,
			Producer: producer,
		}
		go pulsarFromPulsar.Run(pulsarContext)

		// Service that reads from Pulsar and logs events.
		if config.Pulsar.EventsPrinter {
			eventsPrinter := server.EventsPrinter{
				Client:           pulsarClient,
				Topic:            config.Pulsar.JobsetEventsTopic,
				SubscriptionName: config.Pulsar.EventsPrinterSubscription,
			}
			go eventsPrinter.Run(pulsarContext)
		}
	} else {
		log.Info("No Pulsar config provided; submitting directly to Redis and Nats.")
	}

	usageServer := server.NewUsageServer(permissions, config.PriorityHalfTime, &config.Scheduling, usageRepository, queueRepository)
	aggregatedQueueServer := server.NewAggregatedQueueServer(permissions, config.Scheduling, jobRepository, queueCache, queueRepository, usageRepository, eventStore, schedulingInfoRepository)
	eventServer := server.NewEventServer(permissions, redisEventRepository, eventStore, queueRepository)
	leaseManager := scheduling.NewLeaseManager(jobRepository, queueRepository, eventStore, config.Scheduling.Lease.ExpireAfter)

	taskManager.Register(leaseManager.ExpireLeases, config.Scheduling.Lease.ExpiryLoopInterval, "lease_expiry")

	metrics.ExposeDataMetrics(queueRepository, jobRepository, usageRepository, schedulingInfoRepository, queueCache)

	api.RegisterSubmitServer(grpcServer, submitServerToRegister)
	api.RegisterUsageServer(grpcServer, usageServer)
	api.RegisterAggregatedQueueServer(grpcServer, aggregatedQueueServer)
	api.RegisterEventServer(grpcServer, eventServer)

	grpc_prometheus.Register(grpcServer)

	// TODO grpcCommon.Listen is supposed to call wg.Done() internally.
	// Except, that it calls log.Fatalf first, so wg.Done() is never called.
	// There's no clean way for grpcCommon.Listen to return an error.
	grpcCommon.Listen(config.GrpcPort, grpcServer, wg)

	teardown := func() {
		pulsarCancel()
		// Reverse order to close the Pulsar client last
		// (i.e., the same order as if we had used defer).
		for i := len(pulsarComponents) - 1; i >= 0; i-- {
			pulsarComponents[i].Close()
		}
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
