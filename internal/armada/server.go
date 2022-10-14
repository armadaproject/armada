package armada

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/go-redis/redis"
	"github.com/google/uuid"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/nats-io/jsm.go"
	"github.com/nats-io/stan.go"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"

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
	"github.com/G-Research/armada/internal/scheduler"
	"github.com/G-Research/armada/internal/scheduler/schedulerobjects"
	"github.com/G-Research/armada/pkg/api"
)

func Serve(ctx context.Context, config *configuration.ArmadaConfig, healthChecks *health.MultiChecker) error {
	log.Info("Armada server starting")
	defer log.Info("Armada server shutting down")

	if config.Scheduling.Preemption.Enabled {
		log.Info("Armada Job preemption is enabled")
		log.Infof("Supported priority classes are: %v", config.Scheduling.Preemption.PriorityClasses)
		log.Infof("Default priority class is: %s", config.Scheduling.Preemption.DefaultPriorityClass)
	} else {
		log.Info("Armada Job preemption is disabled")
	}

	// We call startupCompleteCheck.MarkComplete() when all services have been started.
	startupCompleteCheck := health.NewStartupCompleteChecker()
	healthChecks.Add(startupCompleteCheck)

	// Run all services within an errgroup to propagate errors between services.
	// Defer cancelling the parent context to ensure the errgroup is cancelled on return.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	g, ctx := errgroup.WithContext(ctx)

	// List of services to run concurrently.
	// Because we want to start services only once all input validation has been completed,
	// we add all services to a slice and start them together at the end of this function.
	var services []func() error

	err := validateArmadaConfig(config)
	if err != nil {
		return err
	}

	// We support multiple simultaneous authentication services (e.g., username/password  OpenId).
	// For each gRPC request, we try them all until one succeeds, at which point the process is
	// short-circuited.
	authServices := auth.ConfigureAuth(config.Auth)
	grpcServer := grpcCommon.CreateGrpcServer(config.Grpc.KeepaliveParams, config.Grpc.KeepaliveEnforcementPolicy, authServices)

	// Shut down grpcServer if the context is cancelled.
	// Give the server 5 seconds to shut down gracefully.
	services = append(services, func() error {
		<-ctx.Done()
		go func() {
			time.Sleep(5 * time.Second)
			grpcServer.Stop()
		}()
		grpcServer.GracefulStop()
		return nil
	})

	// Setup Redis
	db := createRedisClient(&config.Redis)
	defer func() {
		if err := db.Close(); err != nil {
			log.WithError(err).Error("failed to close Redis client")
		}
	}()

	legacyEventDb := createRedisClient(&config.EventsRedis)
	defer func() {
		if err := legacyEventDb.Close(); err != nil {
			log.WithError(err).Error("failed to close events Redis client")
		}
	}()

	eventDb := createRedisClient(&config.EventsApiRedis)
	defer func() {
		if err := legacyEventDb.Close(); err != nil {
			log.WithError(err).Error("failed to close events api Redis client")
		}
	}()

	jobRepository := repository.NewRedisJobRepository(db, config.DatabaseRetention)
	usageRepository := repository.NewRedisUsageRepository(db)
	queueRepository := repository.NewRedisQueueRepository(db)
	schedulingInfoRepository := repository.NewRedisSchedulingInfoRepository(db)
	healthChecks.Add(repository.NewRedisHealth(db))

	legacyEventRepository := repository.NewLegacyRedisEventRepository(legacyEventDb, config.EventRetention)
	eventRepository := repository.NewEventRepository(eventDb)
	var streamEventStore *processor.StreamEventStore
	var eventStore repository.EventStore
	var eventStream eventstream.EventStream

	// TODO It looks like multiple backends can be provided.
	// We should ensure that only 1 system is provided.
	if len(config.EventsNats.Servers) > 0 {
		stanClient, err := eventstream.NewStanClientConnection(
			config.EventsNats.ClusterID,
			"armada-server-"+util.NewULID(),
			config.EventsNats.Servers)
		if err != nil {
			return err
		}

		eventStream = eventstream.NewStanEventStream(
			config.EventsNats.Subject,
			stanClient,
			stan.SetManualAckMode(),
			stan.StartWithLastReceived(),
		)
		defer eventStream.Close() // Closes the stanClient
		healthChecks.Add(stanClient)
	} else if len(config.EventsJetstream.Servers) > 0 {
		stream, err := eventstream.NewJetstreamEventStream(
			&config.EventsJetstream,
			jsm.SamplePercent(100),
			jsm.StartWithLastReceived(),
		)
		if err != nil {
			return err
		}
		defer stream.Close()
		eventStream = stream

		healthChecks.Add(stream)
	}

	var eventProcessor *processor.RedisEventProcessor
	if eventStream != nil {
		streamEventStore = processor.NewEventStore(eventStream)
		eventStore = streamEventStore

		eventRepoBatcher := eventstream.NewTimedEventBatcher(
			config.Events.ProcessorBatchSize,
			config.Events.ProcessorMaxTimeBetweenBatches,
			config.Events.ProcessorTimeout,
		)
		eventProcessor = processor.NewEventRedisProcessor(config.Events.StoreQueue, legacyEventRepository, eventStream, eventRepoBatcher)
		eventProcessor.Start()

		jobStatusBatcher := eventstream.NewTimedEventBatcher(
			config.Events.ProcessorBatchSize,
			config.Events.ProcessorMaxTimeBetweenBatches,
			config.Events.ProcessorTimeout,
		)
		jobStatusProcessor := processor.NewEventJobStatusProcessor(config.Events.JobStatusQueue, jobRepository, eventStream, jobStatusBatcher)
		jobStatusProcessor.Start()

		defer func() {
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
		}()
	} else {
		eventStore = legacyEventRepository
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

	// If pool settings are provided, open a connection pool to be shared by all services.
	var pool *pgxpool.Pool
	if len(config.Postgres.Connection) != 0 {
		pool, err = postgres.OpenPgxPool(config.Postgres)
		if err != nil {
			return err
		}
		defer pool.Close()
	}

	// If Pulsar is enabled, use the Pulsar submit endpoints.
	// Store a list of all Pulsar components to use during cleanup later.
	var pulsarClient pulsar.Client
	var pulsarCompressionType pulsar.CompressionType
	var pulsarCompressionLevel pulsar.CompressionLevel
	if config.Pulsar.Enabled {
		serverId := uuid.New()

		// API endpoints that generate Pulsar messages.
		log.Info("Pulsar config provided; using Pulsar submit endpoints.")
		var err error
		pulsarClient, err = pulsarutils.NewPulsarClient(&config.Pulsar)
		if err != nil {
			return err
		}
		defer pulsarClient.Close()

		pulsarCompressionType, err = pulsarutils.ParsePulsarCompressionType(config.Pulsar.CompressionType)
		if err != nil {
			return err
		}
		pulsarCompressionLevel, err = pulsarutils.ParsePulsarCompressionLevel(config.Pulsar.CompressionLevel)
		if err != nil {
			return err
		}
		serverPulsarProducerName := fmt.Sprintf("armada-server-%s", serverId)
		producer, err := pulsarClient.CreateProducer(pulsar.ProducerOptions{
			Name:             serverPulsarProducerName,
			CompressionType:  pulsarCompressionType,
			CompressionLevel: pulsarCompressionLevel,
			BatchingMaxSize:  config.Pulsar.MaxAllowedMessageSize,
			Topic:            config.Pulsar.JobsetEventsTopic,
		})
		if err != nil {
			return errors.Wrapf(err, "error creating pulsar producer %s", serverPulsarProducerName)
		}
		defer producer.Close()

		pulsarSubmitServer := &server.PulsarSubmitServer{
			Producer:              producer,
			QueueRepository:       queueRepository,
			Permissions:           permissions,
			SubmitServer:          submitServer,
			MaxAllowedMessageSize: config.Pulsar.MaxAllowedMessageSize,
		}
		submitServerToRegister = pulsarSubmitServer

		// If postgres details were provided, enable deduplication.
		if config.Pulsar.DedupTable != "" {
			if pool == nil {
				return errors.New("deduplication is enabled, but no postgres settings are provided")
			}
			log.Info("Pulsar submit API deduplication enabled")

			store, err := pgkeyvalue.New(pool, 1000000, config.Pulsar.DedupTable)
			if err != nil {
				return err
			}
			pulsarSubmitServer.KVStore = store

			// Automatically clean up keys after two weeks.
			services = append(services, func() error {
				return store.PeriodicCleanup(ctx, time.Hour, 14*24*time.Hour)
			})
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
			return errors.WithStack(err)
		}
		defer consumer.Close()

		submitFromLog := server.SubmitFromLog{
			Consumer:     consumer,
			SubmitServer: submitServer,
		}
		services = append(services, func() error {
			return submitFromLog.Run(ctx)
		})

		// Service that reads from Pulsar and submits messages back into Pulsar.
		// E.g., needed to automatically publish JobSucceeded after a JobRunSucceeded.
		consumer, err = pulsarClient.Subscribe(pulsar.ConsumerOptions{
			Topic:            config.Pulsar.JobsetEventsTopic,
			SubscriptionName: config.Pulsar.PulsarFromPulsarSubscription,
			Type:             pulsar.KeyShared,
		})
		if err != nil {
			return errors.WithStack(err)
		}
		defer consumer.Close()

		// Create a new producer for this service.
		p2pPulsarProducer := fmt.Sprintf("armada-pulsar-to-pulsar-%s", serverId)
		producer, err = pulsarClient.CreateProducer(pulsar.ProducerOptions{
			Name:             p2pPulsarProducer,
			CompressionType:  pulsarCompressionType,
			CompressionLevel: pulsarCompressionLevel,
			BatchingMaxSize:  config.Pulsar.MaxAllowedMessageSize,
			Topic:            config.Pulsar.JobsetEventsTopic,
		})
		if err != nil {
			return errors.Wrapf(err, "error creating pulsar producer %s", p2pPulsarProducer)
		}
		defer producer.Close()

		// Service that reads from Pulsar and logs events.
		if config.Pulsar.EventsPrinter {
			eventsPrinter := server.EventsPrinter{
				Client:           pulsarClient,
				Topic:            config.Pulsar.JobsetEventsTopic,
				SubscriptionName: config.Pulsar.EventsPrinterSubscription,
			}
			services = append(services, func() error {
				return eventsPrinter.Run(ctx)
			})
		}
	} else {
		log.Info("No Pulsar config provided; submitting directly to Redis and Nats.")
	}

	// New Pulsar-based scheduler.
	var newSchedulerApiServer *scheduler.ExecutorApi
	if config.NewScheduler.Enabled {
		if !config.Pulsar.Enabled {
			return errors.New("new scheduler enabled, but Pulsar is disabled")
		}
		if pool == nil {
			return errors.New("new scheduler enabled, but postgres is disabled")
		}

		// Scheduler jobs ingester.
		schedulerIngester := &scheduler.Ingester{
			PulsarClient: pulsarClient,
			ConsumerOptions: pulsar.ConsumerOptions{
				Topic:            config.Pulsar.JobsetEventsTopic,
				SubscriptionName: "pulsar-scheduler-ingester",
				Type:             pulsar.KeyShared,
			},
			MaxWriteInterval: time.Second,
			MaxDbOps:         10000,
			Db:               pool,
		}
		services = append(services, func() error {
			return schedulerIngester.Run(ctx)
		})

		// The scheduler itself.
		// TODO: I think we can safely re-use the same producer for all components.
		schedulerProducer, err := pulsarClient.CreateProducer(pulsar.ProducerOptions{
			CompressionType:  pulsarCompressionType,
			CompressionLevel: pulsarCompressionLevel,
			BatchingMaxSize:  config.Pulsar.MaxAllowedMessageSize,
			Topic:            config.Pulsar.JobsetEventsTopic,
		})
		if err != nil {
			return errors.WithStack(err)
		}
		sched := scheduler.NewScheduler(schedulerProducer, pool)
		services = append(services, func() error {
			return sched.Run(ctx)
		})

		// API of the new scheduler.
		apiProducer, err := pulsarClient.CreateProducer(pulsar.ProducerOptions{
			CompressionType:  pulsarCompressionType,
			CompressionLevel: pulsarCompressionLevel,
			BatchingMaxSize:  config.Pulsar.MaxAllowedMessageSize,
			Topic:            config.Pulsar.JobsetEventsTopic,
		})
		if err != nil {
			return errors.WithStack(err)
		}
		newSchedulerApiServer = &scheduler.ExecutorApi{
			Producer:       apiProducer,
			Db:             pool,
			MaxJobsPerCall: 100,
		}
	}

	usageServer := server.NewUsageServer(permissions, config.PriorityHalfTime, &config.Scheduling, usageRepository, queueRepository)
	queueCache := cache.NewQueueCache(&util.UTCClock{}, queueRepository, jobRepository, schedulingInfoRepository)
	aggregatedQueueServer := server.NewAggregatedQueueServer(
		permissions,
		config.Scheduling,
		jobRepository,
		queueCache,
		queueRepository,
		usageRepository,
		eventStore,
		schedulingInfoRepository,
	)
	if config.Scheduling.MaxQueueReportsToStore > 0 || config.Scheduling.MaxJobReportsToStore > 0 {
		aggregatedQueueServer.SchedulingReportsRepository = scheduler.NewSchedulingReportsRepository(
			config.Scheduling.MaxQueueReportsToStore,
			config.Scheduling.MaxJobReportsToStore,
		)
	}

	eventServer := server.NewEventServer(
		permissions,
		eventRepository,
		legacyEventRepository,
		eventStore,
		queueRepository,
		jobRepository,
		config.DefaultToLegacyEvents,
	)
	leaseManager := scheduling.NewLeaseManager(jobRepository, queueRepository, eventStore, config.Scheduling.Lease.ExpireAfter)

	// Allows for registering functions to be run periodically in the background.
	taskManager := task.NewBackgroundTaskManager(metrics.MetricPrefix)
	defer taskManager.StopAll(time.Second * 2)
	taskManager.Register(queueCache.Refresh, config.Metrics.RefreshInterval, "refresh_queue_cache")
	taskManager.Register(leaseManager.ExpireLeases, config.Scheduling.Lease.ExpiryLoopInterval, "lease_expiry")

	metrics.ExposeDataMetrics(queueRepository, jobRepository, usageRepository, schedulingInfoRepository, queueCache)

	api.RegisterSubmitServer(grpcServer, submitServerToRegister)
	api.RegisterUsageServer(grpcServer, usageServer)
	api.RegisterEventServer(grpcServer, eventServer)
	if aggregatedQueueServer.SchedulingReportsRepository != nil {
		schedulerobjects.RegisterSchedulerReportingServer(
			grpcServer,
			aggregatedQueueServer.SchedulingReportsRepository,
		)
	}

	// If the new Pulsar-driven scheduler is provided, run that.
	// Otherwise run the legacy scheduler.
	if newSchedulerApiServer != nil {
		log.Info("Pulsar-based scheduler enabled")
		api.RegisterAggregatedQueueServer(grpcServer, newSchedulerApiServer)
	} else {
		log.Info("legacy scheduler enabled")
		api.RegisterAggregatedQueueServer(grpcServer, aggregatedQueueServer)
	}
	grpc_prometheus.Register(grpcServer)

	// Cancel the errgroup if grpcServer.Serve returns an error.
	log.Infof("Armada gRPC server listening on %d", config.GrpcPort)
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", config.GrpcPort))
	if err != nil {
		return errors.WithStack(err)
	}
	services = append(services, func() error {
		return grpcServer.Serve(lis)
	})

	// Start all services and wait for the context to be cancelled,
	// which if the parent context is cancelled or if any of the services returns an error.
	// We start all services at the end of the function to ensure all services are ready.
	for _, service := range services {
		g.Go(service)
	}

	startupCompleteCheck.MarkComplete()
	return g.Wait()
}

func createRedisClient(config *redis.UniversalOptions) redis.UniversalClient {
	return redis.NewUniversalClient(config)
}

// TODO Is this all validation that needs to be done?
func validateArmadaConfig(config *configuration.ArmadaConfig) error {
	if config.CancelJobsBatchSize <= 0 {
		return errors.WithStack(fmt.Errorf("cancel jobs batch should be greater than 0: is %d", config.CancelJobsBatchSize))
	}
	return nil
}
