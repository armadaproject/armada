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
	"github.com/G-Research/armada/pkg/api"
)

func Serve(ctx context.Context, config *configuration.ArmadaConfig, healthChecks *health.MultiChecker) error {

	log.Info("Armada server starting")
	defer log.Info("Armada server shutting down")

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
	grpcServer := grpcCommon.CreateGrpcServer(authServices)

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

	// Allows for registering functions to be run periodically in the background.
	taskManager := task.NewBackgroundTaskManager(metrics.MetricPrefix)
	defer taskManager.StopAll(time.Second * 2)

	// Setup Redis
	db := createRedisClient(&config.Redis)
	defer func() {
		if err := db.Close(); err != nil {
			log.WithError(err).Error("faled to close Redis client")
		}
	}()

	eventsDb := createRedisClient(&config.EventsRedis)
	defer func() {
		if err := db.Close(); err != nil {
			log.WithError(err).Error("faled to close events Redis client")
		}
	}()

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

		eventRepoBatcher := eventstream.NewTimedEventBatcher(config.Events.ProcessorBatchSize, config.Events.ProcessorMaxTimeBetweenBatches, config.Events.ProcessorTimeout)
		eventProcessor = processor.NewEventRedisProcessor(config.Events.StoreQueue, redisEventRepository, eventStream, eventRepoBatcher)
		eventProcessor.Start()

		jobStatusBatcher := eventstream.NewTimedEventBatcher(config.Events.ProcessorBatchSize, config.Events.ProcessorMaxTimeBetweenBatches, config.Events.ProcessorTimeout)
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
	if config.Pulsar.Enabled {
		serverId := uuid.New()

		// API endpoints that generate Pulsar messages.
		log.Info("Pulsar config provided; using Pulsar submit endpoints.")
		pulsarClient, err := pulsarutils.NewPulsarClient(&config.Pulsar)
		if err != nil {
			return err
		}
		defer pulsarClient.Close()

		compressionType, err := pulsarutils.ParsePulsarCompressionType(config.Pulsar.CompressionType)
		if err != nil {
			return err
		}
		compressionLevel, err := pulsarutils.ParsePulsarCompressionLevel(config.Pulsar.CompressionLevel)
		if err != nil {
			return err
		}
		producer, err := pulsarClient.CreateProducer(pulsar.ProducerOptions{
			Name:             fmt.Sprintf("armada-server-%s", serverId),
			CompressionType:  compressionType,
			CompressionLevel: compressionLevel,
			BatchingMaxSize:  config.Pulsar.MaxAllowedMessageSize,
			Topic:            config.Pulsar.JobsetEventsTopic,
		})
		if err != nil {
			return errors.WithStack(err)
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
			log.Info("Pulsar submit API deduplication enabled")
			db, err := postgres.OpenPgxPool(config.Postgres)
			if err != nil {
				return err
			}
			defer db.Close()

			store, err := pgkeyvalue.New(db, 1000000, config.Pulsar.DedupTable)
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
		producer, err = pulsarClient.CreateProducer(pulsar.ProducerOptions{
			Name:             fmt.Sprintf("armada-pulsar-to-pulsar-%s", serverId),
			CompressionType:  compressionType,
			CompressionLevel: compressionLevel,
			BatchingMaxSize:  config.Pulsar.MaxAllowedMessageSize,
			Topic:            config.Pulsar.JobsetEventsTopic,
		})
		if err != nil {
			return errors.WithStack(err)
		}
		defer producer.Close()

		pulsarFromPulsar := server.PulsarFromPulsar{
			Consumer: consumer,
			Producer: producer,
		}
		services = append(services, func() error {
			return pulsarFromPulsar.Run(ctx)
		})

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
