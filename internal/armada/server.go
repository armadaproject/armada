package armada

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/armadaproject/armada/internal/common/certs"
	"github.com/go-redis/redis"
	"github.com/google/uuid"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

	"github.com/armadaproject/armada/internal/armada/cache"
	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/internal/armada/metrics"
	"github.com/armadaproject/armada/internal/armada/repository"
	"github.com/armadaproject/armada/internal/armada/scheduling"
	"github.com/armadaproject/armada/internal/armada/server"
	"github.com/armadaproject/armada/internal/common/auth"
	"github.com/armadaproject/armada/internal/common/auth/authorization"
	"github.com/armadaproject/armada/internal/common/database"
	grpcCommon "github.com/armadaproject/armada/internal/common/grpc"
	"github.com/armadaproject/armada/internal/common/health"
	commonmetrics "github.com/armadaproject/armada/internal/common/metrics"
	"github.com/armadaproject/armada/internal/common/pgkeyvalue"
	"github.com/armadaproject/armada/internal/common/pulsarutils"
	"github.com/armadaproject/armada/internal/common/task"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/scheduler"
	schedulerdb "github.com/armadaproject/armada/internal/scheduler/database"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/client"
)

func Serve(ctx context.Context, config *configuration.ArmadaConfig, healthChecks *health.MultiChecker) error {
	log.Info("Armada server starting")
	log.Infof("Armada priority classes: %v", config.Scheduling.Preemption.PriorityClasses)
	log.Infof("Default priority class: %s", config.Scheduling.Preemption.DefaultPriorityClass)
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

	err := validateCancelJobsBatchSizeConfig(config)
	if err != nil {
		return err
	}

	err = validatePreemptionConfig(config.Scheduling.Preemption)
	if err != nil {
		return err
	}

	// We support multiple simultaneous authentication services (e.g., username/password  OpenId).
	// For each gRPC request, we try them all until one succeeds, at which point the process is
	// short-circuited.
	authServices, err := auth.ConfigureAuth(config.Auth)
	if err != nil {
		return err
	}
	var cachedCertificateService *certs.CachedCertificateService
	if config.Grpc.Tls.Enabled {
		cachedCertificateService = certs.NewCachedCertificateService(config.Grpc.Tls.CertPath, config.Grpc.Tls.KeyPath)
		services = append(services, func() error {
			return cachedCertificateService.Run(ctx)
		})
	}
	grpcServer := grpcCommon.CreateGrpcServer(config.Grpc.KeepaliveParams, config.Grpc.KeepaliveEnforcementPolicy, authServices, cachedCertificateService)

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

	eventDb := createRedisClient(&config.EventsApiRedis)
	defer func() {
		if err := eventDb.Close(); err != nil {
			log.WithError(err).Error("failed to close events api Redis client")
		}
	}()

	jobRepository := repository.NewRedisJobRepository(db)
	usageRepository := repository.NewRedisUsageRepository(db)
	queueRepository := repository.NewRedisQueueRepository(db)
	schedulingInfoRepository := repository.NewRedisSchedulingInfoRepository(db)
	healthChecks.Add(repository.NewRedisHealth(db))

	eventRepository := repository.NewEventRepository(eventDb)

	permissions := authorization.NewPrincipalPermissionChecker(
		config.Auth.PermissionGroupMapping,
		config.Auth.PermissionScopeMapping,
		config.Auth.PermissionClaimMapping,
	)

	// If pool settings are provided, open a connection pool to be shared by all services.
	var pool *pgxpool.Pool
	if len(config.Postgres.Connection) != 0 {
		pool, err = database.OpenPgxPool(config.Postgres)
		if err != nil {
			return err
		}
		defer pool.Close()
	}

	// Executor Repositories for pulsar and legacy schedulers respectively
	pulsarExecutorRepo := schedulerdb.NewRedisExecutorRepository(db, "pulsar")
	legacyExecutorRepo := schedulerdb.NewRedisExecutorRepository(db, "legacy")

	pulsarSchedulerSubmitChecker := scheduler.NewSubmitChecker(
		30*time.Minute,
		config.Scheduling,
		pulsarExecutorRepo,
	)
	services = append(services, func() error {
		return pulsarSchedulerSubmitChecker.Run(ctx)
	})
	legacySchedulerSubmitChecker := scheduler.NewSubmitChecker(
		30*time.Minute,
		config.Scheduling,
		legacyExecutorRepo,
	)
	services = append(services, func() error {
		return legacySchedulerSubmitChecker.Run(ctx)
	})

	serverId := uuid.New()
	var pulsarClient pulsar.Client
	// API endpoints that generate Pulsar messages.
	pulsarClient, err = pulsarutils.NewPulsarClient(&config.Pulsar)
	if err != nil {
		return err
	}
	defer pulsarClient.Close()

	serverPulsarProducerName := fmt.Sprintf("armada-server-%s", serverId)
	producer, err := pulsarClient.CreateProducer(pulsar.ProducerOptions{
		Name:             serverPulsarProducerName,
		CompressionType:  config.Pulsar.CompressionType,
		CompressionLevel: config.Pulsar.CompressionLevel,
		BatchingMaxSize:  config.Pulsar.MaxAllowedMessageSize,
		Topic:            config.Pulsar.JobsetEventsTopic,
	})
	if err != nil {
		return errors.Wrapf(err, "error creating pulsar producer %s", serverPulsarProducerName)
	}
	defer producer.Close()

	eventStore := repository.NewEventStore(producer, config.Pulsar.MaxAllowedMessageSize)

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

	pulsarSubmitServer := &server.PulsarSubmitServer{
		Producer:                          producer,
		QueueRepository:                   queueRepository,
		Permissions:                       permissions,
		SubmitServer:                      submitServer,
		MaxAllowedMessageSize:             config.Pulsar.MaxAllowedMessageSize,
		PulsarSchedulerSubmitChecker:      pulsarSchedulerSubmitChecker,
		LegacySchedulerSubmitChecker:      legacySchedulerSubmitChecker,
		PulsarSchedulerEnabled:            config.PulsarSchedulerEnabled,
		ProbabilityOfUsingPulsarScheduler: config.ProbabilityOfUsingPulsarScheduler,
		Rand:                              util.NewThreadsafeRand(time.Now().UnixNano()),
		GangIdAnnotation:                  configuration.GangIdAnnotation,
		IgnoreJobSubmitChecks:             config.IgnoreJobSubmitChecks,
	}
	submitServerToRegister := pulsarSubmitServer

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

	// Service that consumes Pulsar messages and writes to Redis
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

	usageServer := server.NewUsageServer(permissions, config.PriorityHalfTime, &config.Scheduling, usageRepository, queueRepository)

	aggregatedQueueServer := server.NewAggregatedQueueServer(
		permissions,
		config.Scheduling,
		jobRepository,
		queueRepository,
		usageRepository,
		eventStore,
		schedulingInfoRepository,
		producer,
		config.Pulsar.MaxAllowedMessageSize,
		legacyExecutorRepo,
	)

	schedulingContextRepository, err := scheduler.NewSchedulingContextRepository(config.Scheduling.MaxJobSchedulingContextsPerExecutor)
	if err != nil {
		return err
	}
	aggregatedQueueServer.SchedulingContextRepository = schedulingContextRepository

	var schedulingReportsServer schedulerobjects.SchedulerReportingServer
	if config.PulsarSchedulerEnabled {
		schedulerApiConnection, err := createApiConnection(config.SchedulerApiConnection)
		if err != nil {
			return errors.Wrapf(err, "error creating connection to scheduler api")
		}
		schedulerApiReportsClient := schedulerobjects.NewSchedulerReportingClient(schedulerApiConnection)
		schedulingReportsServer = scheduler.NewProxyingSchedulingReportsServer(schedulerApiReportsClient)
	} else {
		schedulingReportsServer = schedulingContextRepository
	}

	eventServer := server.NewEventServer(
		permissions,
		eventRepository,
		eventStore,
		queueRepository,
		jobRepository,
	)
	leaseManager := scheduling.NewLeaseManager(jobRepository, queueRepository, eventStore, config.Scheduling.Lease.ExpireAfter)

	// Allows for registering functions to be run periodically in the background.
	taskManager := task.NewBackgroundTaskManager(commonmetrics.MetricPrefix)
	defer taskManager.StopAll(time.Second * 2)
	taskManager.Register(leaseManager.ExpireLeases, config.Scheduling.Lease.ExpiryLoopInterval, "lease_expiry")

	if config.Metrics.ExposeSchedulingMetrics {
		queueCache := cache.NewQueueCache(&util.UTCClock{}, queueRepository, jobRepository, schedulingInfoRepository)
		taskManager.Register(queueCache.Refresh, config.Metrics.RefreshInterval, "refresh_queue_cache")
		metrics.ExposeDataMetrics(queueRepository, jobRepository, usageRepository, schedulingInfoRepository, queueCache)
	}

	api.RegisterSubmitServer(grpcServer, submitServerToRegister)
	api.RegisterUsageServer(grpcServer, usageServer)
	api.RegisterEventServer(grpcServer, eventServer)
	schedulerobjects.RegisterSchedulerReportingServer(grpcServer, schedulingReportsServer)

	api.RegisterAggregatedQueueServer(grpcServer, aggregatedQueueServer)
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

// TODO: Is this all validation that needs to be done?
func validateCancelJobsBatchSizeConfig(config *configuration.ArmadaConfig) error {
	if config.CancelJobsBatchSize <= 0 {
		return errors.WithStack(fmt.Errorf("cancel jobs batch should be greater than 0: is %d", config.CancelJobsBatchSize))
	}
	return nil
}

func validatePreemptionConfig(config configuration.PreemptionConfig) error {
	// Check that the default priority class is in the priority class map.
	if config.DefaultPriorityClass != "" {
		_, ok := config.PriorityClasses[config.DefaultPriorityClass]
		if !ok {
			return errors.WithStack(fmt.Errorf("default priority class was set to %s, but no such priority class has been configured", config.DefaultPriorityClass))
		}
	}

	return nil
}

func createApiConnection(connectionDetails client.ApiConnectionDetails) (*grpc.ClientConn, error) {
	grpc_prometheus.EnableClientHandlingTimeHistogram()
	return client.CreateApiConnectionWithCallOptions(
		&connectionDetails,
		[]grpc.CallOption{},
		grpc.WithChainUnaryInterceptor(grpc_prometheus.UnaryClientInterceptor),
		grpc.WithChainStreamInterceptor(grpc_prometheus.StreamClientInterceptor),
	)
}
