package armada

import (
	"context"
	"fmt"
	"math"
	"net"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/google/uuid"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/jackc/pgx/v5/pgxpool"
	pool "github.com/jolestar/go-commons-pool"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/extra/redisprometheus/v9"
	"github.com/redis/go-redis/v9"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"

	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/internal/armada/queryapi"
	"github.com/armadaproject/armada/internal/armada/repository"
	"github.com/armadaproject/armada/internal/armada/server"
	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/auth"
	"github.com/armadaproject/armada/internal/common/auth/authorization"
	"github.com/armadaproject/armada/internal/common/compress"
	"github.com/armadaproject/armada/internal/common/database"
	grpcCommon "github.com/armadaproject/armada/internal/common/grpc"
	"github.com/armadaproject/armada/internal/common/health"
	"github.com/armadaproject/armada/internal/common/pgkeyvalue"
	"github.com/armadaproject/armada/internal/common/pulsarutils"
	"github.com/armadaproject/armada/internal/scheduler"
	schedulerdb "github.com/armadaproject/armada/internal/scheduler/database"
	"github.com/armadaproject/armada/internal/scheduler/reports"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/client"
)

func Serve(ctx *armadacontext.Context, config *configuration.ArmadaConfig, healthChecks *health.MultiChecker) error {
	log.Info("Armada server starting")
	defer log.Info("Armada server shutting down")

	// We call startupCompleteCheck.MarkComplete() when all services have been started.
	startupCompleteCheck := health.NewStartupCompleteChecker()
	healthChecks.Add(startupCompleteCheck)

	// Run all services within an errgroup to propagate errors between services.
	// Defer cancelling the parent context to ensure the errgroup is cancelled on return.
	ctx, cancel := armadacontext.WithCancel(ctx)
	defer cancel()
	g, ctx := armadacontext.ErrGroup(ctx)

	// List of services to run concurrently.
	// Because we want to start services only once all input validation has been completed,
	// we add all services to a slice and start them together at the end of this function.
	var services []func() error

	if err := validateCancelJobsBatchSizeConfig(config); err != nil {
		return err
	}
	if err := validateSubmissionConfig(config.Submission); err != nil {
		return err
	}

	// We support multiple simultaneous authentication services (e.g., username/password  OpenId).
	// For each gRPC request, we try them all until one succeeds, at which point the process is
	// short-circuited.
	authServices, err := auth.ConfigureAuth(config.Auth)
	if err != nil {
		return err
	}
	grpcServer := grpcCommon.CreateGrpcServer(config.Grpc.KeepaliveParams, config.Grpc.KeepaliveEnforcementPolicy, authServices, config.Grpc.Tls)

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
	prometheus.MustRegister(
		redisprometheus.NewCollector("armada", "redis", db))

	eventDb := createRedisClient(&config.EventsApiRedis)
	defer func() {
		if err := eventDb.Close(); err != nil {
			log.WithError(err).Error("failed to close events api Redis client")
		}
	}()
	prometheus.MustRegister(
		redisprometheus.NewCollector("armada", "events_redis", eventDb))

	jobRepository := repository.NewRedisJobRepository(db)
	queueRepository := repository.NewRedisQueueRepository(db)
	healthChecks.Add(repository.NewRedisHealth(db))

	eventRepository := repository.NewEventRepository(eventDb)

	authorizer := server.NewAuthorizer(
		authorization.NewPrincipalPermissionChecker(
			config.Auth.PermissionGroupMapping,
			config.Auth.PermissionScopeMapping,
			config.Auth.PermissionClaimMapping,
		),
	)

	// If pool settings are provided, open a connection pool to be shared by all services.
	var dbPool *pgxpool.Pool
	if len(config.Postgres.Connection) != 0 {
		dbPool, err = database.OpenPgxPool(config.Postgres)
		if err != nil {
			return err
		}
		defer dbPool.Close()
	}

	// Executor Repositories for pulsar scheduler
	pulsarExecutorRepo := schedulerdb.NewRedisExecutorRepository(db, "pulsar")
	pulsarSchedulerSubmitChecker := scheduler.NewSubmitChecker(
		30*time.Minute,
		config.Scheduling,
		pulsarExecutorRepo,
	)
	services = append(services, func() error {
		return pulsarSchedulerSubmitChecker.Run(ctx)
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

	poolConfig := pool.ObjectPoolConfig{
		MaxTotal:                 100,
		MaxIdle:                  50,
		MinIdle:                  10,
		BlockWhenExhausted:       true,
		MinEvictableIdleTime:     30 * time.Minute,
		SoftMinEvictableIdleTime: math.MaxInt64,
		TimeBetweenEvictionRuns:  0,
		NumTestsPerEvictionRun:   10,
	}

	compressorPool := pool.NewObjectPool(armadacontext.Background(), pool.NewPooledObjectFactorySimple(
		func(context.Context) (interface{}, error) {
			return compress.NewZlibCompressor(512)
		}), &poolConfig)

	pulsarSubmitServer := &server.PulsarSubmitServer{
		Producer:              producer,
		QueueRepository:       queueRepository,
		JobRepository:         jobRepository,
		SubmissionConfig:      config.Submission,
		MaxAllowedMessageSize: config.Pulsar.MaxAllowedMessageSize,
		GangIdAnnotation:      configuration.GangIdAnnotation,
		SubmitChecker:         pulsarSchedulerSubmitChecker,
		Authorizer:            authorizer,
		CompressorPool:        compressorPool,
	}

	// If postgres details were provided, enable deduplication.
	if config.Pulsar.DedupTable != "" {
		if dbPool == nil {
			return errors.New("deduplication is enabled, but no postgres settings are provided")
		}
		log.Info("Pulsar submit API deduplication enabled")

		store, err := pgkeyvalue.New(ctx, dbPool, config.Pulsar.DedupTable)
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

	// Consumer that's used for deleting pulsarJob details
	// Need to use the old config.Pulsar.RedisFromPulsarSubscription name so we continue processing where we left off
	// TODO: delete this when we finally remove redis
	consumer, err := pulsarClient.Subscribe(pulsar.ConsumerOptions{
		Topic:             config.Pulsar.JobsetEventsTopic,
		SubscriptionName:  config.Pulsar.RedisFromPulsarSubscription,
		Type:              pulsar.KeyShared,
		ReceiverQueueSize: config.Pulsar.ReceiverQueueSize,
	})
	if err != nil {
		return errors.WithStack(err)
	}
	defer consumer.Close()

	jobExpirer := &server.PulsarJobExpirer{
		Consumer:      consumer,
		JobRepository: jobRepository,
	}
	services = append(services, func() error {
		return jobExpirer.Run(ctx)
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

	schedulerApiConnection, err := createApiConnection(config.SchedulerApiConnection)
	if err != nil {
		return errors.Wrapf(err, "error creating connection to scheduler api")
	}
	schedulerApiReportsClient := schedulerobjects.NewSchedulerReportingClient(schedulerApiConnection)
	schedulingReportsServer := reports.NewProxyingSchedulingReportsServer(schedulerApiReportsClient)

	eventServer := server.NewEventServer(
		authorizer,
		eventRepository,
		queueRepository,
		jobRepository,
	)

	if config.QueryApi.Enabled {
		queryDb, err := database.OpenPgxPool(config.QueryApi.Postgres)
		if err != nil {
			return errors.WithMessage(err, "error creating QueryApi postgres pool")
		}
		queryapiServer := queryapi.New(
			queryDb,
			config.QueryApi.MaxQueryItems,
			func() compress.Decompressor { return compress.NewZlibDecompressor() })
		api.RegisterJobsServer(grpcServer, queryapiServer)
	}

	api.RegisterSubmitServer(grpcServer, pulsarSubmitServer)
	api.RegisterEventServer(grpcServer, eventServer)

	schedulerobjects.RegisterSchedulerReportingServer(grpcServer, schedulingReportsServer)
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

func validateSubmissionConfig(config configuration.SubmissionConfig) error {
	// Check that the default priority class is allowed to be submitted.
	if config.DefaultPriorityClassName != "" {
		if !config.AllowedPriorityClassNames[config.DefaultPriorityClassName] {
			return errors.WithStack(fmt.Errorf(
				"defaultPriorityClassName %s is not allowed; allowedPriorityClassNames is %v",
				config.DefaultPriorityClassName, config.AllowedPriorityClassNames,
			))
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
