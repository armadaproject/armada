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
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"golang.org/x/exp/slices"
	"golang.org/x/sync/errgroup"

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
	"github.com/armadaproject/armada/internal/common/pgkeyvalue"
	"github.com/armadaproject/armada/internal/common/pulsarutils"
	"github.com/armadaproject/armada/internal/common/task"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/scheduler"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/pkg/api"
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

	eventDb := createRedisClient(&config.EventsApiRedis)
	defer func() {
		if err := eventDb.Close(); err != nil {
			log.WithError(err).Error("failed to close events api Redis client")
		}
	}()

	jobRepository := repository.NewRedisJobRepository(db, config.DatabaseRetention)
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

	// If Pulsar is enabled, use the Pulsar submit endpoints.
	// Store a list of all Pulsar components to use during cleanup later.
	var pulsarClient pulsar.Client
	var pulsarCompressionType pulsar.CompressionType
	var pulsarCompressionLevel pulsar.CompressionLevel
	submitChecker := scheduler.NewSubmitChecker(
		10*time.Minute,
		config.Scheduling.Preemption.PriorityClasses,
		config.Scheduling.GangIdAnnotation,
	)

	serverId := uuid.New()

	// API endpoints that generate Pulsar messages.
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

	eventStore := repository.NewEventStore(producer, int(config.Pulsar.MaxAllowedMessageSize))

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
		Producer:              producer,
		QueueRepository:       queueRepository,
		Permissions:           permissions,
		SubmitServer:          submitServer,
		MaxAllowedMessageSize: config.Pulsar.MaxAllowedMessageSize,
		SubmitChecker:         submitChecker,
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
	queueCache := cache.NewQueueCache(&util.UTCClock{}, queueRepository, jobRepository, schedulingInfoRepository)
	aggregatedQueueServer := server.NewAggregatedQueueServer(
		permissions,
		config.Scheduling,
		jobRepository,
		queueRepository,
		usageRepository,
		eventStore,
		schedulingInfoRepository,
	)
	aggregatedQueueServer.SubmitChecker = submitChecker
	if config.Scheduling.MaxQueueReportsToStore > 0 || config.Scheduling.MaxJobReportsToStore > 0 {
		aggregatedQueueServer.SchedulingReportsRepository = scheduler.NewSchedulingReportsRepository(
			config.Scheduling.MaxQueueReportsToStore,
			config.Scheduling.MaxJobReportsToStore,
		)
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
	if !config.Enabled {
		return nil
	}

	// validate that the default priority class is in the priority class map
	if config.DefaultPriorityClass != "" {
		_, ok := config.PriorityClasses[config.DefaultPriorityClass]
		if !ok {
			return errors.WithStack(fmt.Errorf("default priority class was set to %s, but no such priority class has been configured", config.DefaultPriorityClass))
		}
	}

	// validate that as priority increase, the limit decreases
	type priorityClass struct {
		name     string
		priority int32
		limits   map[string]float64
	}
	priorityClasses := make([]priorityClass, 0, len(config.PriorityClasses))
	for k, pc := range config.PriorityClasses {
		priorityClasses = append(priorityClasses, priorityClass{
			name:     k,
			priority: pc.Priority,
			limits:   pc.MaximalResourceFractionPerQueue,
		})
	}

	slices.SortFunc(priorityClasses, func(a priorityClass, b priorityClass) bool {
		return a.priority > b.priority
	})

	var prevLimits map[string]float64 = nil
	prevPriorityName := ""
	for i, pc := range priorityClasses {
		if i != 0 {
			// check that the limit exists and that it is greater than the previous limit
			for k, v := range prevLimits {
				limit, ok := pc.limits[k]
				if !ok {
					return errors.WithStack(fmt.Errorf("invalid priority class configuration: Limit for resource %s missing at priority %s", k, pc.name))
				}
				if limit < v {
					return errors.WithStack(
						fmt.Errorf("invalid priority class configuration: Limit for resource %s at priority %s [%.3f] is lower than at priority %s [%.3f] ", k, pc.name, limit, prevPriorityName, v))
				}
			}

			// Check that we don't have a limit for some new resource defined
			for k := range pc.limits {
				_, ok := prevLimits[k]
				if !ok {
					return errors.WithStack(fmt.Errorf("invalid priority class configuration: Limit for resource %s missing at priority %s", k, prevPriorityName))
				}
			}
		}
		prevLimits = pc.limits
		prevPriorityName = pc.name
	}

	return nil
}
